// A bridge-release canister used by Migration.Functional.Test.mo.  Unlike a
// mock, both generations below are real ZenDB collections in stable memory.

import Principal "mo:core@2.4/Principal";
import Runtime "mo:core@2.4/Runtime";

import ZenDB "../../src/EmbeddedInstance";
import Migration "../../src/EmbeddedInstance/Migration";
import MigrationTemplates "../../src/EmbeddedInstance/MigrationTemplates";

shared ({ caller = owner }) persistent actor class MigrationExample() = this_app {
    type LegacyUser = {
        firstName : Text;
        lastName : Text;
    };

    // `legacyId` is the old collection's real ZenDB document id.  It gives the
    // target table a durable idempotency key for copy retries.
    type UserV2 = {
        legacyId : Blob;
        displayName : Text;
    };

    let canisterId = Principal.fromActor(this_app);
    var zendb = ZenDB.newStableStore(canisterId, ?{
        log_level = ?#Error;
        is_running_locally = ?true;
        memory_type = ?#stableMemory;
        cache_capacity = null;
        is_compression_enabled = null;
    });

    // This cursor survives bridge-release upgrades and resumes each bounded
    // B-tree scan without materializing every legacy id in stable actor state.
    var migrationState = Migration.newState<Blob>(1);
    var writesFrozen = false;
    var migrationOperator : ?Principal = null;

    let legacySchema : ZenDB.Types.Schema = #Record([
        ("firstName", #Text),
        ("lastName", #Text),
    ]);
    let v2Schema : ZenDB.Types.Schema = #Record([
        ("legacyId", #Blob),
        ("displayName", #Text),
    ]);

    // Codec closures are re-created on each install/upgrade; functions cannot
    // be part of the persistent actor's stable state.
    transient let legacyCandify : ZenDB.Types.Candify<LegacyUser> = {
        from_blob = func(blob : Blob) : ?LegacyUser { from_candid (blob) };
        to_blob = func(user : LegacyUser) : Blob { to_candid (user) };
    };
    transient let v2Candify : ZenDB.Types.Candify<UserV2> = {
        from_blob = func(blob : Blob) : ?UserV2 { from_candid (blob) };
        to_blob = func(user : UserV2) : Blob { to_candid (user) };
    };

    func db() : ZenDB.Database { ZenDB.launchDefaultDB(zendb) };

    func requireOwner(caller : Principal) {
        if (caller != owner) Runtime.trap("Only the owner may configure migrations");
    };

    func requireMigrationOperator(caller : Principal) {
        if (migrationOperator != ?caller) {
            Runtime.trap("Caller is not the authorized migration operator");
        };
    };

    func legacyUsers() : ZenDB.Collection<LegacyUser> {
        let #ok(collection) = db().getCollection<LegacyUser>("users", legacyCandify) else {
            Runtime.trap("The legacy users collection has not been installed");
        };
        collection;
    };

    func usersV2() : ZenDB.Collection<UserV2> {
        let #ok(collection) = db().getCollection<UserV2>("users_v2", v2Candify) else {
            Runtime.trap("The users_v2 collection has not been installed");
        };
        collection;
    };

    func controller() : Migration.Controller<Blob, (Blob, LegacyUser)> {
        Migration.Controller<Blob, (Blob, LegacyUser)>(migrationState)
    };

    func nextLegacyUsers(cursor : ?Blob, limit : Nat) : [(Blob, (Blob, LegacyUser))] {
        MigrationTemplates.nextFromCursor(legacyUsers(), cursor, limit);
    };

    func toV2((legacyId, user) : (Blob, LegacyUser)) : UserV2 {
        { legacyId; displayName = user.firstName # " " # user.lastName };
    };

    func upsertV2(item : (Blob, LegacyUser)) : Migration.Result<()> {
        MigrationTemplates.copyBySourceId(usersV2(), "legacyId", item, toV2);
    };

    func copiedCorrectly(item : (Blob, LegacyUser)) : Bool {
        MigrationTemplates.verifyBySourceId(
            usersV2(),
            "legacyId",
            item,
            func(source, target) : Bool { target == toV2(source) },
        );
    };

    func removeLegacyUser(item : (Blob, LegacyUser)) : Migration.Result<()> {
        MigrationTemplates.removeSource(controller(), legacyUsers(), item);
    };

    /// Represents the pre-migration release: it creates and fills a real ZenDB
    /// `users` collection. Production code already has this collection.
    public shared ({ caller }) func installLegacyExampleData() : async () {
        requireOwner(caller);
        let #ok(source) = db().createCollection<LegacyUser>("users", legacySchema, legacyCandify, null) else {
            Runtime.trap("Could not create the legacy users collection");
        };
        for (user in [
            { firstName = "Ada"; lastName = "Lovelace" },
            { firstName = "Grace"; lastName = "Hopper" },
            { firstName = "Edsger"; lastName = "Dijkstra" },
        ].vals()) {
            let #ok(_) = source.insert(user) else Runtime.trap("Could not seed legacy user");
        };
    };

    /// Bind all destructive migration calls to the dedicated orchestrator.
    /// Production code should use an equally strict authorization policy.
    public shared ({ caller }) func authorizeMigrationOperator(operator : Principal) : async () {
        requireOwner(caller);
        if (migrationState.phase != #idle) {
            Runtime.trap("The migration operator cannot change after migration begins");
        };
        migrationOperator := ?operator;
    };

    /// Start a bridge release. Freeze old writes here, or dual-write them to
    /// both collections until `commitMigration` has completed.
    public shared ({ caller }) func beginMigration() : async Migration.Progress {
        requireMigrationOperator(caller);
        let #ok(progress) = MigrationTemplates.begin(
            controller(), db(), "users_v2", v2Schema, v2Candify,
            ?{ schema_constraints = [#Unique(["legacyId"])] }, "users-v2", 2,
        ) else Runtime.trap("Could not prepare the users_v2 collection");
        writesFrozen := true;
        progress;
    };

    /// Each operation runs one bounded, retry-safe migration step.
    public shared ({ caller }) func copyNext(step : Nat, limit : Nat) : async Migration.Progress {
        requireMigrationOperator(caller);
        controller().copyStep(step, limit, nextLegacyUsers, upsertV2);
    };

    public shared ({ caller }) func verifyNext(step : Nat, limit : Nat) : async Migration.Progress {
        requireMigrationOperator(caller);
        controller().verifyStep(step, limit, nextLegacyUsers, copiedCorrectly);
    };

    /// This update atomically makes the `users_v2` ZenDB table authoritative.
    public shared ({ caller }) func commitMigration(step : Nat) : async Migration.Progress {
        requireMigrationOperator(caller);
        let #ok(progress) = MigrationTemplates.commit(
            controller(),
            usersV2(),
            step,
            func() {},
        ) else Runtime.trap("The migration target is not complete");
        progress;
    };

    public shared ({ caller }) func cleanUpNext(step : Nat, limit : Nat) : async Migration.Progress {
        requireMigrationOperator(caller);
        controller().cleanupStep(step, limit, nextLegacyUsers, removeLegacyUser);
    };

    public shared ({ caller }) func sealMigration(step : Nat) : async Migration.Progress {
        requireMigrationOperator(caller);
        controller().seal(step, func() : Bool { legacyUsers().isEmpty() });
    };

    /// A final Wasm that removes the old stable field should guard itself with
    /// this check before relying on the legacy collection no longer existing.
    public shared ({ caller }) func requireSealedForFinalUpgrade() : async () {
        requireMigrationOperator(caller);
        controller().requireSealed();
    };

    public query func progress() : async Migration.Progress { controller().getProgress() };
    public query func oldGenerationSize() : async Nat { legacyUsers().size() };
    public query func activeGenerationSize() : async Nat {
        if (migrationState.activeGeneration == 2) usersV2().size() else legacyUsers().size();
    };
    public query func hasActiveUser(displayName : Text) : async Bool {
        let collection = if (migrationState.activeGeneration == 2) usersV2() else return false;
        switch (collection.search(ZenDB.QueryBuilder().Where("displayName", #eq(#Text(displayName))))) {
            case (#ok(result)) result.documents.size() == 1;
            case (#err(_)) false;
        };
    };
    public query func writesAreFrozen() : async Bool { writesFrozen };
};
