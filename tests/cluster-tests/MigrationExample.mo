// A bridge-release canister used by Migration.Functional.Test.mo.  Unlike a
// mock, both generations below are real ZenDB collections in stable memory.

import Principal "mo:core@2.4/Principal";
import Runtime "mo:core@2.4/Runtime";

import ZenDB "../../src/EmbeddedInstance";
import Migration "../../src/EmbeddedInstance/Migration";
import MigrationTemplates "../../src/EmbeddedInstance/MigrationTemplates";

persistent actor class MigrationExample() = this_app {
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

    // The migration state and the source-id snapshot must survive every bridge
    // release upgrade. The snapshot gives each batch a deterministic cursor even
    // while cleanup removes documents from the legacy ZenDB collection.
    var migrationState = Migration.newState<Nat>(1);
    var legacyIds : [Blob] = [];
    var writesFrozen = false;

    let legacySchema : ZenDB.Types.Schema = #Record([
        ("firstName", #Text),
        ("lastName", #Text),
    ]);
    let v2Schema : ZenDB.Types.Schema = #Record([
        ("legacyId", #Blob),
        ("displayName", #Text),
    ]);

    let legacyCandify : ZenDB.Types.Candify<LegacyUser> = {
        from_blob = func(blob : Blob) : ?LegacyUser { from_candid (blob) };
        to_blob = func(user : LegacyUser) : Blob { to_candid (user) };
    };
    let v2Candify : ZenDB.Types.Candify<UserV2> = {
        from_blob = func(blob : Blob) : ?UserV2 { from_candid (blob) };
        to_blob = func(user : UserV2) : Blob { to_candid (user) };
    };

    func db() : ZenDB.Database { ZenDB.launchDefaultDB(zendb) };

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

    func controller() : Migration.Controller<Nat, (Blob, LegacyUser)> {
        Migration.Controller<Nat, (Blob, LegacyUser)>(migrationState)
    };

    func nextLegacyUsers(cursor : ?Nat, limit : Nat) : [(Nat, (Blob, LegacyUser))] {
        MigrationTemplates.nextFromSnapshot(legacyUsers(), legacyIds, cursor, limit);
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
        MigrationTemplates.removeSource(legacyUsers(), item);
    };

    /// Represents the pre-migration release: it creates and fills a real ZenDB
    /// `users` collection. Production code already has this collection.
    public func installLegacyExampleData() : async () {
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

    /// Start a bridge release. Freeze old writes here, or dual-write them to
    /// both collections until `commitMigration` has completed.
    public func beginMigration() : async Migration.Progress {
        let source = legacyUsers();
        let #ok(started) = MigrationTemplates.begin(
            controller(), source, legacyIds, db(), "users_v2", v2Schema, v2Candify,
            ?{ schema_constraints = [#Unique(["legacyId"])] }, "users-v2", 2,
        ) else Runtime.trap("Could not prepare the users_v2 collection");
        legacyIds := started.sourceIds;
        writesFrozen := true;
        started.progress;
    };

    /// Each operation runs one bounded, retry-safe migration step.
    public func copyNext(step : Nat, limit : Nat) : async Migration.Progress {
        controller().copyStep(step, limit, nextLegacyUsers, upsertV2);
    };

    public func verifyNext(step : Nat, limit : Nat) : async Migration.Progress {
        controller().verifyStep(step, limit, nextLegacyUsers, copiedCorrectly);
    };

    /// This update atomically makes the `users_v2` ZenDB table authoritative.
    public func commitMigration(step : Nat) : async Migration.Progress {
        controller().commit(step, func() {});
    };

    public func cleanUpNext(step : Nat, limit : Nat) : async Migration.Progress {
        controller().cleanupStep(step, limit, nextLegacyUsers, removeLegacyUser);
    };

    public func sealMigration(step : Nat) : async Migration.Progress {
        controller().seal(step, func() : Bool { legacyUsers().isEmpty() });
    };

    /// A final Wasm that removes the old stable field should guard itself with
    /// this check before relying on the legacy collection no longer existing.
    public func requireSealedForFinalUpgrade() : async () {
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
