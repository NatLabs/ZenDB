// @testmode wasi

import Principal "mo:core@2.4/Principal";
import Iter "mo:core@2.4/Iter";
import { test; suite } "mo:test";

import ZenDB "../../src/EmbeddedInstance";
import Migration "../../src/EmbeddedInstance/Migration";
import MigrationTemplates "../../src/EmbeddedInstance/MigrationTemplates";

type LegacyUser = {
    firstName : Text;
    lastName : Text;
};

type UserV2 = {
    legacyId : Blob;
    displayName : Text;
};

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

func newDatabase() : ZenDB.Database {
    let store = ZenDB.newStableStore(
        Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai"),
        ?{
            log_level = ?#Error;
            is_running_locally = ?true;
            memory_type = ?#stableMemory;
            cache_capacity = ?10;
            is_compression_enabled = ?false;
        },
    );
    ZenDB.launchDefaultDB(store);
};

func toV2((legacyId, user) : (Blob, LegacyUser)) : UserV2 {
    { legacyId; displayName = user.firstName # " " # user.lastName };
};

suite(
    "Migration templates",
    func() {
        test(
            "copies, verifies, and removes a collection through bounded scans",
            func() {
                let database = newDatabase();
                let #ok(source) = database.createCollection<LegacyUser>(
                    "users",
                    legacySchema,
                    legacyCandify,
                    null,
                ) else return assert false;

                for (user in [
                    { firstName = "Ada"; lastName = "Lovelace" },
                    { firstName = "Grace"; lastName = "Hopper" },
                    { firstName = "Edsger"; lastName = "Dijkstra" },
                ].vals()) {
                    let #ok(_) = source.insert(user) else return assert false;
                };

                let state = Migration.newState<Blob>(1);
                let controller = Migration.Controller<Blob, (Blob, LegacyUser)>(state);
                let #ok(_) = MigrationTemplates.begin(
                    controller,
                    database,
                    "users_v2",
                    v2Schema,
                    v2Candify,
                    ?{ schema_constraints = [#Unique(["legacyId"])] },
                    "users-v2",
                    2,
                ) else return assert false;

                // A successful begin may be retried without adopting or
                // recreating a different target collection.
                let #ok(_) = MigrationTemplates.begin(
                    controller,
                    database,
                    "users_v2",
                    v2Schema,
                    v2Candify,
                    ?{ schema_constraints = [#Unique(["legacyId"])] },
                    "users-v2",
                    2,
                ) else return assert false;

                let #ok(target) = database.getCollection<UserV2>("users_v2", v2Candify) else {
                    return assert false;
                };
                let next = func(cursor : ?Blob, limit : Nat) : [(Blob, (Blob, LegacyUser))] {
                    MigrationTemplates.nextFromCursor(source, cursor, limit);
                };
                let copy = func(item : (Blob, LegacyUser)) : Migration.Result<()> {
                    MigrationTemplates.copyBySourceId(target, "legacyId", item, toV2);
                };
                let verify = func(item : (Blob, LegacyUser)) : Bool {
                    MigrationTemplates.verifyBySourceId(
                        target,
                        "legacyId",
                        item,
                        func(sourceItem, targetItem) : Bool {
                            targetItem == toV2(sourceItem);
                        },
                    );
                };
                let remove = func(item : (Blob, LegacyUser)) : Migration.Result<()> {
                    MigrationTemplates.removeSource(controller, source, item);
                };

                ignore controller.copyStep(1, 2, next, copy);
                ignore controller.copyStep(2, 2, next, copy);
                assert controller.copyStep(3, 2, next, copy).phase == #verifying;
                ignore controller.verifyStep(4, 2, next, verify);
                ignore controller.verifyStep(5, 2, next, verify);
                assert controller.verifyStep(6, 2, next, verify).phase == #readyToCutover;

                // Per-source verification cannot see unrelated target rows.
                // The template's cutover guard must reject that contamination
                // without consuming the step.
                let #ok(unrelatedId) = target.insert({
                    legacyId = "\ff" : Blob;
                    displayName = "Unrelated row";
                }) else return assert false;
                assert switch (MigrationTemplates.commit(controller, target, 7, func() {})) {
                    case (#err(_)) true;
                    case (#ok(_)) false;
                };
                let #ok(_) = target.deleteById(unrelatedId) else return assert false;
                var cutovers = 0;
                let cutover = func() { cutovers += 1 };
                let #ok(_) = MigrationTemplates.commit(controller, target, 7, cutover) else {
                    return assert false;
                };
                assert cutovers == 1;

                // Once cutover completes, ordinary writes may resume. Retrying
                // the uncertain commit response must remain a no-op even though
                // target cardinality has legitimately changed.
                let #ok(postCutoverId) = target.insert({
                    legacyId = "\fe" : Blob;
                    displayName = "Post-cutover row";
                }) else return assert false;
                let #ok(_) = MigrationTemplates.commit(controller, target, 7, cutover) else {
                    return assert false;
                };
                assert cutovers == 1;
                let #ok(_) = target.deleteById(postCutoverId) else return assert false;

                ignore controller.cleanupStep(8, 2, next, remove);
                ignore controller.cleanupStep(9, 2, next, remove);
                assert controller.cleanupStep(10, 2, next, remove).phase == #readyToSeal;
                assert controller.seal(11, func() : Bool { source.isEmpty() }).phase == #sealed;
                assert source.isEmpty();
                assert target.size() == 3;
            },
        );

        test(
            "rejects a target that existed before migration began",
            func() {
                let database = newDatabase();
                let #ok(_) = database.createCollection<UserV2>(
                    "users_v2",
                    v2Schema,
                    v2Candify,
                    ?{ schema_constraints = [#Unique(["legacyId"])] },
                ) else return assert false;

                let state = Migration.newState<Blob>(1);
                let controller = Migration.Controller<Blob, (Blob, LegacyUser)>(state);
                let result = MigrationTemplates.begin(
                    controller,
                    database,
                    "users_v2",
                    v2Schema,
                    v2Candify,
                    ?{ schema_constraints = [#Unique(["legacyId"])] },
                    "users-v2",
                    2,
                );

                assert switch (result) {
                    case (#err(_)) true;
                    case (#ok(_)) false;
                };
                assert controller.getProgress().phase == #idle;
            },
        );

        test(
            "rejects idempotent copy without a unique source-id constraint",
            func() {
                let database = newDatabase();
                let #ok(target) = database.createCollection<UserV2>(
                    "users_v2",
                    v2Schema,
                    v2Candify,
                    null,
                ) else return assert false;

                let result = MigrationTemplates.copyBySourceId(
                    target,
                    "legacyId",
                    (
                        "\00" : Blob,
                        { firstName = "Ada"; lastName = "Lovelace" },
                    ),
                    toV2,
                );

                assert switch (result) {
                    case (#err(_)) true;
                    case (#ok(_)) false;
                };
                assert target.isEmpty();
            },
        );

        test(
            "scans inclusive document-id bounds and resumes from a removed bound",
            func() {
                let database = newDatabase();
                let #ok(source) = database.createCollection<LegacyUser>(
                    "users",
                    legacySchema,
                    legacyCandify,
                    null,
                ) else return assert false;

                let #ok(firstId) = source.insert({
                    firstName = "Ada";
                    lastName = "Lovelace";
                }) else return assert false;
                let #ok(secondId) = source.insert({
                    firstName = "Grace";
                    lastName = "Hopper";
                }) else return assert false;
                let #ok(thirdId) = source.insert({
                    firstName = "Edsger";
                    lastName = "Dijkstra";
                }) else return assert false;

                let inclusive = Iter.toArray(source.scan(?firstId, ?secondId));
                assert inclusive.size() == 2;
                assert inclusive[0].0 == firstId;
                assert inclusive[1].0 == secondId;

                let #ok(_) = source.deleteById(secondId) else return assert false;
                let resumed = Iter.toArray(source.scan(?secondId, ?thirdId));
                assert resumed.size() == 1;
                assert resumed[0].0 == thirdId;
            },
        );
    },
);
