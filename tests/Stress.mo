import Region "mo:base@0.16.0/Region";
import Debug "mo:base@0.16.0/Debug";
import Nat64 "mo:base@0.16.0/Nat64";
import Buffer "mo:base@0.16.0/Buffer";

import { test; suite } "mo:test";
import Itertools "mo:itertools@0.2.2/Iter";
import Fuzz "mo:fuzz";
import Map "mo:map@9.0.1/Map";

import ZenDB "../src";

actor {

    let fuzz = Fuzz.fromSeed(0x7eadbeef);
    let { QueryBuilder } = ZenDB;

    let limit = 10_000;

    let canister_id = fuzz.principal.randomPrincipal(29);
    stable let sstore = ZenDB.newStableStore(
        canister_id,
        ?{
            log_level = ?#Error;
            is_running_locally = ?true;
            memory_type = ?(#stableMemory);
        },
    );

    let zendb = ZenDB.launchDefaultDB(sstore);

    type UserId = { id : Nat };
    let UserIdSchema : ZenDB.Types.Schema = #Record([("id", #Nat)]);

    let candify : ZenDB.Types.Candify<UserId> = {
        from_blob = func(blob : Blob) : ?UserId = from_candid (blob);
        to_blob = func(r : UserId) : Blob = to_candid (r);
    };

    let collections = Buffer.Buffer<ZenDB.Collection<UserId>>(limit);

    public func runTests() {

        suite(
            "Stress Tests",
            func() {
                test(
                    "Stress Test: Create 5000 collections",
                    func() {
                        for (i in Itertools.range(zendb.size(), 5000)) {
                            let #ok(collection) = zendb.createCollection(
                                "collection_" # debug_show (i),
                                UserIdSchema,
                                candify,
                                ?{ schemaConstraints = [#Unique(["id"])] },
                            );

                            collections.add(collection);
                            Debug.print("Created collection: " # debug_show (i));
                        };
                    },
                );

            },
        );

    };

};
