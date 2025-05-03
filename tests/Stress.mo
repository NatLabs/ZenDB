import Region "mo:base/Region";
import Debug "mo:base/Debug";
import Nat64 "mo:base/Nat64";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Fuzz "mo:fuzz";
import Map "mo:map/Map";

import ZenDB "../src";

actor {

    let fuzz = Fuzz.fromSeed(0x7eadbeef);
    let { QueryBuilder } = ZenDB;

    let limit = 10_000;

    stable let sstore = ZenDB.newStableStore(
        ?{
            logging = ?{
                log_level = #Error;
                is_running_locally = true;
            };
            memory_type = ?(#stableMemory);
        }
    );

    let zendb = ZenDB.launchDefaultDB(sstore);

    type UserId = { id : Nat };
    let UserIdSchema : ZenDB.Types.Schema = #Record([("id", #Nat)]);

    let candify : ZenDB.Types.Candify<UserId> = {
        from_blob = func(blob : Blob) : ?UserId = from_candid (blob);
        to_blob = func(r : UserId) : Blob = to_candid (r);
    };

    public func runTests() {

        suite(
            "Stress Tests",
            func() {
                test(
                    "Stress Test: Create 5000 collections",
                    func() {
                        for (i in Itertools.range(zendb.size(), 5000)) {
                            let #ok(_) = zendb.create_collection("collection_" # debug_show (i), UserIdSchema, candify, []);
                            Debug.print("Created collection: " # debug_show (i));
                        };
                    },
                );

            },
        );

    };

};
