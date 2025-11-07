import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Array "mo:base@0.16.0/Array";
import Char "mo:base@0.16.0/Char";
import Cycles "mo:base@0.16.0/ExperimentalCycles";
import Principal "mo:base@0.16.0/Principal";

import { test; suite } "mo:test/async";

import CanisterDB "../../src/RemoteInstance/CanisterDB";
import Client "../../src/RemoteInstance/Client";
import ZenDB "../../src";

persistent actor {
    transient let TRILLION = 1_000_000_000_000;
    public func runTests() : async () {
        Cycles.add(5 * TRILLION);
        let canister_db = await CanisterDB.CanisterDB();

        type User = {
            name : Text;
            age : Nat;
            is_active : Bool;
        };

        await suite(
            "CanisterDB Tests",
            func() : async () {

                let schema : ZenDB.Types.Schema = #Record([
                    ("name", #Text),
                    ("age", #Nat),
                    ("is_active", #Bool),
                ]);

                let #ok(_) = await canister_db.zendb_create_collection("default", "users", schema) else return assert false;

                let user_blob = to_candid ({
                    name = "Alice";
                    age = 30;
                    is_active = true;
                });

                let #ok(user_id) = await canister_db.zendb_collection_insert_document("default", "users", user_blob) else return assert false;

                assert #ok(user_blob) == (await canister_db.zendb_collection_get_document("default", "users", user_id));

                let query_results = await canister_db.zendb_collection_search(
                    "default",
                    "users",
                    ZenDB.QueryBuilder().Where(
                        "age",
                        #gte(#Nat(18)),
                    ).build(),
                );

                switch (query_results) {
                    case (#ok(result)) {
                        assert result.documents == [(user_id, user_blob)];
                    };
                    case (#err(err)) {
                        Debug.trap("Search failed: " # err);
                    };
                };

                Debug.print("Search results: " # debug_show (query_results));

            },
        );

        func named<A>(x : A) : async* (Nat) { 1 };

        await suite(
            "using client",
            func() : async () {
                let canister_id = Principal.toText(Principal.fromActor(canister_db));
                let zendb_client = Client.Client(canister_id);

                let db = zendb_client.launchDefaultDB();

                let candify : ZenDB.Types.Candify<User> = {
                    to_blob = func(user : User) : Blob { to_candid (user) };
                    from_blob = func(blob : Blob) : ?User { from_candid (blob) };
                };

                let users = db.get_collection<User>("users", candify);

                let user_bob : User = {
                    name = "Bob";
                    age = 25;
                    is_active = false;
                };

                let #ok(user_bob_id) = await* users.insert(user_bob) else return assert false;

                assert #ok(
                    user_bob
                ) == users.from_get(
                    await* users.get(user_bob_id)
                );

            },
        );

    };
};
