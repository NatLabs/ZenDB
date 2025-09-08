import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Char "mo:base/Char";
import Cycles "mo:base/ExperimentalCycles";
import Principal "mo:base/Principal";

import { test; suite } "mo:test/async";

import CanisterDB "../../src/Cluster/CanisterDB";
import Client "../../src/Cluster/Client";
import ClusterManager "../../src/Cluster/ClusterManager";
import Cluster "../../src/Cluster";
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

                // Create database
                let #ok(_) = await canister_db.zendb_create_database("test_db") else return assert false;

                let schema : ZenDB.Types.Schema = #Record([
                    ("name", #Text),
                    ("age", #Nat),
                    ("is_active", #Bool),
                ]);

                // Create collection
                let #ok(_) = await canister_db.zendb_create_collection("test_db", "users", schema) else return assert false;

                // Insert document
                let user_blob = to_candid ({
                    name = "Alice";
                    age = 30;
                    is_active = true;
                });

                let #ok(user_id) = await canister_db.zendb_collection_insert_document("test_db", "users", user_blob) else return assert false;

                // Get document
                assert #ok(user_blob) == (await canister_db.zendb_collection_get_document("test_db", "users", user_id));

                // Search documents
                let query_results = await canister_db.zendb_collection_search(
                    "test_db",
                    "users",
                    ZenDB.QueryBuilder().Where(
                        "age",
                        #gte(#Nat(18)),
                    ).build(),
                );

                assert query_results == #ok([(user_id, user_blob)]);

                Debug.print("Cluster search results: " # debug_show (query_results));

                await suite(
                    "Create a ClusterManager from an existing CanisterDB Instance",
                    func() : async () {
                        Cycles.add(5 * TRILLION);
                        let cluster_manager = await ClusterManager.ClusterManager([Principal.fromActor(canister_db)], null);

                        let cluster_manager_id = Principal.fromActor(cluster_manager);
                        let roles = [Cluster.Roles.MANAGER, Cluster.Roles.USER];
                        assert #ok() == (await canister_db.grant_roles(cluster_manager_id, roles));

                        await cluster_manager.init(); // !required

                        // Get document
                        assert #ok(user_blob) == (await cluster_manager.zendb_collection_get_document("test_db", "users", user_id));

                        // Search documents
                        let _query_results = await cluster_manager.zendb_collection_search(
                            "test_db",
                            "users",
                            ZenDB.QueryBuilder().Where(
                                "age",
                                #gte(#Nat(18)),
                            ).build(),
                        );

                        assert _query_results == #ok([(user_id, user_blob)]);

                    },
                );

            },
        );

        func named<A>(x : A) : async* (Nat) { 1 };

        await suite(
            "ClusterManager using Client",
            func() : async () {
                let cluster_id = Principal.toText(Principal.fromActor(canister_db));
                let zendb_client = Client.ClusterClient(cluster_id);

                let db = zendb_client.get_database("test_db");

                let candify : ZenDB.Types.Candify<User> = {
                    to_blob = func(user : User) : Blob { to_candid (user) };
                    from_blob = func(blob : Blob) : ?User { from_candid (blob) };
                };

                let users = db.get_collection<User>("users", candify);

                let user_charlie : User = {
                    name = "Charlie";
                    age = 35;
                    is_active = true;
                };

                let #ok(user_charlie_id) = await* users.insert(user_charlie) else return assert false;

                assert #ok(
                    user_charlie
                ) == users.from_get(
                    await* users.get(user_charlie_id)
                );

                Debug.print("Client test completed successfully");
            },
        );

    };
};
