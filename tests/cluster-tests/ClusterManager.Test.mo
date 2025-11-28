// import Debug "mo:base@0.16.0/Debug";
// import Text "mo:base@0.16.0/Text";
// import Array "mo:base@0.16.0/Array";
// import Char "mo:base@0.16.0/Char";
// import Cycles "mo:base@0.16.0/ExperimentalCycles";
// import Principal "mo:base@0.16.0/Principal";
// import Iter "mo:base@0.16.0/Iter";

// import { test; suite } "mo:test/async";

// import ClusterManager "../../src/RemoteInstance/ClusterManager";
// import Client "../../src/RemoteInstance/Client";
// import ZenDB "../../src";

// persistent actor {
//     transient let TRILLION = 1_000_000_000_000;

//     public func runTests() : async () {
//         Cycles.add(20 * TRILLION); // More cycles for multiple canisters
//         let cluster_manager = await ClusterManager.ClusterManager([], null);

//         await cluster_manager.init(); // !required

//         type User = {
//             name : Text;
//             age : Nat;
//             is_active : Bool;
//         };

//         await suite(
//             "ClusterManager Basic Tests",
//             func() : async () {

//                 await test(
//                     "Create database",
//                     func() : async () {
//                         // Create database
//                         let #ok(_) = await cluster_manager.zendb_create_database("test_db") else return assert false;

//                     },
//                 );

//                 let schema : ZenDB.Types.Schema = #Record([
//                     ("name", #Text),
//                     ("age", #Nat),
//                     ("is_active", #Bool),
//                 ]);

//                 let user_blob = to_candid ({
//                     name = "Alice";
//                     age = 30;
//                     is_active = true;
//                 });

//                 await test(
//                     "Create collection, insert, get, search document",
//                     func() : async () {
//                         // Create collection
//                         let #ok(_) = await cluster_manager.zendb_create_collection("test_db", "users", schema) else return assert false;

//                         // Insert document
//                         let #ok(user_id) = await cluster_manager.zendb_collection_insert_document("test_db", "users", user_blob) else return assert false;

//                         // Get document
//                         assert #ok(user_blob) == (await cluster_manager.zendb_collection_get_document("test_db", "users", user_id));

//                         // Search documents
//                         let query_results = await cluster_manager.zendb_collection_search(
//                             "test_db",
//                             "users",
//                             ZenDB.QueryBuilder().Where(
//                                 "age",
//                                 #gte(#Nat(18)),
//                             ).build(),
//                         );

//                         assert query_results == #ok([(user_id, user_blob)]);

//                         Debug.print("Cluster search results: " # debug_show (query_results));
//                     },
//                 );

//                 await test(
//                     "Scaling - Auto creates canisters when threshold and max size is reached",
//                     func() : async () {

//                         await cluster_manager.set_sharding_strategy(
//                             #fill_first({
//                                 max_canister_size_in_bytes = 262_144; /* ~ 240 KiB (4 pages) - init size for each collection */
//                                 threshold_percent = 0.9;
//                             })
//                         );

//                         let canisters = await cluster_manager.get_canisters();
//                         assert canisters.size() == 1;

//                         ignore await cluster_manager.zendb_create_collection("test_db", "2nd_collection", schema);
//                         Debug.print("Canisters after creating 2nd collection: " # debug_show (await cluster_manager.get_canisters()));
//                         // assert (await cluster_manager.get_canisters()).size() == 2;

//                         ignore await cluster_manager.zendb_create_collection("test_db", "3rd_collection", schema);
//                         Debug.print("Canisters after creating 3rd collection: " # debug_show (await cluster_manager.get_canisters()));
//                         // assert (await cluster_manager.get_canisters()).size() == 3;

//                     },
//                 );

//             },
//         );

//         await suite(
//             "ClusterManager using Client",
//             func() : async () {
//                 let cluster_id = Principal.toText(Principal.fromActor(cluster_manager));
//                 let zendb_client = Client.Client(cluster_id);

//                 let db = zendb_client.get_database("test_db");

//                 let candify : ZenDB.Types.Candify<User> = {
//                     to_blob = func(user : User) : Blob { to_candid (user) };
//                     from_blob = func(blob : Blob) : ?User { from_candid (blob) };
//                 };

//                 let users = db.get_collection<User>("users", candify);

//                 let user_charlie : User = {
//                     name = "Charlie";
//                     age = 35;
//                     is_active = true;
//                 };

//                 let #ok(user_charlie_id) = await* users.insert(user_charlie) else return assert false;

//                 assert #ok(
//                     user_charlie
//                 ) == users.from_get(
//                     await* users.get(user_charlie_id)
//                 );

//                 Debug.print("Client test completed successfully");
//             },
//         );

//     };
// };
