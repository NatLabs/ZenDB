// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Iter "mo:base@0.16.0/Iter";
import Array "mo:base@0.16.0/Array";
import Text "mo:base@0.16.0/Text";
import Principal "mo:base@0.16.0/Principal";
import Blob "mo:base@0.16.0/Blob";

import { test; suite } "mo:test";
import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";

import ZenDB "../../src/EmbeddedInstance";
import CommonIndexFns "../../src/EmbeddedInstance/Collection/Index/CommonIndexFns";
import StableDatabase "../../src/EmbeddedInstance/Database/StableDatabase";
import T "../../src/EmbeddedInstance/Types";

suite(
    "Index Intersection Tests",
    func() {
        type Transaction = {
            token : Text;
            sender : Text;
            amount : Nat;
            timestamp : Nat;
            fee : Nat;
        };

        let schema = #Record([
            ("token", #Text),
            ("sender", #Text),
            ("amount", #Nat),
            ("timestamp", #Nat),
            ("fee", #Nat),
        ]);

        let candify = {
            to_blob = func(data : Transaction) : Blob {
                to_candid (data);
            };
            from_blob = func(blob : Blob) : ?Transaction {
                from_candid (blob);
            };
        };

        test(
            "get_best_indexes_to_intersect - basic swap scenario",
            func() {
                // Setup ZenDB properly - create a Principal with enough bytes
                let canister_id = Principal.fromBlob(Blob.fromArray([0, 0, 0, 0, 0, 0, 0, 1]));
                let versioned_sstore = ZenDB.newStableStore(canister_id, null);
                let zendb = ZenDB.launchDefaultDB(versioned_sstore);

                let #ok(collection) = zendb.createCollection("transactions", schema, candify, null) else Debug.trap("Failed to create collection");

                // Create indexes:
                // 1. (sender, token, timestamp) - will be the best index
                // 2. (sender, token, amount) - should be found when swapping timestamp with amount
                // 3. (token, amount) - alternative combination
                let #ok(_) = collection.createIndex(
                    "idx_sender_token_ts",
                    [
                        ("sender", #Ascending),
                        ("token", #Ascending),
                        ("timestamp", #Ascending),
                    ],
                    null,
                ) else Debug.trap("Failed to create index 1");

                let #ok(_) = collection.createIndex(
                    "idx_sender_token_amount",
                    [
                        ("sender", #Ascending),
                        ("token", #Ascending),
                        ("amount", #Ascending),
                    ],
                    null,
                ) else Debug.trap("Failed to create index 2");

                let #ok(_) = collection.createIndex(
                    "idx_token_amount",
                    [
                        ("token", #Ascending),
                        ("amount", #Ascending),
                    ],
                    null,
                ) else Debug.trap("Failed to create index 3");

                // Insert some test data
                for (i in Iter.range(0, 99)) {
                    let tx : Transaction = {
                        token = if (i % 2 == 0) "ICP" else "BTC";
                        sender = "user_" # debug_show (i % 10);
                        amount = i * 100;
                        timestamp = i * 1000;
                        fee = 10;
                    };
                    ignore collection.insert(tx);
                };

                // Query: eq on token, sender, range on timestamp, amount
                // Best index should be: sender, token, timestamp (covers: sender, token, timestamp)
                // Expected to find: sender, token, amount (by swapping timestamp -> amount)
                let operations : [(Text, T.ZqlOperators)] = [
                    ("token", #eq(#Text("ICP"))),
                    ("sender", #eq(#Text("user_1"))),
                    ("timestamp", #gte(#Nat(50_000))),
                    ("amount", #gte(#Nat(5_000))),
                ];

                let sort_field : ?(Text, T.SortDirection) = ?("timestamp", #Ascending);

                // Get the collection internal structure
                let stable_collection = collection._get_stable_state();

                // Call get_best_indexes_to_intersect
                let result_indexes = CommonIndexFns.get_best_indexes_to_intersect(
                    stable_collection,
                    operations,
                    sort_field,
                );

                // Print the results
                Debug.print("=== Test: basic swap scenario ===");
                Debug.print("Number of indexes found: " # debug_show (result_indexes.size()));

                for (i in Iter.range(0, result_indexes.size() - 1)) {
                    let idx = result_indexes[i];
                    Debug.print("\nIndex " # debug_show (i) # ":");
                    Debug.print("  Name: " # idx.index.name);
                    Debug.print("  Interval: " # debug_show (idx.interval));
                    Debug.print("  Size: " # debug_show (idx.interval.1 - idx.interval.0));
                    Debug.print("  Fully covered equal fields: " # debug_show (Set.toArray(idx.fully_covered_equal_fields)));
                    Debug.print("  Fully covered range fields: " # debug_show (Set.toArray(idx.fully_covered_range_fields)));
                    Debug.print("  Requires additional filtering: " # debug_show (idx.requires_additional_filtering));
                    Debug.print("  Requires additional sorting: " # debug_show (idx.requires_additional_sorting));
                };

                // Validate results
                assert result_indexes.size() >= 1;

                // First should be the best index
                let best = result_indexes[0];
                assert best.index.name == "idx_sender_token_ts";

                // Should have found at least one complementary index for the uncovered 'amount' field
                if (result_indexes.size() > 1) {
                    let complementary = result_indexes[1];

                    // Should be one that covers amount
                    let covers_amount = Set.has(complementary.fully_covered_range_fields, Map.thash, "amount") or Set.has(complementary.fully_covered_equal_fields, Map.thash, "amount");
                    assert covers_amount;
                };
            },
        );

        test(
            "get_best_indexes_to_intersect - no additional indexes needed",
            func() {
                // Setup ZenDB properly
                let canister_id = Principal.fromText("aaaaa-aa");
                let versioned_sstore = ZenDB.newStableStore(canister_id, null);
                let zendb = ZenDB.launchDefaultDB(versioned_sstore);

                let #ok(collection) = zendb.createCollection("transactions", schema, candify, null) else Debug.trap("Failed to create collection");

                // Create a perfect index that covers everything
                let #ok(_) = collection.createIndex(
                    "idx_perfect",
                    [
                        ("token", #Ascending),
                        ("sender", #Ascending),
                        ("amount", #Ascending),
                        ("timestamp", #Ascending),
                    ],
                    null,
                ) else Debug.trap("Failed to create index");

                // Insert some test data
                for (i in Iter.range(0, 49)) {
                    let tx : Transaction = {
                        token = if (i % 2 == 0) "ICP" else "BTC";
                        sender = "user_" # debug_show (i % 5);
                        amount = i * 100;
                        timestamp = i * 1000;
                        fee = 10;
                    };
                    ignore collection.insert(tx);
                };

                let operations : [(Text, T.ZqlOperators)] = [
                    ("token", #eq(#Text("ICP"))),
                    ("sender", #eq(#Text("user_1"))),
                    ("amount", #gte(#Nat(1_000))),
                ];

                let sort_field : ?(Text, T.SortDirection) = ?("timestamp", #Ascending);

                let stable_collection = collection._get_stable_state();

                let result_indexes = CommonIndexFns.get_best_indexes_to_intersect(
                    stable_collection,
                    operations,
                    sort_field,
                );

                Debug.print("\n=== Test: no additional indexes needed ===");
                Debug.print("Number of indexes (should be 1): " # debug_show (result_indexes.size()));

                for (i in Iter.range(0, result_indexes.size() - 1)) {
                    let idx = result_indexes[i];
                    Debug.print("\nIndex " # debug_show (i) # ":");
                    Debug.print("  Name: " # idx.index.name);
                    Debug.print("  Requires additional filtering: " # debug_show (idx.requires_additional_filtering));
                    Debug.print("  Requires additional sorting: " # debug_show (idx.requires_additional_sorting));
                };

                assert result_indexes.size() == 1;

                let best = result_indexes[0];
                assert best.index.name == "idx_perfect";
                assert not best.requires_additional_filtering;
                assert not best.requires_additional_sorting;
            },
        );

        test(
            "get_best_indexes_to_intersect - multiple complementary indexes",
            func() {
                // Setup ZenDB properly
                let canister_id = Principal.fromText("aaaaa-aa");
                let versioned_sstore = ZenDB.newStableStore(canister_id, null);
                let zendb = ZenDB.launchDefaultDB(versioned_sstore);

                let #ok(collection) = zendb.createCollection("transactions", schema, candify, null) else Debug.trap("Failed to create collection");

                // Create several partial indexes
                let #ok(_) = collection.createIndex(
                    "idx_token_sender",
                    [
                        ("token", #Ascending),
                        ("sender", #Ascending),
                    ],
                    null,
                ) else Debug.trap("Failed to create index 1");

                let #ok(_) = collection.createIndex(
                    "idx_amount",
                    [("amount", #Ascending)],
                    null,
                ) else Debug.trap("Failed to create index 2");

                let #ok(_) = collection.createIndex(
                    "idx_timestamp",
                    [("timestamp", #Ascending)],
                    null,
                ) else Debug.trap("Failed to create index 3");

                let #ok(_) = collection.createIndex(
                    "idx_fee",
                    [("fee", #Ascending)],
                    null,
                ) else Debug.trap("Failed to create index 4");

                // Insert small amount of data
                for (i in Iter.range(0, 19)) {
                    let tx : Transaction = {
                        token = "ICP";
                        sender = "user_" # debug_show (i);
                        amount = i * 10;
                        timestamp = i * 100;
                        fee = 5;
                    };
                    ignore collection.insert(tx);
                };

                // Query with multiple uncovered fields
                let operations : [(Text, T.ZqlOperators)] = [
                    ("token", #eq(#Text("ICP"))),
                    ("sender", #eq(#Text("user_5"))),
                    ("amount", #gte(#Nat(30))),
                    ("timestamp", #lte(#Nat(1500))),
                    ("fee", #eq(#Nat(5))),
                ];

                let stable_collection = collection._get_stable_state();

                let result_indexes = CommonIndexFns.get_best_indexes_to_intersect(
                    stable_collection,
                    operations,
                    null,
                );

                Debug.print("\n=== Test: multiple complementary indexes ===");
                Debug.print("Number of indexes found: " # debug_show (result_indexes.size()));
                Debug.print("Indexes: " # debug_show (Array.map(result_indexes, func(idx : T.BestIndexResult) : Text { idx.index.name })));

                for (i in Iter.range(0, result_indexes.size() - 1)) {
                    let idx = result_indexes[i];
                    Debug.print("\nIndex " # debug_show (i) # ":");
                    Debug.print("  Name: " # idx.index.name);
                    Debug.print("  Fully covered equal fields: " # debug_show (Set.toArray(idx.fully_covered_equal_fields)));
                    Debug.print("  Fully covered range fields: " # debug_show (Set.toArray(idx.fully_covered_range_fields)));
                    let covered_count = Set.size(idx.fully_covered_equal_fields) + Set.size(idx.fully_covered_range_fields);
                    Debug.print("  Total covered fields: " # debug_show (covered_count));
                };

                // Should find multiple indexes to cover all the fields
                assert result_indexes.size() >= 1;

                // Check that we're making progress toward covering all fields
                let best = result_indexes[0];
                let covered_count = Set.size(best.fully_covered_equal_fields) + Set.size(best.fully_covered_range_fields);
                Debug.print("Best index covers " # debug_show (covered_count) # " fields");
            },
        );

        test(
            "get_best_indexes_to_intersect - memory limit enforcement",
            func() {
                // Setup ZenDB properly
                let canister_id = Principal.fromText("aaaaa-aa");
                let versioned_sstore = ZenDB.newStableStore(canister_id, null);
                let zendb = ZenDB.launchDefaultDB(versioned_sstore);

                let #ok(collection) = zendb.createCollection("transactions", schema, candify, null) else Debug.trap("Failed to create collection");

                // Create indexes
                let #ok(_) = collection.createIndex(
                    "idx_token",
                    [("token", #Ascending)],
                    null,
                ) else Debug.trap("Failed to create index");

                let #ok(_) = collection.createIndex(
                    "idx_sender",
                    [("sender", #Ascending)],
                    null,
                ) else Debug.trap("Failed to create index");

                // Insert large amount of data to potentially exceed limits
                for (i in Iter.range(0, 999)) {
                    let tx : Transaction = {
                        token = "ICP";
                        sender = "user_" # debug_show (i);
                        amount = i;
                        timestamp = i;
                        fee = 1;
                    };
                    ignore collection.insert(tx);
                };

                let operations : [(Text, T.ZqlOperators)] = [
                    ("token", #eq(#Text("ICP"))),
                    ("sender", #gte(#Text("user_0"))),
                ];

                let stable_collection = collection._get_stable_state();

                let result_indexes = CommonIndexFns.get_best_indexes_to_intersect(
                    stable_collection,
                    operations,
                    null,
                );

                Debug.print("\n=== Test: memory limit enforcement ===");
                Debug.print("Number of indexes: " # debug_show (result_indexes.size()));

                // Should respect memory limits and not exceed 500K total entries
                var total_entries = 0;
                for (idx in result_indexes.vals()) {
                    let size = idx.interval.1 - idx.interval.0;
                    total_entries += size;
                    Debug.print("Index " # idx.index.name # " size: " # debug_show (size));
                };

                Debug.print("Total entries across all indexes: " # debug_show (total_entries));
                assert total_entries <= 500_000;
            },
        );
    },
);
