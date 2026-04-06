import Debug "mo:core@2.4/Debug";
import Nat "mo:core@2.4/Nat";
import Nat64 "mo:core@2.4/Nat64";
import Array "mo:core@2.4/Array";
import Blob "mo:core@2.4/Blob";
import Iter "mo:core@2.4/Iter";
import Principal "mo:core@2.4/Principal";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Map "mo:map@9.0/Map";
import Set "mo:map@9.0/Set";
import Vector "mo:vector@0.4";
import Candid "mo:serde@3.5/Candid";

import Index "../../src/EmbeddedInstance/Collection/Index";
import CompositeIndex "../../src/EmbeddedInstance/Collection/Index/CompositeIndex";
import T "../../src/EmbeddedInstance/Types";
import C "../../src/EmbeddedInstance/Constants";
import BTree "../../src/EmbeddedInstance/BTree";
import DocumentStore "../../src/EmbeddedInstance/Collection/DocumentStore";
import CandidMap "../../src/EmbeddedInstance/CandidMap";
import SchemaMap "../../src/EmbeddedInstance/Collection/SchemaMap";
import ByteUtils "mo:byte-utils@0.2";
import Logger "../../src/EmbeddedInstance/Logger";
import Ids "../../src/EmbeddedInstance/Ids";
import TwoQueueCache "../../src/EmbeddedInstance/TwoQueueCache";

module {
    type Schema = T.Schema;
    type Candid = T.Candid;

    public func init() : Bench.Bench {
        let fuzz = Fuzz.fromSeed(0xdeadbeef);

        let bench = Bench.Bench();
        bench.name("Benchmarking Index Operations");
        bench.description("Measuring Index insert/remove performance during document operations");

        bench.cols([
            "insertWithCandidMap() - 1 field",
            "insertWithCandidMap() - 3 fields",
            "insertWithCandidMap() - 5 fields",
            "removeWithCandidMap() - 1 field",
            "removeWithCandidMap() - 3 fields",
        ]);

        bench.rows([
            // "Heap - Nat index",
            // "Heap - Text index",
            // "Heap - Composite (Nat+Text)",
            // "Heap - Composite (Nat+Text+Bool)",
            "Stable Memory - Nat index",
            "Stable Memory - Text index",
            "Stable Memory - Composite (Nat+Text)",
        ]);

        let limit = 1_000;

        // Helper to create document ID
        func makeDocId(n : Nat) : Blob {
            let instance_id = Blob.fromArray([0, 0, 0, 1]);
            let id_bytes = ByteUtils.BigEndian.fromNat64(Nat64.fromNat(n));
            Blob.fromArray(Array.concat(Blob.toArray(instance_id), id_bytes));
        };

        // Helper to create mock collection
        func makeMockCollection(memory_type : T.MemoryType) : T.StableCollection {
            let instance_id = Blob.fromArray([0, 0, 0, 1]);
            let canister_id = Principal.fromText("aaaaa-aa");
            let schema = #Record([]);

            {
                ids = Ids.new();
                name = "test";
                instance_id;
                schema;
                schema_map = SchemaMap.new(schema);
                schema_keys = [];
                schema_keys_set = Set.new<Text>();
                documents = if (memory_type == #heap) {
                    DocumentStore.new_heap();
                } else {
                    DocumentStore.new_stable_memory();
                };
                indexes = Map.new<Text, T.Index>();
                indexes_in_batch_operations = Map.new<Text, T.Index>();
                populate_index_batches = Map.new<Nat, T.BatchPopulateIndex>();
                hidden_indexes = Set.new<Text>();
                candid_serializer = Candid.TypedSerializer.new(
                    [schema],
                    ?{ Candid.defaultOptions with types = ?[schema] },
                );
                field_constraints = Map.new<Text, [T.SchemaFieldConstraint]>();
                unique_constraints = [];
                fields_with_unique_constraints = Map.new<Text, Set.Set<Nat>>();
                candid_map_cache = TwoQueueCache.new(100_000);
                freed_btrees = Vector.new<T.MemoryBTree>();
                logger = Logger.init(#Warn, false);
                memory_type;
                is_running_locally = false;
                is_compression_enabled = null;
            };
        };

        // ===== Test Schema and Documents =====
        let user_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("age", #Nat),
            ("active", #Bool),
            ("score", #Float),
        ]);

        let user_schema_map = SchemaMap.new(user_schema);

        let user_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("name", #Text(fuzz.text.randomAlphanumeric(20))),
                    ("age", #Nat(fuzz.nat.randomRange(18, 80))),
                    ("active", #Bool(fuzz.bool.random())),
                    ("score", #Float(fuzz.float.randomRange(0.0, 100.0))),
                ]);
            },
        );

        let doc_ids = Array.tabulate<Blob>(limit, func(i : Nat) : Blob = makeDocId(i));

        let candid_maps = Array.tabulate<T.CandidMap>(
            limit,
            func(i : Nat) : T.CandidMap {
                CandidMap.new(user_schema_map, doc_ids[i], user_docs[i]);
            },
        );

        // ===== Helper functions to create indexes =====
        func createNatIndex(collection : T.StableCollection, field : Text) : T.CompositeIndex {
            CompositeIndex.new(
                collection,
                "idx_" # field,
                [(field, #Ascending)],
                false,
                false,
            );
        };

        func createTextIndex(collection : T.StableCollection, field : Text) : T.CompositeIndex {
            CompositeIndex.new(
                collection,
                "idx_" # field,
                [(field, #Ascending)],
                false,
                false,
            );
        };

        func createComposite2Index(collection : T.StableCollection) : T.CompositeIndex {
            CompositeIndex.new(
                collection,
                "idx_composite_2",
                [("age", #Ascending), ("name", #Ascending)],
                false,
                false,
            );
        };

        func createComposite3Index(collection : T.StableCollection) : T.CompositeIndex {
            CompositeIndex.new(
                collection,
                "idx_composite_3",
                [("age", #Ascending), ("name", #Ascending), ("active", #Ascending)],
                false,
                false,
            );
        };

        func createComposite5Index(collection : T.StableCollection) : T.CompositeIndex {
            CompositeIndex.new(
                collection,
                "idx_composite_5",
                [
                    ("id", #Ascending),
                    ("name", #Ascending),
                    ("age", #Ascending),
                    ("active", #Ascending),
                    ("score", #Ascending),
                ],
                false,
                false,
            );
        };

        bench.runner(
            func(row, col) {
                switch (row, col) {
                    // Row 1: Heap - Nat index
                    case ("Heap - Nat index", "insertWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#heap);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Nat index", "insertWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Nat index", "insertWithCandidMap() - 5 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite5Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Nat index", "removeWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#heap);
                        let idx = createNatIndex(collection, "age");
                        // Pre-populate
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        // Benchmark remove
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Nat index", "removeWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite3Index(collection);
                        // Pre-populate
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        // Benchmark remove
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    // Row 2: Heap - Text index
                    case ("Heap - Text index", "insertWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#heap);
                        let idx = createTextIndex(collection, "name");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Text index", "insertWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Text index", "insertWithCandidMap() - 5 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite5Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Text index", "removeWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#heap);
                        let idx = createTextIndex(collection, "name");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Text index", "removeWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    // Row 3: Heap - Composite (Nat+Text)
                    case ("Heap - Composite (Nat+Text)", "insertWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#heap);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Composite (Nat+Text)", "insertWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite2Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Composite (Nat+Text)", "insertWithCandidMap() - 5 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite5Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Composite (Nat+Text)", "removeWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#heap);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Composite (Nat+Text)", "removeWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite2Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    // Row 4: Heap - Composite (Nat+Text+Bool)
                    case ("Heap - Composite (Nat+Text+Bool)", "insertWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#heap);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Composite (Nat+Text+Bool)", "insertWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Composite (Nat+Text+Bool)", "insertWithCandidMap() - 5 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite5Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Composite (Nat+Text+Bool)", "removeWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#heap);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Heap - Composite (Nat+Text+Bool)", "removeWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#heap);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    // Row 5: Stable Memory - Nat index
                    case ("Stable Memory - Nat index", "insertWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Nat index", "insertWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Nat index", "insertWithCandidMap() - 5 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite5Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Nat index", "removeWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Nat index", "removeWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    // Row 6: Stable Memory - Text index
                    case ("Stable Memory - Text index", "insertWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createTextIndex(collection, "name");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Text index", "insertWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Text index", "insertWithCandidMap() - 5 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite5Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Text index", "removeWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createTextIndex(collection, "name");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Text index", "removeWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite3Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    // Row 7: Stable Memory - Composite (Nat+Text)
                    case ("Stable Memory - Composite (Nat+Text)", "insertWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Composite (Nat+Text)", "insertWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite2Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Composite (Nat+Text)", "insertWithCandidMap() - 5 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite5Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Composite (Nat+Text)", "removeWithCandidMap() - 1 field") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createNatIndex(collection, "age");
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case ("Stable Memory - Composite (Nat+Text)", "removeWithCandidMap() - 3 fields") {
                        let collection = makeMockCollection(#stableMemory);
                        let idx = createComposite2Index(collection);
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.insertWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                        for (i in Nat.rangeInclusive(0, limit - 1)) {
                            ignore CompositeIndex.removeWithCandidMap(collection, idx, doc_ids[i], candid_maps[i]);
                        };
                    };
                    case (_) {};
                };
            }
        );

        bench;
    };
};
