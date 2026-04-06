import Debug "mo:core@2.4/Debug";
import Nat "mo:core@2.4/Nat";
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

import StableCollection "../../src/EmbeddedInstance/Collection/StableCollection";
import CompositeIndex "../../src/EmbeddedInstance/Collection/Index/CompositeIndex";
import T "../../src/EmbeddedInstance/Types";
import DocumentStore "../../src/EmbeddedInstance/Collection/DocumentStore";
import SchemaMap "../../src/EmbeddedInstance/Collection/SchemaMap";
import Logger "../../src/EmbeddedInstance/Logger";
import Ids "../../src/EmbeddedInstance/Ids";
import TwoQueueCache "../../src/EmbeddedInstance/TwoQueueCache";
import Runtime "mo:core@2.4/Runtime";

module {
    type Schema = T.Schema;
    type Candid = T.Candid;

    public func init() : Bench.Bench {
        let fuzz = Fuzz.fromSeed(0xdeadbeef);

        let bench = Bench.Bench();
        bench.name("Benchmarking Insert Flow");
        bench.description("End-to-end insert performance with different configurations");

        bench.cols([
            "insert() - no indexes",
            "insert() - 1 index",
            "insert() - 3 indexes",
            "insert() - 5 indexes",
        ]);

        bench.rows([
            "Heap - Simple doc (5 fields)",
            "Heap - Medium doc (15 fields)",
            "Heap - Complex doc (30 fields)",
            "Stable Memory - Simple doc",
            "Stable Memory - Medium doc",
            "Stable Memory - Complex doc",
        ]);

        let limit = 500; // Reduced for full insert benchmarks

        // Helper to create mock collection
        func makeMockCollection(memory_type : T.MemoryType, schema : Schema) : T.StableCollection {
            let instance_id = Blob.fromArray([0, 0, 0, 1]);

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
                field_constraints = Map.new();
                unique_constraints = [];
                fields_with_unique_constraints = Map.new();
                candid_map_cache = TwoQueueCache.new(100_000);
                freed_btrees = Vector.new<T.MemoryBTree>();
                logger = Logger.init(#Warn, false);
                memory_type;
                is_running_locally = false;
                is_compression_enabled = null;
            };
        };

        // ===== Simple Schema (5 fields) =====
        let simple_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("age", #Nat),
            ("active", #Bool),
            ("score", #Float),
        ]);

        let simple_docs_candid = Array.tabulate<Candid>(
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

        let simple_docs_blob = Array.map<Candid, Blob>(
            simple_docs_candid,
            func(c : Candid) : Blob {
                switch (Candid.encode([c], null)) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
            },
        );

        // ===== Medium Schema (15 fields) =====
        let medium_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("age", #Nat),
            ("email", #Text),
            ("phone", #Text),
            ("address", #Text),
            ("city", #Text),
            ("country", #Text),
            ("zipcode", #Text),
            ("active", #Bool),
            ("score", #Float),
            ("rating", #Nat),
            ("category", #Text),
            ("tags", #Array(#Text)),
            ("metadata", #Text),
        ]);

        let medium_docs_candid = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("name", #Text(fuzz.text.randomAlphanumeric(20))),
                    ("age", #Nat(fuzz.nat.randomRange(18, 80))),
                    ("email", #Text(fuzz.text.randomAlphanumeric(15))),
                    ("phone", #Text(fuzz.text.randomAlphanumeric(10))),
                    ("address", #Text(fuzz.text.randomAlphanumeric(30))),
                    ("city", #Text(fuzz.text.randomAlphanumeric(15))),
                    ("country", #Text(fuzz.text.randomAlphanumeric(10))),
                    ("zipcode", #Text(fuzz.text.randomAlphanumeric(6))),
                    ("active", #Bool(fuzz.bool.random())),
                    ("score", #Float(fuzz.float.randomRange(0.0, 100.0))),
                    ("rating", #Nat(fuzz.nat.randomRange(1, 5))),
                    ("category", #Text(fuzz.text.randomAlphanumeric(10))),
                    ("tags", #Array([#Text("tag1"), #Text("tag2")])),
                    ("metadata", #Text(fuzz.text.randomAlphanumeric(50))),
                ]);
            },
        );

        let medium_docs_blob = Array.map<Candid, Blob>(
            medium_docs_candid,
            func(c : Candid) : Blob {
                switch (Candid.encode([c], null)) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
            },
        );

        // ===== Complex Schema (30 fields) =====
        let complex_schema : Schema = #Record([
            ("f1", #Nat),
            ("f2", #Text),
            ("f3", #Bool),
            ("f4", #Float),
            ("f5", #Nat),
            ("f6", #Text),
            ("f7", #Bool),
            ("f8", #Float),
            ("f9", #Nat),
            ("f10", #Text),
            ("f11", #Bool),
            ("f12", #Float),
            ("f13", #Nat),
            ("f14", #Text),
            ("f15", #Bool),
            ("f16", #Float),
            ("f17", #Nat),
            ("f18", #Text),
            ("f19", #Bool),
            ("f20", #Float),
            ("f21", #Nat),
            ("f22", #Text),
            ("f23", #Bool),
            ("f24", #Float),
            ("f25", #Nat),
            ("f26", #Text),
            ("f27", #Bool),
            ("f28", #Float),
            ("f29", #Nat),
            ("f30", #Text),
        ]);

        let complex_docs_candid = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("f1", #Nat(i)),
                    ("f2", #Text("a")),
                    ("f3", #Bool(true)),
                    ("f4", #Float(1.0)),
                    ("f5", #Nat(i)),
                    ("f6", #Text("b")),
                    ("f7", #Bool(false)),
                    ("f8", #Float(2.0)),
                    ("f9", #Nat(i)),
                    ("f10", #Text("c")),
                    ("f11", #Bool(true)),
                    ("f12", #Float(3.0)),
                    ("f13", #Nat(i)),
                    ("f14", #Text("d")),
                    ("f15", #Bool(false)),
                    ("f16", #Float(4.0)),
                    ("f17", #Nat(i)),
                    ("f18", #Text("e")),
                    ("f19", #Bool(true)),
                    ("f20", #Float(5.0)),
                    ("f21", #Nat(i)),
                    ("f22", #Text("f")),
                    ("f23", #Bool(false)),
                    ("f24", #Float(6.0)),
                    ("f25", #Nat(i)),
                    ("f26", #Text("g")),
                    ("f27", #Bool(true)),
                    ("f28", #Float(7.0)),
                    ("f29", #Nat(i)),
                    ("f30", #Text("h")),
                ]);
            },
        );

        let complex_docs_blob = Array.map<Candid, Blob>(
            complex_docs_candid,
            func(c : Candid) : Blob {
                switch (Candid.encode([c], null)) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
            },
        );

        // ===== Helper to add indexes =====
        func addIndexes(collection : T.StableCollection, count : Nat, schema_type : Text) {
            if (count == 0) return;

            if (schema_type == "simple") {
                // 1 index: age
                let idx1 = CompositeIndex.new(collection, "idx_age", [("age", #Ascending)], false, false);
                ignore Map.put(collection.indexes, Map.thash, "idx_age", #composite_index(idx1));

                if (count >= 3) {
                    // 3 indexes: age, name, score
                    let idx2 = CompositeIndex.new(collection, "idx_name", [("name", #Ascending)], false, false);
                    let idx3 = CompositeIndex.new(collection, "idx_score", [("score", #Ascending)], false, false);
                    ignore Map.put(collection.indexes, Map.thash, "idx_name", #composite_index(idx2));
                    ignore Map.put(collection.indexes, Map.thash, "idx_score", #composite_index(idx3));
                };

                if (count >= 5) {
                    // 5 indexes: age, name, score, id, active
                    let idx4 = CompositeIndex.new(collection, "idx_id", [("id", #Ascending)], false, false);
                    let idx5 = CompositeIndex.new(collection, "idx_active", [("active", #Ascending)], false, false);
                    ignore Map.put(collection.indexes, Map.thash, "idx_id", #composite_index(idx4));
                    ignore Map.put(collection.indexes, Map.thash, "idx_active", #composite_index(idx5));
                };
            } else if (schema_type == "medium") {
                // 1 index: age
                let idx1 = CompositeIndex.new(collection, "idx_age", [("age", #Ascending)], false, false);
                ignore Map.put(collection.indexes, Map.thash, "idx_age", #composite_index(idx1));

                if (count >= 3) {
                    // 3 indexes: age, name, email
                    let idx2 = CompositeIndex.new(collection, "idx_name", [("name", #Ascending)], false, false);
                    let idx3 = CompositeIndex.new(collection, "idx_email", [("email", #Ascending)], false, false);
                    ignore Map.put(collection.indexes, Map.thash, "idx_name", #composite_index(idx2));
                    ignore Map.put(collection.indexes, Map.thash, "idx_email", #composite_index(idx3));
                };

                if (count >= 5) {
                    // 5 indexes: age, name, email, city, category
                    let idx4 = CompositeIndex.new(collection, "idx_city", [("city", #Ascending)], false, false);
                    let idx5 = CompositeIndex.new(collection, "idx_category", [("category", #Ascending)], false, false);
                    ignore Map.put(collection.indexes, Map.thash, "idx_city", #composite_index(idx4));
                    ignore Map.put(collection.indexes, Map.thash, "idx_category", #composite_index(idx5));
                };
            } else if (schema_type == "complex") {
                // 1 index: f1
                let idx1 = CompositeIndex.new(collection, "idx_f1", [("f1", #Ascending)], false, false);
                ignore Map.put(collection.indexes, Map.thash, "idx_f1", #composite_index(idx1));

                if (count >= 3) {
                    // 3 indexes: f1, f5, f10
                    let idx2 = CompositeIndex.new(collection, "idx_f5", [("f5", #Ascending)], false, false);
                    let idx3 = CompositeIndex.new(collection, "idx_f10", [("f10", #Ascending)], false, false);
                    ignore Map.put(collection.indexes, Map.thash, "idx_f5", #composite_index(idx2));
                    ignore Map.put(collection.indexes, Map.thash, "idx_f10", #composite_index(idx3));
                };

                if (count >= 5) {
                    // 5 indexes: f1, f5, f10, f15, f20
                    let idx4 = CompositeIndex.new(collection, "idx_f15", [("f15", #Ascending)], false, false);
                    let idx5 = CompositeIndex.new(collection, "idx_f20", [("f20", #Ascending)], false, false);
                    ignore Map.put(collection.indexes, Map.thash, "idx_f15", #composite_index(idx4));
                    ignore Map.put(collection.indexes, Map.thash, "idx_f20", #composite_index(idx5));
                };
            };
        };

        bench.runner(
            func(row, col) {
                switch (row, col) {
                    // Row 1: Heap - Simple doc
                    case ("Heap - Simple doc (5 fields)", "insert() - no indexes") {
                        let collection = makeMockCollection(#heap, simple_schema);
                        for (doc_blob in simple_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Simple doc (5 fields)", "insert() - 1 index") {
                        let collection = makeMockCollection(#heap, simple_schema);
                        addIndexes(collection, 1, "simple");
                        for (doc_blob in simple_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Simple doc (5 fields)", "insert() - 3 indexes") {
                        let collection = makeMockCollection(#heap, simple_schema);
                        addIndexes(collection, 3, "simple");
                        for (doc_blob in simple_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Simple doc (5 fields)", "insert() - 5 indexes") {
                        let collection = makeMockCollection(#heap, simple_schema);
                        addIndexes(collection, 5, "simple");
                        for (doc_blob in simple_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    // Row 2: Heap - Medium doc
                    case ("Heap - Medium doc (15 fields)", "insert() - no indexes") {
                        let collection = makeMockCollection(#heap, medium_schema);
                        for (doc_blob in medium_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Medium doc (15 fields)", "insert() - 1 index") {
                        let collection = makeMockCollection(#heap, medium_schema);
                        addIndexes(collection, 1, "medium");
                        for (doc_blob in medium_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Medium doc (15 fields)", "insert() - 3 indexes") {
                        let collection = makeMockCollection(#heap, medium_schema);
                        addIndexes(collection, 3, "medium");
                        for (doc_blob in medium_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Medium doc (15 fields)", "insert() - 5 indexes") {
                        let collection = makeMockCollection(#heap, medium_schema);
                        addIndexes(collection, 5, "medium");
                        for (doc_blob in medium_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    // Row 3: Heap - Complex doc
                    case ("Heap - Complex doc (30 fields)", "insert() - no indexes") {
                        let collection = makeMockCollection(#heap, complex_schema);
                        for (doc_blob in complex_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Complex doc (30 fields)", "insert() - 1 index") {
                        let collection = makeMockCollection(#heap, complex_schema);
                        addIndexes(collection, 1, "complex");
                        for (doc_blob in complex_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Complex doc (30 fields)", "insert() - 3 indexes") {
                        let collection = makeMockCollection(#heap, complex_schema);
                        addIndexes(collection, 3, "complex");
                        for (doc_blob in complex_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Heap - Complex doc (30 fields)", "insert() - 5 indexes") {
                        let collection = makeMockCollection(#heap, complex_schema);
                        addIndexes(collection, 5, "complex");
                        for (doc_blob in complex_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    // Row 4: Stable Memory - Simple doc
                    case ("Stable Memory - Simple doc", "insert() - no indexes") {
                        let collection = makeMockCollection(#stableMemory, simple_schema);
                        for (doc_blob in simple_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Simple doc", "insert() - 1 index") {
                        let collection = makeMockCollection(#stableMemory, simple_schema);
                        addIndexes(collection, 1, "simple");
                        for (doc_blob in simple_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Simple doc", "insert() - 3 indexes") {
                        let collection = makeMockCollection(#stableMemory, simple_schema);
                        addIndexes(collection, 3, "simple");
                        for (doc_blob in simple_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Simple doc", "insert() - 5 indexes") {
                        let collection = makeMockCollection(#stableMemory, simple_schema);
                        addIndexes(collection, 5, "simple");
                        for (doc_blob in simple_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    // Row 5: Stable Memory - Medium doc
                    case ("Stable Memory - Medium doc", "insert() - no indexes") {
                        let collection = makeMockCollection(#stableMemory, medium_schema);
                        for (doc_blob in medium_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Medium doc", "insert() - 1 index") {
                        let collection = makeMockCollection(#stableMemory, medium_schema);
                        addIndexes(collection, 1, "medium");
                        for (doc_blob in medium_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Medium doc", "insert() - 3 indexes") {
                        let collection = makeMockCollection(#stableMemory, medium_schema);
                        addIndexes(collection, 3, "medium");
                        for (doc_blob in medium_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Medium doc", "insert() - 5 indexes") {
                        let collection = makeMockCollection(#stableMemory, medium_schema);
                        addIndexes(collection, 5, "medium");
                        for (doc_blob in medium_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    // Row 6: Stable Memory - Complex doc
                    case ("Stable Memory - Complex doc", "insert() - no indexes") {
                        let collection = makeMockCollection(#stableMemory, complex_schema);
                        for (doc_blob in complex_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Complex doc", "insert() - 1 index") {
                        let collection = makeMockCollection(#stableMemory, complex_schema);
                        addIndexes(collection, 1, "complex");
                        for (doc_blob in complex_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Complex doc", "insert() - 3 indexes") {
                        let collection = makeMockCollection(#stableMemory, complex_schema);
                        addIndexes(collection, 3, "complex");
                        for (doc_blob in complex_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case ("Stable Memory - Complex doc", "insert() - 5 indexes") {
                        let collection = makeMockCollection(#stableMemory, complex_schema);
                        addIndexes(collection, 5, "complex");
                        for (doc_blob in complex_docs_blob.vals()) {
                            ignore StableCollection.insert(collection, doc_blob);
                        };
                    };
                    case (_) {};
                };
            }
        );

        bench;
    };
};
