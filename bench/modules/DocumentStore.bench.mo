import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Nat64 "mo:base@0.16.0/Nat64";
import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde@3.4.0/Candid";
import Vector "mo:vector@0.4.2";

import DocumentStore "../../src/EmbeddedInstance/Collection/DocumentStore";
import T "../../src/EmbeddedInstance/Types";
import BTree "../../src/EmbeddedInstance/BTree";
import ByteUtils "mo:byte-utils@0.1.1";
import Ids "../../src/EmbeddedInstance/Ids";

module {
    type Schema = T.Schema;

    public func init() : Bench.Bench {
        let fuzz = Fuzz.fromSeed(0xdeadbeef);

        let bench = Bench.Bench();
        bench.name("Benchmarking DocumentStore Operations");
        bench.description("Measuring DocumentStore (BTree) performance during insert operations");

        bench.cols([
            "put() - heap",
            "get() - heap",
            "remove() - heap",
            "put() - stable memory",
            "get() - stable memory",
            "remove() - stable memory",
        ]);

        bench.rows([
            "Small docs (100 bytes)",
            "Medium docs (500 bytes)",
            "Large docs (2KB)",
            "Very Large docs (10KB)",
            "Sequential inserts",
            "Random inserts",
        ]);

        let limit = 1_000;

        // Helper to create document ID
        func makeDocId(n : Nat) : Blob {
            let instance_id = Blob.fromArray([0, 0, 0, 1]);
            let id_bytes = ByteUtils.BigEndian.fromNat64(Nat64.fromNat(n));
            Blob.fromArray(Array.append(Blob.toArray(instance_id), id_bytes));
        };

        // Helper to create candid blob of specific size
        func makeCandidBlob(size : Nat, fuzz : Fuzz.Fuzzer) : Blob {
            let text = fuzz.text.randomAlphanumeric(size);
            let candid : Candid.Candid = #Record([
                ("data", #Text(text)),
            ]);
            switch (Candid.encode([candid], null)) {
                case (#ok(blob)) blob;
                case (#err(msg)) Debug.trap("Failed to encode: " # msg);
            };
        };

        // Helper collection type
        type MockCollection = {
            documents : T.BTree<T.DocumentId, T.Document>;
        };

        func makeHeapCollection() : MockCollection {
            {
                documents = DocumentStore.new_heap();
            };
        };

        func makeStableMemoryCollection() : MockCollection {
            {
                documents = DocumentStore.new_stable_memory();
            };
        };

        // Generate test documents
        let small_docs = Array.tabulate<Blob>(limit, func(_ : Nat) : Blob = makeCandidBlob(100, fuzz));
        let medium_docs = Array.tabulate<Blob>(limit, func(_ : Nat) : Blob = makeCandidBlob(500, fuzz));
        let large_docs = Array.tabulate<Blob>(limit, func(_ : Nat) : Blob = makeCandidBlob(2000, fuzz));
        let very_large_docs = Array.tabulate<Blob>(limit, func(_ : Nat) : Blob = makeCandidBlob(10000, fuzz));

        // Pre-generate sequential IDs
        let sequential_ids = Array.tabulate<Blob>(limit, func(i : Nat) : Blob = makeDocId(i));

        // Pre-generate random IDs
        let random_ids = Array.tabulate<Blob>(
            limit,
            func(_ : Nat) : Blob = makeDocId(fuzz.nat.randomRange(0, limit * 10)),
        );

        bench.runner(
            func(row, col) {
                switch (row, col) {
                    // Row 1: Small docs (100 bytes)
                    case ("Small docs (100 bytes)", "put() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                    };
                    case ("Small docs (100 bytes)", "get() - heap") {
                        let collection = makeHeapCollection();
                        // Pre-populate
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                        // Benchmark get
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Small docs (100 bytes)", "remove() - heap") {
                        let collection = makeHeapCollection();
                        // Pre-populate
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                        // Benchmark remove
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.remove(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Small docs (100 bytes)", "put() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                    };
                    case ("Small docs (100 bytes)", "get() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        // Pre-populate
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                        // Benchmark get
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    case ("Small docs (100 bytes)", "remove() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        // Pre-populate
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                        // Benchmark remove
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.remove(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    // Row 2: Medium docs (500 bytes)
                    case ("Medium docs (500 bytes)", "put() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(medium_docs[i]));
                        };
                    };
                    case ("Medium docs (500 bytes)", "get() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(medium_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Medium docs (500 bytes)", "remove() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(medium_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.remove(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Medium docs (500 bytes)", "put() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(medium_docs[i]));
                        };
                    };
                    case ("Medium docs (500 bytes)", "get() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(medium_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    case ("Medium docs (500 bytes)", "remove() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(medium_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.remove(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    // Row 3: Large docs (2KB)
                    case ("Large docs (2KB)", "put() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(large_docs[i]));
                        };
                    };
                    case ("Large docs (2KB)", "get() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(large_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Large docs (2KB)", "remove() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(large_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.remove(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Large docs (2KB)", "put() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(large_docs[i]));
                        };
                    };
                    case ("Large docs (2KB)", "get() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(large_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    case ("Large docs (2KB)", "remove() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(large_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.remove(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    // Row 4: Very Large docs (10KB)
                    case ("Very Large docs (10KB)", "put() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(very_large_docs[i]));
                        };
                    };
                    case ("Very Large docs (10KB)", "get() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(very_large_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Very Large docs (10KB)", "remove() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(very_large_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.remove(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Very Large docs (10KB)", "put() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(very_large_docs[i]));
                        };
                    };
                    case ("Very Large docs (10KB)", "get() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(very_large_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    case ("Very Large docs (10KB)", "remove() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(very_large_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.remove(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    // Row 5: Sequential inserts
                    case ("Sequential inserts", "put() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                    };
                    case ("Sequential inserts", "get() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.HeapUtils, sequential_ids[i]);
                        };
                    };
                    case ("Sequential inserts", "put() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                    };
                    case ("Sequential inserts", "get() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i], #v0(small_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.StableMemoryUtils, sequential_ids[i]);
                        };
                    };
                    // Row 6: Random inserts
                    case ("Random inserts", "put() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, random_ids[i], #v0(small_docs[i]));
                        };
                    };
                    case ("Random inserts", "get() - heap") {
                        let collection = makeHeapCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.HeapUtils, random_ids[i], #v0(small_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.HeapUtils, random_ids[i]);
                        };
                    };
                    case ("Random inserts", "put() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, random_ids[i], #v0(small_docs[i]));
                        };
                    };
                    case ("Random inserts", "get() - stable memory") {
                        let collection = makeStableMemoryCollection();
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.put(collection.documents, DocumentStore.StableMemoryUtils, random_ids[i], #v0(small_docs[i]));
                        };
                        for (i in Iter.range(0, limit - 1)) {
                            ignore BTree.get(collection.documents, DocumentStore.StableMemoryUtils, random_ids[i]);
                        };
                    };
                    case (_) {};
                };
            }
        );

        bench;
    };
};
