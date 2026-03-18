import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Array "mo:base/Array";
import Blob "mo:base/Blob";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Map "mo:map@9.0.1/Map";

import CandidMap "../../src/EmbeddedInstance/CandidMap";
import SchemaMap "../../src/EmbeddedInstance/Collection/SchemaMap";
import T "../../src/EmbeddedInstance/Types";
import ByteUtils "mo:byte-utils@0.1.1";

module {
    type Schema = T.Schema;
    type Candid = T.Candid;

    public func init() : Bench.Bench {
        let fuzz = Fuzz.fromSeed(0xdeadbeef);

        let bench = Bench.Bench();
        bench.name("Benchmarking CandidMap Operations");
        bench.description("Measuring CandidMap performance during document insert operations");

        bench.cols([
            "new()",
            "get() - shallow",
            "get() - nested 2 levels",
            "get() - nested 4 levels",
            "clone()",
            "put() - shallow",
            "put() - nested",
        ]);

        bench.rows([
            "Simple Record (5 fields)",
            "Medium Record (15 fields)",
            "Complex Record (30 fields)",
            "Nested Record (3 levels)",
            "Array Fields (10 items)",
            "Variant Fields",
            "Optional Fields (50% null)",
            "Mixed Types",
        ]);

        let limit = 10_000;

        // Helper to create document ID
        func makeDocId(n : Nat) : Blob {
            Blob.fromArray(ByteUtils.BigEndian.fromNat64(Nat64.fromNat(n)));
        };

        // ===== Simple Record Schema (5 fields) =====
        let simple_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("age", #Nat),
            ("active", #Bool),
            ("score", #Float),
        ]);

        let simple_schema_map = SchemaMap.new(simple_schema);

        let simple_docs = Array.tabulate<Candid>(
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

        // ===== Medium Record Schema (15 fields) =====
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

        let medium_schema_map = SchemaMap.new(medium_schema);

        let medium_docs = Array.tabulate<Candid>(
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

        // ===== Complex Record Schema (30 fields) =====
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

        let complex_schema_map = SchemaMap.new(complex_schema);

        let complex_docs = Array.tabulate<Candid>(
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

        // ===== Nested Record Schema (3 levels deep) =====
        let nested_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("level1", #Record([("field1", #Text), ("level2", #Record([("field2", #Nat), ("level3", #Record([("field3", #Bool), ("value", #Float)]))]))])),
        ]);

        let nested_schema_map = SchemaMap.new(nested_schema);

        let nested_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("name", #Text(fuzz.text.randomAlphanumeric(20))),
                    ("level1", #Record([("field1", #Text(fuzz.text.randomAlphanumeric(10))), ("level2", #Record([("field2", #Nat(fuzz.nat.randomRange(1, 100))), ("level3", #Record([("field3", #Bool(fuzz.bool.random())), ("value", #Float(fuzz.float.randomRange(0.0, 1.0)))]))]))])),
                ]);
            },
        );

        // ===== Array Fields Schema =====
        let array_schema : Schema = #Record([
            ("id", #Nat),
            ("items", #Array(#Nat)),
            ("tags", #Array(#Text)),
            ("scores", #Array(#Float)),
        ]);

        let array_schema_map = SchemaMap.new(array_schema);

        let array_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("items", #Array(Array.tabulate<Candid>(10, func(j : Nat) : Candid = #Nat(j)))),
                    ("tags", #Array(Array.tabulate<Candid>(10, func(j : Nat) : Candid = #Text(Nat.toText(j))))),
                    ("scores", #Array(Array.tabulate<Candid>(10, func(_ : Nat) : Candid = #Float(fuzz.float.randomRange(0.0, 100.0))))),
                ]);
            },
        );

        // ===== Variant Fields Schema =====
        let variant_schema : Schema = #Record([
            ("id", #Nat),
            ("status", #Variant([("active", #Null), ("inactive", #Null), ("pending", #Record([("reason", #Text)]))])),
        ]);

        let variant_schema_map = SchemaMap.new(variant_schema);

        let variant_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                let status = switch (i % 3) {
                    case (0) #Variant(("active", #Null));
                    case (1) #Variant(("inactive", #Null));
                    case (_) #Variant(("pending", #Record([("reason", #Text("processing"))])));
                };
                #Record([
                    ("id", #Nat(i)),
                    ("status", status),
                ]);
            },
        );

        // ===== Optional Fields Schema =====
        let optional_schema : Schema = #Record([
            ("id", #Nat),
            ("opt1", #Option(#Text)),
            ("opt2", #Option(#Nat)),
            ("opt3", #Option(#Bool)),
            ("opt4", #Option(#Float)),
        ]);

        let optional_schema_map = SchemaMap.new(optional_schema);

        let optional_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("opt1", if (i % 2 == 0) #Option(#Text("value")) else #Null),
                    ("opt2", if (i % 2 == 0) #Option(#Nat(42)) else #Null),
                    ("opt3", if (i % 2 == 0) #Option(#Bool(true)) else #Null),
                    ("opt4", if (i % 2 == 0) #Option(#Float(3.14)) else #Null),
                ]);
            },
        );

        // ===== Mixed Types Schema =====
        let mixed_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("nested", #Record([("value", #Nat)])),
            ("array", #Array(#Text)),
            ("optional", #Option(#Bool)),
            ("variant", #Variant([("a", #Null), ("b", #Text)])),
        ]);

        let mixed_schema_map = SchemaMap.new(mixed_schema);

        let mixed_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("name", #Text(fuzz.text.randomAlphanumeric(15))),
                    ("nested", #Record([("value", #Nat(i))])),
                    ("array", #Array([#Text("a"), #Text("b")])),
                    ("optional", if (i % 2 == 0) #Option(#Bool(true)) else #Null),
                    ("variant", if (i % 2 == 0) #Variant(("a", #Null)) else #Variant(("b", #Text("test")))),
                ]);
            },
        );

        // ===== Single Benchmark Runner =====
        bench.runner(
            func(row, col) {
                let doc_id = makeDocId(0);

                switch (row, col) {
                    // Row 1: Simple Record (5 fields)
                    case ("Simple Record (5 fields)", "new()") {
                        for (doc in simple_docs.vals()) {
                            ignore CandidMap.new(simple_schema_map, doc_id, doc);
                        };
                    };
                    case ("Simple Record (5 fields)", "get() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(simple_docs, func(doc) = CandidMap.new(simple_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, simple_schema_map, "name");
                        };
                    };
                    case ("Simple Record (5 fields)", "clone()") {
                        let maps = Array.map<Candid, T.CandidMap>(simple_docs, func(doc) = CandidMap.new(simple_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.clone(cm, simple_schema_map);
                        };
                    };
                    case ("Simple Record (5 fields)", "put() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(simple_docs, func(doc) = CandidMap.new(simple_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, simple_schema_map, "name", #Text("updated"));
                        };
                    };

                    // Row 2: Medium Record (15 fields)
                    case ("Medium Record (15 fields)", "new()") {
                        for (doc in medium_docs.vals()) {
                            ignore CandidMap.new(medium_schema_map, doc_id, doc);
                        };
                    };
                    case ("Medium Record (15 fields)", "get() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(medium_docs, func(doc) = CandidMap.new(medium_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, medium_schema_map, "email");
                        };
                    };
                    case ("Medium Record (15 fields)", "clone()") {
                        let maps = Array.map<Candid, T.CandidMap>(medium_docs, func(doc) = CandidMap.new(medium_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.clone(cm, medium_schema_map);
                        };
                    };
                    case ("Medium Record (15 fields)", "put() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(medium_docs, func(doc) = CandidMap.new(medium_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, medium_schema_map, "email", #Text("new@email.com"));
                        };
                    };

                    // Row 3: Complex Record (30 fields)
                    case ("Complex Record (30 fields)", "new()") {
                        for (doc in complex_docs.vals()) {
                            ignore CandidMap.new(complex_schema_map, doc_id, doc);
                        };
                    };
                    case ("Complex Record (30 fields)", "get() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(complex_docs, func(doc) = CandidMap.new(complex_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, complex_schema_map, "f15");
                        };
                    };
                    case ("Complex Record (30 fields)", "clone()") {
                        let maps = Array.map<Candid, T.CandidMap>(complex_docs, func(doc) = CandidMap.new(complex_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.clone(cm, complex_schema_map);
                        };
                    };
                    case ("Complex Record (30 fields)", "put() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(complex_docs, func(doc) = CandidMap.new(complex_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, complex_schema_map, "f15", #Bool(false));
                        };
                    };

                    // Row 4: Nested Record (3 levels)
                    case ("Nested Record (3 levels)", "new()") {
                        for (doc in nested_docs.vals()) {
                            ignore CandidMap.new(nested_schema_map, doc_id, doc);
                        };
                    };
                    case ("Nested Record (3 levels)", "get() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(nested_docs, func(doc) = CandidMap.new(nested_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, nested_schema_map, "name");
                        };
                    };
                    case ("Nested Record (3 levels)", "get() - nested 2 levels") {
                        let maps = Array.map<Candid, T.CandidMap>(nested_docs, func(doc) = CandidMap.new(nested_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, nested_schema_map, "level1.field1");
                        };
                    };
                    case ("Nested Record (3 levels)", "get() - nested 4 levels") {
                        let maps = Array.map<Candid, T.CandidMap>(nested_docs, func(doc) = CandidMap.new(nested_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, nested_schema_map, "level1.level2.level3.value");
                        };
                    };
                    case ("Nested Record (3 levels)", "clone()") {
                        let maps = Array.map<Candid, T.CandidMap>(nested_docs, func(doc) = CandidMap.new(nested_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.clone(cm, nested_schema_map);
                        };
                    };
                    case ("Nested Record (3 levels)", "put() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(nested_docs, func(doc) = CandidMap.new(nested_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, nested_schema_map, "name", #Text("updated"));
                        };
                    };
                    case ("Nested Record (3 levels)", "put() - nested") {
                        let maps = Array.map<Candid, T.CandidMap>(nested_docs, func(doc) = CandidMap.new(nested_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, nested_schema_map, "level1.level2.field2", #Nat(999));
                        };
                    };

                    // Row 5: Array Fields (10 items)
                    case ("Array Fields (10 items)", "new()") {
                        for (doc in array_docs.vals()) {
                            ignore CandidMap.new(array_schema_map, doc_id, doc);
                        };
                    };
                    case ("Array Fields (10 items)", "get() - shallow") {
                        // Get array elements by index instead of the whole array
                        let maps = Array.map<Candid, T.CandidMap>(array_docs, func(doc) = CandidMap.new(array_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, array_schema_map, "items.0");
                            ignore CandidMap.get(cm, array_schema_map, "items.5");
                        };
                    };
                    case ("Array Fields (10 items)", "clone()") {
                        let maps = Array.map<Candid, T.CandidMap>(array_docs, func(doc) = CandidMap.new(array_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.clone(cm, array_schema_map);
                        };
                    };
                    case ("Array Fields (10 items)", "put() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(array_docs, func(doc) = CandidMap.new(array_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, array_schema_map, "items", #Array([#Nat(1), #Nat(2)]));
                        };
                    };

                    // Row 6: Variant Fields
                    case ("Variant Fields", "new()") {
                        for (doc in variant_docs.vals()) {
                            ignore CandidMap.new(variant_schema_map, doc_id, doc);
                        };
                    };
                    case ("Variant Fields", "get() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(variant_docs, func(doc) = CandidMap.new(variant_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, variant_schema_map, "status");
                        };
                    };
                    case ("Variant Fields", "clone()") {
                        let maps = Array.map<Candid, T.CandidMap>(variant_docs, func(doc) = CandidMap.new(variant_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.clone(cm, variant_schema_map);
                        };
                    };

                    // Row 7: Optional Fields (50% null)
                    case ("Optional Fields (50% null)", "new()") {
                        for (doc in optional_docs.vals()) {
                            ignore CandidMap.new(optional_schema_map, doc_id, doc);
                        };
                    };
                    case ("Optional Fields (50% null)", "get() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(optional_docs, func(doc) = CandidMap.new(optional_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, optional_schema_map, "opt1");
                        };
                    };
                    case ("Optional Fields (50% null)", "clone()") {
                        let maps = Array.map<Candid, T.CandidMap>(optional_docs, func(doc) = CandidMap.new(optional_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.clone(cm, optional_schema_map);
                        };
                    };
                    case ("Optional Fields (50% null)", "put() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(optional_docs, func(doc) = CandidMap.new(optional_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, optional_schema_map, "opt1", #Option(#Text("new")));
                        };
                    };

                    // Row 8: Mixed Types
                    case ("Mixed Types", "new()") {
                        for (doc in mixed_docs.vals()) {
                            ignore CandidMap.new(mixed_schema_map, doc_id, doc);
                        };
                    };
                    case ("Mixed Types", "get() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(mixed_docs, func(doc) = CandidMap.new(mixed_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, mixed_schema_map, "name");
                        };
                    };
                    case ("Mixed Types", "get() - nested 2 levels") {
                        let maps = Array.map<Candid, T.CandidMap>(mixed_docs, func(doc) = CandidMap.new(mixed_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.get(cm, mixed_schema_map, "nested.value");
                        };
                    };
                    case ("Mixed Types", "clone()") {
                        let maps = Array.map<Candid, T.CandidMap>(mixed_docs, func(doc) = CandidMap.new(mixed_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.clone(cm, mixed_schema_map);
                        };
                    };
                    case ("Mixed Types", "put() - shallow") {
                        let maps = Array.map<Candid, T.CandidMap>(mixed_docs, func(doc) = CandidMap.new(mixed_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, mixed_schema_map, "name", #Text("updated"));
                        };
                    };
                    case ("Mixed Types", "put() - nested") {
                        let maps = Array.map<Candid, T.CandidMap>(mixed_docs, func(doc) = CandidMap.new(mixed_schema_map, doc_id, doc));
                        for (cm in maps.vals()) {
                            ignore CandidMap.set(cm, mixed_schema_map, "nested.value", #Nat(999));
                        };
                    };

                    case (_) {};
                };
            }
        );

        bench;
    };
};
