import Debug "mo:core@2.4/Debug";
import Nat "mo:core@2.4/Nat";
import Array "mo:core@2.4/Array";
import Blob "mo:core@2.4/Blob";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde@3.5/Candid";

import T "../../src/EmbeddedInstance/Types";
import Runtime "mo:core@2.4/Runtime";

module {
    type Schema = T.Schema;
    type Candid = T.Candid;

    public func init() : Bench.Bench {
        let fuzz = Fuzz.fromSeed(0xdeadbeef);

        let bench = Bench.Bench();
        bench.name("Benchmarking Candid Encoding/Decoding");
        bench.description("Measuring Candid blob encode/decode performance during insert operations");

        bench.cols([
            "encode()",
            "decode()",
        ]);

        bench.rows([
            "Simple Record (5 fields)",
            "Medium Record (15 fields)",
            "Complex Record (30 fields)",
            "Nested Record (3 levels)",
            "Array Fields (10 items)",
            "Large Record (50 fields)",
        ]);

        let limit = 1_000;

        // ===== Simple Record Schema (5 fields) =====
        let simple_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("age", #Nat),
            ("active", #Bool),
            ("score", #Float),
        ]);

        let simple_serializer = Candid.TypedSerializer.new(
            [simple_schema],
            ?{ Candid.defaultOptions with types = ?[simple_schema] },
        );

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

        // Pre-encode simple docs for decode benchmark
        let simple_blobs = Array.map<Candid, Blob>(
            simple_docs,
            func(doc) : Blob {
                switch (Candid.TypedSerializer.encode(simple_serializer, [doc])) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
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

        let medium_serializer = Candid.TypedSerializer.new(
            [medium_schema],
            ?{ Candid.defaultOptions with types = ?[medium_schema] },
        );

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

        let medium_blobs = Array.map<Candid, Blob>(
            medium_docs,
            func(doc) : Blob {
                switch (Candid.TypedSerializer.encode(medium_serializer, [doc])) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
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

        let complex_serializer = Candid.TypedSerializer.new(
            [complex_schema],
            ?{ Candid.defaultOptions with types = ?[complex_schema] },
        );

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

        let complex_blobs = Array.map<Candid, Blob>(
            complex_docs,
            func(doc) : Blob {
                switch (Candid.TypedSerializer.encode(complex_serializer, [doc])) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
            },
        );

        // ===== Nested Record Schema (3 levels deep) =====
        let nested_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("level1", #Record([("field1", #Text), ("level2", #Record([("field2", #Nat), ("level3", #Record([("field3", #Bool), ("value", #Float)]))]))])),
        ]);

        let nested_serializer = Candid.TypedSerializer.new(
            [nested_schema],
            ?{ Candid.defaultOptions with types = ?[nested_schema] },
        );

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

        let nested_blobs = Array.map<Candid, Blob>(
            nested_docs,
            func(doc) : Blob {
                switch (Candid.TypedSerializer.encode(nested_serializer, [doc])) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
            },
        );

        // ===== Array Fields Schema =====
        let array_schema : Schema = #Record([
            ("id", #Nat),
            ("items", #Array(#Nat)),
            ("tags", #Array(#Text)),
            ("scores", #Array(#Float)),
        ]);

        let array_serializer = Candid.TypedSerializer.new(
            [array_schema],
            ?{ Candid.defaultOptions with types = ?[array_schema] },
        );

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

        let array_blobs = Array.map<Candid, Blob>(
            array_docs,
            func(doc) : Blob {
                switch (Candid.TypedSerializer.encode(array_serializer, [doc])) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
            },
        );

        // ===== Large Record Schema (50 fields) =====
        let large_schema : Schema = #Record([
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
            ("f31", #Nat),
            ("f32", #Text),
            ("f33", #Bool),
            ("f34", #Float),
            ("f35", #Nat),
            ("f36", #Text),
            ("f37", #Bool),
            ("f38", #Float),
            ("f39", #Nat),
            ("f40", #Text),
            ("f41", #Bool),
            ("f42", #Float),
            ("f43", #Nat),
            ("f44", #Text),
            ("f45", #Bool),
            ("f46", #Float),
            ("f47", #Nat),
            ("f48", #Text),
            ("f49", #Bool),
            ("f50", #Float),
        ]);

        let large_serializer = Candid.TypedSerializer.new(
            [large_schema],
            ?{ Candid.defaultOptions with types = ?[large_schema] },
        );

        let large_docs = Array.tabulate<Candid>(
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
                    ("f31", #Nat(i)),
                    ("f32", #Text("i")),
                    ("f33", #Bool(true)),
                    ("f34", #Float(8.0)),
                    ("f35", #Nat(i)),
                    ("f36", #Text("j")),
                    ("f37", #Bool(false)),
                    ("f38", #Float(9.0)),
                    ("f39", #Nat(i)),
                    ("f40", #Text("k")),
                    ("f41", #Bool(true)),
                    ("f42", #Float(10.0)),
                    ("f43", #Nat(i)),
                    ("f44", #Text("l")),
                    ("f45", #Bool(false)),
                    ("f46", #Float(11.0)),
                    ("f47", #Nat(i)),
                    ("f48", #Text("m")),
                    ("f49", #Bool(true)),
                    ("f50", #Float(12.0)),
                ]);
            },
        );

        let large_blobs = Array.map<Candid, Blob>(
            large_docs,
            func(doc) : Blob {
                switch (Candid.TypedSerializer.encode(large_serializer, [doc])) {
                    case (#ok(blob)) blob;
                    case (#err(msg)) Runtime.trap("Failed to encode: " # msg);
                };
            },
        );

        // ===== Single Benchmark Runner =====
        bench.runner(
            func(row, col) {
                switch (row, col) {
                    // Row 1: Simple Record (5 fields)
                    case ("Simple Record (5 fields)", "encode()") {
                        for (doc in simple_docs.vals()) {
                            ignore Candid.TypedSerializer.encode(simple_serializer, [doc]);
                        };
                    };
                    case ("Simple Record (5 fields)", "decode()") {
                        for (blob in simple_blobs.vals()) {
                            ignore Candid.TypedSerializer.decode(simple_serializer, blob);
                        };
                    };

                    // Row 2: Medium Record (15 fields)
                    case ("Medium Record (15 fields)", "encode()") {
                        for (doc in medium_docs.vals()) {
                            ignore Candid.TypedSerializer.encode(medium_serializer, [doc]);
                        };
                    };
                    case ("Medium Record (15 fields)", "decode()") {
                        for (blob in medium_blobs.vals()) {
                            ignore Candid.TypedSerializer.decode(medium_serializer, blob);
                        };
                    };

                    // Row 3: Complex Record (30 fields)
                    case ("Complex Record (30 fields)", "encode()") {
                        for (doc in complex_docs.vals()) {
                            ignore Candid.TypedSerializer.encode(complex_serializer, [doc]);
                        };
                    };
                    case ("Complex Record (30 fields)", "decode()") {
                        for (blob in complex_blobs.vals()) {
                            ignore Candid.TypedSerializer.decode(complex_serializer, blob);
                        };
                    };

                    // Row 4: Nested Record (3 levels)
                    case ("Nested Record (3 levels)", "encode()") {
                        for (doc in nested_docs.vals()) {
                            ignore Candid.TypedSerializer.encode(nested_serializer, [doc]);
                        };
                    };
                    case ("Nested Record (3 levels)", "decode()") {
                        for (blob in nested_blobs.vals()) {
                            ignore Candid.TypedSerializer.decode(nested_serializer, blob);
                        };
                    };

                    // Row 5: Array Fields (10 items)
                    case ("Array Fields (10 items)", "encode()") {
                        for (doc in array_docs.vals()) {
                            ignore Candid.TypedSerializer.encode(array_serializer, [doc]);
                        };
                    };
                    case ("Array Fields (10 items)", "decode()") {
                        for (blob in array_blobs.vals()) {
                            ignore Candid.TypedSerializer.decode(array_serializer, blob);
                        };
                    };

                    // Row 6: Large Record (50 fields)
                    case ("Large Record (50 fields)", "encode()") {
                        for (doc in large_docs.vals()) {
                            ignore Candid.TypedSerializer.encode(large_serializer, [doc]);
                        };
                    };
                    case ("Large Record (50 fields)", "decode()") {
                        for (blob in large_blobs.vals()) {
                            ignore Candid.TypedSerializer.decode(large_serializer, blob);
                        };
                    };

                    case (_) {};
                };
            }
        );

        bench;
    };
};
