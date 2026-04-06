import Debug "mo:core@2.4/Debug";
import Nat "mo:core@2.4/Nat";
import Array "mo:core@2.4/Array";

import Bench "mo:bench";
import Fuzz "mo:fuzz";

import Schema "../../src/EmbeddedInstance/Collection/Schema";
import T "../../src/EmbeddedInstance/Types";

module {
    type Schema = T.Schema;
    type Candid = T.Candid;

    public func init() : Bench.Bench {
        let fuzz = Fuzz.fromSeed(0xdeadbeef);

        let bench = Bench.Bench();
        bench.name("Benchmarking Schema Validation");
        bench.description("Measuring schema validation performance during insert operations");

        bench.cols([
            "validate() - valid",
            "validate() - invalid",
        ]);

        bench.rows([
            "Simple Record (5 fields)",
            "Medium Record (15 fields)",
            "Complex Record (30 fields)",
            "Nested Record (3 levels)",
            "Array Fields",
            "Variant Fields",
            "Optional Fields",
            "Mixed Types Record",
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

        let simple_valid_docs = Array.tabulate<Candid>(
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

        let simple_invalid_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("name", #Text(fuzz.text.randomAlphanumeric(20))),
                    ("age", #Text("invalid")), // Wrong type
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

        let medium_valid_docs = Array.tabulate<Candid>(
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

        let medium_invalid_docs = Array.tabulate<Candid>(
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
                    ("zipcode", #Nat(12345)), // Wrong type
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

        let complex_valid_docs = Array.tabulate<Candid>(
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

        let complex_invalid_docs = Array.tabulate<Candid>(
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
                    ("f15", #Nat(99)), // Wrong type
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

        // ===== Nested Record Schema =====
        let nested_schema : Schema = #Record([
            ("id", #Nat),
            ("name", #Text),
            ("level1", #Record([("field1", #Text), ("level2", #Record([("field2", #Nat), ("level3", #Record([("field3", #Bool), ("value", #Float)]))]))])),
        ]);

        let nested_valid_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("name", #Text(fuzz.text.randomAlphanumeric(20))),
                    ("level1", #Record([("field1", #Text(fuzz.text.randomAlphanumeric(10))), ("level2", #Record([("field2", #Nat(fuzz.nat.randomRange(1, 100))), ("level3", #Record([("field3", #Bool(fuzz.bool.random())), ("value", #Float(fuzz.float.randomRange(0.0, 1.0)))]))]))])),
                ]);
            },
        );

        let nested_invalid_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("name", #Text(fuzz.text.randomAlphanumeric(20))),
                    ("level1", #Record([("field1", #Text(fuzz.text.randomAlphanumeric(10))), ("level2", #Record([("field2", #Nat(fuzz.nat.randomRange(1, 100))), ("level3", #Record([("field3", #Text("invalid")), /* Wrong type */
                    ("value", #Float(fuzz.float.randomRange(0.0, 1.0)))]))]))])),
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

        let array_valid_docs = Array.tabulate<Candid>(
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

        let array_invalid_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("items", #Array(Array.tabulate<Candid>(10, func(j : Nat) : Candid = #Nat(j)))),
                    ("tags", #Array(Array.tabulate<Candid>(10, func(j : Nat) : Candid = #Nat(j)))), // Wrong element type
                    ("scores", #Array(Array.tabulate<Candid>(10, func(_ : Nat) : Candid = #Float(fuzz.float.randomRange(0.0, 100.0))))),
                ]);
            },
        );

        // ===== Variant Fields Schema =====
        let variant_schema : Schema = #Record([
            ("id", #Nat),
            ("status", #Variant([("active", #Null), ("inactive", #Null), ("pending", #Record([("reason", #Text)]))])),
        ]);

        let variant_valid_docs = Array.tabulate<Candid>(
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

        let variant_invalid_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("status", #Variant(("unknown", #Null))), // Invalid variant tag
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

        let optional_valid_docs = Array.tabulate<Candid>(
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

        let optional_invalid_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("opt1", if (i % 2 == 0) #Option(#Nat(123)) else #Null), // Wrong inner type
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

        let mixed_valid_docs = Array.tabulate<Candid>(
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

        let mixed_invalid_docs = Array.tabulate<Candid>(
            limit,
            func(i : Nat) : Candid {
                #Record([
                    ("id", #Nat(i)),
                    ("name", #Text(fuzz.text.randomAlphanumeric(15))),
                    ("nested", #Record([("value", #Text("wrong"))])), // Wrong nested type
                    ("array", #Array([#Text("a"), #Text("b")])),
                    ("optional", if (i % 2 == 0) #Option(#Bool(true)) else #Null),
                    ("variant", if (i % 2 == 0) #Variant(("a", #Null)) else #Variant(("b", #Text("test")))),
                ]);
            },
        );

        bench.runner(
            func(row, col) {
                switch (row, col) {
                    // Row 1: Simple Record
                    case ("Simple Record (5 fields)", "validate() - valid") {
                        for (doc in simple_valid_docs.vals()) {
                            ignore Schema.validate(simple_schema, doc);
                        };
                    };
                    case ("Simple Record (5 fields)", "validate() - invalid") {
                        for (doc in simple_invalid_docs.vals()) {
                            ignore Schema.validate(simple_schema, doc);
                        };
                    };
                    // Row 2: Medium Record
                    case ("Medium Record (15 fields)", "validate() - valid") {
                        for (doc in medium_valid_docs.vals()) {
                            ignore Schema.validate(medium_schema, doc);
                        };
                    };
                    case ("Medium Record (15 fields)", "validate() - invalid") {
                        for (doc in medium_invalid_docs.vals()) {
                            ignore Schema.validate(medium_schema, doc);
                        };
                    };
                    // Row 3: Complex Record
                    case ("Complex Record (30 fields)", "validate() - valid") {
                        for (doc in complex_valid_docs.vals()) {
                            ignore Schema.validate(complex_schema, doc);
                        };
                    };
                    case ("Complex Record (30 fields)", "validate() - invalid") {
                        for (doc in complex_invalid_docs.vals()) {
                            ignore Schema.validate(complex_schema, doc);
                        };
                    };
                    // Row 4: Nested Record
                    case ("Nested Record (3 levels)", "validate() - valid") {
                        for (doc in nested_valid_docs.vals()) {
                            ignore Schema.validate(nested_schema, doc);
                        };
                    };
                    case ("Nested Record (3 levels)", "validate() - invalid") {
                        for (doc in nested_invalid_docs.vals()) {
                            ignore Schema.validate(nested_schema, doc);
                        };
                    };
                    // Row 5: Array Fields
                    case ("Array Fields", "validate() - valid") {
                        for (doc in array_valid_docs.vals()) {
                            ignore Schema.validate(array_schema, doc);
                        };
                    };
                    case ("Array Fields", "validate() - invalid") {
                        for (doc in array_invalid_docs.vals()) {
                            ignore Schema.validate(array_schema, doc);
                        };
                    };
                    // Row 6: Variant Fields
                    case ("Variant Fields", "validate() - valid") {
                        for (doc in variant_valid_docs.vals()) {
                            ignore Schema.validate(variant_schema, doc);
                        };
                    };
                    case ("Variant Fields", "validate() - invalid") {
                        for (doc in variant_invalid_docs.vals()) {
                            ignore Schema.validate(variant_schema, doc);
                        };
                    };
                    // Row 7: Optional Fields
                    case ("Optional Fields", "validate() - valid") {
                        for (doc in optional_valid_docs.vals()) {
                            ignore Schema.validate(optional_schema, doc);
                        };
                    };
                    case ("Optional Fields", "validate() - invalid") {
                        for (doc in optional_invalid_docs.vals()) {
                            ignore Schema.validate(optional_schema, doc);
                        };
                    };
                    // Row 8: Mixed Types
                    case ("Mixed Types Record", "validate() - valid") {
                        for (doc in mixed_valid_docs.vals()) {
                            ignore Schema.validate(mixed_schema, doc);
                        };
                    };
                    case ("Mixed Types Record", "validate() - invalid") {
                        for (doc in mixed_invalid_docs.vals()) {
                            ignore Schema.validate(mixed_schema, doc);
                        };
                    };
                    case (_) {};
                };
            }
        );

        bench;
    };
};
