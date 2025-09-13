// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Blob "mo:base@0.16.0/Blob";
import Text "mo:base@0.16.0/Text";
import Order "mo:base@0.16.0/Order";
import Nat8 "mo:base@0.16.0/Nat8";
import Nat16 "mo:base@0.16.0/Nat16";
import Nat32 "mo:base@0.16.0/Nat32";
import Nat64 "mo:base@0.16.0/Nat64";
import Int8 "mo:base@0.16.0/Int8";
import Int16 "mo:base@0.16.0/Int16";
import Int32 "mo:base@0.16.0/Int32";
import Int64 "mo:base@0.16.0/Int64";
import Int "mo:base@0.16.0/Int";
import Nat "mo:base@0.16.0/Nat";
import Float "mo:base@0.16.0/Float";
import Principal "mo:base@0.16.0/Principal";
import Iter "mo:base@0.16.0/Iter";
import Bool "mo:base@0.16.0/Bool";
import Array "mo:base@0.16.0/Array";
import Char "mo:base@0.16.0/Char";
import Result "mo:base@0.16.0/Result";

import ZenDB "../../src";
import Index "../../src/Collection/Index";

import { test; suite } "mo:test";
import Itertools "mo:itertools@0.2.2/Iter";
import Fuzz "mo:fuzz";
import Map "mo:map@9.0.1/Map";
import ZenDBSuite "../test-utils/TestFramework";

let fuzz = Fuzz.fromSeed(0x7eadbeef);
let { QueryBuilder } = ZenDB;

let limit = 10;

ZenDBSuite.newSuite(
    "ZenDB Index Tests",
    ?{
        ZenDBSuite.onlyWithIndex with log_level = #Error;
    },
    func index_tests(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {

        type SupportedIndexTypes = {
            text : Text;
            nat : Nat;
            nat8 : Nat8;
            nat16 : Nat16;
            nat32 : Nat32;
            nat64 : Nat64;
            int : Int;
            int8 : Int8;
            int16 : Int16;
            int32 : Int32;
            int64 : Int64;
            float : Float;
            principal : Principal;
            blob : Blob;
            bool : Bool;
        };

        let SupportedIndexTypes : ZenDB.Types.Schema = #Record([
            ("text", #Text),
            ("nat", #Nat),
            ("nat8", #Nat8),
            ("nat16", #Nat16),
            ("nat32", #Nat32),
            ("nat64", #Nat64),
            ("int", #Int),
            ("int8", #Int8),
            ("int16", #Int16),
            ("int32", #Int32),
            ("int64", #Int64),
            ("float", #Float),
            ("principal", #Principal),
            ("blob", #Blob),
            ("bool", #Bool),
        ]);

        let candify_data = {
            to_blob = func(data : SupportedIndexTypes) : Blob {
                to_candid (data);
            };
            from_blob = func(blob : Blob) : ?SupportedIndexTypes {
                from_candid (blob);
            };
        };

        let #ok(sorted_index_types) = zendb.createCollection("sorted_index_types", SupportedIndexTypes, candify_data, null) else return assert false;
        let inputs = Map.new<Nat, SupportedIndexTypes>();

        func get_field_and_sort_document_ids<A>(getter : (document : SupportedIndexTypes) -> A, cmp : (A, A) -> Order.Order) : ((Nat, Nat) -> Order.Order) {
            func(id1 : Nat, id2 : Nat) : Order.Order {
                let ?r1 = Map.get(inputs, Map.nhash, id1);
                let ?r2 = Map.get(inputs, Map.nhash, id2);

                let v1 = getter(r1);
                let v2 = getter(r2);

                switch (cmp(v1, v2)) {
                    case (#equal) Nat.compare(id1, id2);
                    case (rest) rest;
                };
            };
        };

        for (i in Itertools.range(0, limit)) {

            let document = {
                text = fuzz.text.randomAlphanumeric(
                    fuzz.nat.randomRange(0, 100)
                );
                nat = fuzz.nat.randomRange(0, (2 ** 64) - 1);
                nat8 = fuzz.nat8.random();
                nat16 = fuzz.nat16.random();
                nat32 = fuzz.nat32.random();
                nat64 = fuzz.nat64.random();
                int = fuzz.int.randomRange(-(2 ** 63), (2 ** 63) - 1);
                int8 = fuzz.int8.random();
                int16 = fuzz.int16.random();
                int32 = fuzz.int32.random();
                int64 = fuzz.int64.random();
                float = fuzz.float.random();
                principal = fuzz.principal.randomPrincipal(
                    fuzz.nat.randomRange(0, 28)
                );
                blob = fuzz.blob.randomBlob(
                    fuzz.nat.randomRange(0, 100)
                );
                bool = fuzz.bool.random();
            };

            // Debug.print("Inserting document: " # debug_show document);

            let #ok(id) = sorted_index_types.insert(document) else return assert false;

            ignore Map.put(inputs, Map.nhash, id, document);

        };

        func get_candid_text(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Text(document.text);
        };
        func get_candid_nat(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Nat(document.nat);
        };
        func get_candid_bool(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Bool(document.bool);
        };
        func get_candid_blob(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Blob(document.blob);
        };
        func get_candid_principal(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Principal(document.principal);
        };
        func get_candid_float(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Float(document.float);
        };
        func get_candid_int(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Int(document.int);
        };
        func get_candid_int8(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Int8(document.int8);
        };
        func get_candid_int16(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Int16(document.int16);
        };
        func get_candid_int32(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Int32(document.int32);
        };
        func get_candid_int64(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Int64(document.int64);
        };
        func get_candid_nat8(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Nat8(document.nat8);
        };
        func get_candid_nat16(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Nat16(document.nat16);
        };
        func get_candid_nat32(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Nat32(document.nat32);
        };
        func get_candid_nat64(document : SupportedIndexTypes, field : Text) : ZenDB.Types.Candid {
            return #Nat64(document.nat64);
        };

        suite(
            "Serialized values are orderd correctly",
            func() {

                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "text", [("text", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "nat", [("nat", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "nat8", [("nat8", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "nat16", [("nat16", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "nat32", [("nat32", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "nat64", [("nat64", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "int", [("int", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "int8", [("int8", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "int16", [("int16", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "int32", [("int32", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "int64", [("int64", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "float", [("float", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "principal", [("principal", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "blob", [("blob", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(sorted_index_types.name(), "bool", [("bool", #Ascending)], null) else return assert false;

                let sorted_texts = Buffer.Buffer<Nat>(inputs.size());
                let sorted_nats = Buffer.Buffer<Nat>(inputs.size());
                let sorted_nat8s = Buffer.Buffer<Nat>(inputs.size());
                let sorted_nat16s = Buffer.Buffer<Nat>(inputs.size());
                let sorted_nat32s = Buffer.Buffer<Nat>(inputs.size());
                let sorted_nat64s = Buffer.Buffer<Nat>(inputs.size());
                let sorted_ints = Buffer.Buffer<Nat>(inputs.size());
                let sorted_int8s = Buffer.Buffer<Nat>(inputs.size());
                let sorted_int16s = Buffer.Buffer<Nat>(inputs.size());
                let sorted_int32s = Buffer.Buffer<Nat>(inputs.size());
                let sorted_int64s = Buffer.Buffer<Nat>(inputs.size());
                let sorted_floats = Buffer.Buffer<Nat>(inputs.size());
                let sorted_principals = Buffer.Buffer<Nat>(inputs.size());
                let sorted_blobs = Buffer.Buffer<Nat>(inputs.size());
                let sorted_bools = Buffer.Buffer<Nat>(inputs.size());

                for (id in Map.keys(inputs)) {
                    sorted_texts.add(id);
                    sorted_nats.add(id);
                    sorted_nat8s.add(id);
                    sorted_nat16s.add(id);
                    sorted_nat32s.add(id);
                    sorted_nat64s.add(id);
                    sorted_ints.add(id);
                    sorted_int8s.add(id);
                    sorted_int16s.add(id);
                    sorted_int32s.add(id);
                    sorted_int64s.add(id);
                    sorted_floats.add(id);
                    sorted_principals.add(id);
                    sorted_blobs.add(id);
                    sorted_bools.add(id);
                };

                sorted_texts.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Text = document.text, Text.compare));
                sorted_nats.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Nat = document.nat, Nat.compare));
                sorted_nat8s.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Nat8 = document.nat8, Nat8.compare));
                sorted_nat16s.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Nat16 = document.nat16, Nat16.compare));
                sorted_nat32s.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Nat32 = document.nat32, Nat32.compare));
                sorted_nat64s.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Nat64 = document.nat64, Nat64.compare));
                sorted_ints.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Int = document.int, Int.compare));
                sorted_int8s.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Int8 = document.int8, Int8.compare));
                sorted_int16s.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Int16 = document.int16, Int16.compare));
                sorted_int32s.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Int32 = document.int32, Int32.compare));
                sorted_int64s.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Int64 = document.int64, Int64.compare));
                sorted_floats.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Float = document.float, Float.compare));
                sorted_principals.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Principal = document.principal, Principal.compare));
                sorted_blobs.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Blob = document.blob, Blob.compare));
                sorted_bools.sort(get_field_and_sort_document_ids(func(document : SupportedIndexTypes) : Bool = document.bool, Bool.compare));

                func verify_sorted_data(
                    field : Text,
                    sorted_ids : Buffer.Buffer<Nat>,
                ) : Bool {
                    let #ok(results) = sorted_index_types.search(
                        ZenDB.QueryBuilder().Sort(field, #Ascending)
                    );

                    return sorted_ids.size() == limit and Itertools.all(
                        Itertools.zip(
                            Iter.map(
                                results.vals(),
                                func((id, _) : (Nat, SupportedIndexTypes)) : Nat {
                                    return id;
                                },
                            ),
                            sorted_ids.vals(),
                        ),
                        func((id1, id2) : (Nat, Nat)) : Bool { id1 == id2 },
                    );

                };

                test("Sorts correctly for field text", func() { assert verify_sorted_data("text", sorted_texts) });
                test("Sorts correctly for field nat", func() { assert verify_sorted_data("nat", sorted_nats) });
                test("Sorts correctly for field nat8", func() { assert verify_sorted_data("nat8", sorted_nat8s) });
                test("Sorts correctly for field nat16", func() { assert verify_sorted_data("nat16", sorted_nat16s) });
                test("Sorts correctly for field nat32", func() { assert verify_sorted_data("nat32", sorted_nat32s) });
                test("Sorts correctly for field nat64", func() { assert verify_sorted_data("nat64", sorted_nat64s) });
                test("Sorts correctly for field int", func() { assert verify_sorted_data("int", sorted_ints) });
                test("Sorts correctly for field int8", func() { assert verify_sorted_data("int8", sorted_int8s) });
                test("Sorts correctly for field int16", func() { assert verify_sorted_data("int16", sorted_int16s) });
                test("Sorts correctly for field int32", func() { assert verify_sorted_data("int32", sorted_int32s) });
                test("Sorts correctly for field int64", func() { assert verify_sorted_data("int64", sorted_int64s) });
                test("Sorts correctly for field float", func() { assert verify_sorted_data("float", sorted_floats) });
                test("Sorts correctly for field principal", func() { assert verify_sorted_data("principal", sorted_principals) });
                test("Sorts correctly for field blob", func() { assert verify_sorted_data("blob", sorted_blobs) });
                test("Sorts correctly for field bool", func() { assert verify_sorted_data("bool", sorted_bools) });

                func verify_entries<A>(
                    field : Text,
                    sorted_ids : Buffer.Buffer<Nat>,
                    getter : (document : SupportedIndexTypes, field : Text) -> ZenDB.Types.Candid,
                ) : Bool {

                    // Debug.print("collection size: " # debug_show sorted_index_types.size());

                    for ((id, r) in Map.entries(inputs)) {
                        // Debug.print("got " # debug_show getter(r, field) # " for field " # field # " with id " # debug_show id);
                        // Debug.print("Verifying entry for id " # debug_show id # " with document " # debug_show r # " and field " # debug_show field);

                        let #ok(results) = sorted_index_types.search(
                            QueryBuilder().Where(field, #eq(getter(r, field))).Limit(1)
                        );

                        // Debug.print("Looking for id " # debug_show id # " with document " # debug_show r);

                        // assert Itertools.any(
                        //     results.vals(),
                        //     func((search_id, document) : (Nat, SupportedIndexTypes)) : Bool {
                        //         return search_id == id;
                        //     },
                        // );
                    };

                    return true;

                };

                test("Correctly retrieves entries for field nat", func() { assert verify_entries("nat", sorted_nats, get_candid_nat) });
                test("Correctly retrieves entries for field bool", func() { assert verify_entries("bool", sorted_bools, get_candid_bool) });
                test("Correctly retrieves entries for field blob", func() { assert verify_entries("blob", sorted_blobs, get_candid_blob) });
                test("Correctly retrieves entries for field principal", func() { assert verify_entries("principal", sorted_principals, get_candid_principal) });
                test("Correctly retrieves entries for field int", func() { assert verify_entries("int", sorted_ints, get_candid_int) });
                test("Correctly retrieves entries for field int8", func() { assert verify_entries("int8", sorted_int8s, get_candid_int8) });
                test("Correctly retrieves entries for field int16", func() { assert verify_entries("int16", sorted_int16s, get_candid_int16) });
                test("Correctly retrieves entries for field int32", func() { assert verify_entries("int32", sorted_int32s, get_candid_int32) });
                test("Correctly retrieves entries for field int64", func() { assert verify_entries("int64", sorted_int64s, get_candid_int64) });
                test("Correctly retrieves entries for field nat64", func() { assert verify_entries("nat64", sorted_nat64s, get_candid_nat64) });
                test("Correctly retrieves entries for field float", func() { assert verify_entries("float", sorted_floats, get_candid_float) });
                test("Correctly retrieves entries for field nat8", func() { assert verify_entries("nat8", sorted_nat8s, get_candid_nat8) });
                test("Correctly retrieves entries for field nat16", func() { assert verify_entries("nat16", sorted_nat16s, get_candid_nat16) });
                test("Correctly retrieves entries for field nat32", func() { assert verify_entries("nat32", sorted_nat32s, get_candid_nat32) });
                test("Correctly retrieves entries for field nat64", func() { assert verify_entries("nat64", sorted_nat64s, get_candid_nat64) });
                test("Correctly retrieves entries for field text", func() { assert verify_entries("text", sorted_texts, get_candid_text) });

            },
        );

        // suite(
        //     "Compound Indexes",
        //     func() {
        //         let types = [
        //             "text",
        //             "nat",
        //             "nat8",
        //             "nat16",
        //             "nat32",
        //             "nat64",
        //             "int",
        //             "int8",
        //             "int16",
        //             "int32",
        //             "int64",
        //             "principal",
        //             "blob",
        //             // "bool",
        //         ];

        //         let compound_types = Buffer.Buffer<(Text, Text)>(types.size() ** 2);

        //         for (type_a in types.vals()) {
        //             for (type_b in types.vals()) {
        //                 compound_types.add((type_a, type_b));
        //                 let #ok(_) = sorted_index_types.createIndex(
        //                     type_a # "_" # type_b,
        //                     [(type_a, #Ascending), (type_b, #Ascending)],
        //                     null,
        //                 );
        //             };
        //         };

        //         func verify_compound_entries(
        //             field : Text,
        //             getter_a : (document : SupportedIndexTypes, field : Text) -> ZenDB.Types.Candid,
        //             field_b : Text,
        //             getter_b : (document : SupportedIndexTypes, field : Text) -> ZenDB.Types.Candid,
        //         ) : Bool {

        //             for (id in Itertools.range(0, limit)) {
        //                 let ?r = Map.get(inputs, Map.nhash, id);
        //                 let #ok(results) = sorted_index_types.search(
        //                     QueryBuilder().Where(
        //                         field,
        //                         #eq(getter_a(r, field)),
        //                     ).And(
        //                         field_b,
        //                         #eq(getter_b(r, field_b)),
        //                     )
        //                 );

        //                 Debug.print(
        //                     "Searching for id " # debug_show id # " with document " # debug_show r # " and fields " # debug_show field # " and " # debug_show field_b
        //                 );

        //                 assert Itertools.any(
        //                     results.vals(),
        //                     func((search_id, document) : (Nat, SupportedIndexTypes)) : Bool {
        //                         return search_id == id;
        //                     },
        //                 );
        //             };

        //             return true;

        //         };

        //         for ((type_a, type_b) in compound_types.vals()) {
        //             let getter_a = switch (type_a) {
        //                 case ("text") get_candid_text;
        //                 case ("nat") get_candid_nat;
        //                 case ("nat8") get_candid_nat8;
        //                 case ("nat16") get_candid_nat16;
        //                 case ("nat32") get_candid_nat32;
        //                 case ("nat64") get_candid_nat64;
        //                 case ("int") get_candid_int;
        //                 case ("int8") get_candid_int8;
        //                 case ("int16") get_candid_int16;
        //                 case ("int32") get_candid_int32;
        //                 case ("int64") get_candid_int64;
        //                 case ("float") get_candid_float;
        //                 case ("principal") get_candid_principal;
        //                 case ("blob") get_candid_blob;
        //                 case ("bool") get_candid_bool;
        //                 case (_) Debug.trap("Unsupported type");
        //             };

        //             let getter_b = switch (type_b) {
        //                 case ("text") get_candid_text;
        //                 case ("nat") get_candid_nat;
        //                 case ("nat8") get_candid_nat8;
        //                 case ("nat16") get_candid_nat16;
        //                 case ("nat32") get_candid_nat32;
        //                 case ("nat64") get_candid_nat64;
        //                 case ("int") get_candid_int;
        //                 case ("int8") get_candid_int8;
        //                 case ("int16") get_candid_int16;
        //                 case ("int32") get_candid_int32;
        //                 case ("int64") get_candid_int64;
        //                 case ("float") get_candid_float;
        //                 case ("principal") get_candid_principal;
        //                 case ("blob") get_candid_blob;
        //                 case ("bool") get_candid_bool;
        //                 case (_) Debug.trap("Unsupported type");
        //             };

        //             test(
        //                 "Correctly retrieves entries for field " # type_a # " and field " # type_b,
        //                 func() {
        //                     assert verify_compound_entries(type_a, getter_a, type_b, getter_b);
        //                 },
        //             );
        //         };
        //     },
        // );

        suite(
            "Schema Constraints",
            func() {

                suite(
                    "#Min and #Max value constraints",
                    func() {

                        type TestRecord = {
                            nat : Nat;
                            int : Int;
                            float : Float;
                        };

                        let TestSchema : ZenDB.Types.Schema = #Record([
                            ("nat", #Nat),
                            ("int", #Int),
                            ("float", #Float),
                        ]);

                        let candify_test = {
                            to_blob = func(data : TestRecord) : Blob {
                                to_candid (data);
                            };
                            from_blob = func(blob : Blob) : ?TestRecord {
                                from_candid (blob);
                            };
                        };

                        let schemaConstraints : [ZenDB.Types.SchemaConstraint] = [
                            #Field("nat", [#Min(1_000), #Max(32_000)]),
                            #Field("int", [#Min(-10), #Max(10)]),
                            #Field("float", [#Min(-1.0), #Max(1.0)]),
                        ];

                        let #ok(test_collection) = zendb.createCollection("schema_constraints_test", TestSchema, candify_test, ?{ schemaConstraints }) else return assert false;

                        let valid_values : TestRecord = {
                            nat = 10_000;
                            int = 5;
                            float = 0.5;
                        };

                        test(
                            "Succeeds with valid values",
                            func() {
                                assert Result.isOk(test_collection.insert(valid_values));
                            },
                        );

                        test(
                            "Fails with invalid values",
                            func() {
                                let invalid_values : TestRecord = {
                                    valid_values with nat = 50_000;
                                };

                                assert Result.isErr(test_collection.insert(invalid_values));

                                let invalid_values_2 : TestRecord = {
                                    valid_values with nat = 999;
                                };

                                assert Result.isErr(test_collection.insert(invalid_values_2));

                                let invalid_values_3 : TestRecord = {
                                    valid_values with int = 20;
                                };

                                assert Result.isErr(test_collection.insert(invalid_values_3));

                                let invalid_values_4 : TestRecord = {
                                    valid_values with int = -20;
                                };

                                assert Result.isErr(test_collection.insert(invalid_values_4));

                                let invalid_values_5 : TestRecord = {
                                    valid_values with float = 2.0;
                                };

                                assert Result.isErr(test_collection.insert(invalid_values_5));

                                let invalid_values_6 : TestRecord = {
                                    valid_values with float = -2.0;
                                };

                                assert Result.isErr(test_collection.insert(invalid_values_6));

                            },
                        );

                    },
                );

                suite(
                    "#MinSize, #MaxSize and #Size constraints",
                    func() {

                        type TestRecord = {
                            text : Text;
                            blob : Blob;
                        };

                        let TestSchema : ZenDB.Types.Schema = #Record([
                            ("text", #Text),
                            ("blob", #Blob),
                        ]);

                        let candify_test = {
                            to_blob = func(data : TestRecord) : Blob {
                                to_candid (data);
                            };
                            from_blob = func(blob : Blob) : ?TestRecord {
                                from_candid (blob);
                            };
                        };

                        let schemaConstraints : [ZenDB.Types.SchemaConstraint] = [
                            #Field("text", [#MinSize(5), #MaxSize(10)]),
                            #Field("blob", [#Size(3, 5)]),
                        ];

                        let #ok(test_collection) = zendb.createCollection("schema_constraints_test_2", TestSchema, candify_test, ?{ schemaConstraints }) else return assert false;

                        let valid_values : TestRecord = {
                            text = "hello";
                            blob = Blob.fromArray([0, 1, 2, 3, 4]);
                        };

                        test(
                            "Succeeds with valid values",
                            func() {
                                assert Result.isOk(test_collection.insert(valid_values));
                            },
                        );

                        test(
                            "Fails with invalid values",
                            func() {
                                let invalid_values : TestRecord = {
                                    valid_values with text = "hi";
                                };

                                assert Result.isErr(test_collection.insert(invalid_values));

                                let invalid_values_2 : TestRecord = {
                                    valid_values with text = "hello world";
                                };

                                assert Result.isErr(test_collection.insert(invalid_values_2));

                                let invalid_values_3 : TestRecord = {
                                    valid_values with blob = Blob.fromArray([0, 1, 2, 3, 4, 5]);
                                };

                                assert Result.isErr(test_collection.insert(invalid_values_3));

                                let invalid_values_4 : TestRecord = {
                                    valid_values with blob = Blob.fromArray([0]);
                                };

                            },
                        );
                    }

                );

                suite(
                    "#Unique constraints",
                    func() {
                        type TestRecord = {
                            text : Text;
                            nat : Nat;
                            compound : (Text, Nat);
                        };

                        let TestSchema : ZenDB.Types.Schema = #Record([
                            ("text", #Text),
                            ("nat", #Nat),
                            ("compound", #Tuple([#Text, #Nat])),
                        ]);

                        let candify_test = {
                            to_blob = func(data : TestRecord) : Blob {
                                to_candid (data);
                            };
                            from_blob = func(blob : Blob) : ?TestRecord {
                                from_candid (blob);
                            };
                        };

                        let schemaConstraints : [ZenDB.Types.SchemaConstraint] = [
                            #Unique(["text"]),
                            #Unique(["nat"]),
                            #Unique(["compound.0", "compound.1"]),
                        ];

                        let #ok(test_collection) = zendb.createCollection("schema_constraints_test_3", TestSchema, candify_test, ?{ schemaConstraints }) else return assert false;

                        test(
                            "Succeeds with unique values",
                            func() {
                                assert Result.isOk(test_collection.insert({ text = "a"; nat = 1; compound = ("a", 1) }));
                                assert Result.isOk(test_collection.insert({ text = "b"; nat = 2; compound = ("b", 2) }));
                                assert Result.isOk(test_collection.insert({ text = "z"; nat = 10; compound = ("c", 3) }));
                                assert Result.isOk(test_collection.insert({ text = "zz"; nat = 100; compound = ("z", 10) }));

                            },
                        );

                        test(
                            "Fails with duplicate values",
                            func() {
                                assert Result.isErr(test_collection.insert({ text = "a"; nat = 3; compound = ("d", 4) })); // text is duplicate
                                assert Result.isErr(test_collection.insert({ text = "bbb"; nat = 2; compound = ("e", 6) })); // nat is duplicate
                                assert Result.isErr(test_collection.insert({ text = "c"; nat = 4; compound = ("a", 1) })); // compound is duplicate

                                assert Result.isErr(test_collection.insert({ text = "a"; nat = 2; compound = ("b", 2) })); // all duplicated

                            },
                        );
                    },
                )

            },
        );

        suite(
            "Unique Indexes",
            func() {

                type TestRecord = {
                    opt_nat : ?Nat;
                };

                let OptNatSchema : ZenDB.Types.Schema = #Record([
                    ("opt_nat", #Option(#Nat)),
                ]);

                let candify_test : ZenDB.Types.Candify<TestRecord> = {
                    to_blob = func(data : TestRecord) : Blob {
                        to_candid (data);
                    };
                    from_blob = func(blob : Blob) : ?TestRecord {
                        from_candid (blob);
                    };
                };

                let #ok(test) = zendb.createCollection("unique_index_test", OptNatSchema, candify_test, null) else return assert false;

                let #ok(_) = suite_utils.createIndex(test.name(), "opt_nat_idx", [("opt_nat", #Ascending)], ?{ isUnique = true }) else return assert false;

                let #ok(id1) = test.insert({ opt_nat = ?1 }) else return assert false;
                let #ok(id2) = test.insert({ opt_nat = ?2 }) else return assert false;
                let #ok(id3) = test.insert({ opt_nat = null }) else return assert false;
                let #ok(id4) = test.insert({ opt_nat = null }) else return assert false; // should succeed

                assert test.search(
                    QueryBuilder().Where("opt_nat", #eq(#Null))
                ) == #ok([(id3, { opt_nat = null }), (id4, { opt_nat = null })]);

                assert test.size() == 4;

                let #err(_) = test.insert({ opt_nat = ?1 }); // should fail

                assert test.size() == 4;

                assert test.search(
                    QueryBuilder().Where("opt_nat", #not_(#eq(#Null)))
                ) == #ok([(id1, { opt_nat = ?1 }), (id2, { opt_nat = ?2 })]);

            },
        );

        suite(
            "composite indexes",
            func() {
                type CompositeRecord = {
                    first : Nat;
                    second : Text;
                    third : Blob;
                };

                let CompositeSchema : ZenDB.Types.Schema = #Record([
                    ("first", #Nat),
                    ("second", #Text),
                    ("third", #Blob),
                ]);

                let candify_composite = {
                    to_blob = func(data : CompositeRecord) : Blob {
                        to_candid (data);
                    };
                    from_blob = func(blob : Blob) : ?CompositeRecord {
                        from_candid (blob);
                    };
                };

                let #ok(composite_collection) = zendb.createCollection("composite_test", CompositeSchema, candify_composite, null) else return assert false;
                let #ok(_) = suite_utils.createIndex(
                    composite_collection.name(),
                    "composite",
                    [
                        ("first", #Ascending),
                        ("second", #Ascending),
                        ("third", #Ascending),
                    ],
                    null,
                );

                // Insert test data with specific patterns to test composite index behavior
                let documents = [
                    { first = 10; second = "a"; third = Blob.fromArray([0x01]) },
                    { first = 10; second = "a"; third = Blob.fromArray([0x02]) },
                    { first = 10; second = "b"; third = Blob.fromArray([0x01]) },
                    { first = 20; second = "a"; third = Blob.fromArray([0x01]) },
                ];

                var document_ids = Buffer.Buffer<Nat>(4);

                for (document in documents.vals()) {
                    let #ok(id) = composite_collection.insert(document) else return assert false;
                    document_ids.add(id);
                };

                // Test partial key queries
                test(
                    "Partial key queries return all matching documents",
                    func() {
                        let #ok(results) = composite_collection.search(
                            QueryBuilder().Where("first", #eq(#Nat(10)))
                        );

                        assert results.size() == 3;
                        for ((_, document) in results.vals()) {
                            assert document.first == 10;
                        };
                    },
                );

                // Test exclusive range on composite keys
                test(
                    "Exclusive range on composite keys excludes boundary",
                    func() {
                        // This tests #gt [10, "a"]
                        let #ok(results) = composite_collection.search(
                            QueryBuilder().Where("first", #eq(#Nat(10))).Where("second", #gt(#Text("a")))
                        );

                        assert results.size() == 1;
                        assert results[0].1.first == 10 and results[0].1.second == "b";
                    },
                );
            },
        );

        suite(
            "boundary value tests",
            func() {
                type BoundaryRecord = {
                    nat_val : Nat;
                    text_val : Text;
                    blob_val : Blob;
                };

                let BoundarySchema : ZenDB.Types.Schema = #Record([
                    ("nat_val", #Nat),
                    ("text_val", #Text),
                    ("blob_val", #Blob),
                ]);

                let candify_boundary = {
                    to_blob = func(data : BoundaryRecord) : Blob {
                        to_candid (data);
                    };
                    from_blob = func(blob : Blob) : ?BoundaryRecord {
                        from_candid (blob);
                    };
                };

                let #ok(boundary_collection) = zendb.createCollection("boundary_test", BoundarySchema, candify_boundary, null) else return assert false;
                let #ok(_) = suite_utils.createIndex(boundary_collection.name(), "nat_val", [("nat_val", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(boundary_collection.name(), "text_val", [("text_val", #Ascending)], null) else return assert false;
                let #ok(_) = suite_utils.createIndex(boundary_collection.name(), "blob_val", [("blob_val", #Ascending)], null) else return assert false;

                // Insert consecutive values to test boundary behavior
                let #ok(id1) = boundary_collection.insert({
                    nat_val = 0;
                    text_val = "";
                    blob_val = Blob.fromArray([]);
                }) else return assert false;

                let #ok(id2) = boundary_collection.insert({
                    nat_val = 1;
                    text_val = "\00";
                    blob_val = Blob.fromArray([0]);
                }) else return assert false;

                // Test consecutive value boundaries
                test(
                    "Nat consecutive values respect exclusive range",
                    func() {
                        let #ok(results) = boundary_collection.search(
                            QueryBuilder().Where("nat_val", #gt(#Nat 0))
                        );

                        assert results.size() == 1;
                        assert results[0].1.nat_val == 1;
                    },
                );

                test(
                    "Text empty string and null byte respect exclusive range",
                    func() {
                        let #ok(results) = boundary_collection.search(
                            QueryBuilder().Where("text_val", #gt(#Text("")))
                        );

                        assert results.size() == 1;
                        assert results[0].1.text_val == "\00";
                    },
                );

                test(
                    "Blob empty and single byte respect exclusive range",
                    func() {
                        let #ok(results) = boundary_collection.search(
                            QueryBuilder().Where("blob_val", #gt(#Blob(Blob.fromArray([]))))
                        );

                        assert results.size() == 1;
                        assert Blob.equal(results[0].1.blob_val, Blob.fromArray([0]));
                    },
                );

                // Test embedded nulls in blobs
                let #ok(id3) = boundary_collection.insert({
                    nat_val = 3;
                    text_val = "test";
                    blob_val = Blob.fromArray([0, 0]);
                });
                let #ok(id4) = boundary_collection.insert({
                    nat_val = 4;
                    text_val = "test";
                    blob_val = Blob.fromArray([0, 1]);
                });

                test(
                    "Blobs with embedded nulls respect ordering and ranges",
                    func() {
                        let #ok(results) = boundary_collection.search(
                            QueryBuilder().Where("blob_val", #gt(#Blob(Blob.fromArray([0, 0]))))
                        );

                        assert results.size() == 1;
                        assert Blob.equal(results[0].1.blob_val, Blob.fromArray([0, 1]));
                    },
                );
            },
        );

    },

);
