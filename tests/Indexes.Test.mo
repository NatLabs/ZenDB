// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Order "mo:base/Order";
import Nat8 "mo:base/Nat8";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Int8 "mo:base/Int8";
import Int16 "mo:base/Int16";
import Int32 "mo:base/Int32";
import Int64 "mo:base/Int64";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Float "mo:base/Float";
import Principal "mo:base/Principal";
import Iter "mo:base/Iter";
import Bool "mo:base/Bool";
import Array "mo:base/Array";
import Char "mo:base/Char";
import Result "mo:base/Result";

import ZenDB "../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Fuzz "mo:fuzz";
import Map "mo:map/Map";

let fuzz = Fuzz.fromSeed(0x7eadbeef);
let { QueryBuilder } = ZenDB;

let limit = 10_000;

func index_tests(zendb : ZenDB.Database) {

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

    let #ok(sorted_index_types) = zendb.create_collection("sorted_index_types", SupportedIndexTypes, candify_data, []);
    let inputs = Map.new<Nat, SupportedIndexTypes>();

    func get_field_and_sort_record_ids<A>(getter : (record : SupportedIndexTypes) -> A, cmp : (A, A) -> Order.Order) : ((Nat, Nat) -> Order.Order) {
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

        let record = {
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

        let #ok(id) = sorted_index_types.insert(record);

        ignore Map.put(inputs, Map.nhash, id, record);

    };

    suite(
        "Serialized values are orderd correctly",
        func() {

            let #ok(_) = sorted_index_types.create_index("text", [("text", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("nat", [("nat", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("nat8", [("nat8", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("nat16", [("nat16", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("nat32", [("nat32", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("nat64", [("nat64", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("int", [("int", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("int8", [("int8", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("int16", [("int16", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("int32", [("int32", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("int64", [("int64", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("float", [("float", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("principal", [("principal", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("blob", [("blob", #Ascending)], false);
            let #ok(_) = sorted_index_types.create_index("bool", [("bool", #Ascending)], false);

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

            sorted_texts.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Text = record.text, Text.compare));
            sorted_nats.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Nat = record.nat, Nat.compare));
            sorted_nat8s.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Nat8 = record.nat8, Nat8.compare));
            sorted_nat16s.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Nat16 = record.nat16, Nat16.compare));
            sorted_nat32s.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Nat32 = record.nat32, Nat32.compare));
            sorted_nat64s.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Nat64 = record.nat64, Nat64.compare));
            sorted_ints.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Int = record.int, Int.compare));
            sorted_int8s.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Int8 = record.int8, Int8.compare));
            sorted_int16s.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Int16 = record.int16, Int16.compare));
            sorted_int32s.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Int32 = record.int32, Int32.compare));
            sorted_int64s.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Int64 = record.int64, Int64.compare));
            sorted_floats.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Float = record.float, Float.compare));
            sorted_principals.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Principal = record.principal, Principal.compare));
            sorted_blobs.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Blob = record.blob, Blob.compare));
            sorted_bools.sort(get_field_and_sort_record_ids(func(record : SupportedIndexTypes) : Bool = record.bool, Bool.compare));

            func verify_sorted_data(
                field : Text,
                sorted_ids : Buffer.Buffer<Nat>,
            ) : Bool {
                let #ok(results) = sorted_index_types.search(
                    ZenDB.QueryBuilder().Sort(field, #Ascending)
                );

                // Debug.print(
                //     "Sorted results for field " # field # ": " # debug_show (results)
                // );

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
            // test("Sorts correctly for field int", func() { assert verify_sorted_data("int", sorted_ints) });
            test("Sorts correctly for field int8", func() { assert verify_sorted_data("int8", sorted_int8s) });
            test("Sorts correctly for field int16", func() { assert verify_sorted_data("int16", sorted_int16s) });
            test("Sorts correctly for field int32", func() { assert verify_sorted_data("int32", sorted_int32s) });
            test("Sorts correctly for field int64", func() { assert verify_sorted_data("int64", sorted_int64s) });
            // test("Sorts correctly for field float", func() { assert verify_sorted_data("float", sorted_floats) });
            test("Sorts correctly for field principal", func() { assert verify_sorted_data("principal", sorted_principals) });
            test("Sorts correctly for field blob", func() { assert verify_sorted_data("blob", sorted_blobs) });
            test("Sorts correctly for field bool", func() { assert verify_sorted_data("bool", sorted_bools) });

        },
    );

    suite(
        "edge cases",
        func() {

            type User = {
                name : Text;
                id : Nat;
            };

            let UserSchema : ZenDB.Types.Schema = #Record([
                ("name", #Text),
                ("id", #Nat),
            ]);

            let candify_user = {
                to_blob = func(user : User) : Blob {
                    to_candid (user);
                };
                from_blob = func(blob : Blob) : ?User {
                    from_candid (blob);
                };
            };

            let #ok(users) = zendb.create_collection("users", UserSchema, candify_user, []);
            let #ok(_) = users.create_index("name_id", [("name", #Ascending), ("id", #Ascending)], false);
            let user_0 = { name = "a"; id = 0 };

            let text_1 = Text.fromIter(
                [
                    Char.fromNat32(0xFE),
                    Char.fromNat32(0x00),
                    Char.fromNat32(0xEE),
                ].vals()
            );

            let text_2 = Text.fromIter(
                [
                    Char.fromNat32(0xFE),
                    Char.fromNat32(0x00),
                    Char.fromNat32(0xEE),
                    Char.fromNat32(0x00),
                ].vals()
            );

            let user_1 = { name = text_1; id = 2 };
            let user_2 = { name = text_2; id = 0 };

            let #ok(id1) = users.insert(user_1);
            let #ok(id2) = users.insert(user_2);

            let #ok(results) = users.search(
                QueryBuilder().Sort("name", #Ascending)
            );

            Debug.print(
                "Results: " # debug_show (results)
            );

            Debug.print(
                "Sorted: " # debug_show Array.sort([user_1.name, user_2.name], Text.compare)
            );

            Debug.print(
                "Sorted text: " # debug_show (Text.encodeUtf8(text_1), Text.encodeUtf8(text_2))
            );

            assert results == [(id1, user_1), (id2, user_2)];

        },
    );

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

                    let schema_constraints : [ZenDB.Types.SchemaConstraint] = [
                        #Field("nat", [#Min(1_000), #Max(32_000)]),
                        #Field("int", [#Min(-10), #Max(10)]),
                        #Field("float", [#Min(-1.0), #Max(1.0)]),
                    ];

                    let #ok(test_collection) = zendb.create_collection("schema_constraints_test", TestSchema, candify_test, schema_constraints);

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
                        // array : [Nat];
                    };

                    let TestSchema : ZenDB.Types.Schema = #Record([
                        ("text", #Text),
                        ("blob", #Blob),
                        // ("array", #Array(#Nat)),
                    ]);

                    let candify_test = {
                        to_blob = func(data : TestRecord) : Blob {
                            to_candid (data);
                        };
                        from_blob = func(blob : Blob) : ?TestRecord {
                            from_candid (blob);
                        };
                    };

                    let schema_constraints : [ZenDB.Types.SchemaConstraint] = [
                        #Field("text", [#MinSize(5), #MaxSize(10)]),
                        #Field("blob", [#Size(3, 5)]),
                        // #Field("array", [#MaxSize(5)]),
                    ];

                    let #ok(test_collection) = zendb.create_collection("schema_constraints_test_2", TestSchema, candify_test, schema_constraints);

                    let valid_values : TestRecord = {
                        text = "hello";
                        blob = Blob.fromArray([0, 1, 2, 3, 4]);
                        // array = [1, 2, 3];
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

                            // let invalid_values_5 : TestRecord = {
                            //     valid_values with array = [1, 2, 3, 4, 5, 6];
                            // };

                            // assert Result.isErr(test_collection.insert(invalid_values_5));

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

                    let schema_constraints : [ZenDB.Types.SchemaConstraint] = [
                        #Unique(["text"]),
                        #Unique(["nat"]),
                        #Unique(["compound.0", "compound.1"]),
                    ];

                    let #ok(test_collection) = zendb.create_collection("schema_constraints_test_3", TestSchema, candify_test, schema_constraints);

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

            let #ok(test) = zendb.create_collection("unique_index_test", OptNatSchema, candify_test, []);

            let #ok(_) = test.create_index("opt_nat_idx", [("opt_nat", #Ascending)], true);

            let #ok(_) = test.insert({ opt_nat = ?1 });
            let #ok(_) = test.insert({ opt_nat = ?2 });
            let #ok(_) = test.insert({ opt_nat = null });
            let #ok(_) = test.insert({ opt_nat = null });

        },
    );

    // suite(
    //     "exclusive range queries",
    //     func() {
    //         type TestRecord = {
    //             text_val : Text;
    //             nat_val : Nat;
    //             int_val : Int;
    //             blob_val : Blob;
    //         };

    //         let TestSchema : ZenDB.Types.Schema = #Record([
    //             ("text_val", #Text),
    //             ("nat_val", #Nat),
    //             ("int_val", #Int),
    //             ("blob_val", #Blob),
    //         ]);

    //         let candify_test = {
    //             to_blob = func(data : TestRecord) : Blob {
    //                 to_candid (data);
    //             };
    //             from_blob = func(blob : Blob) : ?TestRecord {
    //                 from_candid (blob)
    //             };
    //         };

    //         let #ok(test_collection) = zendb.create_collection("range_test", TestSchema, candify_test);
    //         let #ok(_) = test_collection.create_index("text_val", [("text_val", #Ascending)]);
    //         let #ok(_) = test_collection.create_index("nat_val", [("nat_val", #Ascending)]);
    //         let #ok(_) = test_collection.create_index("int_val", [("int_val", #Ascending)]);
    //         let #ok(_) = test_collection.create_index("blob_val", [("blob_val", #Ascending)]);

    //         // Insert test records with carefully selected values
    //         let records = [
    //             {
    //                 text_val = "a";
    //                 nat_val = 0;
    //                 int_val = -10;
    //                 blob_val = Blob.fromArray([0]);
    //             },
    //             {
    //                 text_val = "aa";
    //                 nat_val = 1;
    //                 int_val = -5;
    //                 blob_val = Blob.fromArray([0, 0]);
    //             },
    //             {
    //                 text_val = "ab";
    //                 nat_val = 5;
    //                 int_val = 0;
    //                 blob_val = Blob.fromArray([0, 1]);
    //             },
    //             {
    //                 text_val = "b";
    //                 nat_val = 10;
    //                 int_val = 5;
    //                 blob_val = Blob.fromArray([1]);
    //             },
    //             {
    //                 text_val = "ba";
    //                 nat_val = 100;
    //                 int_val = 10;
    //                 blob_val = Blob.fromArray([1, 0]);
    //             },
    //         ];

    //         var record_ids = Buffer.Buffer<Nat>(5);

    //         for (record in records.vals()) {
    //             let #ok(id) = test_collection.insert(record);
    //             record_ids.add(id);
    //         };

    //         // Text exclusive range tests
    //         test(
    //             "Text #gt query excludes the boundary value",
    //             func() {
    //                 let #ok(results) = test_collection.search(
    //                     QueryBuilder().Where("text_val", #gt("a"))
    //                 );

    //                 assert results.size() == 4;
    //                 for ((_, record) in results.vals()) {
    //                     assert record.text_val != "a";
    //                 };
    //             },
    //         );

    //         test(
    //             "Text #lt query excludes the boundary value",
    //             func() {
    //                 let #ok(results) = test_collection.search(
    //                     QueryBuilder().Where("text_val", #lt("b"))
    //                 );

    //                 assert results.size() == 3;
    //                 for ((_, record) in results.vals()) {
    //                     assert record.text_val != "b";
    //                 };
    //             },
    //         );

    //         // Nat exclusive range tests
    //         test(
    //             "Nat #gt query excludes the boundary value",
    //             func() {
    //                 let #ok(results) = test_collection.search(
    //                     QueryBuilder().Where("nat_val", #gt(5))
    //                 );

    //                 assert results.size() == 2;
    //                 for ((_, record) in results.vals()) {
    //                     assert record.nat_val > 5;
    //                 };
    //             },
    //         );

    //         test(
    //             "Nat #lt query excludes the boundary value",
    //             func() {
    //                 let #ok(results) = test_collection.search(
    //                     QueryBuilder().Where("nat_val", #lt(5))
    //                 );

    //                 assert results.size() == 2;
    //                 for ((_, record) in results.vals()) {
    //                     assert record.nat_val < 5;
    //                 };
    //             },
    //         );

    //         // Int exclusive range tests
    //         test(
    //             "Int #gt query excludes the boundary value",
    //             func() {
    //                 let #ok(results) = test_collection.search(
    //                     QueryBuilder().Where("int_val", #gt(0))
    //                 );

    //                 assert results.size() == 2;
    //                 for ((_, record) in results.vals()) {
    //                     assert record.int_val > 0;
    //                 };
    //             },
    //         );

    //         test(
    //             "Int #lt query excludes the boundary value",
    //             func() {
    //                 let #ok(results) = test_collection.search(
    //                     QueryBuilder().Where("int_val", #lt(0))
    //                 );

    //                 assert results.size() == 2;
    //                 for ((_, record) in results.vals()) {
    //                     assert record.int_val < 0;
    //                 };
    //             },
    //         );

    //         // Blob exclusive range tests
    //         test(
    //             "Blob #gt query excludes the boundary value",
    //             func() {
    //                 let #ok(results) = test_collection.search(
    //                     QueryBuilder().Where("blob_val", #gt(Blob.fromArray([0, 0])))
    //                 );

    //                 assert results.size() == 3;
    //                 for ((_, record) in results.vals()) {
    //                     assert Blob.equal(record.blob_val, Blob.fromArray([0, 0])) == false;
    //                 };
    //             },
    //         );

    //         test(
    //             "Blob #lt query excludes the boundary value",
    //             func() {
    //                 let #ok(results) = test_collection.search(
    //                     QueryBuilder().Where("blob_val", #lt(Blob.fromArray([1])))
    //                 );

    //                 assert results.size() == 3;
    //                 for ((_, record) in results.vals()) {
    //                     assert Blob.equal(record.blob_val, Blob.fromArray([1])) == false;
    //                 };
    //             },
    //         );
    //     },
    // );

    // suite(
    //     "composite indexes",
    //     func() {
    //         type CompositeRecord = {
    //             first : Nat;
    //             second : Text;
    //             third : Blob;
    //         };

    //         let CompositeSchema : ZenDB.Types.Schema = #Record([
    //             ("first", #Nat),
    //             ("second", #Text),
    //             ("third", #Blob),
    //         ]);

    //         let candify_composite = {
    //             to_blob = func(data : CompositeRecord) : Blob {
    //                 to_candid (data);
    //             };
    //             from_blob = func(blob : Blob) : ?CompositeRecord {
    //                  from_candid (blob)
    //             };
    //         };

    //         let #ok(composite_collection) = zendb.create_collection("composite_test", CompositeSchema, candify_composite);
    //         let #ok(_) = composite_collection.create_index(
    //             "composite",
    //             [
    //                 ("first", #Ascending),
    //                 ("second", #Ascending),
    //                 ("third", #Ascending),
    //             ],
    //         );

    //         // Insert test data with specific patterns to test composite index behavior
    //         let records = [
    //             { first = 10; second = "a"; third = Blob.fromArray([0x01]) },
    //             { first = 10; second = "a"; third = Blob.fromArray([0x02]) },
    //             { first = 10; second = "b"; third = Blob.fromArray([0x01]) },
    //             { first = 20; second = "a"; third = Blob.fromArray([0x01]) },
    //         ];

    //         var record_ids = Buffer.Buffer<Nat>(4);

    //         for (record in records.vals()) {
    //             let #ok(id) = composite_collection.insert(record);
    //             record_ids.add(id);
    //         };

    //         // Test lexicographical ordering of composite index
    //         test(
    //             "Composite index maintains lexicographical ordering",
    //             func() {
    //                 let #ok(results) = composite_collection.search(
    //                     QueryBuilder().Sort("first", #Ascending).Sort("second", #Ascending).Sort("third", #Ascending)
    //                 );

    //                 assert results.size() == 4;

    //                 // Verify the order
    //                 assert results[0].1.first == 10 and results[0].1.second == "a" and Blob.equal(results[0].1.third, Blob.fromArray([0x01]));
    //                 assert results[1].1.first == 10 and results[1].1.second == "a" and Blob.equal(results[1].1.third, Blob.fromArray([0x02]));
    //                 assert results[2].1.first == 10 and results[2].1.second == "b" and Blob.equal(results[2].1.third, Blob.fromArray([0x01]));
    //                 assert results[3].1.first == 20 and results[3].1.second == "a" and Blob.equal(results[3].1.third, Blob.fromArray([0x01]));
    //             },
    //         );

    //         // Test partial key queries
    //         test(
    //             "Partial key queries return all matching records",
    //             func() {
    //                 let #ok(results) = composite_collection.search(
    //                     QueryBuilder().Where("first", #eq(10))
    //                 );

    //                 assert results.size() == 3;
    //                 for ((_, record) in results.vals()) {
    //                     assert record.first == 10;
    //                 };
    //             },
    //         );

    //         // Test exclusive range on composite keys
    //         test(
    //             "Exclusive range on composite keys excludes boundary",
    //             func() {
    //                 // This tests #gt [10, "a"]
    //                 let #ok(results) = composite_collection.search(
    //                     QueryBuilder().Where("first", #eq(10)).Where("second", #gt("a"))
    //                 );

    //                 assert results.size() == 1;
    //                 assert results[0].1.first == 10 and results[0].1.second == "b";
    //             },
    //         );
    //     },
    // );

    // suite(
    //     "boundary value tests",
    //     func() {
    //         type BoundaryRecord = {
    //             nat_val : Nat;
    //             text_val : Text;
    //             blob_val : Blob;
    //         };

    //         let BoundarySchema : ZenDB.Types.Schema = #Record([
    //             ("nat_val", #Nat),
    //             ("text_val", #Text),
    //             ("blob_val", #Blob),
    //         ]);

    //         let candify_boundary = {
    //             to_blob = func(data : BoundaryRecord) : Blob {
    //                 to_candid (data);
    //             };
    //             from_blob = func(blob : Blob) : ?BoundaryRecord {
    //                  from_candid (blob)
    //             };
    //         };

    //         let #ok(boundary_collection) = zendb.create_collection("boundary_test", BoundarySchema, candify_boundary);
    //         let #ok(_) = boundary_collection.create_index("nat_val", [("nat_val", #Ascending)]);
    //         let #ok(_) = boundary_collection.create_index("text_val", [("text_val", #Ascending)]);
    //         let #ok(_) = boundary_collection.create_index("blob_val", [("blob_val", #Ascending)]);

    //         // Insert consecutive values to test boundary behavior
    //         let #ok(id1) = boundary_collection.insert({
    //             nat_val = 0;
    //             text_val = "";
    //             blob_val = Blob.fromArray([]);
    //         });
    //         let #ok(id2) = boundary_collection.insert({
    //             nat_val = 1;
    //             text_val = "\00";
    //             blob_val = Blob.fromArray([0]);
    //         });

    //         // Test consecutive value boundaries
    //         test(
    //             "Nat consecutive values respect exclusive range",
    //             func() {
    //                 let #ok(results) = boundary_collection.search(
    //                     QueryBuilder().Where("nat_val", #gt(0))
    //                 );

    //                 assert results.size() == 1;
    //                 assert results[0].1.nat_val == 1;
    //             },
    //         );

    //         test(
    //             "Text empty string and null byte respect exclusive range",
    //             func() {
    //                 let #ok(results) = boundary_collection.search(
    //                     QueryBuilder().Where("text_val", #gt(""))
    //                 );

    //                 assert results.size() == 1;
    //                 assert results[0].1.text_val == "\00";
    //             },
    //         );

    //         test(
    //             "Blob empty and single byte respect exclusive range",
    //             func() {
    //                 let #ok(results) = boundary_collection.search(
    //                     QueryBuilder().Where("blob_val", #gt(Blob.fromArray([])))
    //                 );

    //                 assert results.size() == 1;
    //                 assert Blob.equal(results[0].1.blob_val, Blob.fromArray([0]));
    //             },
    //         );

    //         // Test embedded nulls in blobs
    //         let #ok(id3) = boundary_collection.insert({
    //             nat_val = 3;
    //             text_val = "test";
    //             blob_val = Blob.fromArray([0, 0]);
    //         });
    //         let #ok(id4) = boundary_collection.insert({
    //             nat_val = 4;
    //             text_val = "test";
    //             blob_val = Blob.fromArray([0, 1]);
    //         });

    //         test(
    //             "Blobs with embedded nulls respect ordering and ranges",
    //             func() {
    //                 let #ok(results) = boundary_collection.search(
    //                     QueryBuilder().Where("blob_val", #gt(Blob.fromArray([0, 0])))
    //                 );

    //                 assert results.size() == 1;
    //                 assert Blob.equal(results[0].1.blob_val, Blob.fromArray([0, 1]));
    //             },
    //         );
    //     },
    // );

};

suite(
    "ZenDB Indexes",
    func() {
        suite(
            "Stable Memory",
            func() {
                let sstore = ZenDB.newStableStore(
                    ?{
                        logging = ?{
                            log_level = #Trap;
                            is_running_locally = true;
                        };
                        memory_type = ?(#stableMemory);
                    }
                );

                let zendb = ZenDB.launchDefaultDB(sstore);
                index_tests(zendb);
            },
        );

        suite(
            "Heap Memory",
            func() {
                let sstore = ZenDB.newStableStore(
                    ?{
                        logging = ?{
                            log_level = #Trap;
                            is_running_locally = true;
                        };
                        memory_type = ?(#heap);
                    }
                );

                let zendb = ZenDB.launchDefaultDB(sstore);
                index_tests(zendb);
            },
        );
    },
);
