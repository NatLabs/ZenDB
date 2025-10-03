// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Blob "mo:base@0.16.0/Blob";
import Text "mo:base@0.16.0/Text";
import Array "mo:base@0.16.0/Array";

import ZenDB "../../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools@0.2.2/Iter";
import Map "mo:map@9.0.1/Map";
import ZenDBSuite "../test-utils/TestFramework";

type SizeVariant = {
    #known : Nat;
    #unknown;
};

type Version = {
    #v0 : { decimal : Float };
    #v1 : { a : Int; b : Text };
    #v2 : { c : Text; d : Bool };
    #v3 : { size : SizeVariant };
    #v4 : {
        units : { products : Nat; sales : Nat; customers : Nat };
        total : ?Nat;
    };
};

type Doc = {
    version : Version;
};

let DocSchema : ZenDB.Types.Schema = #Record([(
    "version",
    #Variant([
        ("v0", #Record([("decimal", #Float)])),
        (
            "v1",
            #Record([("a", #Int), ("b", #Text)]),
        ),
        (
            "v2",
            #Record([("c", #Text), ("d", #Bool)]),
        ),
        (
            "v3",
            #Record([("size", #Variant([("known", #Nat), ("unknown", #Null)]))]),
        ),
        (
            "v4",
            #Record([
                (
                    "units",
                    #Record([("products", #Nat), ("sales", #Nat), ("customers", #Nat)]),

                ),
                ("total", #Option(#Nat)),
            ]),
        ),
    ]),
)]);

let data_type_to_candid : ZenDB.Types.Candify<Doc> = {
    from_blob = func(blob : Blob) : ?Doc { from_candid (blob) };
    to_blob = func(c : Doc) : Blob { to_candid (c) };
};

ZenDBSuite.newSuite(
    "Update Tests",
    ?ZenDBSuite.withAndWithoutIndex,

    func update_tests(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {
        let #ok(data) = zendb.createCollection<Doc>("data", DocSchema, data_type_to_candid, null) else return assert false;

        let #ok(_) = suite_utils.createIndex(data.name(), "index_1", [("version", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(data.name(), "index_2", [("version.v1.a", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(data.name(), "index_3", [("version.v3.size.known", #Ascending)], null) else return assert false;

        var item1 : Doc = { version = #v1({ a = 42; b = "hello" }) };
        var item2 : Doc = { version = #v2({ c = "world"; d = true }) };
        var item3 : Doc = { version = #v3({ size = #known(32) }) };
        var item4 : Doc = { version = #v3({ size = #unknown }) };
        var item5 : Doc = {
            version = #v4({
                units = { products = 1000; sales = 1111; customers = 363 };
                total = null;
            });
        };

        let #ok(item1_id) = data.insert(item1) else return assert false;
        let #ok(item2_id) = data.insert(item2) else return assert false;
        let #ok(item3_id) = data.insert(item3) else return assert false;
        let #ok(item4_id) = data.insert(item4) else return assert false;
        let #ok(item5_id) = data.insert(item5) else return assert false;

        suite(
            "Update Tests",
            func() {
                test(
                    "replace",
                    func() {
                        data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(42)))
                        ) |> Debug.print(debug_show _);

                        assert #ok([(item1_id, item1)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(42)))
                        );

                        let new_doc : Doc = {
                            version = #v1({ a = 33; b = "text" });
                        };
                        let #ok(_) = data.replace(item1_id, (new_doc)) else return assert false;

                        assert #ok([]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(42)))
                        );

                        assert data.search(
                            ZenDB.QueryBuilder().Where(
                                "version.v1.b",
                                #eq(#Text("text")),
                            ).And(
                                "version.v1.a",
                                #eq(#Int(33)),
                            )
                        ) == #ok([(item1_id, new_doc)]);

                        item1 := new_doc;

                    },
                );

                test(
                    "replace field value",
                    func() {
                        assert #ok([(item1_id, item1)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(33)))
                        );

                        let #ok(_) = data.updateById(
                            item1_id,
                            [("version.v1.a", #Nat(0))],
                        );

                        item1 := { version = #v1({ a = 0; b = "text" }) };
                        Debug.print("item1 updated: " # debug_show (data.get(item1_id)));

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(0)))
                        ) == #ok([(item1_id, item1)]);
                    },
                );

                test(
                    "#addAll field",
                    func() {

                        assert #ok([(item1_id, item1)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(0)))
                        );

                        let #ok(_) = data.updateById(
                            item1_id,
                            [
                                ("version.v1.a", #add(#Nat(1), #currValue)),
                                ("version.v1.a", #addAll([#Nat(1), #currValue])),
                            ],
                        );

                        item1 := { version = #v1({ a = 2; b = "text" }) };

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(2)))
                        ) == #ok([(item1_id, item1)]);
                    },
                );

                test(
                    "#subAll field",
                    func() {

                        Debug.print(
                            "#subAll item: " # debug_show (
                                data.search(
                                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(32)))
                                )
                            )
                        );

                        assert #ok([(item3_id, item3)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(32)))
                        );

                        let #ok(_) = data.updateById(
                            item3_id,
                            [
                                ("version.v3.size.known", #sub(#currValue, #Nat(1))),
                                ("version.v3.size.known", #subAll([#currValue, #Nat(1)])),
                            ],
                        );

                        item3 := { version = #v3({ size = #known(30) }) };

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(30)))
                        ) == #ok([(item3_id, item3)]);

                    },
                );

                test(
                    "#mulAll field",
                    func() {

                        assert #ok([(item3_id, item3)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(30)))
                        );

                        let #ok(_) = data.updateById(
                            item3_id,
                            [
                                ("version.v3.size.known", #mulAll([#Nat(2), #currValue()])),
                                ("version.v3.size.known", #mulAll([#Nat(2), #currValue()])),
                            ],
                        );

                        item3 := { version = #v3({ size = #known(120) }) };

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(120)))
                        ) == #ok([(item3_id, item3)]);

                    },
                );

                test(
                    "#divAll field",
                    func() {

                        assert #ok([(item3_id, item3)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(120)))
                        );

                        let #ok(_) = data.updateById(
                            item3_id,
                            ([
                                ("version.v3.size.known", #div(#currValue, #Nat(2))),
                                ("version.v3.size.known", #divAll([#currValue(), #Nat(2)])),
                            ]),
                        );

                        item3 := { version = #v3({ size = #known(30) }) };

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(30)))
                        ) == #ok([(item3_id, item3)]);

                    },
                );

                test(
                    "[#addAll, #subAll, #mulAll, #divAll]",
                    func() {

                        assert #ok([(item1_id, item1)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(2)))
                        );

                        let #ok(_) = data.updateById(
                            item1_id,
                            [
                                ("version.v1.a", #mulAll([#currValue(), #Nat(9)])),
                                ("version.v1.a", #addAll([#currValue(), #Nat(2)])),
                                ("version.v1.a", #divAll([#currValue(), #Nat(5)])),
                                ("version.v1.a", #subAll([#currValue(), #Nat(1)])),
                            ],
                        );

                        item1 := { version = #v1({ a = 3; b = "text" }) };

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Int(3)))
                        ) == #ok([(item1_id, item1)]);

                    },
                );

                test(
                    "compound fields",
                    func() {

                        assert #ok([(item3_id, item3)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(30)))
                        );

                        let #ok(_) = data.updateById(
                            item3_id,
                            [
                                (
                                    "version",
                                    // set to #v3({ size = #known(42) })
                                    (#Variant("v3", #Record([("size", #Variant("known", #Int(42)))]))),
                                ),
                            ],
                        );
                    },
                );

                suite(
                    "multi and nested operations",
                    func() {
                        test(
                            "multi #addAll",
                            func() {
                                assert #ok([(item5_id, item5)]) == data.search(
                                    ZenDB.QueryBuilder().Where("version.v4.units.products", #eq(#Nat(1000)))
                                );

                                let #ok(_) = data.updateById(
                                    item5_id,
                                    [
                                        (
                                            "version.v4.total",
                                            (
                                                #addAll([
                                                    #get("version.v4.units.products"),
                                                    #get("version.v4.units.sales"),
                                                    #get("version.v4.units.customers"),
                                                ])
                                            ),
                                        ),
                                    ],
                                );

                                item5 := {
                                    version = #v4({
                                        units = {
                                            products = 1000;
                                            sales = 1111;
                                            customers = 363;
                                        };
                                        total = ?2474;
                                    });
                                };

                                data.get(item5_id) |> Debug.print("item5 updated: " # debug_show (_));

                                assert data.search(
                                    ZenDB.QueryBuilder().Where("version.v4.total", #eq(#Option(#Nat(2474))))
                                ) == #ok([(item5_id, item5)]);

                            },
                        );

                        test(
                            "#mulAllti #subAll",
                            func() {

                                // fails because sales is greater than products and the type is Nat
                                // should pass if the type is Int or Float
                                let #err(msg) = data.updateById(
                                    item5_id,
                                    [
                                        (
                                            "version.v4.total",
                                            (
                                                #subAll([
                                                    #get("version.v4.units.products"),
                                                    #get("version.v4.units.sales"),
                                                    #get("version.v4.units.customers"),
                                                ])
                                            ),
                                        ),
                                    ],
                                );

                                Debug.print("#subAll update error msg: " # debug_show (msg));

                                assert data.get(item5_id) == ?item5;

                                let #ok(_) = data.updateById(
                                    item5_id,
                                    [
                                        (
                                            "version.v4.total",
                                            (
                                                #subAll([
                                                    #get("version.v4.units.sales"),
                                                    #get("version.v4.units.products"),
                                                ])
                                            ),
                                        ),
                                    ],
                                );

                                item5 := {
                                    version = #v4({
                                        units = {
                                            products = 1000;
                                            sales = 1111;
                                            customers = 363;
                                        };
                                        total = ?111;
                                    });
                                };

                                assert data.get(item5_id) == ?item5;
                            },
                        );

                        test(
                            "multi #mulAll",
                            func() {
                                let #ok(_) = data.updateById(
                                    item5_id,
                                    [
                                        (
                                            "version.v4.total",
                                            (
                                                #mulAll([
                                                    #get("version.v4.units.products"),
                                                    #get("version.v4.units.sales"),
                                                    #get("version.v4.units.customers"),
                                                ])
                                            ),
                                        ),
                                    ],
                                );

                                assert data.get(item5_id) == ?{
                                    version = #v4({
                                        units = {
                                            products = 1000;
                                            sales = 1111;
                                            customers = 363;
                                        };
                                        total = ?403_293_000;
                                    });
                                };
                            },
                        );

                        test(
                            "multi #divAll",
                            func() {
                                let #ok(_) = data.updateById(
                                    item5_id,
                                    [
                                        (
                                            "version.v4.total",
                                            (
                                                #divAll([
                                                    #get("version.v4.units.products"),
                                                    #get("version.v4.units.sales"),
                                                    #get("version.v4.units.customers"),
                                                ])
                                            ),
                                        ),
                                    ],
                                );

                                Debug.print(debug_show { item5 = data.get(item5_id) });

                                assert data.get(item5_id) == ?{
                                    version = #v4({
                                        units = {
                                            products = 1000;
                                            sales = 1111;
                                            customers = 363;
                                        };
                                        total = ?0;
                                    });
                                };

                                let #ok(_) = data.updateById(
                                    item5_id,
                                    ([
                                        (
                                            "version.v4.total",
                                            (
                                                #divAll([
                                                    #get("version.v4.units.sales"),
                                                    #get("version.v4.units.customers"),
                                                ])
                                            ),
                                        ),
                                    ]),
                                );

                                assert data.get(item5_id) == ?{
                                    version = #v4({
                                        units = {
                                            products = 1000;
                                            sales = 1111;
                                            customers = 363;
                                        };
                                        total = ?3;
                                    });
                                };
                            },
                        );

                        test(
                            "multi #addAll, #subAll, #mulAll, #divAll, #get",
                            func() {
                                let #ok(_) = data.updateById(
                                    item5_id,
                                    ([
                                        (
                                            "version.v4.total",
                                            (
                                                #addAll([
                                                    #subAll([
                                                        #mulAll([
                                                            #get("version.v4.units.products"),
                                                            #get("version.v4.units.sales"),
                                                            #get("version.v4.units.customers"),
                                                        ]),
                                                        #mulAll([
                                                            #get("version.v4.total"),
                                                            #divAll([
                                                                #addAll([
                                                                    #get("version.v4.units.products"),
                                                                    #get("version.v4.units.sales"),
                                                                    #get("version.v4.units.customers"),
                                                                ]),
                                                                #get("version.v4.units.customers"),
                                                            ]),
                                                        ]),
                                                    ]),
                                                    (#Int(-400_000_000)),
                                                ])
                                            ),
                                        ),
                                    ]),
                                );

                                Debug.print("item5_id " # debug_show (data.get(item5_id)));

                                assert data.get(item5_id) == ?{
                                    version = #v4({
                                        units = {
                                            products = 1000;
                                            sales = 1111;
                                            customers = 363;
                                        };
                                        total = ?3_292_979;
                                    });
                                };
                            },
                        );

                    }

                );

                test(
                    "#neg operation",
                    func() {
                        let #ok(_) = data.updateById(
                            item1_id,
                            [("version.v1.a", (#neg(#Nat(10))))],
                        );

                        assert data.get(item1_id) == ?{
                            version = #v1({ a = -10; b = "text" });
                        };

                        // Reset
                        let #ok(_) = data.replace(item1_id, item1) else return assert false;
                    },
                );

                suite(
                    "Number Operations",
                    func() {
                        test(
                            "#abs operation",
                            func() {
                                // First set a negative value
                                let new_doc : Doc = {
                                    version = #v1({ a = 0; b = "text" });
                                };
                                let #ok(_) = data.replace(item1_id, new_doc) else return assert false;

                                // Apply negative operation
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.a", (#neg(#Int(42))))],
                                );

                                // Apply absolute value operation
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.a", (#abs(#get("version.v1.a"))))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 42; b = "text" });
                                };

                                // Reset for other tests
                                item1 := {
                                    version = #v1({ a = 42; b = "text" });
                                };
                            },
                        );

                        test(
                            "#floor, #ceil operations",
                            func() {
                                // Create a temporary document with decimal values
                                let temp_doc : Doc = {
                                    version = #v0({ decimal = 7.6 });
                                };
                                let #ok(temp_id) = data.insert(temp_doc) else return assert false;

                                // Test floor operation
                                let #ok(_) = data.updateById(
                                    temp_id,
                                    [("version.v0.decimal", (#floor(#currValue)))],
                                );

                                assert data.get(temp_id) == ?{
                                    version = #v0({ decimal = 7.0 });
                                };

                                // Reset and test ceil
                                let #ok(_) = data.updateById(
                                    temp_id,
                                    [("version.v0.decimal", (#Float(9.2)))],
                                );

                                Debug.print("result after update: " # debug_show (data.get(temp_id)));
                                assert data.get(temp_id) == ?{
                                    version = #v0({ decimal = 9.2 });
                                };

                                let #ok(_) = data.updateById(
                                    temp_id,
                                    [("version.v0.decimal", (#ceil(#currValue)))],
                                );

                                Debug.print("Ceil result: " # debug_show (data.get(temp_id)));
                                assert data.get(temp_id) == ?{
                                    version = #v0({ decimal = 10.0 });
                                };

                                // Clean up
                                let #ok(_) = data.deleteById(temp_id);
                            },
                        );

                        test(
                            "#sqrt operation",
                            func() {
                                // Set value to square number
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.a", (#Nat(64)))],
                                );

                                // Apply square root
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.a", (#sqrt(#get("version.v1.a"))))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 8; b = "text" });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );

                        test(
                            "#pow operation",
                            func() {
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.a", (#pow(#Nat(2), #Nat(5))))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 32; b = "text" });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );

                        test(
                            "#min and #max operations",
                            func() {
                                // Test min
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.a", (#min(#Nat(10), #Int(42))))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 10; b = "text" });
                                };

                                // Test max
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.a", (#max((#Nat(10)), (#Int(42)))))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 42; b = "text" });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );

                        test(
                            "#mod operation",
                            func() {
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.a", #mod(#Int(42), #Nat(10)))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 2; b = "text" });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );
                    },
                );

                suite(
                    "Text Operations",
                    func() {
                        test(
                            "#trim operation",
                            func() {
                                // Set text with whitespace
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#Text("  trimmed text  ")))],
                                );

                                // Apply trim
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#trim(#currValue, " ")))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 42; b = "trimmed text" });
                                };

                                // Set text with dashes
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#Text("----===trimmed text===----")))],
                                );

                                // Apply trim
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#trim(#currValue, "-")))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({
                                        a = 42;
                                        b = "===trimmed text===";
                                    });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );

                        test(
                            "#lowercase and #uppercase operations",
                            func() {
                                // Set mixed case text
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#Text("MiXeD CaSe")))],
                                );

                                // Test lowercase
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#lowercase(#currValue)))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 42; b = "mixed case" });
                                };

                                // Test uppercase
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#uppercase(#currValue)))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 42; b = "MIXED CASE" });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );

                        test(
                            "#replaceSubText operations",
                            func() {
                                // Set text with repeated pattern
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", ((#Text("apple banana apple orange"))))],
                                );

                                // Test replaceSubText (all occurrences)
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#replaceSubText(#currValue, "apple", "kiwi")))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({
                                        a = 42;
                                        b = "kiwi banana kiwi orange";
                                    });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );

                        test(
                            "#slice operation",
                            func() {
                                // Set sample text
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#Text("abcdefghijklmnopqrstuvwxyz")))],
                                );

                                // Test slice (extract substring)
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#slice(#currValue, 3, 8)))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 42; b = "defgh" });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );

                        test(
                            "#concat operation",
                            func() {
                                // Set initial text
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#Text("Hello")))],
                                );

                                // Test concatenation
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#concat(#currValue, #Text(" World!"))))],
                                );

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 42; b = "Hello World!" });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            },
                        );

                        test(
                            "#concatAll operation",
                            func() {
                                // Set initial text
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#Text("Hello")))],
                                );

                                // Test concatenation with multiple strings
                                let #ok(_) = data.updateById(
                                    item1_id,
                                    [("version.v1.b", (#concatAll([#currValue, #Text(" World"), #Text("!")])))],
                                );

                                Debug.print("After concatAll: " # debug_show (data.get(item1_id)));

                                assert data.get(item1_id) == ?{
                                    version = #v1({ a = 42; b = "Hello World!" });
                                };

                                // Reset
                                let #ok(_) = data.replace(item1_id, item1) else return assert false;
                            }

                        );
                    },
                );
            },
        );

    },
);
