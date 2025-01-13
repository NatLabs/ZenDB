// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";

import ZenDB "../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Record "mo:serde/Candid/Text/Parser/Record";

let zendb_sstore = ZenDB.newStableStore();
let zendb = ZenDB.launch(zendb_sstore);

type SizeVariant = {
    #known : Nat;
    #unknown;
};

type Version = {
    #v1 : { a : Nat; b : Text };
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

let DocSchema : ZenDB.Schema = #Record([(
    "version",
    #Variant([
        (
            "v1",
            #Record([("a", #Nat), ("b", #Text)]),
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

let data_type_to_candid : ZenDB.Candify<Doc> = {
    from_blob = func(blob : Blob) : Doc {
        let ?c : ?Doc = from_candid (blob);
        c;
    };
    to_blob = func(c : Doc) : Blob { to_candid (c) };
};

let #ok(data) = zendb.create_collection<Doc>("data", DocSchema, data_type_to_candid);
let #ok(_) = data.create_index(["version"]);
let #ok(_) = data.create_index(["version.v1.a"]);
let #ok(_) = data.create_index(["version.v3.size.known"]);

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

let #ok(item1_id) = data.insert(item1);
let #ok(item2_id) = data.insert(item2);
let #ok(item3_id) = data.insert(item3);
let #ok(item4_id) = data.insert(item4);
let #ok(item5_id) = data.insert(item5);

suite(
    "Update Tests",
    func() {
        test(
            "#doc",
            func() {

                assert #ok([(item1_id, item1)]) == data.search(
                    ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(42)))
                );

                let new_doc : Doc = { version = #v1({ a = 0; b = "text" }) };
                let #ok(_) = data.updateById(item1_id, #doc(new_doc));

                assert #ok([]) == data.search(
                    ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(42)))
                );

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v1.b", #eq(#Text("text")))
                ) == #ok([(item1_id, new_doc)]);

                item1 := new_doc;

            },
        );

        test(
            "#ops: #add field",
            func() {

                assert #ok([(item1_id, item1)]) == data.search(
                    ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(0)))
                );

                let #ok(_) = data.updateById(
                    item1_id,
                    #ops([
                        ("version.v1.a", #add(#Nat(1))),
                        ("version.v1.a", #add(#Nat(1))),
                    ]),
                );

                item1 := { version = #v1({ a = 2; b = "text" }) };

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(2)))
                ) == #ok([(item1_id, item1)]);
            },
        );

        test(
            "#ops: #sub field",
            func() {

                assert #ok([(item3_id, item3)]) == data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(32)))
                );

                let #ok(_) = data.updateById(
                    item3_id,
                    #ops([
                        ("version.v3.size.known", #sub(#Nat(1))),
                        ("version.v3.size.known", #sub(#Nat(1))),
                    ]),
                );

                item3 := { version = #v3({ size = #known(30) }) };

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(30)))
                ) == #ok([(item3_id, item3)]);

            },
        );

        test(
            "#ops: #mul field",
            func() {

                assert #ok([(item3_id, item3)]) == data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(30)))
                );

                let #ok(_) = data.updateById(
                    item3_id,
                    #ops([
                        ("version.v3.size.known", #mul(#Nat(2))),
                        ("version.v3.size.known", #mul(#Nat(2))),
                    ]),
                );

                item3 := { version = #v3({ size = #known(120) }) };

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(120)))
                ) == #ok([(item3_id, item3)]);

            },
        );

        test(
            "#ops: #div field",
            func() {

                assert #ok([(item3_id, item3)]) == data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(120)))
                );

                let #ok(_) = data.updateById(
                    item3_id,
                    #ops([
                        ("version.v3.size.known", #div(#Nat(2))),
                        ("version.v3.size.known", #div(#Nat(2))),
                    ]),
                );

                item3 := { version = #v3({ size = #known(30) }) };

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(30)))
                ) == #ok([(item3_id, item3)]);

            },
        );

        test(
            "#ops: [#add, #sub, #mul, #div]",
            func() {

                assert #ok([(item1_id, item1)]) == data.search(
                    ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(2)))
                );

                let #ok(_) = data.updateById(
                    item1_id,
                    #ops([
                        ("version.v1.a", #mul(#Nat(9))),
                        ("version.v1.a", #add(#Nat(2))),
                        ("version.v1.a", #div(#Nat(5))),
                        ("version.v1.a", #sub(#Nat(1))),
                    ]),
                );

                item1 := { version = #v1({ a = 3; b = "text" }) };

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(3)))
                ) == #ok([(item1_id, item1)]);

            },
        );

        test(
            "#ops: #set compound fields",
            func() {

                assert #ok([(item3_id, item3)]) == data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(30)))
                );

                let #ok(_) = data.updateById(
                    item3_id,
                    #ops([
                        (
                            "version",
                            #set(
                                // #v3({ size = #known(42) })
                                #val(#Variant("v3", #Record([("size", #Variant("known", #Nat(42)))]))),
                            ),
                        ),
                    ]),
                );
            },
        );

        suite(
            "#ops: #set nested operations",
            func() {
                test(
                    "#set: multi #add",
                    func() {
                        assert #ok([(item5_id, item5)]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v4.units.products", #eq(#Nat(1000)))
                        );

                        data.updateById(
                            item5_id,
                            #ops([
                                (
                                    "version.v4.total",
                                    #set(
                                        #add([
                                            #get("version.v4.units.products"),
                                            #get("version.v4.units.sales"),
                                            #get("version.v4.units.customers"),
                                        ])
                                    ),
                                ),
                            ]),
                        ) |> Debug.print(debug_show (_));

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

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v4.total", #eq(#Option(#Nat(2474))))
                        ) == #ok([(item5_id, item5)]);

                    },
                );

                test(
                    "#set: multi #sub",
                    func() {

                        // fails because sales is greater than products and the type is Nat
                        // should pass if the type is Int or Float
                        let #err(msg) = data.updateById(
                            item5_id,
                            #ops([
                                (
                                    "version.v4.total",
                                    #set(
                                        #sub([
                                            #get("version.v4.units.products"),
                                            #get("version.v4.units.sales"),
                                            #get("version.v4.units.customers"),
                                        ])
                                    ),
                                ),
                            ]),
                        );

                        Debug.print("#sub update error msg: " # debug_show (msg));

                        assert data.get(item5_id) == #ok(item5);

                        let #ok(_) = data.updateById(
                            item5_id,
                            #ops([
                                (
                                    "version.v4.total",
                                    #set(
                                        #sub([
                                            #get("version.v4.units.sales"),
                                            #get("version.v4.units.products"),
                                        ])
                                    ),
                                ),
                            ]),
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

                        assert data.get(item5_id) == #ok(item5);
                    },
                );
                test(
                    "#set: multi #mul",
                    func() {
                        let #ok(_) = data.updateById(
                            item5_id,
                            #ops([
                                (
                                    "version.v4.total",
                                    #set(
                                        #mul([
                                            #get("version.v4.units.products"),
                                            #get("version.v4.units.sales"),
                                            #get("version.v4.units.customers"),
                                        ])
                                    ),
                                ),
                            ]),
                        );

                        assert data.get(item5_id) == #ok({
                            version = #v4({
                                units = {
                                    products = 1000;
                                    sales = 1111;
                                    customers = 363;
                                };
                                total = ?403_293_000;
                            });
                        });
                    },
                );

                test(
                    "#set: multi #div",
                    func() {
                        let #ok(_) = data.updateById(
                            item5_id,
                            #ops([
                                (
                                    "version.v4.total",
                                    #set(
                                        #div([
                                            #get("version.v4.units.products"),
                                            #get("version.v4.units.sales"),
                                            #get("version.v4.units.customers"),
                                        ])
                                    ),
                                ),
                            ]),
                        );

                        Debug.print(debug_show { item5 = data.get(item5_id) });

                        assert data.get(item5_id) == #ok({
                            version = #v4({
                                units = {
                                    products = 1000;
                                    sales = 1111;
                                    customers = 363;
                                };
                                total = ?0;
                            });
                        });

                        let #ok(_) = data.updateById(
                            item5_id,
                            #ops([
                                (
                                    "version.v4.total",
                                    #set(
                                        #div([
                                            #get("version.v4.units.sales"),
                                            #get("version.v4.units.customers"),
                                        ])
                                    ),
                                ),
                            ]),
                        );

                        assert data.get(item5_id) == #ok({
                            version = #v4({
                                units = {
                                    products = 1000;
                                    sales = 1111;
                                    customers = 363;
                                };
                                total = ?3;
                            });
                        });
                    },
                );

                test(
                    "#set: multi #add, #sub, #mul, #div, #val, #get",
                    func() {
                        let #ok(_) = data.updateById(
                            item5_id,
                            #ops([
                                (
                                    "version.v4.total",
                                    #set(
                                        #add([
                                            #sub([
                                                #mul([
                                                    #get("version.v4.units.products"),
                                                    #get("version.v4.units.sales"),
                                                    #get("version.v4.units.customers"),
                                                ]),
                                                #mul([
                                                    #get("version.v4.total"),
                                                    #div([
                                                        #add([
                                                            #get("version.v4.units.products"),
                                                            #get("version.v4.units.sales"),
                                                            #get("version.v4.units.customers"),
                                                        ]),
                                                        #get("version.v4.units.customers"),
                                                    ]),
                                                ]),
                                            ]),
                                            #val(#Int(-400_000_000)),
                                        ])
                                    ),
                                ),
                            ]),
                        );

                        Debug.print("item5_id " # debug_show (data.get(item5_id)));

                        assert data.get(item5_id) == #ok({
                            version = #v4({
                                units = {
                                    products = 1000;
                                    sales = 1111;
                                    customers = 363;
                                };
                                total = ?3_292_979;
                            });
                        });
                    },
                );

            }

        )

    },
);
