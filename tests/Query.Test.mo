// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Buffer "mo:base/Buffer";

import { test; suite } "mo:test";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde/Candid";
import Record "mo:serde/Candid/Text/Parser/Record";
import Itertools "mo:itertools/Iter";
import ZenDB "../src";
import TestUtils "TestUtils";

let fuzz = Fuzz.fromSeed(0x7eadbeef);
let { QueryBuilder } = ZenDB;

let sstore = ZenDB.newStableStore();
let zendb = ZenDB.launch(sstore);

let limit = 10_000;

type Data = {
    value : Text;
};

let DataSchema : ZenDB.Schema = #Record([("value", #Text)]);

let candify_data = {
    to_blob = func(data : Data) : Blob {
        to_candid (data);
    };
    from_blob = func(blob : Blob) : Data {
        switch (from_candid (blob) : ?Data) {
            case (?data) data;
            case (_) Debug.trap("Failed to decode data");
        };
    };
};

let #ok(texts) = zendb.create_collection("texts", DataSchema, candify_data);

// let #ok(_) = texts.create_index(["value"]);

let #ok(_) = texts.insert({ value = "a" });
let #ok(_) = texts.insert({ value = "alphabet" });
let #ok(_) = texts.insert({ value = "alphabetical" });
let #ok(_) = texts.insert({ value = "and" });
let #ok(_) = texts.insert({ value = "anderson" });
let #ok(_) = texts.insert({ value = "b" });
let #ok(_) = texts.insert({ value = "berry" });
let #ok(_) = texts.insert({ value = "c" });

func query_tests(texts : ZenDB.Collection<Data>) {
    test(
        "#eq",
        func() {
            assert texts.search(
                QueryBuilder().Where("value", #eq(#Text("a")))
            ) == #ok([(0, { value = "a" })]);
        },
    );

    test(
        "#gt",
        func() {
            assert texts.search(
                QueryBuilder().Where("value", #gt(#Text("and")))
            ) == #ok([
                (4, { value = "anderson" }),
                (5, { value = "b" }),
                (6, { value = "berry" }),
                (7, { value = "c" }),
            ]);

            assert texts.search(
                QueryBuilder().Where("value", #Not(#gt(#Text("and"))))
            ) == #ok([
                (0, { value = "a" }),
                (1, { value = "alphabet" }),
                (2, { value = "alphabetical" }),
                (3, { value = "and" }),
            ]);
        },
    );

    test(
        "#gte",
        func() {
            assert texts.search(
                QueryBuilder().Where("value", #gte(#Text("and")))
            ) == #ok([
                (3, { value = "and" }),
                (4, { value = "anderson" }),
                (5, { value = "b" }),
                (6, { value = "berry" }),
                (7, { value = "c" }),
            ]);

            assert texts.search(
                QueryBuilder().Where("value", #Not(#gte(#Text("and"))))
            ) == #ok([
                (0, { value = "a" }),
                (1, { value = "alphabet" }),
                (2, { value = "alphabetical" }),
            ]);
        },
    );

    test(
        "#lt",
        func() {
            assert texts.search(
                QueryBuilder().Where("value", #lt(#Text("and")))
            ) == #ok([
                (0, { value = "a" }),
                (1, { value = "alphabet" }),
                (2, { value = "alphabetical" }),
            ]);

            assert texts.search(
                QueryBuilder().Where("value", #Not(#lt(#Text("and"))))
            ) == #ok([
                (3, { value = "and" }),
                (4, { value = "anderson" }),
                (5, { value = "b" }),
                (6, { value = "berry" }),
                (7, { value = "c" }),
            ]);
        },
    );

    test(
        "#lte",
        func() {
            assert texts.search(
                QueryBuilder().Where("value", #lte(#Text("and")))
            ) == #ok([
                (0, { value = "a" }),
                (1, { value = "alphabet" }),
                (2, { value = "alphabetical" }),
                (3, { value = "and" }),
            ]);

            assert texts.search(
                QueryBuilder().Where("value", #Not(#lte(#Text("and"))))
            ) == #ok([
                (4, { value = "anderson" }),
                (5, { value = "b" }),
                (6, { value = "berry" }),
                (7, { value = "c" }),
            ]);
        },
    );

    test(
        "#In",
        func() {

            let res = texts.search(
                QueryBuilder().Where("value", #In([#Text("a"), #Text("b"), #Text("c")]))
            );

            assert res == #ok([
                (0, { value = "a" }),
                (5, { value = "b" }),
                (7, { value = "c" }),
            ]);

            assert texts.search(
                QueryBuilder().Where("value", #Not(#In([#Text("a"), #Text("b"), #Text("c")])))
            ) == #ok([
                (1, { value = "alphabet" }),
                (2, { value = "alphabetical" }),
                (3, { value = "and" }),
                (4, { value = "anderson" }),
                (6, { value = "berry" }),
            ]);
        },
    );

    // test(
    //     "#between",
    //     func() {

    //         let expected_response = #ok([
    //             (0, { value = "a" }),
    //             (1, { value = "alphabet" }),
    //             (2, { value = "alphabetical" }),
    //             (3, { value = "and" }),
    //             (4, { value = "anderson" }),
    //         ]);

    //         let res0 = texts.search(
    //             QueryBuilder().Where("value", #gte(#Text("a"))).And("value", #lte(#Text("anderson")))
    //         );

    //         assert res0 == expected_response;

    //         let res1 = texts.search(
    //             QueryBuilder().Where("value", #between(#Text("a"), #Text("anderson")))
    //         );

    //         assert res1 == expected_response;

    //         let expected_negative_response = #ok([
    //             (5, { value = "b" }),
    //             (6, { value = "berry" }),
    //             (7, { value = "c" }),
    //         ]);

    //         Debug.print(
    //             debug_show (
    //                 QueryBuilder().Where("value", #gt(#Text("anderson"))).Or("value", #lt(#Text("a"))).build()
    //             )
    //         );

    //         let res2 = texts.search(
    //             QueryBuilder().Where("value", #gt(#Text("anderson"))).Or("value", #lt(#Text("a")))
    //         );

    //         Debug.print(debug_show { res2 });

    //         // assert res2 == expected_negative_response;

    //         let res3 = texts.search(
    //             QueryBuilder().Where("value", #Not(#between(#Text("a"), #Text("anderson"))))
    //         );

    //         Debug.print(debug_show { res3 });

    //         assert res3 == expected_negative_response;

    //     },
    // );

    test(
        "#exists()",
        func() {

            let res = texts.search(
                QueryBuilder().Where("value", #exists)
            );

            assert res == #ok([
                (0, { value = "a" }),
                (1, { value = "alphabet" }),
                (2, { value = "alphabetical" }),
                (3, { value = "and" }),
                (4, { value = "anderson" }),
                (5, { value = "b" }),
                (6, { value = "berry" }),
                (7, { value = "c" }),
            ]);

            assert texts.search(
                QueryBuilder().Where("value", #Not(#exists))
            ) == #ok([]);

        },
    );

    test(
        "Negative Query Test",
        func() {
            let db_query = QueryBuilder().Where("value", #eq(#Text("item-not-in-store")));
            let #ok(records) = texts.search(db_query);

            assert records == [];
        },
    );

    test(
        "search(): Returns error if query fields are not in schema",
        func() {
            let db_query = QueryBuilder().Where("field-not-in-schema", #eq(#Empty));
            let result = texts.search(db_query);

            switch (result) {
                case (#err(_)) {};
                case (#ok(_)) assert false;
            };
        },
    );

    // test(
    //     "search(): records with price between 100 and 200",
    //     func() {
    //         let db_query = QueryBuilder().Where(
    //             "price",
    //             #gt(#Float(100.0)),
    //         ).And(
    //             "price",
    //             #lt(#Float(200.0)),
    //         );

    //         let #ok(records) = store_items_collection.search(db_query);

    //         let test_records = get_test_records(
    //             func(i : Nat, item : StoreItem) : Bool {
    //                 item.price > 100.0 and item.price < 200.0;
    //             }
    //         );

    //         for ((id, record) in records.vals()) {
    //             assert record.price > 100.0 and record.price < 200.0;
    //         };

    //         assert records.size() == test_records.size();
    //     },

    // );

    // test(
    //     "search(): records with months_in_stock between 7 and 12 (inclusive)",
    //     func() {
    //         let db_query = QueryBuilder().Where(
    //             "months_in_stock",
    //             #gte(#Nat(7)),
    //         ).And(
    //             "months_in_stock",
    //             #lte(#Nat(12)),
    //         );

    //         let #ok(records) = store_items_collection.search(db_query);

    //         let test_records = get_test_records(
    //             func(i : Nat, item : StoreItem) : Bool {
    //                 item.months_in_stock >= 7 and item.months_in_stock <= 12;
    //             }
    //         );

    //         for ((id, record) in records.vals()) {
    //             assert record.months_in_stock >= 7 and record.months_in_stock <= 12;
    //         };

    //         assert records.size() == test_records.size();
    //     },

    // );

    // test(
    //     "search(): records with address in 'Toronto, Canada'",
    //     func() {
    //         let db_query = QueryBuilder().Where(
    //             "address.country",
    //             #eq(#Text("Canada")),
    //         ).And(
    //             "address.city",
    //             #eq(#Text("Toronto")),
    //         );

    //         let #ok(records) = store_items_collection.search(db_query);

    //         let test_records = get_test_records(
    //             func(i : Nat, item : StoreItem) : Bool {
    //                 item.address.city == "Toronto";
    //             }
    //         );

    //         for ((id, record) in records.vals()) {
    //             assert record.address.city == "Toronto";
    //         };

    //         assert records.size() == test_records.size();
    //     },
    // );

};

suite("testing on non indexed field", func() { query_tests(texts) });

suite(
    "testing on indexed field",
    func() {
        let #ok(_) = texts.create_index(["value"]);
        query_tests(texts);
    },
);
