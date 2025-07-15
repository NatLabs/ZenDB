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
import ZenDB "../../src";
import ZenDBSuite "../test-utils/TestFramework";

let fuzz = Fuzz.fromSeed(0x7eadbeef);
let { QueryBuilder } = ZenDB;

let limit = 10_000;

ZenDBSuite.newSuite(
    "Query Tests",
    ?{
        ZenDBSuite.onlyWithIndex with log_level = #Error;
    },
    func query_tests(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {

        type Data = {
            value : Text;
        };

        let DataSchema : ZenDB.Types.Schema = #Record([("value", #Text)]);

        let candify_data = {
            to_blob = func(data : Data) : Blob {
                to_candid (data);
            };
            from_blob = func(blob : Blob) : ?Data {
                from_candid (blob);
            };
        };

        let #ok(texts) = zendb.createCollection("texts", DataSchema, candify_data, null) else return assert false;

        let #ok(_) = suite_utils.createIndex(texts.name(), "value", [("value", #Ascending)], null) else return assert false;

        let #ok(_) = texts.insert({ value = "a" }) else return assert false;
        let #ok(_) = texts.insert({ value = "alphabet" }) else return assert false;
        let #ok(_) = texts.insert({ value = "alphabetical" }) else return assert false;
        let #ok(_) = texts.insert({ value = "and" }) else return assert false;
        let #ok(_) = texts.insert({ value = "anderson" }) else return assert false;
        let #ok(_) = texts.insert({ value = "b" }) else return assert false;
        let #ok(_) = texts.insert({ value = "berry" }) else return assert false;
        let #ok(_) = texts.insert({ value = "c" }) else return assert false;

        func run_query_tests(texts : ZenDB.Collection<Data>) {
            test(
                "#eq",
                func() {
                    Debug.print("Running #eq test ");

                    let results = texts.search(
                        QueryBuilder().Where("value", #eq(#Text("a")))
                    );

                    Debug.print(debug_show (results));

                    assert results == #ok([(0, { value = "a" })]);
                },
            );

            test(
                "#gt",
                func() {
                    Debug.print(
                        "Running #gt test" #
                        debug_show (
                            texts.search(
                                QueryBuilder().Where("value", #gt(#Text("and")))
                            )
                        )
                    );

                    assert texts.search(
                        QueryBuilder().Where("value", #gt(#Text("and")))
                    ) == #ok([
                        (4, { value = "anderson" }),
                        (5, { value = "b" }),
                        (6, { value = "berry" }),
                        (7, { value = "c" }),
                    ]);

                    Debug.print(
                        debug_show (
                            texts.search(
                                QueryBuilder().Where("value", #not_(#gt(#Text("and"))))
                            )
                        )
                    );

                    assert texts.search(
                        QueryBuilder().Where("value", #not_(#gt(#Text("and"))))
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
                        QueryBuilder().Where("value", #not_(#gte(#Text("and"))))
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
                        QueryBuilder().Where("value", #not_(#lt(#Text("and"))))
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
                        QueryBuilder().Where("value", #not_(#lte(#Text("and"))))
                    ) == #ok([
                        (4, { value = "anderson" }),
                        (5, { value = "b" }),
                        (6, { value = "berry" }),
                        (7, { value = "c" }),
                    ]);
                },
            );

            test(
                "#anyOf",
                func() {

                    let res = texts.search(
                        QueryBuilder().Where("value", #anyOf([#Text("a"), #Text("b"), #Text("c")]))
                    );

                    assert res == #ok([
                        (0, { value = "a" }),
                        (5, { value = "b" }),
                        (7, { value = "c" }),
                    ]);

                    //! Executes very slowly
                    // assert texts.search(
                    //     QueryBuilder().Where("value", #not_(#anyOf([#Text("a"), #Text("b"), #Text("c")])))
                    // ) == #ok([
                    //     (1, { value = "alphabet" }),
                    //     (2, { value = "alphabetical" }),
                    //     (3, { value = "and" }),
                    //     (4, { value = "anderson" }),
                    //     (6, { value = "berry" }),
                    // ]);
                },
            );

            test(
                "#between",
                func() {

                    let expected_response = #ok([
                        (0, { value = "a" }),
                        (1, { value = "alphabet" }),
                        (2, { value = "alphabetical" }),
                        (3, { value = "and" }),
                        (4, { value = "anderson" }),
                    ]);

                    let res0 = texts.search(
                        QueryBuilder().Where("value", #gte(#Text("a"))).And("value", #lte(#Text("anderson")))
                    );

                    assert res0 == expected_response;

                    let res1 = texts.search(
                        QueryBuilder().Where("value", #between(#Text("a"), #Text("anderson")))
                    );

                    assert res1 == expected_response;

                    let expected_negative_response = #ok([
                        (5, { value = "b" }),
                        (6, { value = "berry" }),
                        (7, { value = "c" }),
                    ]);

                    Debug.print(
                        debug_show (
                            QueryBuilder().Where("value", #gt(#Text("anderson"))).Or("value", #lt(#Text("a"))).build()
                        )
                    );

                    let res2 = texts.search(
                        QueryBuilder().Where("value", #gt(#Text("anderson"))).Or("value", #lt(#Text("a")))
                    );

                    Debug.print(debug_show { res2 });

                    assert res2 == expected_negative_response;

                    let res3 = texts.search(
                        QueryBuilder().Where("value", #not_(#between(#Text("a"), #Text("anderson"))))
                    );

                    Debug.print(debug_show { res3 });

                    assert res3 == expected_negative_response;

                },
            );

            test(
                "#startsWith()",
                func() {
                    let res = texts.search(
                        QueryBuilder().Where("value", #startsWith(#Text("a")))
                    );

                    Debug.print(debug_show { res });

                    assert res == #ok([
                        (0, { value = "a" }),
                        (1, { value = "alphabet" }),
                        (2, { value = "alphabetical" }),
                        (3, { value = "and" }),
                        (4, { value = "anderson" }),
                    ]);

                    assert texts.search(
                        QueryBuilder().Where("value", #not_(#startsWith(#Text("a"))))
                    ) == #ok([
                        (5, { value = "b" }),
                        (6, { value = "berry" }),
                        (7, { value = "c" }),
                    ]);

                    let res2 = texts.search(
                        QueryBuilder().Where("value", #startsWith(#Text("al")))
                    );

                    assert res2 == #ok([
                        (1, { value = "alphabet" }),
                        (2, { value = "alphabetical" }),
                    ]);

                    assert texts.search(
                        QueryBuilder().Where("value", #not_(#startsWith(#Text("al"))))
                    ) == #ok([
                        (0, { value = "a" }),
                        (3, { value = "and" }),
                        (4, { value = "anderson" }),
                        (5, { value = "b" }),
                        (6, { value = "berry" }),
                        (7, { value = "c" }),
                    ]);

                    let res3 = texts.search(
                        QueryBuilder().Where("value", #startsWith(#Text("and")))
                    );

                    assert res3 == #ok([
                        (3, { value = "and" }),
                        (4, { value = "anderson" }),
                    ]);

                    assert texts.search(
                        QueryBuilder().Where("value", #not_(#startsWith(#Text("and"))))
                    ) == #ok([
                        (0, { value = "a" }),
                        (1, { value = "alphabet" }),
                        (2, { value = "alphabetical" }),
                        (5, { value = "b" }),
                        (6, { value = "berry" }),
                        (7, { value = "c" }),
                    ]);

                    let res4 = texts.search(
                        QueryBuilder().Where("value", #startsWith(#Text("ben")))
                    );

                    assert res4 == #ok([]);

                    assert texts.search(
                        QueryBuilder().Where("value", #not_(#startsWith(#Text("ben"))))
                    ) == #ok([
                        (0, { value = "a" }),
                        (1, { value = "alphabet" }),
                        (2, { value = "alphabetical" }),
                        (3, { value = "and" }),
                        (4, { value = "anderson" }),
                        (5, { value = "b" }),
                        (6, { value = "berry" }),
                        (7, { value = "c" }),
                    ]);

                },
            );

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

                },
            );

            test(
                "Negative Query Test",
                func() {
                    let db_query = QueryBuilder().Where("value", #eq(#Text("item-not-in-store")));
                    let #ok(documents) = texts.search(db_query) else return assert false;

                    assert documents == [];
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

        };

        suite(
            "testing on non indexed field",
            func() {
                run_query_tests(texts);
            },
        );

        suite(
            "testing on indexed field",
            func() {
                Debug.print("trying to index field");
                let #ok(_) = texts.createIndex("value_index", [("value", #Ascending)], null) else return assert false;
                Debug.print("field indexed");

                run_query_tests(texts);
            },
        );

    },
);
