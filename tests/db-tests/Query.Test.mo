// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Iter "mo:base@0.16.0/Iter";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Buffer "mo:base@0.16.0/Buffer";

import { test; suite } "mo:test";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde@3.4.0/Candid";
import Record "mo:serde@3.4.0/Candid/Text/Parser/Record";
import Itertools "mo:itertools@0.2.2/Iter";
import ZenDB "../../src/EmbeddedInstance";
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

        let #ok(text_a_id) = texts.insert({ value = "a" }) else return assert false;
        let #ok(text_alphabet_id) = texts.insert({ value = "alphabet" }) else return assert false;
        let #ok(text_alphabetical_id) = texts.insert({ value = "alphabetical" }) else return assert false;
        let #ok(text_and_id) = texts.insert({ value = "and" }) else return assert false;
        let #ok(text_anderson_id) = texts.insert({ value = "anderson" }) else return assert false;
        let #ok(text_b_id) = texts.insert({ value = "b" }) else return assert false;
        let #ok(text_berry_id) = texts.insert({ value = "berry" }) else return assert false;
        let #ok(text_c_id) = texts.insert({ value = "c" }) else return assert false;

        func run_query_tests(texts : ZenDB.Collection<Data>) {
            test(
                "#eq",
                func() {
                    Debug.print("Running #eq test ");

                    let results = texts.search(
                        QueryBuilder().Where("value", #eq(#Text("a")))
                    );

                    Debug.print(debug_show (results));

                    let #ok(res) = results else return assert false;
                    assert res.documents == [(text_a_id, { value = "a" })];
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

                    let #ok(res) = texts.search(
                        QueryBuilder().Where("value", #gt(#Text("and")))
                    ) else return assert false;
                    assert res.documents == [
                        (text_anderson_id, { value = "anderson" }),
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];

                    Debug.print(
                        debug_show (
                            texts.search(
                                QueryBuilder().Where("value", #not_(#gt(#Text("and"))))
                            )
                        )
                    );

                    let #ok(result) = texts.search(
                        QueryBuilder().Where("value", #not_(#gt(#Text("and"))))
                    ) else return assert false;
                    assert result.documents == [
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                        (text_and_id, { value = "and" }),
                    ];
                },
            );

            test(
                "#gte",
                func() {
                    let #ok(result1) = texts.search(
                        QueryBuilder().Where("value", #gte(#Text("and")))
                    ) else return assert false;
                    assert result1.documents == [
                        (text_and_id, { value = "and" }),
                        (text_anderson_id, { value = "anderson" }),
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];

                    let #ok(result2) = texts.search(
                        QueryBuilder().Where("value", #not_(#gte(#Text("and"))))
                    ) else return assert false;
                    assert result2.documents == [
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                    ];
                },
            );

            test(
                "#lt",
                func() {
                    let #ok(result1) = texts.search(
                        QueryBuilder().Where("value", #lt(#Text("and")))
                    ) else return assert false;
                    assert result1.documents == [
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                    ];

                    let #ok(result2) = texts.search(
                        QueryBuilder().Where("value", #not_(#lt(#Text("and"))))
                    ) else return assert false;
                    assert result2.documents == [
                        (text_and_id, { value = "and" }),
                        (text_anderson_id, { value = "anderson" }),
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];
                },
            );

            test(
                "#lte",
                func() {
                    let #ok(result1) = texts.search(
                        QueryBuilder().Where("value", #lte(#Text("and")))
                    ) else return assert false;
                    assert result1.documents == [
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                        (text_and_id, { value = "and" }),
                    ];

                    let #ok(result2) = texts.search(
                        QueryBuilder().Where("value", #not_(#lte(#Text("and"))))
                    ) else return assert false;
                    assert result2.documents == [
                        (text_anderson_id, { value = "anderson" }),
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];
                },
            );

            test(
                "#anyOf",
                func() {

                    let res = texts.search(
                        QueryBuilder().Where("value", #anyOf([#Text("a"), #Text("b"), #Text("c")]))
                    );

                    let #ok(res_result) = res else return assert false;
                    assert res_result.documents == [
                        (text_a_id, { value = "a" }),
                        (text_b_id, { value = "b" }),
                        (text_c_id, { value = "c" }),
                    ];

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
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                        (text_and_id, { value = "and" }),
                        (text_anderson_id, { value = "anderson" }),
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
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
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

                    let #ok(res_result) = res else return assert false;
                    assert res_result.documents == [
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                        (text_and_id, { value = "and" }),
                        (text_anderson_id, { value = "anderson" }),
                    ];

                    let #ok(result2) = texts.search(
                        QueryBuilder().Where("value", #not_(#startsWith(#Text("a"))))
                    ) else return assert false;
                    assert result2.documents == [
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];

                    let res2 = texts.search(
                        QueryBuilder().Where("value", #startsWith(#Text("al")))
                    );

                    let #ok(res2_result) = res2 else return assert false;
                    assert res2_result.documents == [
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                    ];

                    let #ok(result3) = texts.search(
                        QueryBuilder().Where("value", #not_(#startsWith(#Text("al"))))
                    ) else return assert false;
                    assert result3.documents == [
                        (text_a_id, { value = "a" }),
                        (text_and_id, { value = "and" }),
                        (text_anderson_id, { value = "anderson" }),
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];

                    let res3 = texts.search(
                        QueryBuilder().Where("value", #startsWith(#Text("and")))
                    );

                    let #ok(res3_result) = res3 else return assert false;
                    assert res3_result.documents == [
                        (text_and_id, { value = "and" }),
                        (text_anderson_id, { value = "anderson" }),
                    ];

                    let #ok(result4) = texts.search(
                        QueryBuilder().Where("value", #not_(#startsWith(#Text("and"))))
                    ) else return assert false;
                    assert result4.documents == [
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];

                    let res4 = texts.search(
                        QueryBuilder().Where("value", #startsWith(#Text("ben")))
                    );

                    let #ok(res4_result) = res4 else return assert false;
                    assert res4_result.documents == [];

                    let #ok(result5) = texts.search(
                        QueryBuilder().Where("value", #not_(#startsWith(#Text("ben"))))
                    ) else return assert false;
                    assert result5.documents == [
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                        (text_and_id, { value = "and" }),
                        (text_anderson_id, { value = "anderson" }),
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];

                },
            );

            test(
                "#exists()",
                func() {

                    let res = texts.search(
                        QueryBuilder().Where("value", #exists)
                    );

                    let #ok(res_result) = res else return assert false;
                    assert res_result.documents == [
                        (text_a_id, { value = "a" }),
                        (text_alphabet_id, { value = "alphabet" }),
                        (text_alphabetical_id, { value = "alphabetical" }),
                        (text_and_id, { value = "and" }),
                        (text_anderson_id, { value = "anderson" }),
                        (text_b_id, { value = "b" }),
                        (text_berry_id, { value = "berry" }),
                        (text_c_id, { value = "c" }),
                    ];

                },
            );

            test(
                "Negative Query Test",
                func() {
                    let db_query = QueryBuilder().Where("value", #eq(#Text("item-not-in-store")));
                    let #ok(result) = texts.search(db_query) else return assert false;

                    assert result.documents == [];
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
