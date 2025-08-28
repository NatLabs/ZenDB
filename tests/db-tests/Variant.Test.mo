// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Array "mo:base/Array";

import ZenDB "../../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Map "mo:map/Map";
import ZenDBSuite "../test-utils/TestFramework";

type SizeVariant = {
    #known : Nat;
    #unknown;
};

type Version = {
    #v1 : { a : Nat; b : Text };
    #v2 : { c : Text; d : Bool };
    #v3 : { size : SizeVariant };
};

type Data = {
    version : Version;
};

let DataSchema : ZenDB.Types.Schema = #Record([(
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
    ]),
)]);

let data_type_to_candid : ZenDB.Types.Candify<Data> = {
    from_blob = func(blob : Blob) : ?Data { from_candid (blob) };
    to_blob = func(c : Data) : Blob { to_candid (c) };
};

ZenDBSuite.newSuite(
    "Variant Tests",
    ?{ ZenDBSuite.withAndWithoutIndex with log_level = #Error },
    func suite_setup(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {

        let #ok(data) = zendb.createCollection<Data>("data", DataSchema, data_type_to_candid, null) else return assert false;

        let #ok(_) = suite_utils.createIndex(data.name(), "index_1", [("version", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(data.name(), "index_2", [("version.v1.a", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(data.name(), "index_3", [("version.v3.size.known", #Ascending)], null) else return assert false;

        let #ok(v1_id) = data.insert({ version = #v1({ a = 42; b = "hello" }) }) else return assert false;
        let #ok(v2_id) = data.insert({
            version = #v2({ c = "world"; d = true });
        }) else return assert false;
        let #ok(v3_known_id) = data.insert({
            version = #v3({ size = #known(32) });
        }) else return assert false;
        let #ok(v3_unknown_id) = data.insert({
            version = #v3({ size = #unknown });
        }) else return assert false;

        assert data.size() == 4;

        suite(
            "searching with nested variant fields",
            func() {

                test(
                    "search via indexed fields",
                    func() {

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(42)))
                        ) == #ok([(v1_id, { version = #v1({ a = 42; b = "hello" }) })]);

                        Debug.print(
                            "searching for version.v3.size.known == 32: " # debug_show (
                                data.search(
                                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(32)))
                                )
                            )
                        );

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(32)))
                        ) == #ok([(v3_known_id, { version = #v3({ size = #known(32) }) })]);

                    },
                );

                test(
                    "search via non indexed fields",
                    func() {

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v2.d", #eq(#Bool(true)))
                        ) == #ok([(v2_id, { version = #v2({ c = "world"; d = true }) })]);

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v2.d", #exists)
                        ) == #ok([(v2_id, { version = #v2({ c = "world"; d = true }) })]);

                        Debug.print(
                            "exists: " # debug_show (
                                data.search(
                                    ZenDB.QueryBuilder().Where("version.v2.d", #exists)
                                )
                            )
                        );

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size", #eq(#Text("unknown")))
                        ) == #ok([(v3_unknown_id, { version = #v3({ size = #unknown }) })]);

                    },
                );

                test(
                    "search for variants by their tags ",
                    func() {
                        assert data.search(
                            ZenDB.QueryBuilder().Where("version", #eq(#Text("v1")))
                        ) == #ok([(v1_id, { version = #v1({ a = 42; b = "hello" }) })]);

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version", #eq(#Text("v2")))
                        ) == #ok([(v2_id, { version = #v2({ c = "world"; d = true }) })]);

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version", #eq(#Text("v3")))
                        ) == #ok([
                            (v3_known_id, { version = #v3({ size = #known(32) }) }),
                            (v3_unknown_id, { version = #v3({ size = #unknown }) }),
                        ]);

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size", #eq(#Text("unknown")))
                        ) == #ok([(v3_unknown_id, { version = #v3({ size = #unknown }) })]);

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v3.size", #eq(#Text("known")))
                        ) == #ok([(v3_known_id, { version = #v3({ size = #known(32) }) })]);

                    },
                );
            },
        );

    },

);
