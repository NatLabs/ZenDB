// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";

import ZenDB "../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";

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
};

type Data = {
    version : Version;
};

let DataSchema : ZenDB.Schema = #Record([(
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

let data_type_to_candid : ZenDB.Candify<Data> = {
    from_blob = func(blob : Blob) : Data {
        let ?c : ?Data = from_candid (blob);
        c;
    };
    to_blob = func(c : Data) : Blob { to_candid (c) };
};

let #ok(data) = zendb.create_collection<Data>("data", DataSchema, data_type_to_candid);
let #ok(_) = data.create_index(["version"]);
let #ok(_) = data.create_index(["version.v1.a"]);
let #ok(_) = data.create_index(["version.v3.size.known"]);

let #ok(_) = data.insert({ version = #v1({ a = 42; b = "hello" }) });
let #ok(_) = data.insert({ version = #v2({ c = "world"; d = true }) });
let #ok(_) = data.insert({ version = #v3({ size = #known(32) }) });
let #ok(_) = data.insert({ version = #v3({ size = #unknown }) });

suite(
    "searching with nested variant fields",
    func() {
        test(
            "search for variants by their tags",
            func() {
                assert data.search(
                    ZenDB.QueryBuilder().Where("version", #eq(#Text("v1")))
                ) == #ok([(0, { version = #v1({ a = 42; b = "hello" }) })]);

                assert data.search(
                    ZenDB.QueryBuilder().Where("version", #eq(#Text("v2")))
                ) == #ok([(1, { version = #v2({ c = "world"; d = true }) })]);

                assert data.search(
                    ZenDB.QueryBuilder().Where("version", #eq(#Text("v3")))
                ) == #ok([
                    (2, { version = #v3({ size = #known(32) }) }),
                    (3, { version = #v3({ size = #unknown }) }),
                ]);

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size", #eq(#Text("unknown")))
                ) == #ok([(3, { version = #v3({ size = #unknown }) })]);

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size", #eq(#Text("known")))
                ) == #ok([(2, { version = #v3({ size = #known(32) }) })]);

            },
        );
        test(
            "search via indexed fields",
            func() {
                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(42)))
                ) == #ok([(0, { version = #v1({ a = 42; b = "hello" }) })]);

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Null))
                ) == #ok([
                    (1, { version = #v2({ c = "world"; d = true }) }),
                    (2, { version = #v3({ size = #known(32) }) }),
                    (3, { version = #v3({ size = #unknown }) }),
                ]);

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size.known", #eq(#Nat(32)))
                ) == #ok([(2, { version = #v3({ size = #known(32) }) })]);

            },
        );

        test(
            "search via non indexed fields",
            func() {

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v2.d", #eq(#Bool(true)))
                ) == #ok([(1, { version = #v2({ c = "world"; d = true }) })]);

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v2.d", #eq(#Null))
                ) == #ok([
                    (0, { version = #v1({ a = 42; b = "hello" }) }),
                    (2, { version = #v3({ size = #known(32) }) }),
                    (3, { version = #v3({ size = #unknown }) }),
                ]);

                assert data.search(
                    ZenDB.QueryBuilder().Where("version.v3.size", #eq(#Text("unknown")))
                ) == #ok([(3, { version = #v3({ size = #unknown }) })]);

            },
        );
    },
);
