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

type Version = {
    #v1 : { a : Nat; b : Text };
    #v2 : { c : Text; d : Bool };
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
// let #ok(_) = data.create_index(["version"]);
let #ok(_) = data.create_index(["version.v1.a"]);

let #ok(_) = data.insert({ version = #v1({ a = 42; b = "hello" }) });
let #ok(_) = data.insert({ version = #v2({ c = "world"; d = true }) });

// assert data.search(
//     ZenDB.QueryBuilder().Where("version", #eq(#Text("v1")))
// ) == #ok([(0, { version = #v1({ a = 42; b = "hello" }) })]);

// assert data.search(
//     ZenDB.QueryBuilder().Where("version", #eq(#Text("v2")))
// ) == #ok([(1, { version = #v2({ c = "world"; d = true }) })]);

// assert data.search(
//     ZenDB.QueryBuilder().Where("version", #eq(#Text("v3")))
// ) == #ok([]);

// data.search(
//     ZenDB.QueryBuilder().Where("version.v1.a", #Not(#eq(#Null)))
// ) |> Debug.print(debug_show (_));
