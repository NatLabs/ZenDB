import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";

import ZenDB "../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";

actor {
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

    let #ok(item1_id) = data.insert({ version = #v1({ a = 42; b = "hello" }) });
    let #ok(item2_id) = data.insert({ version = #v2({ c = "world"; d = true }) });
    let #ok(item3_id) = data.insert({ version = #v3({ size = #known(32) }) });
    let #ok(item4_id) = data.insert({ version = #v3({ size = #unknown }) });

    public func runTests() {
        suite(
            "Update Tests",
            func() {
                test(
                    "#doc update by id",
                    func() {

                        let #ok(_) = data.updateById(
                            item1_id,
                            #doc({ version = #v1({ a = 0; b = "text" }) }),
                        );

                        assert #ok([]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v1.a", #eq(#Nat(42)))
                        );

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v1.b", #eq(#Text("hello")))
                        ) == #ok([(item1_id, { version = #v1({ a = 0; b = "text" }) })]);

                    },
                );

                test(
                    "#ops update by id",
                    func() {
                        let #ok(_) = data.updateById(
                            item2_id,
                            #ops({
                                version = #v2({
                                    c = #set(#Text("hello"));
                                    d = #set(#Bool(false));
                                });
                            }),
                        );

                        assert #ok([]) == data.search(
                            ZenDB.QueryBuilder().Where("version.v2.c", #eq(#Text("world")))
                        );

                        assert data.search(
                            ZenDB.QueryBuilder().Where("version.v2.c", #eq(#Text("hello")))
                        ) == #ok([(item2_id, { version = #v2({ c = "hello"; d = false }) })]);
                    },
                )

            },
        );
    };

};
