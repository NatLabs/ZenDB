// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Principal "mo:base/Principal";
import Option "mo:base/Option";
import Iter "mo:base/Iter";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Map "mo:map/Map";
import Fuzz "mo:fuzz";

import ZenDB "../../src";
import CollectionUtils "../../src/Collection/Utils";
import CandidMap "../../src/CandidMap";
import Index "../../src/Collection/Index";

import ZenDBSuite "../test-utils/TestFramework";

let fuzz = Fuzz.fromSeed(0x7eadbeef);

let { QueryBuilder } = ZenDB;

let limit = 1_000;

let indexible_fields = [
    "text",
    "nat",
    "nat8",
    "nat16",
    "nat32",
    "nat64",
    "int",
    "int8",
    "int16",
    "int32",
    "int64",
    "float",
    "principal",
    "blob",
    "bool", // supported but has a low cardinality, has a lot of duplicates and returns a lot of results, which causes us to hit the memory limit faster
];

ZenDBSuite.newSuite(
    "Create, Read, Update, Delete (CRUD) operations",
    ?{
        ZenDBSuite.onlyWithIndex with log_level = #Error;
    },
    func zendb_suite(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {
        type RecordWithAllTypes = {
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

        let RecordWithAllTypesSchema = #Record([
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

        let candify_record : ZenDB.Types.Candify<RecordWithAllTypes> = {
            to_blob = func(data : RecordWithAllTypes) : Blob = to_candid (data);
            from_blob = func(blob : Blob) : ?RecordWithAllTypes = from_candid (blob);
        };

        let inputs = Buffer.Buffer<RecordWithAllTypes>(limit);

        for (i in Itertools.range(0, limit)) {
            let record : RecordWithAllTypes = {
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

            inputs.add(record);
        };

        let input_ids = Buffer.Buffer<Nat>(limit);

        suite(
            "CRUD operations",
            func() {

                let #ok(crud_collection) = zendb.createCollection("CRUD", RecordWithAllTypesSchema, candify_record, null);
                let schema_map = crud_collection._get_schema_map();
                let candid_maps = Map.new<Nat, ZenDB.Types.CandidMap>();

                for (field in indexible_fields.vals()) {
                    let #ok(_) = suite_utils.createIndex(crud_collection.name(), field # "_idx", [(field, #Ascending)], null) else return assert false;
                };

                suite(
                    "Create",
                    func() {
                        for (record in inputs.vals()) {
                            let #ok(id) = crud_collection.insert(record) else return assert false;
                            assert crud_collection.get(id) == ?record;

                            let candid_record = CollectionUtils.decode_candid_blob(
                                crud_collection._get_stable_state(),
                                candify_record.to_blob(record),
                            );

                            let candid_map = CandidMap.new(crud_collection._get_schema_map(), id, candid_record);
                            ignore Map.put(candid_maps, Map.nhash, id, candid_map);
                        };

                    },
                );

                suite(
                    "Read: #eq",
                    func() {
                        for ((id, record) in crud_collection.entries()) {

                            let ?candid_map = Map.get(candid_maps, Map.nhash, id) else return assert false;

                            assert CandidMap.get(candid_map, schema_map, ZenDB.Constants.RECORD_ID) == ?#Nat(id);

                            for (field in indexible_fields.vals()) {
                                let ?field_value = CandidMap.get(candid_map, schema_map, field) else return assert false;
                                // Debug.print("id" # debug_show id # " Field " # field # " value: " # debug_show field_value);

                                let #ok(results) = crud_collection.search(
                                    QueryBuilder().Where(
                                        field,
                                        #eq(field_value),
                                    ).And(
                                        ZenDB.Constants.RECORD_ID,
                                        #eq(#Nat(id)),
                                    )
                                ) else return assert false;

                                // Debug.print("Search result for field " # field # ": " # debug_show (record) # " -> " # debug_show (found_record));
                                assert results[0] == (id, record);
                            };

                        };

                    },
                );

            },
        );

    },
);
