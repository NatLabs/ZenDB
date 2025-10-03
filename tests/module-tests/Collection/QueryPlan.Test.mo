// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Blob "mo:base@0.16.0/Blob";
import Text "mo:base@0.16.0/Text";
import Array "mo:base@0.16.0/Array";
import Principal "mo:base@0.16.0/Principal";
import Option "mo:base@0.16.0/Option";
import Iter "mo:base@0.16.0/Iter";

import { test; suite } "mo:test";
import Itertools "mo:itertools@0.2.2/Iter";
import Map "mo:map@9.0.1/Map";
import Fuzz "mo:fuzz";

import ZenDB "../../../src";
import QueryPlan "../../../src/Collection/QueryPlan";
import CollectionUtils "../../../src/Collection/CollectionUtils";
import CandidMap "../../../src/CandidMap";
import CompositeIndex "../../../src/Collection/Index/CompositeIndex";

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

suite(
    "Query Plan Tests",
    func() {
        let canister_id = fuzz.principal.randomPrincipal(29);
        let zendb = ZenDB.newStableStore(canister_id, null);
        let db = ZenDB.launchDefaultDB(zendb);

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

        let candify_document : ZenDB.Types.Candify<RecordWithAllTypes> = {
            to_blob = func(data : RecordWithAllTypes) : Blob = to_candid (data);
            from_blob = func(blob : Blob) : ?RecordWithAllTypes = from_candid (blob);
        };

        let default_index_scan = {
            filter_bounds = ([], []);
            index_name = "";
            interval = (0, 0);
            requires_additional_filtering = false;
            requires_additional_sorting = false;
            scan_bounds = ([], []);
            simple_operations = [];
            sorted_in_reverse = false;
        };

        test(
            "Query Plan on Collection with no indexes",
            func() {

                let #ok(collection) = db.createCollection("query_plan_test", RecordWithAllTypesSchema, candify_document, null) else return assert false;

                let stable_collection = collection._get_stable_state();

                let query_plan = QueryPlan.create_query_plan(
                    stable_collection,
                    ZenDB.QueryBuilder().Where(
                        "text",
                        #eq(#Text("rand")),
                    ).build().query_operations,
                    null,
                    null,
                );

                assert query_plan == {
                    is_and_operation = true;
                    scans = [
                        #FullScan({
                            filter_bounds = (
                                [("text", ?(#Inclusive(#Text("rand"))))],
                                [("text", ?(#Inclusive(#Text("rand"))))],
                            );
                            requires_additional_filtering = true;
                            requires_additional_sorting = false;
                            scan_bounds = ([], []);
                        })
                    ];
                    simple_operations = [("text", #eq(#Text("rand")))];
                    subplans = [];
                }

            },
        );

        test(
            "Query Plan on Collection with indexes",
            func() {

                let #ok(collection) = db.createCollection("query_plan_test", RecordWithAllTypesSchema, candify_document, null) else return assert false;
                let #ok(_) = collection.createIndex("text_idx", [("text", #Ascending)], null) else return assert false;

                let stable_collection = collection._get_stable_state();

                let query_plan = QueryPlan.create_query_plan(
                    stable_collection,
                    ZenDB.QueryBuilder().Where(
                        "text",
                        #eq(#Text("rand")),
                    ).build().query_operations,
                    null,
                    null,
                );

                Debug.print("Query Plan: " # debug_show (query_plan));

                assert query_plan == {
                    is_and_operation = true;
                    scans = [
                        #IndexScan({
                            default_index_scan with
                            index_name = "text_idx";
                            scan_bounds = (
                                [("text", ?(#Inclusive(#Text("rand")))), (":id", ?#Inclusive(#Minimum))],
                                [("text", ?(#Inclusive(#Text("rand")))), (":id", ?#Inclusive(#Maximum))],
                            );
                            simple_operations = [("text", #eq(#Text("rand")))];

                        })
                    ];
                    simple_operations = [("text", #eq(#Text("rand")))];
                    subplans = [];
                }

            },
        );

    },
);
