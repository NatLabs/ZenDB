// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Array "mo:base/Array";

import ZenDB "../../src/EmbeddedInstance";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Map "mo:map/Map";
import ZenDBSuite "../test-utils/TestFramework";

type Data = {
    name : Text;
    description : Text;
    is_active : Bool;
};

let DataSchema : ZenDB.Types.Schema = #Record([
    ("name", #Text),
    ("description", #Text),
    ("is_active", #Bool),
]);

let data_type_to_candid : ZenDB.Types.Candify<Data> = {
    from_blob = func(blob : Blob) : ?Data { from_candid (blob) };
    to_blob = func(c : Data) : Blob { to_candid (c) };
};

ZenDBSuite.newSuite(
    "Text CompositeIndex Tests",
    ?{ ZenDBSuite.onlyWithIndex with log_level = #Error },
    func suite_setup(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {

        let #ok(collection) = zendb.createCollection<Data>("data", DataSchema, data_type_to_candid, null) else return assert false;

        suite_utils.indexOnlyFns(
            func() {
                let #ok(_) = collection.createTextIndex("name_idx", "name", #basic) else return assert false;
            }
        );

        test(
            "Insert documents",
            func() {
                let docs = [
                    {
                        name = "Alice Johnson";
                        description = "Alice is a software engineer who specializes in distributed systems and blockchain technology.";
                        is_active = true;
                    },
                    {
                        name = "Daniel Carter";
                        description = "Daniel loves hiking and outdoor adventures in the Rocky Mountains and Pacific Northwest.";
                        is_active = false;
                    },
                    {
                        name = "Charlotte Williams";
                        description = "Charlotte is a data scientist working with machine learning algorithms and predictive analytics.";
                        is_active = true;
                    },
                    {
                        name = "Daniel Martinez";
                        description = "Daniel enjoys painting abstract art and teaching creative workshops at local community centers.";
                        is_active = false;
                    },
                    {
                        name = "Eve Martinez";
                        description = "Eve is a cybersecurity expert specializing in network security and threat detection systems.";
                        is_active = true;
                    },
                    {
                        name = "Chen Thompson";
                        description = "Chen is a software engineer working on mobile applications and user interface design.";
                        is_active = true;
                    },
                    {
                        name = "Isabella Chen";
                        description = "Isabella is a data scientist focusing on natural language processing and artificial intelligence.";
                        is_active = false;
                    },
                ];

                for (doc in docs.vals()) {
                    let #ok(_) = collection.insert(doc) else return assert false;
                };

                assert collection.size() == docs.size();

                // search for all documents with "Carter" in the name
                let #ok(res) = collection.search(
                    ZenDB.QueryBuilder().Where("name", #eq(#Text("Carter")))
                );

                Debug.print("response: " # debug_show (res));

                // search for all documents with "Chen" in the name

                // let #ok(documents_owned_by_chen) = collection.search(
                //     ZenDB.QueryBuilder().Where("name", #text(#contains("Chen")))
                // ) else return assert false;

                // Debug.print("Documents owned by Chen: " # debug_show (documents_owned_by_chen));

                // let #ok(results) = collection.search(
                //     ZenDB.QueryBuilder().Where("name", #eq(#Text("Chen")))
                // ) else return assert false;

                // // search for all documents with "Chen" as their last name
                // let #ok(last_name_results) = collection.search(
                //     ZenDB.QueryBuilder().Where(
                //         "name",
                //         #text(
                //             #pos("Chen", #gte(1))
                //         ),
                //     )
                // ) else return assert false;

            },
        );

    },

);
