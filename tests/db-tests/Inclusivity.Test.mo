// @testmode wasi
// crafted by claude-3-sonnet-20240229

import Blob "mo:base/Blob";
import Debug "mo:base/Debug";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Buffer "mo:base/Buffer";
import Option "mo:base/Option";
import Result "mo:base/Result";
import Order "mo:base/Order";

import { test; suite } "mo:test";
import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde/Candid";

import ZenDB "../../src";
import SchemaMap "../../src/Collection/SchemaMap";
import CandidMap "../../src/CandidMap";
import { Orchid } "../../src/Collection/Orchid";
import ZenDBSuite "../test-utils/TestFramework";

let fuzz = Fuzz.fromSeed(0x7eadbeef);
let { QueryBuilder } = ZenDB;

ZenDBSuite.newSuite(
    "ZenDB: Inclusivity Tests",
    ?{ ZenDBSuite.withAndWithoutIndex with log_level = #Debug },
    func inclusivity_tests(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {

        // Define schemas for test collections
        let NumericSchema : ZenDB.Types.Schema = #Record([
            ("id", #Nat),
            ("int_val", #Int),
            ("float_val", #Float),
            ("unindexed_nat", #Nat),
            ("unindexed_int", #Int),
            ("unindexed_float", #Float),
        ]);

        let TextSchema : ZenDB.Types.Schema = #Record([
            ("id", #Nat),
            ("text_val", #Text),
            ("unindexed_text", #Text),
            ("case_sensitive", #Text),
        ]);

        let EdgeCaseSchema : ZenDB.Types.Schema = #Record([
            ("id", #Nat),
            ("opt_field", #Option(#Nat)),
            ("text_field", #Text),
            ("zero_val", #Nat),
            ("min_int", #Int),
            ("max_int", #Int),
        ]);

        type NumericDoc = {
            id : Nat;
            int_val : Int;
            float_val : Float;
            unindexed_nat : Nat;
            unindexed_int : Int;
            unindexed_float : Float;
        };

        type TextDoc = {
            id : Nat;
            text_val : Text;
            unindexed_text : Text;
            case_sensitive : Text;
        };

        type EdgeDoc = {
            id : Nat;
            opt_field : ?Nat;
            text_field : Text;
            zero_val : Nat;
            min_int : Int;
            max_int : Int;
        };

        // Create candify functions
        let candify_numeric = {
            from_blob = func(blob : Blob) : ?NumericDoc {
                from_candid (blob);
            };
            to_blob = func(doc : NumericDoc) : Blob { to_candid (doc) };
        };

        let candify_text = {
            from_blob = func(blob : Blob) : ?TextDoc {
                from_candid (blob);
            };
            to_blob = func(doc : TextDoc) : Blob { to_candid (doc) };
        };

        let candify_edge = {
            from_blob = func(blob : Blob) : ?EdgeDoc {
                from_candid (blob);
            };
            to_blob = func(doc : EdgeDoc) : Blob { to_candid (doc) };
        };

        // Create collections
        let #ok(numeric_collection) = zendb.createCollection<NumericDoc>("numeric_test", NumericSchema, candify_numeric, null) else return assert false;
        let #ok(text_collection) = zendb.createCollection<TextDoc>("text_test", TextSchema, candify_text, null) else return assert false;
        let #ok(edge_collection) = zendb.createCollection<EdgeDoc>("edge_test", EdgeCaseSchema, candify_edge, null) else return assert false;

        // Create indexes
        let #ok(_) = suite_utils.createIndex(numeric_collection.name(), "id_index", [("id", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(numeric_collection.name(), "int_val_index", [("int_val", #Ascending)], null) else return assert false;
        // let #ok(_) = suite_utils.createIndex(numeric_collection.name(), "float_val_index", [("float_val", #Ascending)], null);

        let #ok(_) = suite_utils.createIndex(text_collection.name(), "text_val_index", [("text_val", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(text_collection.name(), "case_sensitive_index", [("case_sensitive", #Ascending)], null) else return assert false;

        let #ok(_) = suite_utils.createIndex(edge_collection.name(), "opt_field_index", [("opt_field", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(edge_collection.name(), "text_field_index", [("text_field", #Ascending)], null) else return assert false;

        // Insert test data for numeric tests
        let #ok(numeric_id_0) = numeric_collection.insert({
            id = 0;
            int_val = 0;
            float_val = 0;
            unindexed_nat = 0;
            unindexed_int = 0;
            unindexed_float = 0;
        }) else return assert false;

        let #ok(numeric_id_1) = numeric_collection.insert({
            id = 1;
            int_val = -1;
            float_val = -1.5;
            unindexed_nat = 1;
            unindexed_int = -1;
            unindexed_float = -1.5;
        }) else return assert false;

        let #ok(numeric_id_2) = numeric_collection.insert({
            id = 5;
            int_val = 5;
            float_val = 5.5;
            unindexed_nat = 5;
            unindexed_int = 5;
            unindexed_float = 5.5;
        }) else return assert false;

        let #ok(numeric_id_3) = numeric_collection.insert({
            id = 10;
            int_val = -10;
            float_val = -10.5;
            unindexed_nat = 10;
            unindexed_int = -10;
            unindexed_float = -10.5;
        }) else return assert false;

        let #ok(numeric_id_4) = numeric_collection.insert({
            id = 100;
            int_val = 100;
            float_val = 100.5;
            unindexed_nat = 100;
            unindexed_int = 100;
            unindexed_float = 100.5;
        }) else return assert false;

        // Insert test data for text tests
        let #ok(text_id_0) = text_collection.insert({
            id = 1;
            text_val = "";
            unindexed_text = "";
            case_sensitive = "";
        }) else return assert false;

        let #ok(text_id_1) = text_collection.insert({
            id = 2;
            text_val = "a";
            unindexed_text = "a";
            case_sensitive = "a";
        }) else return assert false;

        let #ok(text_id_2) = text_collection.insert({
            id = 3;
            text_val = "b";
            unindexed_text = "b";
            case_sensitive = "B";
        }) else return assert false;

        let #ok(text_id_3) = text_collection.insert({
            id = 4;
            text_val = "A";
            unindexed_text = "A";
            case_sensitive = "A";
        }) else return assert false;

        let #ok(text_id_4) = text_collection.insert({
            id = 5;
            text_val = "!@#$%^";
            unindexed_text = "!@#$%^";
            case_sensitive = "!@#$%^";
        }) else return assert false;

        // Insert test data for edge cases
        let #ok(edge_id_0) = edge_collection.insert({
            id = 1;
            opt_field = null;
            text_field = "abbbbbbbbbbbbbbbb";
            zero_val = 0;
            min_int = -2147483648;
            max_int = 2147483647;
        }) else return assert false;

        let #ok(edge_id_1) = edge_collection.insert({
            id = 2;
            opt_field = ?0;
            text_field = " ";
            zero_val = 1;
            min_int = -1000;
            max_int = 1000;
        }) else return assert false;

        let #ok(edge_id_2) = edge_collection.insert({
            id = 3;
            opt_field = ?42;
            text_field = "abc";
            zero_val = 0;
            min_int = -100;
            max_int = 100;
        }) else return assert false;

        // Create a new collection for testing composite keys
        let CompositeSchema : ZenDB.Types.Schema = #Record([
            ("id", #Nat),
            ("category", #Text),
            ("number", #Nat),
            ("data", #Blob),
        ]);

        type CompositeDoc = {
            id : Nat;
            category : Text;
            number : Nat;
            data : Blob;
        };

        // Create candify function
        let candify_composite = {
            from_blob = func(blob : Blob) : ?CompositeDoc { from_candid (blob) };
            to_blob = func(doc : CompositeDoc) : Blob { to_candid (doc) };
        };

        // Create collection
        let #ok(composite_collection) = zendb.createCollection<CompositeDoc>("composite_test", CompositeSchema, candify_composite, null) else return assert false;

        // Create composite index
        let #ok(_) = suite_utils.createIndex(composite_collection.name(), "composite_index", [("category", #Ascending), ("number", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(composite_collection.name(), "blob_index", [("category", #Ascending), ("data", #Ascending)], null) else return assert false;

        // Insert test documents
        let #ok(comp_id_1) = composite_collection.insert({
            id = 1;
            category = "A";
            number = 5;
            data = Text.encodeUtf8("test1");
        }) else return assert false;

        let #ok(comp_id_2) = composite_collection.insert({
            id = 2;
            category = "A";
            number = 10;
            data = Text.encodeUtf8("test2");
        }) else return assert false;

        let #ok(comp_id_3) = composite_collection.insert({
            id = 3;
            category = "B";
            number = 5;
            data = Text.encodeUtf8("test3");
        }) else return assert false;

        let #ok(comp_id_4) = composite_collection.insert({
            id = 4;
            category = "A";
            number = 7;
            data = Text.encodeUtf8("test4");
        }) else return assert false;

        let #ok(comp_id_5) = composite_collection.insert({
            id = 5;
            category = "A";
            number = 15;
            data = Text.encodeUtf8("test5");
        }) else return assert false;

        // For blob testing
        let #ok(blob_id_1) = composite_collection.insert({
            id = 10;
            category = "X";
            number = 1;
            data = Blob.fromArray([12, 32, 45]);
        }) else return assert false;

        let #ok(blob_id_2) = composite_collection.insert({
            id = 11;
            category = "X";
            number = 2;
            data = Blob.fromArray([12, 32, 45, 1]);
        }) else return assert false;

        let #ok(blob_id_3) = composite_collection.insert({
            id = 12;
            category = "X";
            number = 3;
            data = Blob.fromArray([12, 32, 45, 0]);
        }) else return assert false;

        let #ok(blob_id_4) = composite_collection.insert({
            id = 13;
            category = "X";
            number = 4;
            data = Blob.fromArray([12, 32, 44]);
        }) else return assert false;

        suite(
            "Numeric inclusivity operations",
            func() {
                test(
                    "#eq operator matches exact values",
                    func() {
                        // Test equality on indexed fields
                        let result1 = numeric_collection.search(
                            QueryBuilder().Where("id", #eq(#Nat(5)))
                        );
                        assert Result.isOk(result1);
                        let #ok(data1) = result1 else return assert false;
                        assert data1.size() == 1;
                        assert data1[0].0 == numeric_id_2;

                        // Test equality on non-indexed fields
                        let result2 = numeric_collection.search(
                            QueryBuilder().Where("unindexed_nat", #eq(#Nat(5)))
                        );
                        assert Result.isOk(result2);
                        let #ok(data2) = result2 else return assert false;
                        assert data2.size() == 1;
                        assert data2[0].0 == numeric_id_2;
                    },
                );

                test(
                    "#not_(#eq) operator excludes specific values",
                    func() {
                        // Test not-equal on indexed fields
                        let result1 = numeric_collection.search(
                            QueryBuilder().Where("id", #not_(#eq(#Nat(5))))
                        );
                        assert Result.isOk(result1);
                        let #ok(data1) = result1 else return assert false;
                        assert data1.size() == 4;
                        assert Array.find<Nat>(
                            Array.map<(Nat, NumericDoc), Nat>(
                                data1,
                                func((id, _)) { id },
                            ),
                            func(id) { id == numeric_id_2 },
                        ) == null;
                    },
                );

                test(
                    "#lt operator selects values less than target",
                    func() {
                        // Test less than
                        let result = numeric_collection.search(
                            QueryBuilder().Where("id", #lt(#Nat(5)))
                        );
                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 2;

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_0 }) == ?numeric_id_0;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_1 }) == ?numeric_id_1;
                    },
                );

                test(
                    "#lte operator selects values less than or equal to target",
                    func() {
                        // Test less than or equal
                        let result = numeric_collection.search(
                            QueryBuilder().Where("id", #lte(#Nat(5)))
                        );
                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 3;

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_0 }) == ?numeric_id_0;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_1 }) == ?numeric_id_1;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_2 }) == ?numeric_id_2;
                    },
                );

                test(
                    "#gt operator selects values greater than target",
                    func() {
                        // Test greater than
                        let result = numeric_collection.search(
                            QueryBuilder().Where("id", #gt(#Nat(5)))
                        );
                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 2;

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_3 }) == ?numeric_id_3;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_4 }) == ?numeric_id_4;
                    },
                );

                test(
                    "#gte operator selects values greater than or equal to target",
                    func() {
                        // Test greater than or equal
                        let result = numeric_collection.search(
                            QueryBuilder().Where("id", #gte(#Nat(5)))
                        );
                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 3;

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_2 }) == ?numeric_id_2;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_3 }) == ?numeric_id_3;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_4 }) == ?numeric_id_4;
                    },
                );

                test(
                    "Range queries with multiple filters",
                    func() {
                        // Combine multiple filters for range query
                        let result = numeric_collection.search(
                            QueryBuilder().Where("int_val", #gte(#Int(-5))).Where("int_val", #lte(#Int(5)))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show ({ data = data }));
                        assert data.size() == 3;

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        Debug.print(debug_show ({ ids = ids }));

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_0 }) == ?numeric_id_0;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_1 }) == ?numeric_id_1;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_2 }) == ?numeric_id_2;
                    },
                );

                test(
                    "#between operator for inclusive range",
                    func() {
                        // Test between operator
                        let result = numeric_collection.search(
                            QueryBuilder().Where("id", #between(#Nat(1), #Nat(10)))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 3; // ids 1, 2, and 3

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_1 }) == ?numeric_id_1;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_2 }) == ?numeric_id_2;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_3 }) == ?numeric_id_3;
                    },
                );
            },
        );

        suite(
            "Text inclusivity operations",
            func() {
                test(
                    "#eq operator with text values",
                    func() {
                        // Test equality with empty string
                        let result1 = text_collection.search(
                            QueryBuilder().Where("text_val", #eq(#Text("")))
                        );
                        assert Result.isOk(result1);
                        let #ok(data1) = result1 else return assert false;
                        assert data1.size() == 1;
                        assert data1[0].0 == text_id_0;

                        // Test case sensitivity
                        let result2 = text_collection.search(
                            QueryBuilder().Where("case_sensitive", #eq(#Text("A")))
                        );
                        assert Result.isOk(result2);
                        let #ok(data2) = result2 else return assert false;
                        assert data2.size() == 1;
                        assert data2[0].0 == text_id_3;

                        let result3 = text_collection.search(
                            QueryBuilder().Where("case_sensitive", #eq(#Text("a")))
                        );
                        assert Result.isOk(result3);
                        let #ok(data3) = result3 else return assert false;
                        assert data3.size() == 1;
                        assert data3[0].0 == text_id_1;
                    },
                );

                test(
                    "Text ordering operators (#lt, #gt)",
                    func() {
                        // Test less than
                        let result1 = text_collection.search(
                            QueryBuilder().Where("text_val", #lt(#Text("b")))
                        );

                        assert Result.isOk(result1);
                        let #ok(data1) = result1 else return assert false;

                        Debug.print(debug_show { data1 = data1 });
                        assert data1.size() == 4; // "", "a", "A", "!@#$%^"

                        let ids = Array.map<(Nat, TextDoc), Nat>(
                            data1,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == text_id_0 }) == ?text_id_0; // empty string
                        assert Array.find<Nat>(ids, func(id) { id == text_id_1 }) == ?text_id_1; // "a"
                        assert Array.find<Nat>(ids, func(id) { id == text_id_3 }) == ?text_id_3; // "A"
                        assert Array.find<Nat>(ids, func(id) { id == text_id_4 }) == ?text_id_4; // "!@#$%^"

                        // Test greater than
                        let result2 = text_collection.search(
                            QueryBuilder().Where("text_val", #gt(#Text("a")))
                        );
                        assert Result.isOk(result2);
                        let #ok(data2) = result2 else return assert false;

                        // Should include "b"
                        assert data2.size() > 0;
                        assert Array.find<Nat>(
                            Array.map<(Nat, TextDoc), Nat>(
                                data2,
                                func((id, _)) { id },
                            ),
                            func(id) { id == text_id_2 }, // "b"
                        ) == ?text_id_2;
                    },
                );

                test(
                    "Text range queries",
                    func() {
                        // Test range query on text
                        let result = text_collection.search(
                            QueryBuilder().Where("text_val", #gte(#Text("a"))).Where("text_val", #lte(#Text("b")))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 2; // "a" and "b"

                        let ids = Array.map<(Nat, TextDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == text_id_1 }) == ?text_id_1; // "a"
                        assert Array.find<Nat>(ids, func(id) { id == text_id_2 }) == ?text_id_2; // "b"
                    },
                );

                test(
                    "Special character handling",
                    func() {
                        // Test equality with special characters
                        let result = text_collection.search(
                            QueryBuilder().Where("text_val", #eq(#Text("!@#$%^")))
                        );
                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 1;
                        assert data[0].0 == text_id_4;
                    },
                );
            },
        );

        suite(
            "Edge case inclusivity operations",
            func() {
                test(
                    "Null option equality",
                    func() {
                        // Test equality with null option
                        let result = edge_collection.search(
                            QueryBuilder().Where("opt_field", #eq(#Null))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show { data = data });

                        for (doc in edge_collection.vals()) {
                            let blob = candify_edge.to_blob(doc);
                            let #ok(candid) = Candid.decode(blob, ["opt_field"], null) else return assert false;
                            Debug.print(debug_show { candid = candid });
                            let schema_map = SchemaMap.new(EdgeCaseSchema);

                            let candid_map = CandidMap.new(schema_map, 0, candid[0]);
                            let opt_field = CandidMap.get(candid_map, schema_map, "opt_field");
                            Debug.print(debug_show { opt_field = opt_field });

                        };

                        assert data.size() == 1;
                        assert data[0].0 == edge_id_0;
                    },
                );

                test(
                    "Space string equality",
                    func() {
                        // Test equality with space (' ') string
                        let result = edge_collection.search(
                            QueryBuilder().Where("text_field", #eq(#Text(" ")))
                        );
                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show ({ data = data }));

                        assert data.size() == 1;
                        assert data[0].0 == edge_id_1;
                        assert data[0].1.text_field == " ";
                    },
                );

                test(
                    "Zero value equality",
                    func() {
                        // Test equality with zero
                        let result = edge_collection.search(
                            QueryBuilder().Where("zero_val", #eq(#Nat(0)))
                        );
                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 2;

                        let ids = Array.map<(Nat, EdgeDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == edge_id_0 }) == ?edge_id_0;
                        assert Array.find<Nat>(ids, func(id) { id == edge_id_2 }) == ?edge_id_2;
                    },
                );

                test(
                    "Extreme numeric values",
                    func() {
                        // Test with extreme min value
                        let result1 = edge_collection.search(
                            QueryBuilder().Where("min_int", #lt(#Int(-1000)))
                        );
                        assert Result.isOk(result1);
                        let #ok(data1) = result1 else return assert false;
                        assert data1.size() == 1;
                        assert data1[0].0 == edge_id_0;

                        // Test with extreme max value
                        let result2 = edge_collection.search(
                            QueryBuilder().Where("max_int", #gt(#Int(1000)))
                        );
                        assert Result.isOk(result2);
                        let #ok(data2) = result2 else return assert false;
                        assert data2.size() == 1;
                        assert data2[0].0 == edge_id_0;
                    },
                );

                test(
                    "Text closest string smaller than target",
                    func() {
                        // Test closest string smaller than target
                        let result = edge_collection.search(
                            QueryBuilder().Where("text_field", #lt(#Text("abc"))).Sort("text_field", #Descending)
                        );
                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;

                        assert data.size() == 2;

                        assert data[0].0 == edge_id_0;
                        assert data[0].1.text_field == "abbbbbbbbbbbbbbbb";

                        assert data[1].0 == edge_id_1;
                    },
                );
            },
        );

        suite(
            "Logical operator combinations",
            func() {
                test(
                    "AND combination of filters",
                    func() {
                        // Test combining filters with implicit AND
                        let result = numeric_collection.search(
                            QueryBuilder().Where("id", #gte(#Nat(5))).Where("int_val", #gt(#Int(0)))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 2; // ids 2 and 4

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_2 }) == ?numeric_id_2;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_4 }) == ?numeric_id_4;
                    },
                );

                test(
                    "OR combination of filters",
                    func() {
                        // Test combining filters with OR
                        let result = numeric_collection.search(
                            QueryBuilder().Where("id", #eq(#Nat(0))).Or("id", #eq(#Nat(100)))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 2; // ids 0 and 4

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_0 }) == ?numeric_id_0;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_4 }) == ?numeric_id_4;
                    },
                );

                test(
                    "Complex filter combination",
                    func() {
                        // Test complex combination of filters
                        let result = numeric_collection.search(
                            QueryBuilder().Where("int_val", #lt(#Int(0))).Or("float_val", #gt(#Float(50.0)))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        assert data.size() == 3;

                        let ids = Array.map<(Nat, NumericDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_1 }) == ?numeric_id_1;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_3 }) == ?numeric_id_3;
                        assert Array.find<Nat>(ids, func(id) { id == numeric_id_4 }) == ?numeric_id_4;
                    },
                );
            },
        );

        suite(
            "Composite key operations",
            func() {
                test(
                    "Numeric next value operations",
                    func() {
                        // Test greater than on category "A" and number > 7
                        // This should internally use get_next_value() to transform the query
                        let result = composite_collection.search(
                            QueryBuilder().Where("category", #eq(#Text("A"))).And("number", #gt(#Nat(7)))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show ({ gt_nat_data = data }));

                        // Should include A-10 and A-15, but not A-5 or A-7
                        assert data.size() == 2;

                        let ids = Array.map<(Nat, CompositeDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == comp_id_2 }) == ?comp_id_2; // A-10
                        assert Array.find<Nat>(ids, func(id) { id == comp_id_5 }) == ?comp_id_5; // A-15
                    },
                );

                test(
                    "Numeric prev value operations",
                    func() {
                        // Test less than on category "A" and number < 10
                        // This should internally use get_prev_value() to transform the query
                        let result = composite_collection.search(
                            QueryBuilder().Where("category", #eq(#Text("A"))).And("number", #lt(#Nat(10)))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show ({ lt_nat_data = data }));

                        // Should include A-5 and A-7, but not A-10 or A-15
                        assert data.size() == 2;

                        let ids = Array.map<(Nat, CompositeDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == comp_id_1 }) == ?comp_id_1; // A-5
                        assert Array.find<Nat>(ids, func(id) { id == comp_id_4 }) == ?comp_id_4; // A-7
                    },
                );

                test(
                    "Blob next value operations",
                    func() {
                        // Test greater than with blobs
                        // Should match blobs that are lexicographically greater than [12, 32, 45]
                        // get_next_value() will append a 0 to the blob
                        let result = composite_collection.search(
                            QueryBuilder().Where("category", #eq(#Text("X"))).And("data", #gt(#Blob(Blob.fromArray([12, 32, 45]))))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show ({ gt_blob_data = data }));

                        // Should include [12, 32, 45, 0], [12, 32, 45, 1] but not [12, 32, 44]
                        assert data.size() == 2;

                        let ids = Array.map<(Nat, CompositeDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == blob_id_3 }) == ?blob_id_3; // [12, 32, 45, 0]
                        assert Array.find<Nat>(ids, func(id) { id == blob_id_2 }) == ?blob_id_2; // [12, 32, 45, 1]
                    },
                );

                test(
                    "Blob prev value operations",
                    func() {
                        // Test less than with blobs
                        // get_prev_value() will transform the query to match lexicographically smaller blobs
                        let result = composite_collection.search(
                            QueryBuilder().Where("category", #eq(#Text("X"))).And("data", #lt(#Blob(Blob.fromArray([12, 32, 45]))))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show ({ lt_blob_data = data }));

                        // Should include [12, 32, 44] but not [12, 32, 45, x]
                        assert data.size() == 1;

                        let ids = Array.map<(Nat, CompositeDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == blob_id_4 }) == ?blob_id_4; // [12, 32, 44]
                    },
                );

                test(
                    "Edge case: prev value for blob ending in zero",
                    func() {
                        // Test the edge case where we have a blob ending with 0
                        // For [12, 32, 45, 0], get_prev_value() should remove the 0 and return [12, 32, 45]
                        let result = composite_collection.search(
                            QueryBuilder().Where("category", #eq(#Text("X"))).And("data", #lt(#Blob(Blob.fromArray([12, 32, 45, 0]))))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show ({ lt_blob_zero_data = data }));

                        // Should include [12, 32, 44] and [12, 32, 45] but not [12, 32, 45, x]
                        assert data.size() == 2;

                        let ids = Array.map<(Nat, CompositeDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == blob_id_1 }) == ?blob_id_1; // [12, 32, 45]
                        assert Array.find<Nat>(ids, func(id) { id == blob_id_4 }) == ?blob_id_4; // [12, 32, 44]
                    },
                );

                test(
                    "Composite key range queries",
                    func() {
                        // Test range query with composite keys
                        // Should match documents where category="A" and 5 < number < 15
                        let result = composite_collection.search(
                            QueryBuilder().Where("category", #eq(#Text("A"))).And("number", #gt(#Nat(5))).And("number", #lt(#Nat(15)))
                        );

                        assert Result.isOk(result);
                        let #ok(data) = result else return assert false;
                        Debug.print(debug_show ({ range_query_data = data }));

                        // Should include only A-7 and A-10
                        assert data.size() == 2;

                        let ids = Array.map<(Nat, CompositeDoc), Nat>(
                            data,
                            func((id, _)) { id },
                        );

                        assert Array.find<Nat>(ids, func(id) { id == comp_id_2 }) == ?comp_id_2; // A-10
                        assert Array.find<Nat>(ids, func(id) { id == comp_id_4 }) == ?comp_id_4; // A-7
                    },
                );
            },
        );

    },
);
