import Debug "mo:base@0.16.0/Debug";
import Iter "mo:base@0.16.0/Iter";
import Text "mo:base@0.16.0/Text";
import Nat "mo:base@0.16.0/Nat";
import Map "mo:map@9.0.1/Map";

import { test; suite } "mo:test";
import Candid "mo:serde@3.3.2/Candid";
import SchemaMap "../../../src/Collection/SchemaMap";
import T "../../../src/Types";
import ZenDB "../../../src";

suite(
    "SchemaMap",
    func() {
        // Define a complex schema for testing
        let schema : T.Schema = #Record([
            ("name", #Text),
            ("age", #Nat),
            ("email", #Text),
            (
                "details",
                #Record([
                    ("phone", #Text),
                    ("address", #Text),
                    (
                        "settings",
                        #Option(#Record([("theme", #Text), ("notifications", #Bool)])),
                    ),
                ]),
            ),
            (
                "version",
                #Variant([
                    ("v1", #Nat),
                    ("v2", #Text),
                    (
                        "v3",
                        #Record([("major", #Nat), ("minor", #Nat), ("patch", #Nat)]),
                    ),
                ]),
            ),
            ("tuple", #Tuple([#Nat, #Text])),
            ("tags", #Array(#Text)),
            ("comments", #Array(#Record([("content", #Text), ("created_at", #Nat)]))),
        ]);

        let schema_map = SchemaMap.new(schema);

        test(
            "new() - creates a valid schema map with correct structure",
            func() {
                // Verify that the schema map was created successfully
                assert Map.size(schema_map.map) > 0;

                // Check that top-level fields were mapped correctly
                assert SchemaMap.get(schema_map, "") == ?schema;
                assert SchemaMap.get(schema_map, "name") == ?#Text;
                assert SchemaMap.get(schema_map, "age") == ?#Nat;
                assert SchemaMap.get(schema_map, "email") == ?#Text;

                // Check nested documents
                assert SchemaMap.get(schema_map, "details") == ?#Record([
                    ("phone", #Text),
                    ("address", #Text),
                    ("settings", #Option(#Record([("theme", #Text), ("notifications", #Bool)]))),
                ]);
                assert SchemaMap.get(schema_map, "details.phone") == ?#Text;
                assert SchemaMap.get(schema_map, "details.address") == ?#Text;

                // Check deeply nested fields
                assert SchemaMap.get(schema_map, "details.settings") == ?#Option(#Record([("theme", #Text), ("notifications", #Bool)]));
                assert SchemaMap.get(schema_map, "details.settings.theme") == ?#Text;
                assert SchemaMap.get(schema_map, "details.settings.notifications") == ?#Bool;
            },
        );

        test(
            "get() - retrieves correct field types at various nesting levels",
            func() {
                // Basic fields
                assert SchemaMap.get(schema_map, "name") == ?#Text;
                assert SchemaMap.get(schema_map, "age") == ?#Nat;

                // Nested fields
                assert SchemaMap.get(schema_map, "details.phone") == ?#Text;
                assert SchemaMap.get(schema_map, "details.settings.theme") == ?#Text;

                // Variant fields
                assert SchemaMap.get(schema_map, "version") == ?#Variant([
                    ("v1", #Nat),
                    ("v2", #Text),
                    ("v3", #Record([("major", #Nat), ("minor", #Nat), ("patch", #Nat)])),
                ]);
                assert SchemaMap.get(schema_map, "version.v1") == ?#Nat;
                assert SchemaMap.get(schema_map, "version.v3.major") == ?#Nat;
                assert SchemaMap.get(schema_map, "version.v3.minor") == ?#Nat;

                // Tuple fields
                assert SchemaMap.get(schema_map, "tuple") == ?#Tuple([#Nat, #Text]);
                assert SchemaMap.get(schema_map, "tuple.0") == ?#Nat;
                assert SchemaMap.get(schema_map, "tuple.1") == ?#Text;

                // Array fields
                assert SchemaMap.get(schema_map, "tags") == ?#Array(#Text);
                assert SchemaMap.get(schema_map, "comments") == ?#Array(#Record([("content", #Text), ("created_at", #Nat)]));

                // Non-existent fields
                assert SchemaMap.get(schema_map, "nonexistent") == null;
                assert SchemaMap.get(schema_map, "details.nonexistent") == null;
                assert SchemaMap.get(schema_map, "version.v4") == null;
                assert SchemaMap.get(schema_map, "tuple.2") == null;
            },
        );

        test(
            "get() - get array type with field path including element index",
            func() {
                // Check that we can get the type of an array element
                assert SchemaMap.get(schema_map, "comments") == ?#Array(#Record([("content", #Text), ("created_at", #Nat)]));
                assert SchemaMap.get(schema_map, "comments.0") == ?(#Record([("content", #Text), ("created_at", #Nat)]));
                assert SchemaMap.get(schema_map, "comments.0.content") == ?#Text;
                assert SchemaMap.get(schema_map, "comments.0.created_at") == ?#Nat;

                // Check that we can get the type of a nested array element
                assert SchemaMap.get(schema_map, "tags") == ?#Array(#Text);
                assert SchemaMap.get(schema_map, "tags.0") == ?(#Text);

                // check nested array element
                var schema_map2 = SchemaMap.new(#Record([("nested", #Array(#Array(#Text)))]));
                assert SchemaMap.get(schema_map2, "nested") == ?#Array(#Array(#Text));
                assert SchemaMap.get(schema_map2, "nested.0") == ?(#Array(#Text));
                // assert SchemaMap.get(schema_map2, "nested.0.0") == ?#Array(#Text);
                assert SchemaMap.get(schema_map2, "nested.0.0.0") == null;

                // check nested array between tuples
                var schema_map3 = SchemaMap.new(#Record([("nested", #Array(#Tuple([#Array(#Text), #Nat])))]));
                assert SchemaMap.get(schema_map3, "nested") == ?#Array(#Tuple([#Array(#Text), #Nat]));

                // for the schema map, we should not be able to get the inner type of a tuple from the element index
                // these cases succeeds however, because the schema map removes the element index from the path after an initial failed lookup
                // and scans the schema map for the parent array type to return. so 'nested.0' becomes 'nested'
                //
                // In this case, we have a conflict as the schema has a tuple type that would conflict with an attempt to get the array element by the index 'nested.0'
                // The tuple type wins out, and we get the value of the element in the tuple type and not the array type
                assert SchemaMap.get(schema_map3, "nested.0") == ?#Array(#Text);

                // This tries to get the type of the first element of the tuple, nested in the first element of the array
                assert SchemaMap.get(schema_map3, "nested.0.0") == ?(#Text);
                assert SchemaMap.get(schema_map3, "nested.0.1") == ?#Nat;
                assert SchemaMap.get(schema_map3, "nested.0.0.0") == null;

            },
        );

        test(
            "validate_schema_constraints() - validates field constraints correctly",
            func() {
                // Define schema constraints for testing
                let constraints : [T.SchemaConstraint] = [
                    #Field("age", [#Min(0), #Max(120)]),
                    #Field("email", [#MaxSize(100)]),
                    #Field("tags", [#MaxSize(10)]),
                    #Field("details.phone", [#Size(10, 20)]),
                    #Unique(["name", "email"]),
                    #Unique(["age"]),
                ];

                // Validate the constraints
                let validation_result = SchemaMap.validate_schema_constraints(schema_map, constraints);

                switch (validation_result) {
                    case (#ok(response)) {
                        // Check field constraints
                        let ?age_constraints = Map.get(response.field_constraints, T.thash, "age");
                        assert age_constraints.size() == 2;
                        assert age_constraints[0] == #Min(0);
                        assert age_constraints[1] == #Max(120);

                        let ?email_constraints = Map.get(response.field_constraints, T.thash, "email");
                        assert email_constraints.size() == 1;
                        assert email_constraints[0] == #MaxSize(100);

                        let ?tags_constraints = Map.get(response.field_constraints, T.thash, "tags");
                        assert tags_constraints.size() == 1;
                        assert tags_constraints[0] == #MaxSize(10);

                        let ?phone_constraints = Map.get(response.field_constraints, T.thash, "details.phone");
                        assert phone_constraints.size() == 1;
                        assert phone_constraints[0] == #Size(10, 20);

                        // Check unique constraints
                        assert response.unique_constraints.size() == 2;
                        assert response.unique_constraints[0] == ["name", "email"];
                        assert response.unique_constraints[1] == ["age"];
                    };
                    case (#err(msg)) {
                        Debug.print("Unexpected error validating schema constraints: " # msg);
                        assert false;
                    };
                };
            },
        );

        test(
            "validate_schema_constraints() - rejects invalid constraints",
            func() {
                // Test invalid field constraint (non-existent field)
                let invalid_field_constraint : [T.SchemaConstraint] = [
                    #Field("nonexistent", [#Min(0)]),
                ];

                let invalid_field_result = SchemaMap.validate_schema_constraints(schema_map, invalid_field_constraint);
                assert switch (invalid_field_result) {
                    case (#err(_)) true;
                    case (#ok(_)) false;
                };

                // Test invalid constraint type (Min on non-numeric field)
                let invalid_constraint_type : [T.SchemaConstraint] = [
                    #Field("name", [#Min(0)]),
                ];

                let invalid_type_result = SchemaMap.validate_schema_constraints(schema_map, invalid_constraint_type);
                assert switch (invalid_type_result) {
                    case (#err(_)) true;
                    case (#ok(_)) false;
                };

                // Test invalid size constraint (MaxSize on numeric field)
                let invalid_size_constraint : [T.SchemaConstraint] = [
                    #Field("age", [#MaxSize(10)]),
                ];

                let invalid_size_result = SchemaMap.validate_schema_constraints(schema_map, invalid_size_constraint);
                assert switch (invalid_size_result) {
                    case (#err(_)) true;
                    case (#ok(_)) false;
                };

                // Test invalid unique constraint (complex type)
                let invalid_unique_constraint : [T.SchemaConstraint] = [
                    #Unique(["details"]),
                ];

                let invalid_unique_result = SchemaMap.validate_schema_constraints(schema_map, invalid_unique_constraint);
                assert switch (invalid_unique_result) {
                    case (#err(_)) true;
                    case (#ok(_)) false;
                };
            },
        );

        test(
            "validate_schema_constraints() - handles nested and option types properly",
            func() {
                // Test constraints on nested fields and option types
                let nested_constraints : [T.SchemaConstraint] = [
                    #Field("details.settings.theme", [#MaxSize(20)]),
                    #Unique(["details.phone", "email"]),
                ];

                let nested_result = SchemaMap.validate_schema_constraints(schema_map, nested_constraints);

                switch (nested_result) {
                    case (#ok(response)) {
                        let ?theme_constraints = Map.get(response.field_constraints, T.thash, "details.settings.theme");
                        assert theme_constraints.size() == 1;
                        assert theme_constraints[0] == #MaxSize(20);

                        assert response.unique_constraints.size() == 1;
                        assert response.unique_constraints[0] == ["details.phone", "email"];
                    };
                    case (#err(msg)) {
                        Debug.print("Unexpected error validating nested schema constraints: " # msg);
                        assert false;
                    };
                };
            },
        );

        test(
            "comprehensive test with complex schema representation",
            func() {
                // Check that the schema map properly flattens complex structures
                let total_entries = Map.size(schema_map.map);

                // Count expected entries (manually verified)
                let expected_entries = 20; // Count of all fields + parent schema

                Debug.print("Total schema map entries: " # Nat.toText(total_entries));

                // The exact number may vary based on implementation but should be at least this many
                assert total_entries >= expected_entries;

                // Test that the schema map can handle a complex path lookup
                assert SchemaMap.get(schema_map, "details.settings.notifications") == ?#Bool;
                assert SchemaMap.get(schema_map, "version.v3.patch") == ?#Nat;
                assert SchemaMap.get(schema_map, "comments") == ?#Array(#Record([("content", #Text), ("created_at", #Nat)]));

                // Test that wrong paths return null
                assert SchemaMap.get(schema_map, "details.settings.theme.color") == null;
                assert SchemaMap.get(schema_map, "version.v3.patch.subpatch") == null;
            },
        );
    },
);
