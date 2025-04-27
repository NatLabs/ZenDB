import Nat "mo:base/Nat";
import Buffer "mo:base/Buffer";
import Text "mo:base/Text";

import Map "mo:map/Map";
import T "../Types";

import Itertools "mo:itertools/Iter";

module {

    public type SchemaMap = Map.Map<Text, T.Schema>;

    public func new(schema : T.Schema) : T.SchemaMap {
        let schema_map = Map.new<Text, T.Schema>();

        func join_field_names(
            field_name : Text,
            nested_field_name : Text,
        ) : Text {
            if (field_name == "") {
                nested_field_name;
            } else {
                field_name # "." # nested_field_name;
            };
        };

        func store_field_type(
            schema_map : SchemaMap,
            field_name : Text,
            field_type : T.CandidType,
            has_option_type_in_path : Bool,
        ) {

            switch (field_type) {
                case (#Record(fields) or #Map(fields)) {
                    for ((nested_field_name, nested_field_type) in fields.vals()) {
                        store_field_type(
                            schema_map,
                            join_field_names(field_name, nested_field_name),
                            nested_field_type,
                            has_option_type_in_path,
                        );
                    };
                };
                case (#Tuple(types)) {
                    for ((tuple_index, tuple_type) in Itertools.enumerate(types.vals())) {
                        store_field_type(
                            schema_map,
                            join_field_names(field_name, Nat.toText(tuple_index)),
                            tuple_type,
                            has_option_type_in_path,
                        );
                    };
                };
                case (#Variant(variants)) {
                    for ((nested_field_name, nested_field_type) in variants.vals()) {
                        store_field_type(
                            schema_map,
                            join_field_names(field_name, nested_field_name),
                            #Option(nested_field_type),
                            true,
                        );
                    };

                };
                case (#Option(inner)) {
                    store_field_type(schema_map, field_name, inner, true);
                };
                case (_) {};

            };

            if (has_option_type_in_path) {
                switch (field_type) {
                    case (#Option(inner_type)) {
                        ignore Map.put(schema_map, T.thash, field_name, #Option(inner_type));
                    };
                    case (_) {
                        ignore Map.put(schema_map, T.thash, field_name, #Option(field_type));
                    };
                };
            } else {
                ignore Map.put(schema_map, T.thash, field_name, field_type);
            };

        };

        store_field_type(schema_map, "", schema, false);

        schema_map

    };

    public func get_type(schema_map : SchemaMap, field_name : Text) : ?T.CandidType {
        switch (Map.get<Text, T.CandidType>(schema_map, T.thash, field_name)) {
            case (?candid_type) ?candid_type;
            case (null) null;
        };
    };

    public type ValidateSchemaConstraintResponse = {
        field_constraints : Map.Map<Text, [T.SchemaFieldConstraint]>;
        unique_constraints : [[Text]];
    };

    public func validate_schema_constraints(
        schema_map : SchemaMap,
        constraints : [T.SchemaConstraint],
    ) : T.Result<ValidateSchemaConstraintResponse, Text> {
        let field_constraints_map = Map.new<Text, [T.SchemaFieldConstraint]>();
        let unique_constraints_buffer = Buffer.Buffer<[Text]>(8);

        for (constraint in constraints.vals()) {
            switch (constraint) {
                case (#Field(field_name, field_constraints)) {
                    let ?field_type = get_type(schema_map, field_name) else {
                        return #err("Field '" # field_name # "' not found in schema");
                    };

                    func unwrap_option_type(option_type : T.CandidType) : T.CandidType {
                        switch (option_type) {
                            case (#Option(inner)) {
                                unwrap_option_type(inner);
                            };
                            case (unwrapped) { unwrapped };
                        };
                    };

                    let unwrapped_option_type = unwrap_option_type(field_type);

                    let buffer = Buffer.Buffer<T.SchemaFieldConstraint>(8);

                    for (field_constraint in field_constraints.vals()) {
                        switch (field_constraint) {
                            case (#Max(value)) {

                                switch (unwrapped_option_type) {
                                    case (#Int(_) or #Int8(_) or #Int16(_) or #Int32(_) or #Int64(_) or #Float(_)) {};
                                    case (#Nat(_) or #Nat8(_) or #Nat16(_) or #Nat32(_) or #Nat64(_)) {
                                        if (value < 0) {
                                            return #err("Schema constraint " # debug_show (field_constraint) # " is not valid for field '" # field_name # "' of type " # debug_show (field_type) # " because the value is negative.");
                                        };
                                    };

                                    case (_) {
                                        return #err(
                                            "Schema constraint " # debug_show (field_constraint) # " only applies to numeric types. Field '" # field_name # "' is of type " # debug_show (field_type) # "."
                                        );
                                    };
                                };

                            };
                            case (#Min(value)) {

                                switch (unwrapped_option_type) {
                                    case (#Int(_) or #Int8(_) or #Int16(_) or #Int32(_) or #Int64(_) or #Float(_)) {};
                                    case (#Nat(_) or #Nat8(_) or #Nat16(_) or #Nat32(_) or #Nat64(_)) {
                                        if (value < 0) {
                                            return #err("Schema constraint " # debug_show (field_constraint) # " is not valid for field '" # field_name # "' of type " # debug_show (field_type) # " because the value is negative.");
                                        };
                                    };

                                    case (_) {
                                        return #err(
                                            "Schema constraint " # debug_show (field_constraint) # " only applies to numeric types. Field '" # field_name # "' is of type " # debug_show (field_type) # "."
                                        );
                                    };
                                };
                            };
                            case (#MaxSize(_)) {

                                switch (unwrapped_option_type) {
                                    case (#Array(_) or #Blob(_) or #Text(_)) {};
                                    case (_) {
                                        return #err(
                                            "Schema constraint " # debug_show (field_constraint) # " only applies to #Array and #Blob types. Field '" # field_name # "' is of type " # debug_show (field_type) # "."
                                        );
                                    };
                                };

                            };
                            case (#MinSize(_)) {
                                switch (unwrapped_option_type) {
                                    case (#Array(_) or #Blob(_) or #Text(_)) {};
                                    case (_) {
                                        return #err(
                                            "Schema constraint " # debug_show (field_constraint) # " only applies to #Array and #Blob types. Field '" # field_name # "' is of type " # debug_show (field_type) # "."
                                        );
                                    };
                                };
                            };
                            case (#Size(min, max)) {
                                switch (unwrapped_option_type) {
                                    case (#Array(_) or #Blob(_) or #Text(_)) {};
                                    case (_) {
                                        return #err(
                                            "Schema constraint " # debug_show (field_constraint) # " only applies to #Array and #Blob types. Field '" # field_name # "' is of type " # debug_show (field_type) # "."
                                        );
                                    };
                                };
                            };

                        };

                        buffer.add(field_constraint);
                    };

                    ignore Map.put(
                        field_constraints_map,
                        T.thash,
                        field_name,
                        Buffer.toArray(buffer),
                    );

                };
                case (#Unique(unique_field_names)) {
                    for (unique_field_name in unique_field_names.vals()) {
                        let ?unique_field_type = get_type(schema_map, unique_field_name) else {
                            return #err("Field '" # unique_field_name # "' not found in schema");
                        };

                        switch (unique_field_type) {
                            case (#Record(_) or #Map(_) or #Array(_) or #Variant(_) or #Tuple(_) or #Float(_)) {
                                return #err(
                                    "Error creating unique constraint on field " # unique_field_name # "' as unique constraint is not supported with type " # debug_show (unique_field_type) # "."
                                );
                            };
                            case (_) {};
                        };
                    };

                    unique_constraints_buffer.add(unique_field_names);
                };
            };
        };

        #ok({
            field_constraints = field_constraints_map;
            unique_constraints = Buffer.toArray(unique_constraints_buffer);
        });
    };
};
