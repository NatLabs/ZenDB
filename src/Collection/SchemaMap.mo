import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Nat "mo:base@0.16.0/Nat";
import Buffer "mo:base@0.16.0/Buffer";
import Text "mo:base@0.16.0/Text";
import Iter "mo:base@0.16.0/Iter";

import Map "mo:map@9.0.1/Map";
import T "../Types";
import C "../Constants";

import Itertools "mo:itertools@0.2.2/Iter";

module {

    public type SchemaMap = T.SchemaMap;

    public func new(schema : T.Schema) : T.SchemaMap {
        let schema_map = Map.new<Text, T.Schema>();
        ignore Map.put(schema_map, T.thash, C.DOCUMENT_ID, #Nat);

        let list_of_fields_with_array_type = Buffer.Buffer<Text>(8);

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
            schema_map : Map.Map<Text, T.Schema>,
            field_name : Text,
            field_type : T.CandidType,
        ) {

            switch (field_type) {
                case (#Record(fields) or #Map(fields)) {
                    for ((nested_field_name, nested_field_type) in fields.vals()) {
                        store_field_type(
                            schema_map,
                            join_field_names(field_name, nested_field_name),
                            nested_field_type,
                        );
                    };
                };
                case (#Tuple(types)) {
                    for ((tuple_index, tuple_type) in Itertools.enumerate(types.vals())) {
                        store_field_type(
                            schema_map,
                            join_field_names(field_name, Nat.toText(tuple_index)),
                            tuple_type,
                        );
                    };
                };
                case (#Variant(variants)) {
                    for ((nested_field_name, nested_field_type) in variants.vals()) {
                        store_field_type(
                            schema_map,
                            join_field_names(field_name, nested_field_name),
                            nested_field_type,
                        );
                    };

                };
                case (#Array(inner)) {
                    list_of_fields_with_array_type.add(field_name);
                    store_field_type(schema_map, field_name, inner);
                };
                case (#Option(inner)) {
                    store_field_type(schema_map, field_name, inner);
                };
                case (_) {};

            };

            ignore Map.put(schema_map, T.thash, field_name, field_type);

        };

        store_field_type(schema_map, "", schema);

        list_of_fields_with_array_type.sort(Text.compare);

        {
            map = schema_map;
            fields_with_array_type = Buffer.toArray(list_of_fields_with_array_type);
        } : SchemaMap

    };

    public func get(schema_map : SchemaMap, field_name : Text) : ?T.CandidType {
        switch (Map.get<Text, T.CandidType>(schema_map.map, T.thash, field_name)) {
            case (?candid_type) ?candid_type;
            case (null) {
                get_indexed_nested_array_type(schema_map, field_name);

            };
        };
    };

    // e.g -> 'array.0.field_name'
    func get_indexed_nested_array_type(schema_map : SchemaMap, field_name : Text) : ?T.CandidType {
        // Debug.print("Field '" # field_name # "' not found in schema map");
        // Debug.print("fields with array type: " # debug_show (schema_map.fields_with_array_type));
        let ?index : ?Nat = Itertools.findIndex<Text>(
            schema_map.fields_with_array_type.vals(),
            func(field_with_array_type : Text) : Bool {
                Text.startsWith(field_name, #text(field_with_array_type));
            },
        ) else {
            return null;
        };

        // Debug.print("index: " # debug_show (index));

        var i = index;
        var field_path = field_name;

        label exclude_array_indexes while (i < schema_map.fields_with_array_type.size()) {
            let field_with_array_type = schema_map.fields_with_array_type[i];

            // Debug.print("field_with_array_type: " # debug_show (field_with_array_type));

            switch (Text.stripStart(field_name, #text(field_with_array_type))) {
                case (null) break exclude_array_indexes;
                case (?field_suffix_path) {
                    // Debug.print("field_suffix_path: " # debug_show (field_suffix_path));

                    let paths_within_suffix = Text.tokens(field_suffix_path, #text("."));

                    ignore paths_within_suffix.next(); // todo: check if the skipped path is a number; also could have future conflicts between array index and tuple index

                    // if (Text.isNumeric(next_suffix_path))

                    field_path := Text.join(".", Itertools.prepend(field_with_array_type, paths_within_suffix));

                    switch (Map.get<Text, T.CandidType>(schema_map.map, T.thash, field_path)) {
                        case (?#Array(inner_type)) return ?inner_type;
                        case (?candid_type) return ?candid_type;
                        case (null) {};
                    };

                };
            };
            i += 1;

        };

        null;
    };

    public func getParentPath(field_name : Text) : ?(Text, Text) {
        let fields = Iter.toArray(Text.split(field_name, #text(".")));

        if (fields.size() == 0) {
            return null;
        };

        if (fields.size() == 1) {
            return ?("", fields[0]);
        };

        let parent_field_name = Text.join(".", Itertools.take(fields.vals(), fields.size() - 1));
        let last_field_name = fields[fields.size() - 1];

        ?(parent_field_name, last_field_name);

    };

    public func isValidPath(schema_map : SchemaMap, field_name : Text) : Bool {
        switch (Map.get<Text, T.CandidType>(schema_map.map, T.thash, field_name)) {
            case (?_) true;
            case (null) false;
        };
    };

    public type ValidateSchemaConstraintResponse = {
        field_constraints : Map.Map<Text, [T.SchemaFieldConstraint]>;
        unique_constraints : [[Text]];
    };

    public func isNestedVariantField(schema_map : SchemaMap, field_name : Text) : Bool {
        if (field_name == "") return false;

        let ?(parent_field_name, last_field_name) = getParentPath(field_name) else {
            return false;
        };

        let ?parent_field_type = get(schema_map, parent_field_name) else {
            return false;
        };

        switch (parent_field_type) {
            case (#Variant(variants)) {
                Itertools.any(
                    variants.vals(),
                    func((variant_name, variant_type) : (Text, T.Schema)) : Bool {
                        variant_name == last_field_name;
                    },
                );
            };
            case (_) isNestedVariantField(schema_map, parent_field_name);
        };

    };

    public func isNestedOptionField(schema_map : SchemaMap, field_name : Text) : Bool {
        if (field_name == "") return false;

        let ?(parent_field_name, last_field_name) = getParentPath(field_name) else {
            return false;
        };

        let ?parent_field_type = get(schema_map, parent_field_name) else {
            return false;
        };

        switch (parent_field_type) {
            case (#Option(_)) true;
            case (_) isNestedOptionField(schema_map, parent_field_name);
        };
    };

    public func unwrapOptionType(option_type : T.CandidType) : T.CandidType {
        switch (option_type) {
            case (#Option(inner)) {
                unwrapOptionType(inner);
            };
            case (unwrapped) { unwrapped };
        };
    };

    public func unwrapArrayType(array_type : T.CandidType) : T.CandidType {
        switch (array_type) {
            case (#Array(inner)) {
                unwrapArrayType(inner);
            };
            case (unwrapped) { unwrapped };
        };
    };

    public func validateSchemaConstraints(
        schema_map : SchemaMap,
        constraints : [T.SchemaConstraint],
    ) : T.Result<ValidateSchemaConstraintResponse, Text> {
        let field_constraints_map = Map.new<Text, [T.SchemaFieldConstraint]>();
        let unique_constraints_buffer = Buffer.Buffer<[Text]>(8);

        func missing_field_error<A>(field_name : Text) : T.Result<A, Text> {
            return #err("Field '" # field_name # "' not found in schema");
        };

        label validating_field_constraint for (constraint in constraints.vals()) {
            switch (constraint) {
                case (#Field(field_name, field_constraints)) {
                    let field_type = switch (get(schema_map, field_name)) {
                        case (?field_type) field_type;
                        case (null) {

                            // // we can skip validation on a variant field
                            // if (not is_variant_field(field_name)) {
                            //     return missing_field_error(field_name);
                            // } else {
                            //     continue validating_field_constraint;
                            // };

                            return missing_field_error(field_name);

                        };
                    };

                    let unwrapped_type = unwrapOptionType(field_type);

                    let buffer = Buffer.Buffer<T.SchemaFieldConstraint>(8);

                    for (field_constraint in field_constraints.vals()) {
                        switch (field_constraint) {
                            case (#Max(value)) {

                                switch (unwrapped_type) {
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

                                switch (unwrapped_type) {
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

                                switch (unwrapped_type) {
                                    case (#Array(_) or #Blob(_) or #Text(_)) {};
                                    case (_) {
                                        return #err(
                                            "Schema constraint " # debug_show (field_constraint) # " only applies to #Array and #Blob types. Field '" # field_name # "' is of type " # debug_show (field_type) # "."
                                        );
                                    };
                                };

                            };
                            case (#MinSize(_)) {
                                switch (unwrapped_type) {
                                    case (#Array(_) or #Blob(_) or #Text(_)) {};
                                    case (_) {
                                        return #err(
                                            "Schema constraint " # debug_show (field_constraint) # " only applies to #Array and #Blob types. Field '" # field_name # "' is of type " # debug_show (field_type) # "."
                                        );
                                    };
                                };
                            };
                            case (#Size(min, max)) {
                                switch (unwrapped_type) {
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
                        let ?unique_field_type = get(schema_map, unique_field_name) else {
                            return #err("Field '" # unique_field_name # "' not found in schema");
                        };

                        switch (unwrapOptionType(unique_field_type)) {
                            case (#Record(_) or #Map(_) or #Array(_) or #Variant(_) or #Tuple(_) or #Null or #Empty or #Recursive(_)) {
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
