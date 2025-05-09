import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Option "mo:base/Option";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Result "mo:base/Result";
import Nat "mo:base/Nat";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import PeekableIter "mo:itertools/PeekableIter";

import T "Types";
import Schema "Collection/Schema";
import SchemaMap "Collection/SchemaMap";

module {
    type Candid = T.Candid;

    type Result<A, B> = Result.Result<A, B>;

    public type CandidMap = Map.Map<Text, T.Candid>;

    let IS_COMPOUND_TYPE = ":compound_type";

    let compound_types = {
        variant = func(variant_tag : Text) : (T.Schema, Candid) {
            (#Variant([]), #Text(variant_tag));
        };

        tuple = func(tuple_size : Nat) : (T.Schema, Candid) {
            (#Tuple([]), #Nat(tuple_size));
        };

        array = func(array_type : T.Schema, array_size : Nat) : (T.Schema, Candid) {
            (#Array(array_type), #Nat(array_size));
        };
    };

    func join_fields(prefix : Text, field : Text) : Text {
        if (prefix == "") {
            return field;
        } else {
            return prefix # "." # field;
        };
    };

    // this loads the first level of a candid record into the map
    // it stores information about the nested records as is and only caches the nested fields when they are accessed in the get method
    func load_record_into_map(candid_map : CandidMap, field_prefix : Text, fields : [(Text, Candid)]) {

        var i = 0;
        while (i < fields.size()) {
            ignore Map.put(candid_map, T.thash, join_fields(field_prefix, fields[i].0), fields[i].1);
            i += 1;
        };

    };

    public func new(candid : T.Candid) : CandidMap {
        let candid_map = Map.new<Text, T.Candid>();

        let #Record(fields) = candid else Debug.trap("CandidMap only accepts #Record types");
        load_record_into_map(candid_map, "", fields);

        candid_map

    };

    func validate_type_match(field_type : T.Schema, field_value : Candid) : Bool {
        switch (field_type, field_value) {
            case (#Empty, #Empty) true;
            case (#Null, #Null(_)) true;
            case (#Nat, #Nat(_)) true;
            case (#Nat8, #Nat8(_)) true;
            case (#Nat16, #Nat16(_)) true;
            case (#Nat32, #Nat32(_)) true;
            case (#Nat64, #Nat64(_)) true;
            case (#Int, #Int(_)) true;
            case (#Int8, #Int8(_)) true;
            case (#Int16, #Int16(_)) true;
            case (#Int32, #Int32(_)) true;
            case (#Int64, #Int64(_)) true;
            case (#Float, #Float(_)) true;
            case (#Text, #Text(_)) true;
            case (#Bool, #Bool(_)) true;
            case (#Principal, #Principal(_)) true;
            case (#Blob, #Blob(_)) true;
            case (#Option(_), #Option(_)) true;
            case (#Option(_), #Null) true;
            case (#Option(inner_type), candid_value) validate_type_match(inner_type, candid_value);
            case (_, #Null) true; // for storing null values in indexes when the field is not present
            case (#Record(record_types), #Record(record_values)) {
                Itertools.all(
                    record_values.vals(),
                    func((field_name, record_value) : (Text, Candid)) : Bool {

                        let ?(_, record_type) = Array.find<(Text, T.CandidType)>(
                            record_types,
                            func((field, field_type) : (Text, T.CandidType)) : Bool {
                                field == field_name;
                            },
                        ) else return false;

                        validate_type_match(record_type, record_value);
                    },
                );

            };
            case (#Variant(variant_types), #Variant(variant)) {

                let ?(_, variant_type) = Array.find<(Text, T.CandidType)>(
                    variant_types,
                    func((tag, _) : (Text, T.CandidType)) : Bool {
                        tag == variant.0;
                    },
                ) else return false;

                validate_type_match(variant_type, variant.1);

            };
            case (#Tuple(tuple_types), #Tuple(tuple_values)) {
                if (tuple_types.size() != tuple_values.size()) {
                    Debug.trap("CandidMap: Tuple types and values should have the same size");
                };

                Itertools.all(
                    Itertools.zip(tuple_types.vals(), tuple_values.vals()),
                    func((tuple_type, tuple_value) : (T.CandidType, T.Candid)) : Bool {
                        validate_type_match(tuple_type, tuple_value);
                    },
                );

            };
            case (#Tuple(tuple_types), #Record(records)) {
                let tuples = Array.tabulate(
                    tuple_types.size(),
                    func(i : Nat) : Candid {
                        let (field, record_value) = records[i];
                        assert field == Nat.toText(i);
                        record_value;
                    },
                );

                return validate_type_match(#Tuple(tuple_types), #Tuple(tuples));
            };
            case (#Array(inner_type), #Array(array_values)) {
                Itertools.all(
                    array_values.vals(),
                    func(array_value : Candid) : Bool {
                        validate_type_match(inner_type, array_value);
                    },
                );
            };
            case (_) return false;
        };
    };

    public func get(candid_map : CandidMap, schema_map : T.SchemaMap, field_path : Text) : ?Candid {

        func get_and_cache_nested_field_data(field_prefix : Text, fields : PeekableIter.PeekableIter<Text>) : ?Candid {
            var field_path = field_prefix;
            var field_value : Candid = #Empty;

            label searching_for_last_cached_value loop switch (fields.peek()) {
                case (null) return Map.get(candid_map, T.thash, field_path);
                case (?field) {
                    Debug.print("field: " # field);

                    switch (Map.get(candid_map, T.thash, field_path)) {
                        case (null) {};
                        case (?last_cached_field_value) {
                            field_value := last_cached_field_value;
                            break searching_for_last_cached_value;
                        };
                    };

                    field_path := join_fields(field_path, field);
                    ignore fields.next();

                };
            };

            Debug.print("field_path: " # field_path);
            Debug.print("field_value: " # debug_show (field_value));

            let ?field_type = SchemaMap.get(schema_map, field_path) else return null;

            func handle_candid(field_type : T.Schema, field_value : Candid) : ?Candid {
                Debug.print("[" #field_path # "] field_type: " # debug_show (field_type) # " field_value: " # debug_show (field_value));
                switch (field_type, field_value) {
                    case (#Empty, #Empty) ?field_value;
                    case (#Null, #Null(_)) ?field_value;
                    case (#Nat, #Nat(_)) ?field_value;
                    case (#Nat8, #Nat8(_)) ?field_value;
                    case (#Nat16, #Nat16(_)) ?field_value;
                    case (#Nat32, #Nat32(_)) ?field_value;
                    case (#Nat64, #Nat64(_)) ?field_value;
                    case (#Int, #Int(_)) ?field_value;
                    case (#Int8, #Int8(_)) ?field_value;
                    case (#Int16, #Int16(_)) ?field_value;
                    case (#Int32, #Int32(_)) ?field_value;
                    case (#Int64, #Int64(_)) ?field_value;
                    case (#Float, #Float(_)) ?field_value;
                    case (#Text, #Text(_)) ?field_value;
                    case (#Bool, #Bool(_)) ?field_value;
                    case (#Principal, #Principal(_)) ?field_value;
                    case (#Blob, #Blob(_)) ?field_value;
                    case (#Option(inner_type), #Option(inner_value)) handle_candid(inner_type, inner_value);
                    case (#Option(_), #Null) ?field_value;
                    case (#Option(inner_type), candid_value) handle_candid(inner_type, candid_value);
                    case (_, #Null) ?field_value; // for storing null values in indexes when the field is not present
                    case (#Record(_), #Record(records)) {
                        ignore Map.remove(candid_map, T.thash, field_path);
                        load_record_into_map(candid_map, field_path, records);
                        get_and_cache_nested_field_data(field_path, fields);
                    };
                    case (#Variant(variant_types), #Variant(variant)) {
                        ignore Map.remove(candid_map, T.thash, field_path);

                        let variants = Array.map<(Text, T.CandidType), (Text, Candid)>(
                            variant_types,
                            func(variant_tag : Text, variant_type : T.CandidType) : (Text, Candid) {
                                if (variant_tag == variant.0) {
                                    return (variant_tag, variant.1);
                                };
                                (variant_tag, #Null);
                            },
                        );

                        load_record_into_map(candid_map, field_path, variants);
                        get_and_cache_nested_field_data(field_path, fields);

                    };

                    case (#Tuple(tuple_types), #Tuple(tuple_values)) {
                        if (tuple_types.size() != tuple_values.size()) {
                            Debug.trap("CandidMap: Tuple types and values should have the same size");
                        };

                        ignore Map.remove(candid_map, T.thash, field_path);

                        load_record_into_map(
                            candid_map,
                            field_path,
                            Array.tabulate<(Text, Candid)>(
                                tuple_types.size(),
                                func(i : Nat) : (Text, Candid) {
                                    (Nat.toText(i), tuple_values[i]);
                                },
                            ),
                        );

                        get_and_cache_nested_field_data(field_path, fields);

                    };

                    case (#Tuple(tuple_types), #Record(records)) {

                        let tuples = Array.tabulate(
                            tuple_types.size(),
                            func(i : Nat) : Candid {
                                let (field, record_value) = records[i];
                                assert field == Nat.toText(i);
                                record_value;
                            },
                        );

                        return handle_candid(#Tuple(tuple_types), #Tuple(tuples));
                    };
                    case (#Array(_), #Array(array_values)) {
                        ignore Map.remove(candid_map, T.thash, field_path);
                        let array_with_indices = Array.tabulate<(Text, Candid)>(
                            array_values.size(),
                            func(i : Nat) : (Text, Candid) {
                                (Nat.toText(i), array_values[i]);
                            },
                        );

                        Debug.print("array_with_indices: " # debug_show (array_with_indices));
                        Debug.print("field_path: " # field_path);

                        load_record_into_map(candid_map, field_path, array_with_indices);
                        get_and_cache_nested_field_data(field_path, fields);
                    };
                    case (_) return null;
                };
            };

            handle_candid(field_type, field_value);

        };

        let opt_candid_value = switch (Map.get(candid_map, T.thash, field_path)) {
            case (?candid_value) ?candid_value;
            case (null) {
                let peekable_fields = Itertools.peekable(Text.split(field_path, #text(".")));
                get_and_cache_nested_field_data("", peekable_fields);
            };
        };

        switch (opt_candid_value) {
            case (?#Variant(variant_tag, variant_value)) ?#Text(variant_tag); // return the variant tag when the value is a variant
            case (other) other; // return the original value
        };

    };

    public func set(
        candid_map : CandidMap,
        schema_map : T.SchemaMap,
        field_path : Text,
        new_value : Candid,
    ) : T.Result<(), Text> {

        let ?field_type = SchemaMap.get(schema_map, field_path) else return #err("CandidMap: Field " # field_path # " not found in schema map");
        if (not validate_type_match(field_type, new_value)) {
            return #err(
                "CandidMap: Type mismatch for field " # field_path # ": expected " # debug_show (field_type) # ", got " # debug_show (new_value)
            );
        };

        let fields = Iter.toArray(Text.split(field_path, #text(".")));

        switch (get(candid_map, schema_map, field_path)) {
            case (null) {
                // Might indicate that there is a more specific field path that stores the value

            };
            case (?old_value) {
                ignore Map.put(candid_map, T.thash, field_path, new_value);
                return #ok(());

            };
        };

        // now we have to run dfs to find the leaf paths and set their values
        func update_leaf_paths(
            candid_map : CandidMap,
            schema_map : T.SchemaMap,
            field_prefix : Text,
            field_type : T.Schema,
            new_value : Candid,
        ) : Bool {

            Debug.print("field_prefix: " # field_prefix # " new_value: " # debug_show (new_value));

            switch (Map.get(candid_map, T.thash, field_prefix)) {
                case (null) {};
                case (?exists) return true;
            };

            Debug.print("field_prefix is about to be set: " # field_prefix # " new_value: " # debug_show (new_value));

            switch (field_type, new_value) {
                case (#Option(inner_type), #Option(inner_value)) {
                    if (update_leaf_paths(candid_map, schema_map, field_prefix, inner_type, inner_value)) {
                        ignore Map.put(candid_map, T.thash, field_prefix, inner_value);
                    };
                };
                case (#Record(record_types), #Record(records)) {
                    var stored = false;
                    for (((_, field_type), (field_name, field_value)) in Itertools.zip(record_types.vals(), records.vals())) {
                        let field_path = join_fields(field_prefix, field_name);

                        if (update_leaf_paths(candid_map, schema_map, field_path, field_type, field_value)) {
                            stored := true;
                            ignore Map.put(candid_map, T.thash, field_path, field_value);
                        };

                    };

                    if (not stored) {
                        ignore Map.put(candid_map, T.thash, field_prefix, #Record(records));
                    };

                };
                case (#Variant(variant_types), #Variant(variant)) {
                    let variants = Array.map<(Text, T.CandidType), (Text, T.Schema, Candid)>(
                        variant_types,
                        func(variant_tag : Text, variant_type : T.CandidType) : (Text, T.Schema, Candid) {
                            if (variant_tag == variant.0) {
                                return (variant_tag, variant_type, variant.1);
                            };

                            (variant_tag, variant_type, #Null);
                        },
                    );

                    var stored = false;

                    Debug.print("variants: " # debug_show (variants));

                    for ((variant_tag, variant_type, variant_value) in variants.vals()) {
                        let nested_path = join_fields(field_prefix, variant_tag);
                        Debug.print("nested_path: " # nested_path # " variant: " # debug_show (variant_value));

                        if (update_leaf_paths(candid_map, schema_map, nested_path, variant_type, variant_value)) {
                            stored := true;
                            Debug.print("variant_value: " # debug_show (variant_value));
                            ignore Map.put(candid_map, T.thash, nested_path, variant_value);
                        };
                    };

                    if (not stored) {
                        ignore Map.put(candid_map, T.thash, field_prefix, #Variant(variant));
                    };

                };

                case (#Tuple(tuple_types), #Tuple(tuple_values)) {

                    var stored = false;

                    for (i in Itertools.range(0, tuple_values.size())) {
                        let nested_path = join_fields(field_prefix, Nat.toText(i));

                        if (update_leaf_paths(candid_map, schema_map, nested_path, tuple_types[i], tuple_values[i])) {
                            stored := true;
                            ignore Map.put(candid_map, T.thash, nested_path, tuple_values[i]);
                        };

                    };

                    if (not stored) {
                        ignore Map.put(candid_map, T.thash, field_prefix, #Tuple(tuple_values));
                    };

                };

                case (#Array(array_type), #Array(array_values)) {
                    var stored = false;

                    for (i in Itertools.range(0, array_values.size())) {
                        let nested_path = join_fields(field_prefix, Nat.toText(i));

                        if (update_leaf_paths(candid_map, schema_map, nested_path, array_type, array_values[i])) {
                            stored := true;
                            ignore Map.put(candid_map, T.thash, nested_path, array_values[i]);
                        };
                    };

                    if (not stored) {
                        ignore Map.put(candid_map, T.thash, field_prefix, #Array(array_values));
                    };

                };
                case (_) {};
            };

            false;
        };

        ignore update_leaf_paths(candid_map, schema_map, field_path, field_type, new_value);

        #ok();
    };
};
