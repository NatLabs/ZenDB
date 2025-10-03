import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Option "mo:base@0.16.0/Option";
import Iter "mo:base@0.16.0/Iter";
import Text "mo:base@0.16.0/Text";
import Result "mo:base@0.16.0/Result";
import Nat "mo:base@0.16.0/Nat";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Candid "mo:serde@3.3.2/Candid";
import Itertools "mo:itertools@0.2.2/Iter";

import T "Types";
import C "Constants";
import Schema "Collection/Schema";
import SchemaMap "Collection/SchemaMap";
import CandidUtils "CandidUtils";

module CandidMap {

    let { nhash; thash } = Map;

    type Schema = T.Schema;
    type Candid = T.Candid;
    type Result<A, B> = Result.Result<A, B>;

    type NestedCandid = {
        #Candid : (T.Schema, Candid);
        #CandidMap : (Map.Map<Text, NestedCandid>);
    };

    let IS_COMPOUND_TYPE = ":compound_type";
    let IS_OPTIONAL = ":is_optional";

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

    func add_to_map(map : Map.Map<Text, NestedCandid>, field_key : Text, candid_type : T.Schema, candid_value : Candid) {
        ignore Map.put<Text, NestedCandid>(
            map,
            thash,
            field_key,
            #Candid(candid_type, candid_value),
        );
    };

    // this loads the first level of a candid document into the map
    // it stores information about the nested documents as is and only caches the nested fields when they are accessed in the get method
    func load_record_into_map(types : [(Text, T.Schema)], fields : [(Text, Candid)]) : Map.Map<Text, NestedCandid> {
        let map = Map.new<Text, NestedCandid>();

        var i = 0;
        while (i < fields.size()) {
            add_to_map(
                map,
                fields[i].0,
                types[i].1,
                fields[i].1,
            );

            i += 1;

        };

        // Debug.print("map: " # debug_show (Map.toArray(map)));

        map;
    };

    public type CandidMap = {
        candid_map : Map.Map<Text, NestedCandid>;
    };

    public func new(schema_map : T.SchemaMap, document_id : T.DocumentId, candid : T.Candid) : CandidMap {

        let ?types = SchemaMap.get(schema_map, "") else Debug.trap("CandidMap only accepts #Record types");
        let map = Map.new<Text, NestedCandid>();

        // Debug.print("CandidMap.new(): " # debug_show (types, candid));

        ignore Map.put(map, T.thash, "", #Candid(types, candid));
        ignore Map.put(map, T.thash, C.DOCUMENT_ID, #Candid(#Blob, #Blob(document_id)));

        let candid_map = { candid_map = map };

        ignore get_and_cache_map_for_field(candid_map, schema_map, "");

        candid_map;

    };

    func get_and_cache_map_for_field({ candid_map } : CandidMap, schema_map : T.SchemaMap, key : Text) : ?Map.Map<Text, NestedCandid> {

        let fields = Itertools.peekable(Text.split("." # key, #text(".")));
        var map = candid_map;
        var prev_map = map;

        var prefix_path = "";

        // Debug.print("starting get_and_cache_map_for_field: " # debug_show (key));

        label extracting_candid_value loop {
            let ?field = fields.next() else break extracting_candid_value;
            // Debug.print("searching field: " # debug_show (field));

            prev_map := map;
            prefix_path := if (prefix_path == "") field else prefix_path # "." # field;

            // Debug.print("prefix_path: " # debug_show (prefix_path));

            let ?candid = Map.get(map, Map.thash, field) else break extracting_candid_value;

            // Debug.print("candid: " # debug_show (candid));

            func handle_candid(chained_candid_type : T.Schema, candid_value : Candid, is_parent_optional : Bool) : Bool {
                let ?candid_type = if (is_parent_optional) {
                    ?chained_candid_type;
                } else SchemaMap.get(schema_map, prefix_path) else return false;

                // Debug.print("candid_type: " # debug_show (candid_type));

                let nested_map = switch (candid_type, candid_value) {
                    case (#Option(inner_type), #Option(inner_value)) {
                        return handle_candid(inner_type, inner_value, true);
                    };
                    case (#Option(inner_type), #Null) {
                        return false;
                    };
                    case (#Null, #Null) {
                        return false;
                    };
                    case (candid_type, #Null) {
                        //    Debug.print("candid_type: " # debug_show (candid_type));
                        // return_empty_map := true;
                        return false;
                    };
                    case (#Option(inner_type), candid_value) {
                        return handle_candid(inner_type, candid_value, true);
                    };
                    case ((#Record(record_types) or #Map(record_types), #Record(documents) or #Map(documents))) {
                        let nested_map = load_record_into_map(record_types, documents);
                        ignore Map.put(map, Map.thash, field, #CandidMap(nested_map));
                        nested_map;
                    };
                    case (#Variant(variant_types), #Variant(variant)) {

                        let ?variant_type = Array.find<(Text, T.CandidType)>(
                            variant_types,
                            func(variant_type : (Text, T.CandidType)) : Bool {
                                variant.0 == variant_type.0;
                            },
                        ) else Debug.trap("CandidMap: Could not find variant type for tag '" # variant.0 # "' in " # debug_show (variant_types));

                        // Debug.print(debug_show ({ variant; variant_types }));

                        let nested_map = load_record_into_map([variant_type], [variant]);

                        // Debug.print("nested_map: " #debug_show (Map.toArray(nested_map)));
                        ignore Map.put<Text, NestedCandid>(nested_map, Map.thash, IS_COMPOUND_TYPE, #Candid(compound_types.variant(variant.0)));

                        ignore Map.put(map, Map.thash, field, #CandidMap(nested_map));
                        // Debug.print("variant next field: " # debug_show (fields.peek()));

                        switch (fields.peek()) {
                            case (null or ?"") {
                                prev_map := nested_map;
                                return false;
                            };
                            case (_) {};
                        };

                        nested_map;

                    };
                    case (#Tuple(tuple_types), #Tuple(tuple_values)) {
                        if (tuple_types.size() != tuple_values.size()) {
                            Debug.trap("CandidMap: Tuple types and values should have the same size");
                        };

                        let nested_map = Map.new<Text, NestedCandid>();

                        for ((i, (tuple_type, tuple_value)) in Itertools.enumerate(Itertools.zip(tuple_types.vals(), tuple_values.vals()))) {
                            ignore Map.put(nested_map, Map.thash, Nat.toText(i), #Candid(tuple_type, tuple_value));
                        };

                        ignore Map.put(nested_map, Map.thash, IS_COMPOUND_TYPE, #Candid(compound_types.tuple(tuple_values.size())));

                        ignore Map.put(map, Map.thash, field, #CandidMap(nested_map));
                        nested_map;

                    };
                    case (#Tuple(tuple_types), #Record(documents)) {
                        let tuples = Array.tabulate(
                            tuple_types.size(),
                            func(i : Nat) : Candid {
                                let (field, record_value) = documents[i];
                                assert field == Nat.toText(i);
                                record_value;
                            },
                        );

                        return handle_candid(#Tuple(tuple_types), #Tuple(tuples), false);
                    };
                    case (#Array(array_type), #Array(array_values)) {
                        let nested_map = Map.new<Text, NestedCandid>();

                        for ((i, array_value) in Itertools.enumerate(array_values.vals())) {
                            ignore Map.put(nested_map, Map.thash, Nat.toText(i), #Candid(array_type, array_value));
                        };

                        ignore Map.put(nested_map, Map.thash, IS_COMPOUND_TYPE, #Candid(compound_types.array(array_type, array_values.size())));

                        ignore Map.put(map, Map.thash, field, #CandidMap(nested_map));
                        nested_map;

                    };
                    // case (#Null, #Null) {
                    //     let nested_map = Map.new<Text, NestedCandid>();
                    //     ignore Map.put(map, Map.thash, field, #CandidMap(nested_map));
                    //     nested_map;
                    // };
                    // case (_, #Null){

                    // };
                    case (_) {
                        // terminate the loop
                        return false;
                    };
                };

                // Debug.print("map before switch: " # debug_show (Map.toArray(map)));
                map := nested_map;
                // Debug.print("current field: " # debug_show (field));
                // Debug.print("map after switch: " # debug_show (Map.toArray(map)));

                // continue looping
                true;
            };

            // Debug.print("candid[0]: " # debug_show (candid));
            // Debug.print("map[0]: " # debug_show (Map.toArray(map)));
            switch (candid) {
                case (#CandidMap(nested_map)) {
                    // Debug.print("nested_map: " # debug_show (Map.toArray(nested_map)));
                    // Debug.print("field: " # debug_show (field));

                    map := nested_map;

                };
                case (#Candid((candid_type, candid))) {
                    if (not handle_candid(candid_type, candid, false)) {
                        break extracting_candid_value;
                    };
                };
            };

            // Debug.print("candid[+1]: " # debug_show (candid));
            // Debug.print("map[+1]: " # debug_show (Map.toArray(map)));
        };

        // Debug.print("prev_map: " # debug_show (Map.toArray(prev_map)));

        //    Debug.print("map: " # debug_show (Map.toArray(map)));
        //    Debug.print("set: " # debug_show (Set.toArray(paths_with_optional_fields)));

        ?prev_map;

    };

    // Helper function to wrap a Candid value in #Option if is_option_type is true
    func wrap_option(candid_type : T.CandidType, value : Candid) : Candid {
        // CandidUtils.inheritOptionsFromType(
        //     candid_type,
        //     CandidUtils.unwrapOption(value),
        // );
        value;
    };

    public func get(candid_map_state : CandidMap, schema_map : T.SchemaMap, key : Text) : ?Candid {
        let { candid_map } = candid_map_state;

        if (key == C.DOCUMENT_ID) {
            let ?#Candid(#Blob, #Blob(document_id)) = Map.get(candid_map, Map.thash, C.DOCUMENT_ID) else Debug.trap("CandidMap: Could not find candid id");
            return ?#Blob(document_id); // exclude wrap_option for document_id
        };

        let fields = Iter.toArray(Text.split(key, #text(".")));
        let ?types = SchemaMap.get(schema_map, key) else return null;
        var result : ?Candid = null;
        var prefix_path = "";
        var current_field = "";
        var is_compound_type = true;
        let is_option_type = switch (types) {
            case (#Option(_)) true;
            case (_) false;
        };

        let map = switch (get_and_cache_map_for_field(candid_map_state, schema_map, key)) {
            case (?map) map;
            case (_) return null;
        };

        let field = if (fields.size() == 0) "" else fields[fields.size() - 1];
        current_field := field;
        prefix_path := if (prefix_path == "") field else prefix_path # "." # field;

        let candid : NestedCandid = switch (Map.get(map, Map.thash, field)) {
            case (?nested_candid) nested_candid;
            case (null) {
                switch (SchemaMap.unwrap_option_type(types), Map.get(map, Map.thash, IS_COMPOUND_TYPE)) {
                    case (#Variant(schema_map_types), ?#Candid(#Variant(candid_map_nested_types), #Text(tag))) {
                        for ((variant_tag, _) in schema_map_types.vals()) {
                            if (variant_tag == tag) {
                                return ?wrap_option(types, #Text(tag));
                            };
                        };
                        return null;
                    };
                    case (_) return null;
                };
            };
        };

        switch (candid) {
            case (#CandidMap(nested_map)) {
                switch (Map.get(nested_map, Map.thash, IS_COMPOUND_TYPE)) {
                    case (?#Candid(#Variant(_), #Text(tag))) {
                        return ?wrap_option(types, #Text(tag));
                    };
                    case (_) Debug.trap("CandidMap.get(): Recieved unexpected #CandidMap");
                };
            };
            case (#Candid((candid_type, candid))) {
                switch (candid_type, candid) {
                    case ((#Record(_) or #Map(_) or #Variant(_) or #Option(#Record(_)) or #Option(#Map(_)) or #Option(#Variant(_)) or #Tuple(_) or #Array(_), #Record(_) or #Map(_) or #Variant(_) or #Option(#Record(_)) or #Option(#Map(_)) or #Option(#Variant(_)))) {
                        Debug.trap("CandidMap.get(): Should have cached the nested map");
                    };
                    case (_, #Null) return ?wrap_option(types, #Null);
                    case (_) {
                        is_compound_type := false;
                        result := ?wrap_option(types, candid);
                    };
                };
            };
        };

        let opt_compound_tag = Map.get(map, Map.thash, IS_COMPOUND_TYPE);

        if (is_compound_type) switch (SchemaMap.unwrap_option_type(types), opt_compound_tag) {
            case (#Variant(_), ?#Candid(#Variant(_), #Text(tag))) return ?wrap_option(types, #Text(tag));
            case (_) {};
        };

        return result;

    };

    public func set(
        state : CandidMap,
        schema_map : T.SchemaMap,
        key : Text,
        new_value : Candid,
    ) : T.Result<(), Text> {
        // Debug.print("set(): " # debug_show (key, new_value));
        let { candid_map } = state;

        let fields = Iter.toArray(Text.split(key, #text(".")));
        let field = fields[fields.size() - 1];

        // Debug.print("updating field: " # debug_show (field));

        let ?types = SchemaMap.get(schema_map, key) else return #err("set(): Could not retrieve candid type for key '" # key # "'");
        // Debug.print("types: " # debug_show (types));

        let map = switch (get_and_cache_map_for_field(state, schema_map, key)) {
            case (?map) map;
            case (_) return #err("set(): Could not retrieve map with key '" # key # "'");

            // !todo - need to debug code for appending element to array
            // if (Mo.Text.isNumber(field)) {

            //     let fields_excluding_last = Text.join("", Array.take(fields, fields.size() - 1).vals());

            //     let ?(parent_map, is_optional) = get_and_cache_map_for_field(fields_excluding_last) else return #err("set(): Could not retrieve map with key '" # fields_excluding_last # "'");

            //     let ?#CandidMap(nested_map) = Map.get(parent_map, Map.thash, field) else return #err("set(): Could not retrieve map with key '" # field # "'");

            //     let ?#Candid(#Array(_), #Array(array_size_wrapper)) = Map.get(nested_map, Map.thash, IS_COMPOUND_TYPE) else return #err("set(): Expected map to be an array type");

            //     let array_size = array_size_wrapper[0];

            //     let index = Mo.Text.toNat(field);

            //     if (index != array_size) {
            //         return #err("set(): Array index out of bounds, expected to add element at index " # debug_show array_size # " but got " # debug_show index);
            //     };

            //     (nested_map, is_optional);

            // } else return #err("set(): Could not retrieve map with key '" # key # "'");
        };

        // Debug.print("field: " # debug_show (field));
        // Debug.print("map returned for set: " # debug_show (Map.toArray(map)));

        switch (Map.get(map, Map.thash, field)) {
            case (?#Candid((candid_type, prev_candid))) {
                let value : Candid = switch (candid_type) {
                    case (#Option(opt_type)) {
                        // Debug.print(debug_show (opt_type, new_value));
                        let opt_value : Candid = switch (new_value) {
                            case (#Null or #Option(_)) { new_value };
                            case (unwrapped) {
                                #Option(unwrapped);
                            };
                        };

                    };

                    case (_) new_value;
                };

                ignore Map.put<Text, NestedCandid>(map, Map.thash, field, #Candid(candid_type, value));
            };
            case (?#CandidMap(nested_map)) {
                switch (Map.get(nested_map, Map.thash, IS_COMPOUND_TYPE)) {
                    case (?#Candid(#Variant(_), #Text(prev_tag))) {

                        // Debug.print("updating variant from " # prev_tag # " to " # debug_show (new_value));

                        let #Variant(curr_tag, new_candid) = new_value else Debug.trap("Expected a variant type");
                        let #Variant(variant_types) = types else Debug.trap("Expected a variant type");

                        let ?(_, variant_type) = Itertools.find(
                            variant_types.vals(),
                            func(variant_type : (Text, T.Schema)) : Bool {
                                variant_type.0 == curr_tag;
                            },
                        ) else Debug.trap("Expected a variant type");

                        ignore Map.remove(nested_map, Map.thash, prev_tag);
                        ignore Map.put(nested_map, Map.thash, curr_tag, #Candid(variant_type, new_candid));
                        ignore Map.put(nested_map, Map.thash, IS_COMPOUND_TYPE, #Candid(compound_types.variant(curr_tag)));

                        // Debug.print("map after update: " # debug_show (Map.toArray(nested_map)));

                        // switch (Map.get(nested_map, Map.thash, prev_tag), Map.get(nested_map, Map.thash, curr_tag)) {
                        //     case (?#Candid(prev_type, prev_value), ?#Candid(curr_type, curr_value)) {
                        //         if (prev_tag != curr_tag) {
                        //             ignore Map.put(nested_map, Map.thash, prev_tag, #Candid(prev_type, #Null));
                        //         };

                        //         ignore Map.put(nested_map, Map.thash, curr_tag, #Candid(curr_type, new_candid));
                        //         ignore Map.put(nested_map, Map.thash, IS_COMPOUND_TYPE, #Candid(compound_types.variant(curr_tag)));
                        //     };
                        //     case (_) {};
                        // };

                    };
                    case (?#Candid(#Tuple(_), #Nat(tuple_size))) {

                        let #Tuple(new_values) = new_value else Debug.trap("Expected a tuple type");

                        if (new_values.size() != tuple_size) {
                            Debug.trap("set(): Tuple size mismatch");
                        };

                        for ((i, new_value) in Itertools.enumerate(new_values.vals())) {
                            let key = Nat.toText(i);

                            switch (Map.get(nested_map, Map.thash, key)) {
                                case (?#Candid((candid_type, prev_candid))) {
                                    // Debug.print("(candid_type, prev_candid, new_candid): " # debug_show ((candid_type, prev_candid, new_value)));

                                    if (Schema.validate(candid_type, new_value) != #ok()) {
                                        return #err("set(): Invalid candid type for array index '" # key # "' -> " # debug_show (new_value) # ". Expected " # debug_show (candid_type));
                                    };

                                    ignore Map.put(nested_map, Map.thash, key, #Candid(candid_type, new_value));
                                };
                                case (_) return #err("set(): Could not find tuple index '" # key # "' in map");
                            };

                        };

                    };
                    case (?#Candid(#Array(array_type), #Nat(prev_size))) {

                        let #Array(new_values) = new_value else Debug.trap("Expected an array type");

                        for ((i, new_value) in Itertools.enumerate(new_values.vals())) {
                            let key = Nat.toText(i);

                            ignore Map.put(nested_map, Map.thash, key, #Candid(array_type, new_value));

                        };

                        if (prev_size > new_values.size()) {
                            for (i in Itertools.range(new_values.size(), prev_size)) {
                                let key = Nat.toText(i);

                                ignore Map.remove(nested_map, Map.thash, key);
                            };
                        };

                        ignore Map.put(nested_map, Map.thash, IS_COMPOUND_TYPE, #Candid(compound_types.array(array_type, new_values.size())));

                    };

                    case (_) {
                        let ?candid_type = SchemaMap.get(schema_map, key) else return #err("set(): Could not retrieve candid type for key '" # key # "'");

                        ignore Map.put(map, Map.thash, field, #Candid(candid_type, new_value));
                    };
                };

            };
            case (_) return #err("set(): Could not find field '" # field # "' in map");
        };

        //    Debug.print("updated candid: " # debug_show extractCandid());

        #ok()

    };

    /// Assumes the new candid has the same schema as the original candid
    public func reload({ candid_map } : CandidMap, schema_map : T.SchemaMap, new_id : T.DocumentId, new_candid : Candid) {
        let ?(types) = SchemaMap.get(schema_map, "") else Debug.trap("CandidMap only accepts #Record types");

        Map.clear(candid_map);
        ignore Map.put(candid_map, Map.thash, "", #Candid(types, new_candid));
        ignore Map.put(candid_map, Map.thash, C.DOCUMENT_ID, #Candid(#Blob, #Blob(new_id)));

        // // unlike the `load()` method, we already have a map that might have cached nested documents
        // // these cached documents are indicative of the fields that have been accessed and would most likely be accessed again
        // // so we replace the values in the cached document fields with the new values
        // func reload_record_into_map(map : Map.Map<Text, NestedCandid>, types : [(Text, T.Schema)], fields : [(Text, Candid)]) {
        //     var var_fields = fields;

        //     var i = 0;
        //     while (i < var_fields.size()) {
        //         let field = var_fields[i].0;
        //         let candid_type = types[i].1;
        //         let candid_value = var_fields[i].1;

        //         let ?nested_candid = Map.get(map, Map.thash, field) else return Debug.trap("CandidMap: Extra field not present in the original candid during reload");

        //         switch (nested_candid) {
        //             case (#CandidMap(nested_map)) {
        //                 switch (candid_type, candid_value) {
        //                     case (#Record(record_types) or #Map(record_types), #Record(nested_records) or #Map(nested_records)) {
        //                         reload_record_into_map(nested_map, record_types, nested_records);
        //                     };
        //                     case (#Variant(variant_types), #Variant(variant)) {
        //                         for ((variant_tag, variant_type) in variant_types.vals()) {
        //                             ignore Map.put(nested_map, Map.thash, variant_tag, #Candid(variant_type, #Null));
        //                         };

        //                         reload_record_into_map(nested_map, variant_types, [variant]);
        //                     };
        //                     case (_) {
        //                         Debug.trap("CandidMap: Expected #Record, #Map or #Variant type in reload");
        //                     };
        //                 };
        //             };
        //             case (#Candid((candid_type, prev_candid_value))) {
        //                 add_to_map(map, field, candid_type, candid_value);
        //             };
        //         };

        //         i += 1;
        //     };

        //     // Debug.print("map: " # debug_show (Map.toArray(map)));

        // };

        // reload_record_into_map(candid_map, types, fields);
    };

    public func clone(candid_map : CandidMap, schema_map : T.SchemaMap) : CandidMap {
        let extracted_candid = extract_candid(candid_map);
        let ?(#Candid(#Blob, #Blob(document_id))) = Map.get(candid_map.candid_map, Map.thash, C.DOCUMENT_ID) else Debug.trap("CandidMap.clone(): Could not find candid id");
        let cloned = CandidMap.new(schema_map, document_id, extracted_candid);

        cloned;
    };

    public func extract_candid(state : CandidMap) : Candid {
        let { candid_map = map } = state;

        func extract_candid_helper(map : Map.Map<Text, NestedCandid>) : Candid {

            switch (Map.get(map, Map.thash, IS_COMPOUND_TYPE)) {
                case (?#Candid(#Variant(_), #Text(tag))) switch (Map.get(map, Map.thash, tag)) {
                    case (?#Candid(#Text, candid)) {
                        return #Variant(tag, candid);
                    };

                    case (_) {};
                };
                case (?#Candid(#Tuple(_), #Nat(tuple_size))) {

                    let tuples = Array.tabulate(
                        tuple_size,
                        func(i : Nat) : Candid {
                            let index = Nat.toText(i);

                            let ?candid = Map.get(map, Map.thash, index) else Debug.trap("extract_candid_helper: Could not find value for tuple index '" # index # "'");

                            switch (candid) {
                                case (#Candid((candid_type, candid))) {
                                    candid;
                                };
                                case (#CandidMap(nested_map)) {
                                    extract_candid_helper(nested_map);
                                };
                            };

                        },
                    );

                    return #Tuple(tuples);
                };
                case (?#Candid(#Array(_), #Nat(array_size))) {

                    let array = Array.tabulate(
                        array_size,
                        func(i : Nat) : Candid {
                            let index = Nat.toText(i);

                            let ?candid = Map.get(map, Map.thash, index) else Debug.trap("extract_candid_helper: Could not find value for array index '" # index # "'");

                            switch (candid) {
                                case (#Candid((candid_type, candid))) {
                                    candid;
                                };
                                case (#CandidMap(nested_map)) {
                                    extract_candid_helper(nested_map);
                                };
                            };

                        },
                    );

                    return #Array(array);
                };
                case (_) {};
            };

            let fields = Array.map<(Text, NestedCandid), (Text, Candid)>(
                Map.toArray<Text, NestedCandid>(map),
                func((field, nested_candid) : (Text, NestedCandid)) : (Text, Candid) {
                    switch (nested_candid) {
                        case (#CandidMap(nested_map)) {
                            switch (Map.get(nested_map, Map.thash, IS_COMPOUND_TYPE)) {
                                case (?#Candid(#Variant(_), #Text(tag))) {
                                    let ?variant = Map.get(nested_map, Map.thash, tag) else Debug.trap("extract_candid_helper: Could not find value for variant tag '" # tag # "'");

                                    switch (variant) {
                                        case (#Candid((candid_type, candid))) {
                                            return (field, #Variant(tag, candid));
                                        };
                                        case (#CandidMap(deeper_nested_map)) (field, #Variant(tag, extract_candid_helper(deeper_nested_map)));
                                    };

                                };
                                case (_) (field, extract_candid_helper(nested_map));
                            };
                        };
                        case (#Candid((_candid_type, candid))) {
                            (field, candid);
                        };
                    };
                },
            );

            #Record(fields);
        };

        switch (Map.get(map, Map.thash, "")) {
            case (?#Candid((types, candid))) {
                return candid;
            };
            case (?#CandidMap(candid_map)) extract_candid_helper(candid_map);
            case (null) extract_candid_helper(map);
        };

    };

};
