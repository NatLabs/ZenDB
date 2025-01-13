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
// import Mo "mo:moh";

import T "Types";
import Schema "Collection/Schema";

module {

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

    // this loads the first level of a candid record into the map
    // it stores information about the nested records as is and only caches the nested fields when they are accessed in the get method
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

    public class CandidMap(schema : T.Schema, candid : Candid) = self {

        let #Record(types) = schema else Debug.trap("CandidMap only accepts #Record types");
        let #Record(fields) = candid else Debug.trap("CandidMap only accepts #Record types");

        public let candid_map : Map.Map<Text, NestedCandid> = load_record_into_map(types, fields);
        let paths_with_optional_fields = Set.new<Text>();

        // unlike the `load()` method, we already have a map that might have cached nested records
        // these cached records are indicative of the fields that have been accessed and would most likely be accessed again
        // so we replace the values in the cached record fields with the new values
        func reload_record_into_map(map : Map.Map<Text, NestedCandid>, types : [(Text, T.Schema)], fields : [(Text, Candid)]) {
            var var_fields = fields;

            var i = 0;
            while (i < var_fields.size()) {
                let field = var_fields[i].0;
                let candid_type = types[i].1;
                let candid_value = var_fields[i].1;

                let ?nested_candid = Map.get(map, thash, field) else return Debug.trap("CandidMap: Extra field not present in the original candid during reload");

                switch (nested_candid) {
                    case (#CandidMap(nested_map)) {
                        switch (candid_type, candid_value) {
                            case (#Record(record_types) or #Map(record_types), #Record(nested_records) or #Map(nested_records)) {
                                reload_record_into_map(nested_map, record_types, nested_records);
                            };
                            case (#Variant(variant_types), #Variant(variant)) {
                                for ((variant_tag, variant_type) in variant_types.vals()) {
                                    ignore Map.put(nested_map, thash, variant_tag, #Candid(variant_type, #Null));
                                };

                                reload_record_into_map(nested_map, variant_types, [variant]);
                            };
                            case (_) {
                                Debug.trap("CandidMap: Expected #Record, #Map or #Variant type in reload");
                            };
                        };
                    };
                    case (#Candid((candid_type, prev_candid_value))) {
                        add_to_map(map, field, candid_type, candid_value);
                    };
                };

                i += 1;
            };

            // Debug.print("map: " # debug_show (Map.toArray(map)));

        };

        func get_and_cache_map_for_field(key : Text) : (Bool, ?Map.Map<Text, NestedCandid>) {

            let fields = Itertools.peekable(Text.split(key, #text(".")));
            var map = candid_map;
            var prev_map = map;
            var types = schema;
            var is_optional = false;
            var prefix_path = "";
            var return_empty_map = false;

            // Debug.print("starting get_and_cache_map_for_field: " # debug_show (key));

            label extracting_candid_value loop {
                let ?field = fields.next() else break extracting_candid_value;

                if (Set.has(paths_with_optional_fields, thash, prefix_path)) {
                    is_optional := is_optional or true;
                };

                prev_map := map;
                prefix_path := if (prefix_path == "") field else prefix_path # "." # field;

                //    Debug.print("prefix_path: " # debug_show (prefix_path));

                let ?candid = Map.get(map, thash, field) else return (is_optional, null);

                //    Debug.print("candid: " # debug_show (candid));

                func handle_candid(candid_type : Schema, candid_value : Candid, is_parent_optional : Bool) : Bool {
                    let nested_map = switch (candid_type, candid_value) {
                        case (#Option(inner_type), #Option(inner_value)) {
                            ignore Set.put(paths_with_optional_fields, thash, prefix_path);
                            return handle_candid(inner_type, inner_value, true);
                        };
                        case (#Option(inner_type), #Null) {
                            ignore Set.put(paths_with_optional_fields, thash, prefix_path);
                            if (Option.isSome(fields.peek())) {
                                return_empty_map := true;
                            };
                            return false;
                        };
                        case (#Null, #Null) {
                            return false;
                        };
                        case (candid_type, #Null) {
                            //    Debug.print("candid_type: " # debug_show (candid_type));
                            is_optional := true;
                            return_empty_map := true;
                            return false;
                        };
                        case (#Option(inner_type), candid_value) {
                            ignore Set.put(paths_with_optional_fields, thash, prefix_path);
                            return handle_candid(inner_type, candid_value, true);
                        };
                        case ((#Record(record_types) or #Map(record_types), #Record(records) or #Map(records))) {
                            let nested_map = load_record_into_map(record_types, records);
                            ignore Map.put(map, thash, field, #CandidMap(nested_map));
                            nested_map;
                            // if (is_parent_optional) {
                            //     ignore Map.put(nested_map, thash, IS_OPTIONAL, #Candid(#Bool, #True));
                            // };
                        };
                        case (#Variant(variant_types), #Variant(variant)) {

                            let variants = Array.map<(Text, T.CandidType), (Text, Candid)>(
                                variant_types,
                                func(variant_tag : Text, variant_type : T.CandidType) : (Text, Candid) {
                                    if (variant_tag == variant.0) {
                                        is_optional := variant.1 == #Null;
                                        return (variant_tag, variant.1);
                                    };
                                    (variant_tag, #Null);
                                },
                            );

                            //    Debug.print(debug_show ({ variant }));

                            let nested_map = load_record_into_map(variant_types, variants);

                            //    Debug.print(debug_show (Map.toArray(nested_map)));
                            ignore Map.put<Text, NestedCandid>(nested_map, thash, IS_COMPOUND_TYPE, #Candid(compound_types.variant(variant.0)));

                            ignore Map.put(map, thash, field, #CandidMap(nested_map));
                            nested_map;

                            // if (is_parent_optional) {
                            //     ignore Map.put(nested_map, thash, IS_OPTIONAL, #Candid(#Bool, #True));
                            // };
                        };
                        case (#Tuple(tuple_types), #Tuple(tuple_values)) {
                            if (tuple_types.size() != tuple_values.size()) {
                                Debug.trap("CandidMap: Tuple types and values should have the same size");
                            };

                            let nested_map = Map.new<Text, NestedCandid>();

                            for ((i, (tuple_type, tuple_value)) in Itertools.enumerate(Itertools.zip(tuple_types.vals(), tuple_values.vals()))) {
                                ignore Map.put(nested_map, thash, Nat.toText(i), #Candid(tuple_type, tuple_value));
                            };

                            ignore Map.put(nested_map, thash, IS_COMPOUND_TYPE, #Candid(compound_types.tuple(tuple_values.size())));

                            ignore Map.put(map, thash, field, #CandidMap(nested_map));
                            nested_map;

                            // if (is_parent_optional) {
                            //     ignore Map.put(nested_map, thash, IS_OPTIONAL, #Candid(#Bool, #True));
                            // };
                        };
                        case (#Array(array_type), #Array(array_values)) {
                            let nested_map = Map.new<Text, NestedCandid>();

                            for ((i, array_value) in Itertools.enumerate(array_values.vals())) {
                                ignore Map.put(nested_map, thash, Nat.toText(i), #Candid(array_type, array_value));
                            };

                            ignore Map.put(nested_map, thash, IS_COMPOUND_TYPE, #Candid(compound_types.array(array_type, array_values.size())));

                            ignore Map.put(map, thash, field, #CandidMap(nested_map));
                            nested_map;

                            // if (is_parent_optional) {
                            //     ignore Map.put<Text, NestedCandid>(nested_map, thash, IS_OPTIONAL, #Candid(#Bool, #True));
                            // };
                        };
                        // case (#Null, #Null) {
                        //     let nested_map = Map.new<Text, NestedCandid>();
                        //     ignore Map.put(map, thash, field, #CandidMap(nested_map));
                        //     nested_map;
                        // };
                        // case (_, #Null){

                        // };
                        case (_) {
                            // terminate the loop
                            return false;
                        };
                    };

                    map := nested_map;

                    // continue looping
                    true;
                };

                switch (candid) {
                    case (#CandidMap(nested_map)) {
                        //    Debug.print("nested_map: " # debug_show (Map.toArray(nested_map)));
                        //    Debug.print("field: " # debug_show (field));

                        is_optional := is_optional or (
                            switch (Map.get(nested_map, thash, IS_COMPOUND_TYPE)) {
                                case (?#Candid(#Variant(_), #Text(variant_tag))) {
                                    switch (Map.get(nested_map, thash, Option.get(fields.peek(), ""))) {
                                        case (?#Candid(_, #Null)) true;
                                        case (_) false;
                                    };
                                };
                                case (_) false;
                            }
                        );

                        map := nested_map;

                    };
                    case (#Candid((candid_type, candid))) {
                        if (not handle_candid(candid_type, candid, false)) {
                            break extracting_candid_value;
                        };
                    };
                };
            };

            if (Set.has(paths_with_optional_fields, thash, prefix_path)) {
                is_optional := is_optional or true;
            };

            //    Debug.print("map: " # debug_show (Map.toArray(map)));
            //    Debug.print("set: " # debug_show (Set.toArray(paths_with_optional_fields)));
            //    Debug.print("is_optional: " # debug_show (is_optional));
            //    Debug.print("return_empty_map: " # debug_show (return_empty_map));

            if (return_empty_map) {
                //    Debug.print(debug_show (is_optional, null));
                return (is_optional, null);
            };

            (is_optional, ?prev_map);

        };

        public func clone() : CandidMap {
            let extracted_candid = self.extract_candid();
            let cloned = CandidMap(schema, extracted_candid);

            cloned;
        };

        public func get(key : Text) : ?Candid {

            let fields = Iter.toArray(Text.split(key, #text(".")));
            var types = schema;
            var result : ?Candid = null;
            var prefix_path = "";
            var current_field = "";
            var is_compound_type = true;

            let (is_optional, map) = switch (get_and_cache_map_for_field(key)) {
                case ((is_optional, ?map)) (is_optional, map);
                case ((true, null)) return ?#Null;
                case (_) return null;
            };

            //    Debug.print("map: " # debug_show Map.toArray(map));
            //    Debug.print("is_optional: " # debug_show is_optional);

            let field = fields[fields.size() - 1];
            current_field := field;
            prefix_path := if (prefix_path == "") field else prefix_path # "." # field;

            //    Debug.print("field: " # debug_show field);
            //    Debug.print("opt_candid: " # debug_show Map.get(map, thash, field));

            let ?candid = Map.get(map, thash, field) else return null;

            switch (candid) {
                case (#CandidMap(nested_map)) {
                    switch (Map.get(nested_map, thash, IS_COMPOUND_TYPE)) {
                        case (?#Candid(#Variant(_), #Text(tag))) {
                            return ?#Text(tag);
                        };
                        case (_) Debug.trap("CandidMap.get(): Recieved unexpected #CandidMap");
                    };
                };
                case (#Candid((candid_type, candid))) {
                    switch (candid_type, candid) {
                        case ((#Record(_) or #Map(_) or #Variant(_) or #Option(#Record(_)) or #Option(#Map(_)) or #Option(#Variant(_)) or #Tuple(_) or #Array(_), #Record(_) or #Map(_) or #Variant(_) or #Option(#Record(_)) or #Option(#Map(_)) or #Option(#Variant(_)))) {
                            Debug.trap("CandidMap.get(): Should have cached the nested map");
                        };
                        case (_, #Null) return ?#Null;
                        case (_) {
                            is_compound_type := false;
                            result := ?candid;
                        };
                    };
                };
            };

            if (is_optional) switch (result) {
                case (?#Option(val)) return ?#Option(val);
                case (?val) return ?#Option(val : Candid);
                case (null) return ?#Null;
            };

            let opt_compound_tag = Map.get(map, thash, IS_COMPOUND_TYPE);

            if (is_compound_type) switch (opt_compound_tag) {
                case (?#Candid(#Variant(_), #Text(tag))) return ?#Text(tag);
                case (_) {};
            };

            return result;
        };

        /// Assumes the new candid has the same schema as the original candid
        public func reload(new_candid : Candid) {
            let #Record(fields) = new_candid else Debug.trap("CandidMap only accepts #Record types");
            let #Record(types) = schema else Debug.trap("CandidMap only accepts #Record types");
            reload_record_into_map(candid_map, types, fields);
        };

        public func set(key : Text, new_value : Candid) : Result<(), Text> {

            let fields = Iter.toArray(Text.split(key, #text(".")));
            let field = fields[fields.size() - 1];

            let (is_optional, map) = switch (get_and_cache_map_for_field(key)) {
                case (is_optional, ?map) (is_optional, map);
                case (_) return #err("set(): Could not retrieve map with key '" # key # "'");

                // !todo - need to debug code for appending element to array
                // if (Mo.Text.isNumber(field)) {

                //     let fields_excluding_last = Text.join("", Array.take(fields, fields.size() - 1).vals());

                //     let ?(parent_map, is_optional) = get_and_cache_map_for_field(fields_excluding_last) else return #err("set(): Could not retrieve map with key '" # fields_excluding_last # "'");

                //     let ?#CandidMap(nested_map) = Map.get(parent_map, thash, field) else return #err("set(): Could not retrieve map with key '" # field # "'");

                //     let ?#Candid(#Array(_), #Array(array_size_wrapper)) = Map.get(nested_map, thash, IS_COMPOUND_TYPE) else return #err("set(): Expected map to be an array type");

                //     let array_size = array_size_wrapper[0];

                //     let index = Mo.Text.toNat(field);

                //     if (index != array_size) {
                //         return #err("set(): Array index out of bounds, expected to add element at index " # debug_show array_size # " but got " # debug_show index);
                //     };

                //     (nested_map, is_optional);

                // } else return #err("set(): Could not retrieve map with key '" # key # "'");
            };

            switch (Map.get(map, thash, field)) {
                case (?#Candid((candid_type, prev_candid))) {
                    let value : Candid = switch (candid_type) {
                        case (#Option(opt_type)) {
                            let opt_value : Candid = switch (new_value) {
                                case (#Null or #Option(_)) { new_value };
                                case (unwrapped) {
                                    #Option(unwrapped);
                                };
                            };

                        };

                        case (_) new_value;
                    };

                    ignore Map.put<Text, NestedCandid>(map, thash, field, #Candid(candid_type, value));
                };
                case (?#CandidMap(nested_map)) {
                    switch (Map.get(nested_map, thash, IS_COMPOUND_TYPE)) {
                        case (?#Candid(#Variant(_), #Text(prev_tag))) {

                            let #Variant(curr_tag, new_candid) = new_value else Debug.trap("Expected a variant type");

                            switch (Map.get(nested_map, thash, prev_tag), Map.get(nested_map, thash, curr_tag)) {
                                case (?#Candid(prev_type, prev_value), ?#Candid(curr_type, curr_value)) {
                                    if (prev_tag != curr_tag) {
                                        ignore Map.put(nested_map, thash, prev_tag, #Candid(prev_type, #Null));
                                    };

                                    ignore Map.put(nested_map, thash, curr_tag, #Candid(curr_type, new_candid));
                                    ignore Map.put(nested_map, thash, IS_COMPOUND_TYPE, #Candid(compound_types.variant(curr_tag)));
                                };
                                case (_) {};
                            };

                        };
                        case (?#Candid(#Tuple(_), #Nat(tuple_size))) {

                            let #Tuple(new_values) = new_value else Debug.trap("Expected a tuple type");

                            if (new_values.size() != tuple_size) {
                                Debug.trap("set(): Tuple size mismatch");
                            };

                            for ((i, new_value) in Itertools.enumerate(new_values.vals())) {
                                let key = Nat.toText(i);

                                switch (Map.get(nested_map, thash, key)) {
                                    case (?#Candid((candid_type, prev_candid))) {
                                        // Debug.print("(candid_type, prev_candid, new_candid): " # debug_show ((candid_type, prev_candid, new_value)));

                                        if (Schema.validate(candid_type, new_value) != #ok()) {
                                            return #err("set(): Invalid candid type for array index '" # key # "' -> " # debug_show (new_value) # ". Expected " # debug_show (candid_type));
                                        };

                                        ignore Map.put(nested_map, thash, key, #Candid(candid_type, new_value));
                                    };
                                    case (_) return #err("set(): Could not find tuple index '" # key # "' in map");
                                };

                            };

                        };
                        case (?#Candid(#Array(array_type), #Nat(prev_size))) {

                            let #Array(new_values) = new_value else Debug.trap("Expected an array type");

                            for ((i, new_value) in Itertools.enumerate(new_values.vals())) {
                                let key = Nat.toText(i);

                                ignore Map.put(nested_map, thash, key, #Candid(array_type, new_value));

                            };

                            if (prev_size > new_values.size()) {
                                for (i in Itertools.range(new_values.size(), prev_size)) {
                                    let key = Nat.toText(i);

                                    ignore Map.remove(nested_map, thash, key);
                                };
                            };

                            ignore Map.put(nested_map, thash, IS_COMPOUND_TYPE, #Candid(compound_types.array(array_type, new_values.size())));

                        };

                        case (_) {
                            let ?candid_type = get_nested_candid_type(schema, key) else return #err("set(): Could not retrieve candid type for key '" # key # "'");

                            ignore Map.put(map, thash, field, #Candid(candid_type, new_value));
                        };
                    };

                };
                case (_) return #err("set(): Could not find field '" # field # "' in map");
            };

            //    Debug.print("updated candid: " # debug_show extract_candid());

            #ok()

        };

        func extract_candid_helper(map : Map.Map<Text, NestedCandid>) : Candid {

            switch (Map.get(map, thash, IS_COMPOUND_TYPE)) {
                case (?#Candid(#Variant(_), #Text(tag))) switch (Map.get(map, thash, tag)) {
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

                            let ?candid = Map.get(map, thash, index) else Debug.trap("extract_candid_helper: Could not find value for tuple index '" # index # "'");

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

                            let ?candid = Map.get(map, thash, index) else Debug.trap("extract_candid_helper: Could not find value for array index '" # index # "'");

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
                            switch (Map.get(nested_map, thash, IS_COMPOUND_TYPE)) {
                                case (?#Candid(#Variant(_), #Text(tag))) {
                                    let ?variant = Map.get(nested_map, thash, tag) else Debug.trap("extract_candid_helper: Could not find value for variant tag '" # tag # "'");

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

        public func extract_candid() : Candid {
            extract_candid_helper(candid_map);
        };

        public func get_type(key : Text) : ?Schema {
            get_nested_candid_type(schema, key);
        };

    };

    public func get_nested_candid_type(_schema : Schema, key : Text) : ?Schema {
        let nested_field_keys = Text.split(key, #text("."));

        var schema = _schema;

        for (key in nested_field_keys) {
            let #Record(record_fields) or #Option(#Record(record_fields)) or #Variant(record_fields) or #Option(#Variant(record_fields)) = schema else return null;

            let ?found_field = Array.find<(Text, Schema)>(
                record_fields,
                func((variant_name, _) : (Text, Schema)) : Bool {
                    variant_name == key;
                },
            ) else return null;

            schema := found_field.1;
        };

        return ?schema;
    };

};
