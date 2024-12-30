import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Candid "mo:serde/Candid";

import T "Types";

module {

    let { nhash; thash } = Map;

    type NestedCandid = {
        #Candid : (T.Schema, Candid.Candid);
        #CandidMap : (Map.Map<Text, NestedCandid>);
    };

    let IS_VARIANT_TAG = ":variant_tag";

    public class CandidMap(schema : T.Schema, candid : Candid.Candid) {

        func add_to_map(map : Map.Map<Text, NestedCandid>, field_key : Text, candid_type : T.Schema, candid_value : Candid.Candid) {
            ignore Map.put<Text, NestedCandid>(
                map,
                thash,
                field_key,
                #Candid(candid_type, candid_value),
            );
        };

        // this loads the first level of a candid record into the map
        // it stores information about the nested records as is and only caches the nested fields when they are accessed in the get method
        func load_record_into_map(types : [(Text, T.Schema)], fields : [(Text, Candid.Candid)]) : Map.Map<Text, NestedCandid> {
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

        // unlike the `load()` method, we already have a map that might have cached nested records
        // these cached records are indicative of the fields that have been accessed and would most likely be accessed again
        // so we replace the values in the cached record fields with the new values
        func reload_record_into_map(map : Map.Map<Text, NestedCandid>, types : [(Text, T.Schema)], fields : [(Text, Candid.Candid)]) {
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

        let #Record(types) = schema else Debug.trap("CandidMap only accepts #Record types");
        let #Record(fields) = candid else Debug.trap("CandidMap only accepts #Record types");

        // Debug.print("types: " # debug_show (types));
        // Debug.print("fields: " # debug_show (fields));

        let candid_map = load_record_into_map(types, fields);
        let paths_with_optional_fields = Set.new<Text>();

        // Debug.print("candid_map: " # debug_show (Map.toArray(candid_map)));

        public func get(key : Text) : ?Candid.Candid {

            let fields = Text.split(key, #text("."));
            var map = candid_map;
            var types = schema;
            var result : ?Candid.Candid = null;
            var is_optional = false;
            var prefix_path = "";
            var current_field = "";
            var is_compound_type = true;

            label extracting_candid_value loop {
                let ?field = fields.next() else break extracting_candid_value;
                current_field := field;
                prefix_path := if (prefix_path == "") field else prefix_path # "." # field;

                if (Set.has(paths_with_optional_fields, thash, prefix_path)) {
                    is_optional := is_optional or true;
                };

                let ?candid = Map.get(map, thash, field) else return null;

                // Debug.print("(prefix, value): " # debug_show (prefix_path, candid));

                switch (candid) {
                    case (#CandidMap(nested_map)) {
                        map := nested_map;
                    };
                    case (#Candid((candid_type, candid))) {
                        switch (candid_type, candid) {
                            case ((#Record(record_types) or #Map(record_types), #Record(records) or #Map(records))) {
                                let nested_map = load_record_into_map(record_types, records);
                                ignore Map.put(map, thash, field, #CandidMap(nested_map));
                                map := nested_map;
                            };
                            case (#Variant(variant_types), #Variant(variant)) {

                                let variants = Array.map<(Text, T.CandidType), (Text, Candid.Candid)>(
                                    variant_types,
                                    func(variant_tag : Text, variant_type : T.CandidType) : (Text, Candid.Candid) {
                                        if (variant_tag == variant.0) {
                                            return (variant_tag, variant.1);
                                        };
                                        (variant_tag, #Null);
                                    },
                                );

                                let nested_map = load_record_into_map(variant_types, variants);
                                ignore Map.put<Text, NestedCandid>(nested_map, thash, IS_VARIANT_TAG, #Candid(#Text, #Text(variant.0)));

                                ignore Map.put(map, thash, field, #CandidMap(nested_map));
                                map := nested_map;
                            };
                            case (#Option(#Record(record_types)) or #Option(#Map(record_types)), #Option(#Record(records)) or #Option(#Map(records))) {
                                ignore Set.put(paths_with_optional_fields, thash, prefix_path);
                                let nested_map = load_record_into_map(record_types, records);
                                ignore Map.put(map, thash, field, #CandidMap(nested_map));
                                map := nested_map;
                            };
                            case (#Option(#Variant(variant_types)), #Option(#Variant(variant))) {
                                let variants = Array.map<(Text, T.CandidType), (Text, Candid.Candid)>(
                                    variant_types,
                                    func(variant_tag : Text, variant_type : T.CandidType) : (Text, Candid.Candid) {
                                        if (variant_tag == variant.0) {
                                            return (variant_tag, variant.1);
                                        };
                                        (variant_tag, #Null);
                                    },
                                );

                                ignore Set.put(paths_with_optional_fields, thash, prefix_path);
                                let nested_map = load_record_into_map(variant_types, variants);
                                ignore Map.put<Text, NestedCandid>(nested_map, thash, IS_VARIANT_TAG, #Candid(#Text, #Text(variant.0)));

                                ignore Map.put(map, thash, field, #CandidMap(nested_map));
                                map := nested_map;
                            };
                            case (_, #Null) return ?#Null;
                            case (_) {
                                is_compound_type := false;
                                result := ?candid;
                                break extracting_candid_value;
                            };
                        };
                    };
                };
            };

            // Debug.print("candid_map: " # debug_show (Map.toArray(candid_map)));

            // Debug.print("(key, prefix): " # debug_show (key, prefix_path));

            let last_field = fields.next();
            // Debug.print("(last_field, value) ->  " # debug_show (last_field, result));
            assert last_field == null;

            if (is_optional) switch (result) {
                // case (? #Minimum) return Debug.trap("CandidMap: Cannot return #Minimum for an optional field");
                // case (? #Maximum) return Debug.trap("CandidMap: Cannot return #Maximum for an optional field");
                case (?val) return ?#Option(val : Candid.Candid);
                case (null) return null;
            };

            // Debug.print("is_compound_type: " # debug_show (is_compound_type));

            let opt_variant_tag = Map.get(map, thash, IS_VARIANT_TAG);

            // Debug.print("opt_variant_tag: " # debug_show (opt_variant_tag));

            if (is_compound_type) switch (opt_variant_tag) {
                case (?#Candid(#Text, #Text(tag))) return ?#Text(tag);
                case (_) {};
            };

            // Debug.print("result: " # debug_show (result));

            return result;
        };

        /// Assumes the new candid has the same schema as the original candid
        public func reload(new_candid : Candid.Candid) {
            let #Record(fields) = new_candid else Debug.trap("CandidMap only accepts #Record types");
            let #Record(types) = schema else Debug.trap("CandidMap only accepts #Record types");
            reload_record_into_map(candid_map, types, fields);
        };

    };

};
