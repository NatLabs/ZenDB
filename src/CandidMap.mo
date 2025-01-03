import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
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

        func get_and_cache_map_for_field(key : Text) : ?(Map.Map<Text, NestedCandid>, Bool) {

            let fields = Text.split(key, #text("."));
            var map = candid_map;
            var prev_map = map;
            var types = schema;
            var result : ?Candid.Candid = null;
            var is_optional = false;
            var prefix_path = "";
            var current_field = "";
            var is_compound_type = true;

            label extracting_candid_value loop {
                prev_map := map;
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
                            case (_) {
                                is_compound_type := false;
                                result := ?candid;
                                break extracting_candid_value;
                            };
                        };
                    };
                };
            };

            ?(prev_map, is_optional);

        };

        public func get(key : Text) : ?Candid.Candid {

            let fields = Iter.toArray(Text.split(key, #text(".")));
            var types = schema;
            var result : ?Candid.Candid = null;
            var prefix_path = "";
            var current_field = "";
            var is_compound_type = true;

            let ?(map, is_optional) = get_and_cache_map_for_field(key) else return null;

            let field = fields[fields.size() - 1];
            current_field := field;
            prefix_path := if (prefix_path == "") field else prefix_path # "." # field;

            let ?candid = Map.get(map, thash, field) else return null;

            switch (candid) {
                case (#CandidMap(_)) {
                    Debug.trap("CandidMap.get(): Should have cached the nested map");
                };
                case (#Candid((candid_type, candid))) {
                    switch (candid_type, candid) {
                        case ((#Record(_) or #Map(_) or #Variant(_) or #Option(#Record(_)) or #Option(#Map(_)) or #Option(#Variant(_)), #Record(_) or #Map(_) or #Variant(_) or #Option(#Record(_)) or #Option(#Map(_)) or #Option(#Variant(_)))) {
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
                case (?val) return ?#Option(val : Candid.Candid);
                case (null) return null;
            };

            let opt_variant_tag = Map.get(map, thash, IS_VARIANT_TAG);

            if (is_compound_type) switch (opt_variant_tag) {
                case (?#Candid(#Text, #Text(tag))) return ?#Text(tag);
                case (_) {};
            };

            return result;
        };

        /// Assumes the new candid has the same schema as the original candid
        public func reload(new_candid : Candid.Candid) {
            let #Record(fields) = new_candid else Debug.trap("CandidMap only accepts #Record types");
            let #Record(types) = schema else Debug.trap("CandidMap only accepts #Record types");
            reload_record_into_map(candid_map, types, fields);
        };

        public func set(key : Text, new_value : Candid.Candid) {

            let ?(map, is_optional) = get_and_cache_map_for_field(key) else Debug.trap("Field not present in schema"); // this will cache the nested fields

            let fields = Iter.toArray(Text.split(key, #text(".")));
            let field = fields[fields.size() - 1];

            switch (Map.get(map, thash, IS_VARIANT_TAG)) {
                case (?#Candid(#Text, #Text(prev_tag))) {

                    let #Variant(curr_tag, new_candid) = new_value else Debug.trap("Expected a variant type");

                    switch (Map.get(map, thash, prev_tag), Map.get(map, thash, curr_tag)) {
                        case (?#Candid(prev_type, #Variant(prev_tag, prev_candid)), ?#Candid(curr_type, #Variant(curr_tag, curr_candid))) {
                            if (prev_tag != curr_tag) {
                                ignore Map.put(map, thash, prev_tag, #Candid(prev_type, #Null));
                            };

                            ignore Map.put(map, thash, curr_tag, #Candid(curr_type, new_candid));
                            ignore Map.put(map, thash, IS_VARIANT_TAG, #Candid(#Text, #Text(curr_tag)));
                        };
                        case (_) {};
                    };

                };

                case (_) {
                    switch (Map.get(map, thash, field)) {
                        case (?#Candid((candid_type, prev_candid))) {
                            ignore Map.put(map, thash, field, #Candid(candid_type, new_value));
                        };
                        case (_) {
                            Debug.trap("Field not present in schema");
                        };
                    };
                };
            };

        };

        func extract_candid_helper(map : Map.Map<Text, NestedCandid>) : Candid.Candid {

            switch (Map.get(map, thash, IS_VARIANT_TAG)) {
                case (?#Candid(#Text, #Text(tag))) switch (Map.get(map, thash, tag)) {
                    case (?#Candid(#Text, candid)) {
                        return #Variant(tag, candid);
                    };
                    case (_) {};
                };
                case (_) {};
            };

            let fields = Array.map<(Text, NestedCandid), (Text, Candid.Candid)>(
                Map.toArray<Text, NestedCandid>(map),
                func((field, nested_candid) : (Text, NestedCandid)) : (Text, Candid.Candid) {
                    switch (nested_candid) {
                        case (#CandidMap(nested_map)) {
                            (field, extract_candid_helper(nested_map));
                        };
                        case (#Candid((_candid_type, candid))) {
                            (field, candid);
                        };
                    };
                },
            );

            #Record(fields);
        };

        public func extract_candid() : Candid.Candid {
            extract_candid_helper(candid_map);
        };

    };

};
