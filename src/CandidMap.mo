import Debug "mo:base/Debug";
import Text "mo:base/Text";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Candid "mo:serde/Candid";

import T "Types";

module {
    let { nhash; thash } = Map;

    type NestedCandid = {
        #Candid : Candid.Candid;
        #CandidMap : Map.Map<Text, NestedCandid>;
    };

    public class CandidMap(candid : Candid.Candid) {

        // this loads the first level of a candid record into the map
        // it stores information about the nested records as is and only caches the nested fields when they are accessed in the get method
        func load_record_into_map(fields : [(Text, Candid.Candid)]) : Map.Map<Text, NestedCandid> {
            let map = Map.new<Text, NestedCandid>();

            var i = 0;
            while (i < fields.size()) {
                ignore Map.put(map, thash, fields[i].0, #Candid(fields[i].1));
                i += 1;
            };

            map;
        };

        // unlike the `load()` method, we already have a map that might have cached nested records
        // these cached records are indicative of the fields that have been accessed and would most likely be accessed again
        // so we replace the values in the cached record fields with the new values
        func reload_record_into_map(map : Map.Map<Text, NestedCandid>, fields : [(Text, Candid.Candid)]) {
            var var_fields = fields;

            var i = 0;
            while (i < var_fields.size()) {
                let field = var_fields[i].0;
                let candid_value = var_fields[i].1;

                let ?nested_candid = Map.get(map, thash, field) else return Debug.trap("CandidMap: Extra field not present in the original candid during reload");

                switch (nested_candid) {
                    case (#CandidMap(nested_map)) {
                        let #Record(nested_fields) = candid_value else Debug.trap("CandidMap: Expected #Record type in reload");
                        reload_record_into_map(nested_map, nested_fields);
                    };
                    case (#Candid(prev_candid_value)) {
                        ignore Map.put(map, thash, field, #Candid(candid_value));
                    };
                };
            };

            i += 1;

        };

        let #Record(fields) = candid else Debug.trap("CandidMap only accepts #Record types");
        let candid_map = load_record_into_map(fields);
        let paths_with_optional_fields = Set.new<Text>();

        public func get(key : Text) : ?Candid.Candid {

            let fields = Text.split(key, #text("."));
            var map = candid_map;
            var result : ?Candid.Candid = null;
            var is_optional = false;
            var prefix_path = "";

            label extracting_candid_value loop {
                let ?field = fields.next() else break extracting_candid_value;
                prefix_path := if (prefix_path == "") field else prefix_path # "." # field;

                if (Set.has(paths_with_optional_fields, thash, prefix_path)) {
                    is_optional := is_optional or true;
                };

                let ?candid = Map.get(map, thash, field) else return null;

                switch (candid) {
                    case (#CandidMap(nested_map)) {
                        map := nested_map;
                    };
                    case (#Candid(candid)) {
                        switch (candid) {
                            case (#Record(record) or #Map(record)) {
                                let nested_map = load_record_into_map(record);
                                ignore Map.put(map, thash, field, #CandidMap(nested_map));
                                map := nested_map;
                            };
                            case (#Option(#Record(record)) or #Option(#Map(record))) {
                                ignore Set.put(paths_with_optional_fields, thash, prefix_path);
                                let nested_map = load_record_into_map(record);
                                ignore Map.put(map, thash, field, #CandidMap(nested_map));
                                map := nested_map;
                            };
                            case (#Null) return ? #Null;
                            case (_) {
                                result := ?candid;
                                break extracting_candid_value;
                            };
                        };
                    };
                };
            };

            let last_field = fields.next();
            // Debug.print("(last_field, value) ->  " # debug_show (last_field, result));
            assert last_field == null;

            if (is_optional) switch (result) {
                // case (? #Minimum) return Debug.trap("CandidMap: Cannot return #Minimum for an optional field");
                // case (? #Maximum) return Debug.trap("CandidMap: Cannot return #Maximum for an optional field");
                case (?val) return ? #Option(val : Candid.Candid);
                case (null) return null;
            };

            return result;
        };

        /// Assumes the new candid has the same schema as the original candid
        public func reload(new_candid : Candid.Candid) {
            let #Record(fields) = new_candid else Debug.trap("CandidMap only accepts #Record types");
            reload_record_into_map(candid_map, fields);
        };

    };

};
