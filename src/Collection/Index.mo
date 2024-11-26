import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Tag "mo:candid/Tag";
import BitMap "mo:bit-map";

import T "../Types";
import C "Constants";

module {

    public type BestIndexResult = {
        index : T.Index;
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        sorted_in_reverse : Bool;
        fully_covered_equality_and_range_fields : Set.Set<Text>;
    };

    type StableCollection = T.StableCollection;
    type Buffer<A> = Buffer.Buffer<A>;
    type Index = T.Index;
    let { nhash; thash } = Map;

    public func get_best_index(collection : StableCollection, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) : ?BestIndexResult {
        let equal_fields = Set.new<Text>();
        let sort_fields = Buffer.Buffer<(Text, T.SortDirection)>(8);
        let range_fields = Set.new<Text>();
        // let partially_covered_fields = Set.new<Text>();

        func fill_field_maps(equal_fields : Set.Set<Text>, sort_fields : Buffer<(Text, T.SortDirection)>, range_fields : Set.Set<Text>, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) {

            sort_fields.clear();

            switch (sort_field) {
                case (?(field, direction)) sort_fields.add(field, direction);
                case (null) {};
            };

            // sort_fields.reverse(); or add in reverse order

            for ((field, op) in operations.vals()) {
                switch (op) {
                    case (#eq(_)) ignore Set.put(equal_fields, thash, field);
                    case (_) ignore Set.put(range_fields, thash, field);
                };
            };
        };

        fill_field_maps(equal_fields, sort_fields, range_fields, operations, sort_field);

        var best_score = 0;
        var best_index : ?Index = null;
        var best_requires_additional_sorting = false;
        var best_requires_additional_filtering = false;
        var best_fully_covered_equality_and_range_fields = Set.new<Text>();

        // the sorting direction of the query and the index can either be a direct match
        // or a direct opposite in order to return the results without additional sorting
        var is_query_and_index_direction_a_match : ?Bool = null;

        let EQUALITY_SCORE = 4;
        let SORT_SCORE = 2;
        let RANGE_SCORE = 1;

        for (index in Map.vals(collection.indexes)) {

            var num_of_equal_fields_evaluated = 0;
            var num_of_sort_fields_evaluated = 0;
            var num_of_range_fields_evaluated = 0;

            var index_score = 0;
            var requires_additional_filtering = false;
            var requires_additional_sorting = false;
            var positions_matching_equality_or_range = Set.new<Nat>();
            let fully_covered_equality_and_range_fields = Set.new<Text>();

            var index_key_details_position = 0;

            label scoring_indexes for ((index_key, direction) in index.key_details.vals()) {
                index_key_details_position += 1;

                if (index_key == C.RECORD_ID_FIELD) break scoring_indexes;

                var matches_at_least_one_column = false;

                switch (Set.has(equal_fields, thash, index_key)) {
                    case (true) {
                        index_score += EQUALITY_SCORE;
                        num_of_equal_fields_evaluated += 1;
                        matches_at_least_one_column := true;
                        Set.add(positions_matching_equality_or_range, nhash, index_key_details_position);
                        Set.add(fully_covered_equality_and_range_fields, thash, index_key);
                    };
                    case (false) {};
                };

                if (num_of_sort_fields_evaluated < sort_fields.size()) {
                    let i = sort_fields.size() - 1 - num_of_sort_fields_evaluated;
                    let sort_field = sort_fields.get(i);

                    if (index_key == sort_field.0) {

                        matches_at_least_one_column := true;

                        num_of_sort_fields_evaluated += 1;
                        switch (is_query_and_index_direction_a_match) {
                            case (null) {
                                is_query_and_index_direction_a_match := ?(direction == sort_field.1);
                                index_score += SORT_SCORE;
                            };
                            case (?is_a_match) {
                                if (is_a_match == (direction == sort_field.1)) {
                                    index_score += SORT_SCORE;
                                } else {
                                    requires_additional_sorting := true;
                                };
                            };
                        };
                    };
                };

                if (Set.has(range_fields, thash, index_key)) {
                    index_score += RANGE_SCORE;
                    num_of_range_fields_evaluated += 1;
                    matches_at_least_one_column := true;

                    Set.add(positions_matching_equality_or_range, nhash, index_key_details_position);
                    Set.add(fully_covered_equality_and_range_fields, thash, index_key);

                    break scoring_indexes;
                };

                // Debug.print("index_key, index_score: " # debug_show (index_key, index_score));

                if (not matches_at_least_one_column) break scoring_indexes;

            };

            if (
                num_of_range_fields_evaluated < Set.size(range_fields) or num_of_equal_fields_evaluated < Set.size(equal_fields)
            ) {
                requires_additional_filtering := true;
            };

            if ((Set.size(positions_matching_equality_or_range) == 0 and operations.size() > 0)) {
                requires_additional_filtering := true;
            };

            label searching_for_holes for ((prev, current) in Itertools.slidingTuples(Set.keys(positions_matching_equality_or_range))) {
                if (current - prev > 1) {
                    requires_additional_filtering := true;
                    break searching_for_holes;
                };
            };

            Debug.print("index matching results:");
            Debug.print("index, score: " # debug_show (index.name, index_score));
            Debug.print("operations: " # debug_show operations);

            Debug.print("index_key_details: " # debug_show index.key_details);
            Debug.print("equal_fields: " # debug_show Set.toArray(equal_fields));
            Debug.print("  num_of_equal_fields_evaluated: " # debug_show num_of_equal_fields_evaluated);

            Debug.print("sort_fields: " # debug_show Buffer.toArray(sort_fields));
            Debug.print("  num_of_sort_fields_evaluated: " # debug_show num_of_sort_fields_evaluated);
            Debug.print("range_fields: " # debug_show Set.toArray(range_fields));
            Debug.print("  num_of_range_fields_evaluated: " # debug_show num_of_range_fields_evaluated);

            Debug.print("requires_additional_filtering: " # debug_show requires_additional_filtering);
            Debug.print("requires_additional_sorting: " # debug_show requires_additional_sorting);
            Debug.print("num, range_size: " # debug_show (num_of_range_fields_evaluated, Set.size(range_fields)));
            Debug.print("num, equal_size: " # debug_show (num_of_equal_fields_evaluated, Set.size(equal_fields)));
            Debug.print("fully_covered_equality_and_range_fields: " # debug_show Set.toArray(fully_covered_equality_and_range_fields));

            if (index_score > best_score) {
                best_score := index_score;
                best_index := ?index;
                best_requires_additional_filtering := requires_additional_filtering;
                best_requires_additional_sorting := requires_additional_sorting or num_of_sort_fields_evaluated < sort_fields.size();
                best_fully_covered_equality_and_range_fields := fully_covered_equality_and_range_fields;
            };

        };

        let index = switch (best_index) {
            case (null) return null;
            case (?index) index;
        };

        let index_response = {
            index;
            requires_additional_sorting = best_requires_additional_sorting;
            requires_additional_filtering = best_requires_additional_filtering;
            sorted_in_reverse = switch (is_query_and_index_direction_a_match) {
                case (null) false;
                case (?is_a_match) not is_a_match;
            };
            fully_covered_equality_and_range_fields = best_fully_covered_equality_and_range_fields;
        };

        ?index_response;

    };
};
