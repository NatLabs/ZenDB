import Text "mo:base/Text";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Order "mo:base/Order";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Option "mo:base/Option";
import Iter "mo:base/Iter";
import Float "mo:base/Float";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Tag "mo:candid/Tag";
import BitMap "mo:bit-map";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "../Types";
import CandidMap "../CandidMap";
import Utils "../Utils";
import C "../Constants";

import CollectionUtils "Utils";
import Schema "Schema";

module {

    type BestIndexResult = T.BestIndexResult;

    public type IndexCmpDetails = {
        index : T.Index;

        num_of_range_fields : Nat;
        num_of_sort_fields : Nat;
        num_of_equal_fields : Nat;

        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        sorted_in_reverse : Bool;

        fully_covered_equality_and_range_fields : Set.Set<Text>;

        interval : (Nat, Nat);
    };

    type StableCollection = T.StableCollection;
    type Buffer<A> = Buffer.Buffer<A>;
    type Index = T.Index;
    type Map<A, B> = Map.Map<A, B>;
    type Iter<A> = Iter.Iter<A>;
    type State<A> = T.State<A>;
    type Candid = T.Candid;
    type Bounds = T.Bounds;
    type SortDirection = T.SortDirection;
    type FieldLimit = T.FieldLimit;
    type Order = Order.Order;

    let { nhash; thash } = Map;

    let EQUALITY_SCORE = 4;
    let SORT_SCORE = 2;
    let RANGE_SCORE = 1;

    let ADDITIONAL_FILTER_SCORE = 1;
    let ADDITIONAL_SORT_SCORE = 1;

    func operation_eval(
        field : Text,
        op : T.ZqlOperators,
        lower : Map<Text, T.State<Candid>>,
        upper : Map<Text, T.State<Candid>>,
    ) {
        switch (op) {
            case (#eq(candid)) {
                ignore Map.put(lower, thash, field, #True(candid));
                ignore Map.put(upper, thash, field, #True(candid));
            };
            case (#gte(candid)) {
                switch (Map.get(lower, thash, field)) {
                    case (? #True(val) or ? #False(val)) {
                        if (Schema.cmp_candid(#Empty, candid, val) == 1) {
                            ignore Map.put(lower, thash, field, #True(candid));
                        };
                    };
                    case (null) ignore Map.put(lower, thash, field, #True(candid));
                };
            };
            case (#lte(candid)) {
                switch (Map.get(upper, thash, field)) {
                    case (? #True(val) or ? #False(val)) {
                        if (Schema.cmp_candid(#Empty, candid, val) == -1) {
                            ignore Map.put(upper, thash, field, #True(candid));
                        };
                    };
                    case (null) ignore Map.put(upper, thash, field, #True(candid));
                };
            };
            case (#lt(candid)) {
                switch (Map.get(upper, thash, field)) {
                    case (? #True(val) or ? #False(val)) {
                        let cmp = Schema.cmp_candid(#Empty, candid, val);
                        if (cmp == -1 or cmp == 0) {
                            ignore Map.put(upper, thash, field, #False(candid));
                        };
                    };
                    case (null) ignore Map.put(upper, thash, field, #False(candid));
                };
            };
            case (#gt(candid)) {
                switch (Map.get(lower, thash, field)) {
                    case (? #True(val) or ? #False(val)) {
                        let cmp = Schema.cmp_candid(#Empty, candid, val);
                        if (cmp == 1 or cmp == 0) {
                            ignore Map.put(lower, thash, field, #False(candid));
                        };
                    };
                    case (null) ignore Map.put(lower, thash, field, #False(candid));
                };
            };
            case (#In(_) or #Not(_)) {
                Debug.trap(debug_show op # " not allowed in this context. Should have been expanded by the query builder");
            };
        };
    };

    public func scan<Record>(
        collection : T.StableCollection,
        index : T.Index,
        start_query : [(Text, ?T.State<Candid>)],
        end_query : [(Text, ?T.State<Candid>)],
        opt_cursor : ?(Nat, Candid.Candid),
    ) : (Nat, Nat) {
        // Debug.print("start_query: " # debug_show start_query);
        // Debug.print("end_query: " # debug_show end_query);

        let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);

        func sort_by_key_details(a : (Text, Any), b : (Text, Any)) : Order {
            let pos_a = switch (Array.indexOf<(Text, SortDirection)>((a.0, #Ascending), index.key_details, Utils.tuple_eq(Text.equal))) {
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            let pos_b = switch (Array.indexOf<(Text, SortDirection)>((b.0, #Ascending), index.key_details, Utils.tuple_eq(Text.equal))) {
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            if (pos_a > pos_b) return #greater;
            if (pos_a < pos_b) return #less;
            #equal;
        };

        let sorted_start_query = Array.sort(start_query, sort_by_key_details);
        let sorted_end_query = Array.sort(end_query, sort_by_key_details);

        // Debug.print("scan cursor: " # debug_show opt_cursor);

        let full_start_query = switch (opt_cursor) {
            case (null) do {

                Array.tabulate<(Candid)>(
                    index.key_details.size(),
                    func(i : Nat) : (Candid) {

                        if (i >= sorted_start_query.size()) {
                            return (#Minimum);
                        };

                        let val = switch (sorted_start_query[i].1) {
                            case (? #True(val) or ? #False(val)) val;
                            case (null) #Minimum;
                        };

                        (val);
                    },
                );
            };
            case (?(id, cursor)) {
                let cursor_map = CandidMap.fromCandid(cursor);
                Array.tabulate<Candid>(
                    index.key_details.size(),
                    func(i : Nat) : (Candid) {
                        if (index.key_details[i].0 == C.RECORD_ID_FIELD) {
                            // RECORD_ID_FIELD is only added in the query if it is a cursor
                            return #Nat(id + 1);
                        };

                        let key = index.key_details[i].0;

                        let val = if (i >= sorted_start_query.size()) {
                            (#Minimum);
                        } else switch (sorted_start_query[i].1) {
                            case (? #True(val) or ? #False(val)) val;
                            case (null) #Minimum;
                        };

                        val;

                    },
                );
            };
        };

        // Debug.print("full_scan_query: " # debug_show full_start_query);

        let full_end_query = do {
            Array.tabulate<Candid>(
                index.key_details.size(),
                func(i : Nat) : (Candid) {
                    if (i >= sorted_end_query.size()) {
                        return (#Maximum);
                    };

                    let key = sorted_end_query[i].0;
                    let ?(#True(val)) or ?(#False(val)) = sorted_end_query[i].1 else return (#Maximum);

                    (val);
                },
            );
        };

      // Debug.print("Index_key_details: " # debug_show index.key_details);
      // Debug.print("full_start_query: " # debug_show full_start_query);
      // Debug.print("full_end_query: " # debug_show full_end_query);

        let scans = CollectionUtils.memorybtree_scan_interval(index.data, index_data_utils, ?full_start_query, ?full_end_query);
      // Debug.print("scan_intervals: " # debug_show scans);
        scans

        // let records_iter = MemoryBTree.scan(index.data, index_data_utils, ?full_start_query, ?full_end_query);

        // let record_ids_iter = Iter.map<([Candid], Nat), Nat>(
        //     records_iter,
        //     func((_, id) : ([Candid], Nat)) : (Nat) { id },
        // );

        // record_ids_iter;

        // return id_to_record_iter(collection, blobify,record_ids_iter );

    };

    public func extract_scan_and_filter_bounds(lower : Map<Text, T.State<Candid>>, upper : Map<Text, T.State<Candid>>, opt_index_key_details : ?[(Text, T.SortDirection)], opt_fully_covered_equality_and_range_fields : ?Set.Set<Text>) : (Bounds, Bounds) {

        assert Option.isSome(opt_index_key_details) == Option.isSome(opt_fully_covered_equality_and_range_fields);

        let scan_bounds = switch (opt_index_key_details) {
            case (null) ([], []);
            case (?index_key_details) {

                let scan_lower_bound = Array.map(
                    index_key_details,
                    func((field, _) : (Text, SortDirection)) : FieldLimit {
                        let lower_bound = Map.get(lower, thash, field);
                        (field, lower_bound);
                    },
                );

                let scan_upper_bound = Array.map(
                    index_key_details,
                    func((field, _) : (Text, SortDirection)) : FieldLimit {
                        let upper_bound = Map.get(upper, thash, field);
                        (field, upper_bound);
                    },
                );

                (scan_lower_bound, scan_upper_bound);

            };
        };

        let (partially_covered_lower, partially_covered_upper) = switch (opt_fully_covered_equality_and_range_fields) {
            case (null) (lower, upper);
            case (?fully_covered_equality_and_range_fields) {

                let partially_covered_lower = Map.new<Text, T.State<Candid>>();
                let partially_covered_upper = Map.new<Text, T.State<Candid>>();

                for ((field, value) in Map.entries(lower)) {
                    if (not Set.has(fully_covered_equality_and_range_fields, thash, field)) {
                        ignore Map.put(partially_covered_lower, thash, field, value);
                    };
                };

                for ((field, value) in Map.entries(upper)) {
                    if (not Set.has(fully_covered_equality_and_range_fields, thash, field)) {
                        ignore Map.put(partially_covered_upper, thash, field, value);
                    };
                };

                (partially_covered_lower, partially_covered_upper);

            };
        };

        let lower_bound_size = Map.size(partially_covered_lower);
        let upper_bound_size = Map.size(partially_covered_upper);

        let is_lower_bound_larger = lower_bound_size > upper_bound_size;
        let max_size = Nat.max(lower_bound_size, upper_bound_size);

        let (a, b) = if (is_lower_bound_larger) {
            (partially_covered_lower, partially_covered_upper);
        } else {
            (partially_covered_upper, partially_covered_lower);
        };

        let iter = Map.entries(a);
        let arr1 = Array.tabulate<(Text, ?State<Candid>)>(
            max_size,
            func(i : Nat) : (Text, ?State<Candid>) {
                let ?(key, value) = iter.next();
                (key, ?value);
            },
        );

        let iter_2 = Map.entries(a);
        let arr2 = Array.tabulate<(Text, ?State<Candid>)>(
            max_size,
            func(i : Nat) : (Text, ?State<Candid>) {
                let ?(key, _) = iter_2.next();
                let value = Map.get(b, thash, key);
                (key, value);
            },
        );

        let filter_bounds = if (is_lower_bound_larger) (arr1, arr2) else (arr2, arr1);

        (scan_bounds, filter_bounds)

    };

    public func convert_simple_operations_to_scan_and_filter_bounds(is_and_operation : Bool, simple_operations : [(Text, T.ZqlOperators)], opt_index_key_details : ?[(Text, T.SortDirection)], opt_fully_covered_equality_and_range_fields : ?Set.Set<Text>) : (T.Bounds, T.Bounds) {

        let lower_bound = Map.new<Text, T.State<T.Candid>>();
        let upper_bound = Map.new<Text, T.State<T.Candid>>();

        let fields_with_equality_ops = Set.new<Text>();

        for ((field, op) in simple_operations.vals()) {
            // if the field is already in the lower or upper bounds, then we can't add it again
            // because it would be a contradiction
            // for example, if we have an equal operation on a field (x = 5), we can't have another operation on the same field (like x > 5 or x < 5 or x = 8)

            switch (op) {
                case (#eq(_)) {
                    let opt_exists_in_lower = Map.get(lower_bound, thash, field);
                    let opt_exists_in_upper = Map.get(upper_bound, thash, field);

                    if (false) {
                        // move to a seperate function that validates the operations before executing them

                        if (is_and_operation) {

                            let has_equality = Set.has(fields_with_equality_ops, thash, field);

                            if (Option.isSome(opt_exists_in_lower) or Option.isSome(opt_exists_in_upper) or has_equality) {
                                Debug.trap("Contradictory operations on the same field");
                            };

                            Set.add(fields_with_equality_ops, thash, field);
                        };
                    };

                };
                case (_) {};
            };

            operation_eval(field, op, lower_bound, upper_bound);

        };

        extract_scan_and_filter_bounds(lower_bound, upper_bound, opt_index_key_details, opt_fully_covered_equality_and_range_fields);

    };

    func calculate_score(index_details : IndexCmpDetails, subtract_negative_features : Bool) : Float {
        let {
            num_of_range_fields;
            num_of_sort_fields;
            num_of_equal_fields;
            requires_additional_filtering;
            requires_additional_sorting;
            sorted_in_reverse;
            interval;
        } = index_details;

        let range_score = num_of_range_fields * 50;
        let sort_score = num_of_sort_fields * 75;
        let equality_score = num_of_equal_fields * 100;

        let additional_filter_score = 0;
        let additional_sort_score = 0;

        var score = Float.fromInt(
            range_score + sort_score + equality_score
        );

        let size = Float.fromInt(interval.1 - interval.0);

        var size_score = 300 - Float.min(Utils.log2(size) * 20, 300);

        if (requires_additional_filtering) {
            size_score *= 0.7;
        };

        if (requires_additional_sorting) {
            size_score *= 0.3;
        };

        score += size_score;

        score

    };

    public func fill_field_maps(equal_fields : Set.Set<Text>, sort_fields : Buffer<(Text, T.SortDirection)>, range_fields : Set.Set<Text>, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) {

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

    public func get_best_index(collection : StableCollection, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) : ?BestIndexResult {
        let equal_fields = Set.new<Text>();
        let sort_fields = Buffer.Buffer<(Text, T.SortDirection)>(8);
        let range_fields = Set.new<Text>();
        // let partially_covered_fields = Set.new<Text>();

        fill_field_maps(equal_fields, sort_fields, range_fields, operations, sort_field);

        // the sorting direction of the query and the index can either be a direct match
        // or a direct opposite in order to return the results without additional sorting
        var is_query_and_index_direction_a_match : ?Bool = null;

        let indexes = Buffer.Buffer<IndexCmpDetails>(collection.indexes.size());

        for (index in Map.vals(collection.indexes)) {

            var num_of_sort_fields_evaluated = 0;

            var num_of_equal_fields_covered = 0;
            var num_of_sort_fields_covered = 0;
            var num_of_range_fields_covered = 0;

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
                        num_of_equal_fields_covered += 1;
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
                                num_of_sort_fields_covered += 1;
                            };
                            case (?is_a_match) {
                                if (is_a_match == (direction == sort_field.1)) {
                                    num_of_sort_fields_covered += 1;
                                } else {
                                    requires_additional_sorting := true;
                                };
                            };
                        };
                    };
                };

                if (Set.has(range_fields, thash, index_key)) {
                    num_of_range_fields_covered += 1;
                    matches_at_least_one_column := true;

                    Set.add(positions_matching_equality_or_range, nhash, index_key_details_position);
                    Set.add(fully_covered_equality_and_range_fields, thash, index_key);

                    break scoring_indexes;
                };

                // Debug.print("index_key, index_score: " # debug_show (index_key, index_score));

                if (not matches_at_least_one_column) break scoring_indexes;

            };

            if (
                num_of_range_fields_covered < Set.size(range_fields) or num_of_equal_fields_covered < Set.size(equal_fields)
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

            if (num_of_equal_fields_covered > 0 or num_of_range_fields_covered > 0 or num_of_sort_fields_covered > 0) {

                let (scan_bounds, filter_bounds) = convert_simple_operations_to_scan_and_filter_bounds(false, operations, ?index.key_details, ?fully_covered_equality_and_range_fields);

                let interval = scan(collection, index, scan_bounds.0, scan_bounds.1, null);

                let index_details : IndexCmpDetails = {
                    index;

                    num_of_range_fields = num_of_range_fields_covered;
                    num_of_sort_fields = num_of_sort_fields_covered;
                    num_of_equal_fields = num_of_equal_fields_covered;

                    requires_additional_filtering;
                    requires_additional_sorting = requires_additional_sorting or num_of_sort_fields_evaluated < sort_fields.size();
                    fully_covered_equality_and_range_fields;
                    sorted_in_reverse = switch (is_query_and_index_direction_a_match) {
                        case (null) false;
                        case (?is_a_match) not is_a_match;
                    };

                    interval;
                };

              // Debug.print("index matching results:");
              // Debug.print("index, score: " # debug_show (index.name, calculate_score(index_details, false)));
              // Debug.print("operations: " # debug_show operations);

              // Debug.print("index_key_details: " # debug_show index.key_details);
              // Debug.print("equal_fields: " # debug_show Set.toArray(equal_fields));
              // Debug.print("  num_of_equal_fields_covered: " # debug_show num_of_equal_fields_covered);

              // Debug.print("sort_fields: " # debug_show Buffer.toArray(sort_fields));
              // Debug.print("  num_of_sort_fields_evaluated: " # debug_show num_of_sort_fields_evaluated);
              // Debug.print("range_fields: " # debug_show Set.toArray(range_fields));
              // Debug.print("  num_of_range_fields_covered: " # debug_show num_of_range_fields_covered);

              // Debug.print("requires_additional_filtering: " # debug_show requires_additional_filtering);
              // Debug.print("requires_additional_sorting: " # debug_show requires_additional_sorting);
              // Debug.print("num, range_size: " # debug_show (num_of_range_fields_covered, Set.size(range_fields)));
              // Debug.print("num, equal_size: " # debug_show (num_of_equal_fields_covered, Set.size(equal_fields)));
              // Debug.print("fully_covered_equality_and_range_fields: " # debug_show Set.toArray(fully_covered_equality_and_range_fields));

                indexes.add(index_details);
            };

        };

        func sort_indexes_based_on_calculated_features(a : IndexCmpDetails, b : IndexCmpDetails) : Order.Order {
            let a_score = calculate_score(a, false);
            let b_score = calculate_score(b, false);

            switch (Float.compare(a_score, b_score)) {
                case (#greater) #greater;
                case (#less) #less;
                case (#equal) {
                    Float.compare(
                        calculate_score(a, true),
                        calculate_score(b, true),
                    );
                };
            };
        };

        indexes.sort(sort_indexes_based_on_calculated_features);

        switch (indexes.getOpt(indexes.size() - 1)) {
            case (null) null;
            case (?best_index_details) {
                let {
                    index;
                    requires_additional_sorting;
                    requires_additional_filtering;
                    fully_covered_equality_and_range_fields;
                    sorted_in_reverse;
                } = best_index_details;

                let best_index_result : BestIndexResult = {
                    index;
                    requires_additional_sorting;
                    requires_additional_filtering;
                    sorted_in_reverse = sorted_in_reverse;
                    fully_covered_equality_and_range_fields;
                    score = calculate_score(best_index_details, false);
                };

                return ?best_index_result;
            };
        };

    };

    // public func get_best_index_v1(collection : StableCollection, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) : ?BestIndexResult {
    //     let equal_fields = Set.new<Text>();
    //     let sort_fields = Buffer.Buffer<(Text, T.SortDirection)>(8);
    //     let range_fields = Set.new<Text>();
    //     // let partially_covered_fields = Set.new<Text>();

    //     func fill_field_maps(equal_fields : Set.Set<Text>, sort_fields : Buffer<(Text, T.SortDirection)>, range_fields : Set.Set<Text>, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) {

    //         sort_fields.clear();

    //         switch (sort_field) {
    //             case (?(field, direction)) sort_fields.add(field, direction);
    //             case (null) {};
    //         };

    //         // sort_fields.reverse(); or add in reverse order

    //         for ((field, op) in operations.vals()) {
    //             switch (op) {
    //                 case (#eq(_)) ignore Set.put(equal_fields, thash, field);
    //                 case (_) ignore Set.put(range_fields, thash, field);
    //             };
    //         };
    //     };

    //     fill_field_maps(equal_fields, sort_fields, range_fields, operations, sort_field);

    //     var best_score = 0;
    //     var best_index : ?Index = null;
    //     var best_requires_additional_sorting = false;
    //     var best_requires_additional_filtering = false;
    //     var best_fully_covered_equality_and_range_fields = Set.new<Text>();

    //     // the sorting direction of the query and the index can either be a direct match
    //     // or a direct opposite in order to return the results without additional sorting
    //     var is_query_and_index_direction_a_match : ?Bool = null;

    //     let EQUALITY_SCORE = 4;
    //     let SORT_SCORE = 2;
    //     let RANGE_SCORE = 1;

    //     // let indexes = Buffer.Buffer<BestIndexResult>(indexes.size());

    //     for (index in Map.vals(collection.indexes)) {

    //         var num_of_equal_fields_evaluated = 0;
    //         var num_of_sort_fields_evaluated = 0;
    //         var num_of_range_fields_evaluated = 0;

    //         var index_score = 0;
    //         var requires_additional_filtering = false;
    //         var requires_additional_sorting = false;
    //         var positions_matching_equality_or_range = Set.new<Nat>();
    //         let fully_covered_equality_and_range_fields = Set.new<Text>();

    //         var index_key_details_position = 0;

    //         label scoring_indexes for ((index_key, direction) in index.key_details.vals()) {
    //             index_key_details_position += 1;

    //             if (index_key == C.RECORD_ID_FIELD) break scoring_indexes;

    //             var matches_at_least_one_column = false;

    //             switch (Set.has(equal_fields, thash, index_key)) {
    //                 case (true) {
    //                     index_score += EQUALITY_SCORE;
    //                     num_of_equal_fields_evaluated += 1;
    //                     matches_at_least_one_column := true;
    //                     Set.add(positions_matching_equality_or_range, nhash, index_key_details_position);
    //                     Set.add(fully_covered_equality_and_range_fields, thash, index_key);
    //                 };
    //                 case (false) {};
    //             };

    //             if (num_of_sort_fields_evaluated < sort_fields.size()) {
    //                 let i = sort_fields.size() - 1 - num_of_sort_fields_evaluated;
    //                 let sort_field = sort_fields.get(i);

    //                 if (index_key == sort_field.0) {

    //                     matches_at_least_one_column := true;

    //                     num_of_sort_fields_evaluated += 1;
    //                     switch (is_query_and_index_direction_a_match) {
    //                         case (null) {
    //                             is_query_and_index_direction_a_match := ?(direction == sort_field.1);
    //                             index_score += SORT_SCORE;
    //                         };
    //                         case (?is_a_match) {
    //                             if (is_a_match == (direction == sort_field.1)) {
    //                                 index_score += SORT_SCORE;
    //                             } else {
    //                                 requires_additional_sorting := true;
    //                             };
    //                         };
    //                     };
    //                 };
    //             };

    //             if (Set.has(range_fields, thash, index_key)) {
    //                 index_score += RANGE_SCORE;
    //                 num_of_range_fields_evaluated += 1;
    //                 matches_at_least_one_column := true;

    //                 Set.add(positions_matching_equality_or_range, nhash, index_key_details_position);
    //                 Set.add(fully_covered_equality_and_range_fields, thash, index_key);

    //                 break scoring_indexes;
    //             };

    //             // Debug.print("index_key, index_score: " # debug_show (index_key, index_score));

    //             if (not matches_at_least_one_column) break scoring_indexes;

    //         };

    //         if (
    //             num_of_range_fields_evaluated < Set.size(range_fields) or num_of_equal_fields_evaluated < Set.size(equal_fields)
    //         ) {
    //             requires_additional_filtering := true;
    //         };

    //         if ((Set.size(positions_matching_equality_or_range) == 0 and operations.size() > 0)) {
    //             requires_additional_filtering := true;
    //         };

    //         label searching_for_holes for ((prev, current) in Itertools.slidingTuples(Set.keys(positions_matching_equality_or_range))) {
    //             if (current - prev > 1) {
    //                 requires_additional_filtering := true;
    //                 break searching_for_holes;
    //             };
    //         };

    //       // Debug.print("index matching results:");
    //       // Debug.print("index, score: " # debug_show (index.name, index_score));
    //       // Debug.print("operations: " # debug_show operations);

    //       // Debug.print("index_key_details: " # debug_show index.key_details);
    //       // Debug.print("equal_fields: " # debug_show Set.toArray(equal_fields));
    //       // Debug.print("  num_of_equal_fields_evaluated: " # debug_show num_of_equal_fields_evaluated);

    //       // Debug.print("sort_fields: " # debug_show Buffer.toArray(sort_fields));
    //       // Debug.print("  num_of_sort_fields_evaluated: " # debug_show num_of_sort_fields_evaluated);
    //       // Debug.print("range_fields: " # debug_show Set.toArray(range_fields));
    //       // Debug.print("  num_of_range_fields_evaluated: " # debug_show num_of_range_fields_evaluated);

    //       // Debug.print("requires_additional_filtering: " # debug_show requires_additional_filtering);
    //       // Debug.print("requires_additional_sorting: " # debug_show requires_additional_sorting);
    //       // Debug.print("num, range_size: " # debug_show (num_of_range_fields_evaluated, Set.size(range_fields)));
    //       // Debug.print("num, equal_size: " # debug_show (num_of_equal_fields_evaluated, Set.size(equal_fields)));
    //       // Debug.print("fully_covered_equality_and_range_fields: " # debug_show Set.toArray(fully_covered_equality_and_range_fields));

    //         if (index_score > best_score) {
    //             best_score := index_score;
    //             best_index := ?index;
    //             best_requires_additional_filtering := requires_additional_filtering;
    //             best_requires_additional_sorting := requires_additional_sorting or num_of_sort_fields_evaluated < sort_fields.size();
    //             best_fully_covered_equality_and_range_fields := fully_covered_equality_and_range_fields;
    //         };

    //     };

    //     let index = switch (best_index) {
    //         case (null) return null;
    //         case (?index) index;
    //     };

    //     let index_response = {
    //         index;
    //         requires_additional_sorting = best_requires_additional_sorting;
    //         requires_additional_filtering = best_requires_additional_filtering;
    //         sorted_in_reverse = switch (is_query_and_index_direction_a_match) {
    //             case (null) false;
    //             case (?is_a_match) not is_a_match;
    //         };
    //         fully_covered_equality_and_range_fields = best_fully_covered_equality_and_range_fields;
    //         score = best_score;
    //     };

    //     ?index_response;

    // };
};
