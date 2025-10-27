import Text "mo:base@0.16.0/Text";
import Array "mo:base@0.16.0/Array";
import Buffer "mo:base@0.16.0/Buffer";
import Order "mo:base@0.16.0/Order";
import Debug "mo:base@0.16.0/Debug";
import Nat "mo:base@0.16.0/Nat";
import Int "mo:base@0.16.0/Int";
import Option "mo:base@0.16.0/Option";
import Iter "mo:base@0.16.0/Iter";
import Float "mo:base@0.16.0/Float";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.3.3";
import Decoder "mo:serde@3.3.3/Candid/Blob/Decoder";
import Candid "mo:serde@3.3.3/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import BitMap "mo:bit-map@0.1.2";
import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";

import T "../../Types";
import Logger "../../Logger";
import Utils "../../Utils";

import DocumentStore "../DocumentStore";
import CollectionUtils "../CollectionUtils";
import Schema "../Schema";
import CandidMap "../../CandidMap";

import CompositeIndex "CompositeIndex";
import TextIndex "TextIndex";

module {
    public func clear_index(
        collection : T.StableCollection,
        index : T.Index,
    ) : T.Result<(), Text> {

        switch (index) {
            case (#composite_index(composite_index)) {
                CompositeIndex.clear(collection, composite_index);
            };
            case (#text_index(text_index)) {
                TextIndex.clear(collection, text_index);
            };

        };

        #ok()

    };

    public func size(index : T.Index) : Nat {

        switch (index) {
            case (#composite_index(composite_index)) {
                return CompositeIndex.size(composite_index);
            };
            case (#text_index(text_index)) {
                return CompositeIndex.size(text_index.internal_index);
            };
        };

    };

    public func name(index : T.Index) : Text {

        switch (index) {
            case (#composite_index(composite_index)) {
                return composite_index.name;
            };
            case (#text_index(text_index)) {
                return text_index.internal_index.name;
            };
        };

    };

    public func get_internal_index(index : T.Index) : T.CompositeIndex {

        switch (index) {
            case (#composite_index(composite_index)) {
                return composite_index;
            };
            case (#text_index(text_index)) {
                return text_index.internal_index;
            };
        };

    };

    public func deallocate(
        collection : T.StableCollection,
        index : T.Index,
    ) {

        switch (index) {
            case (#text_index(text_index)) {
                TextIndex.deallocate(collection, text_index);
            };
            case (#composite_index(composite_index)) {
                CompositeIndex.deallocate(collection, composite_index);
            };
        };

    };

    public func insertWithCandidMap(
        collection : T.StableCollection,
        index : T.Index,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : T.Result<(), Text> {

        switch (index) {
            case (#text_index(text_index)) {
                return TextIndex.insertWithCandidMap(collection, text_index, document_id, candid_map);
            };
            case (#composite_index(composite_index)) {
                return CompositeIndex.insertWithCandidMap(collection, composite_index, document_id, candid_map);
            };
        };

    };

    public func removeWithCandidMap(
        collection : T.StableCollection,
        index : T.Index,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : T.Result<(), Text> {

        switch (index) {
            case (#text_index(text_index)) {
                return TextIndex.removeWithCandidMap(collection, text_index, document_id, candid_map);
            };
            case (#composite_index(composite_index)) {
                return CompositeIndex.removeWithCandidMap(collection, composite_index, document_id, candid_map);
            };
        };

    };

    public func clear(
        collection : T.StableCollection,
        index : T.Index,
    ) {

        switch (index) {
            case (#text_index(text_index)) {
                TextIndex.clear(collection, text_index);
            };
            case (#composite_index(composite_index)) {
                CompositeIndex.clear(collection, composite_index);
            };
        };

    };

    public func stats(

        index : T.Index,
        entries : Nat,
    ) : T.IndexStats {

        switch (index) {
            case (#text_index(text_index)) {
                return CompositeIndex.stats(text_index.internal_index, entries);
            };
            case (#composite_index(composite_index)) {
                return CompositeIndex.stats(composite_index, entries);
            };
        };
    };

    public func populate_indexes(
        collection : T.StableCollection,
        indexes : [T.Index],
    ) : T.Result<(), Text> {

        let non_empty_indexes = Buffer.Buffer<T.Index>(indexes.size());

        for (index in indexes.vals()) {
            if (size(index) > 0) {
                non_empty_indexes.add(index);
            };
        };

        if (non_empty_indexes.size() > 0) {
            let index_names = Array.map(Buffer.toArray(non_empty_indexes), func(index : T.Index) : Text { name(index) });
            return #err("Cannot populate non-empty indexes: " # debug_show index_names);
        };

        let doc_store_utils = DocumentStore.getBtreeUtils(collection.documents);

        for ((document_id, candid_blob) in DocumentStore.entries(collection.documents, doc_store_utils)) {
            let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);
            let candid_map = CandidMap.new(collection.schema_map, document_id, candid);

            for (index in indexes.vals()) {

                var index_name = "";

                let res = switch (index) {
                    case (#text_index(text_index)) {
                        index_name := text_index.internal_index.name;
                        TextIndex.insertWithCandidMap(collection, text_index, document_id, candid_map);
                    };
                    case (#composite_index(composite_index)) {
                        index_name := composite_index.name;
                        CompositeIndex.insertWithCandidMap(collection, composite_index, document_id, candid_map);
                    };
                };

                switch (res) {
                    case (#err(err)) {
                        return #err("populate_index() failed on index '" # index_name # "' for document '" # debug_show (document_id) # "': " # err);
                    };
                    case (#ok(_)) {};
                };

            };

        };

        #ok();

    };

    // public func repopulate_indexes(
    //     collection : T.StableCollection,
    //     index_names : [Text],
    // ) : T.Result<(), Text> {

    //     Logger.lazyInfo(
    //         collection.logger,
    //         func() = "Starting to populate indexes: " # debug_show index_names,
    //     );

    //     let indexes = Buffer.Buffer<T.Index>(index_names.size());

    //     for (index_name in index_names.vals()) {
    //         let ?index = Map.get(collection.indexes, Map.thash, index_name) else {
    //             return #err("CompositeIndex '" # index_name # "' does not exist");
    //         };

    //         indexes.add(index);
    //     };

    //     Logger.lazyDebug(
    //         collection.logger,
    //         func() = "Collected " # debug_show indexes.size() # " indexes to populate",
    //     );

    //     CompositeIndex.repopulate_indexes(collection, Buffer.toArray(indexes));

    // };

    let EQUALITY_SCORE = 4;
    let SORT_SCORE = 2;
    let RANGE_SCORE = 1;

    let ADDITIONAL_FILTER_SCORE = 1;
    let ADDITIONAL_SORT_SCORE = 1;

    func operation_eval(
        field : Text,
        op : T.ZqlOperators,
        lower : Map.Map<Text, T.CandidInclusivityQuery>,
        upper : Map.Map<Text, T.CandidInclusivityQuery>,
    ) {
        switch (op) {
            case (#eq(candid)) {
                ignore Map.put(lower, Map.thash, field, #Inclusive(candid));
                ignore Map.put(upper, Map.thash, field, #Inclusive(candid));
            };
            case (#gte(candid)) {
                switch (Map.get(lower, Map.thash, field)) {
                    case (?#Inclusive(val) or ?#Exclusive(val)) {
                        if (Schema.cmp_candid(#Empty, candid, val) == 1) {
                            ignore Map.put(lower, Map.thash, field, #Inclusive(candid));
                        };
                    };
                    case (null) ignore Map.put(lower, Map.thash, field, #Inclusive(candid));
                };
            };
            case (#lte(candid)) {
                switch (Map.get(upper, Map.thash, field)) {
                    case (?#Inclusive(val) or ?#Exclusive(val)) {
                        if (Schema.cmp_candid(#Empty, candid, val) == -1) {
                            ignore Map.put(upper, Map.thash, field, #Inclusive(candid));
                        };
                    };
                    case (null) ignore Map.put(upper, Map.thash, field, #Inclusive(candid));
                };
            };
            case (#lt(candid)) {
                switch (Map.get(upper, Map.thash, field)) {
                    case (?#Inclusive(val) or ?#Exclusive(val)) {
                        let cmp = Schema.cmp_candid(#Empty, candid, val);
                        if (cmp == -1 or cmp == 0) {
                            ignore Map.put(upper, Map.thash, field, #Exclusive(candid));
                        };
                    };
                    case (null) ignore Map.put(upper, Map.thash, field, #Exclusive(candid));
                };
            };
            case (#gt(candid)) {
                switch (Map.get(lower, Map.thash, field)) {
                    case (?#Inclusive(val) or ?#Exclusive(val)) {
                        let cmp = Schema.cmp_candid(#Empty, candid, val);
                        if (cmp == 1 or cmp == 0) {
                            ignore Map.put(lower, Map.thash, field, #Exclusive(candid));
                        };
                    };
                    case (null) ignore Map.put(lower, Map.thash, field, #Exclusive(candid));
                };
            };

            case (#exists) {
                ignore Map.put(lower, Map.thash, field, #Inclusive(#Minimum));
                ignore Map.put(upper, Map.thash, field, #Inclusive(#Maximum));
            };

            // aliases should be handled by the query builder
            case (#anyOf(_) or #between(_, _) or #startsWith(_) or #not_(_)) {
                Debug.trap(debug_show op # " not allowed in this context. Should have been expanded by the query builder");
            };
        };
    };

    public func extract_bounds(lower : Map.Map<Text, T.CandidInclusivityQuery>, upper : Map.Map<Text, T.CandidInclusivityQuery>, opt_index_key_details : ?[(Text, T.SortDirection)], opt_fully_covered_equality_and_range_fields : ?Set.Set<Text>) : (T.Bounds, T.Bounds) {

        assert Option.isSome(opt_index_key_details) == Option.isSome(opt_fully_covered_equality_and_range_fields);

        let scan_bounds = switch (opt_index_key_details) {
            case (null) ([], []);
            case (?index_key_details) {

                let scan_lower_bound = Array.map(
                    index_key_details,
                    func((field, _) : (Text, T.SortDirection)) : T.FieldLimit {
                        let lower_bound = switch (Map.get(lower, Map.thash, field)) {
                            case (?lower_bound) lower_bound;
                            case (null) #Inclusive(#Minimum);
                        };

                        (field, ?lower_bound);
                    },
                );

                let scan_upper_bound = Array.map(
                    index_key_details,
                    func((field, _) : (Text, T.SortDirection)) : T.FieldLimit {
                        let upper_bound = switch (Map.get(upper, Map.thash, field)) {
                            case (?upper_bound) upper_bound;
                            case (null) #Inclusive(#Maximum);
                        };

                        (field, ?upper_bound);
                    },
                );

                (scan_lower_bound, scan_upper_bound);

            };
        };

        let (partially_covered_lower, partially_covered_upper) = switch (opt_fully_covered_equality_and_range_fields) {
            case (null) (lower, upper);
            case (?fully_covered_equality_and_range_fields) {

                let partially_covered_lower = Map.new<Text, T.CandidInclusivityQuery>();
                let partially_covered_upper = Map.new<Text, T.CandidInclusivityQuery>();

                for ((field, value) in Map.entries(lower)) {
                    if (not Set.has(fully_covered_equality_and_range_fields, Map.thash, field)) {
                        ignore Map.put(partially_covered_lower, Map.thash, field, value);
                    };
                };

                for ((field, value) in Map.entries(upper)) {
                    if (not Set.has(fully_covered_equality_and_range_fields, Map.thash, field)) {
                        ignore Map.put(partially_covered_upper, Map.thash, field, value);
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
        let arr1 = Array.tabulate<(Text, ?T.State<T.CandidQuery>)>(
            max_size,
            func(i : Nat) : (Text, ?T.State<T.CandidQuery>) {
                let ?(key, value) = iter.next();
                (key, ?value);
            },
        );

        let iter_2 = Map.entries(a);
        let arr2 = Array.tabulate<(Text, ?T.State<T.CandidQuery>)>(
            max_size,
            func(i : Nat) : (Text, ?T.State<T.CandidQuery>) {
                let ?(key, _) = iter_2.next();
                let value = Map.get(b, Map.thash, key);
                (key, value);
            },
        );

        let filter_bounds = if (is_lower_bound_larger) (arr1, arr2) else (arr2, arr1);

        (scan_bounds, filter_bounds)

    };

    public func convert_simple_ops_to_bounds(is_and_operation : Bool, simple_operations : [(Text, T.ZqlOperators)], opt_index_key_details : ?[(Text, T.SortDirection)], opt_fully_covered_equality_and_range_fields : ?Set.Set<Text>) : (T.Bounds, T.Bounds) {

        let lower_bound = Map.new<Text, T.State<T.CandidQuery>>();
        let upper_bound = Map.new<Text, T.State<T.CandidQuery>>();

        let fields_with_equality_ops = Set.new<Text>();

        for ((field, op) in simple_operations.vals()) {
            // if the field is already in the lower or upper bounds, then we can't add it again
            // because it would be a contradiction
            // for example, if we have an equal operation on a field (x = 5), we can't have another operation on the same field (like x > 5 or x < 5 or x = 8)

            switch (op) {
                case (#eq(_)) {
                    let opt_exists_in_lower = Map.get(lower_bound, Map.thash, field);
                    let opt_exists_in_upper = Map.get(upper_bound, Map.thash, field);

                    if (false) {
                        // move to a seperate function that validates the operations before executing them

                        if (is_and_operation) {

                            let has_equality = Set.has(fields_with_equality_ops, Map.thash, field);

                            if (Option.isSome(opt_exists_in_lower) or Option.isSome(opt_exists_in_upper) or has_equality) {
                                Debug.trap("Contradictory operations on the same field");
                            };

                            Set.add(fields_with_equality_ops, Map.thash, field);
                        };
                    };

                };
                case (_) {};
            };

            operation_eval(field, op, lower_bound, upper_bound);

        };

        extract_bounds(lower_bound, upper_bound, opt_index_key_details, opt_fully_covered_equality_and_range_fields);

    };

    public type IndexCmpDetails = {
        index : T.CompositeIndex;

        num_of_range_fields : Nat;
        num_of_sort_fields : Nat;
        num_of_equal_fields : Nat;

        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        sorted_in_reverse : Bool;

        fully_covered_equality_and_range_fields : Set.Set<Text>;

        interval : T.Interval;
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

    public func fill_field_maps(equal_fields : Set.Set<Text>, sort_fields : Buffer.Buffer<(Text, T.SortDirection)>, range_fields : Set.Set<Text>, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) {

        sort_fields.clear();

        switch (sort_field) {
            case (?(field, direction)) sort_fields.add(field, direction);
            case (null) {};
        };

        // sort_fields.reverse(); or add in reverse order

        for ((field, op) in operations.vals()) {
            switch (op) {
                case (#eq(_)) ignore Set.put(equal_fields, Map.thash, field);
                case (_) ignore Set.put(range_fields, Map.thash, field);
            };
        };
    };

    public func get_best_index(collection : T.StableCollection, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) : ?T.BestIndexResult {
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

            switch (index) {
                case (#text_index(_)) {};
                case (#composite_index(index)) {

                    // Debug.print("scoring indexes");
                    label scoring_indexes for ((index_key, direction) in index.key_details.vals()) {
                        index_key_details_position += 1;

                        // if (index_key == C.DOCUMENT_ID) break scoring_indexes;

                        var matches_at_least_one_column = false;

                        switch (Set.has(equal_fields, Map.thash, index_key)) {
                            case (true) {
                                num_of_equal_fields_covered += 1;
                                matches_at_least_one_column := true;
                                Set.add(positions_matching_equality_or_range, Map.nhash, index_key_details_position);
                                Set.add(fully_covered_equality_and_range_fields, Map.thash, index_key);
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

                        if (Set.has(range_fields, Map.thash, index_key)) {
                            num_of_range_fields_covered += 1;
                            matches_at_least_one_column := true;

                            Set.add(positions_matching_equality_or_range, Map.nhash, index_key_details_position);
                            Set.add(fully_covered_equality_and_range_fields, Map.thash, index_key);

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

                    // Debug.print("searching_for_holes");
                    label searching_for_holes for ((prev, current) in Itertools.slidingTuples(Set.keys(positions_matching_equality_or_range))) {
                        if (current - prev > 1) {
                            requires_additional_filtering := true;
                            break searching_for_holes;
                        };
                    };

                    if (num_of_equal_fields_covered > 0 or num_of_range_fields_covered > 0 or num_of_sort_fields_covered > 0) {

                        let (scan_bounds, filter_bounds) = convert_simple_ops_to_bounds(false, operations, ?index.key_details, ?fully_covered_equality_and_range_fields);

                        let interval = CompositeIndex.scan(collection, index, scan_bounds.0, scan_bounds.1, null);

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

        // Debug.print("extracting best index");
        let last_index = if (indexes.size() == 0) 0 else (indexes.size() - 1 : Nat);

        switch (indexes.getOpt(last_index)) {
            case (null) null;
            case (?best_index_details) {
                let {
                    index;
                    requires_additional_sorting;
                    requires_additional_filtering;
                    fully_covered_equality_and_range_fields;
                    sorted_in_reverse;
                } = best_index_details;

                let best_index_result : T.BestIndexResult = {
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

};
