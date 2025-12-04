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
import Serde "mo:serde@3.4.0";
import Decoder "mo:serde@3.4.0/Candid/Blob/Decoder";
import Candid "mo:serde@3.4.0/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import SparseBitMap64 "mo:bit-map@1.1.0/SparseBitMap64";
import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";

import T "../../Types";
import Logger "../../Logger";
import Utils "../../Utils";
import C "../../Constants";
import BTree "../../BTree";

import DocumentStore "../DocumentStore";
import CollectionUtils "../CollectionUtils";
import Schema "../Schema";
import CandidMap "../../CandidMap";

import CompositeIndex "CompositeIndex";
import TextIndex "TextIndex";

module {
    // Helper function to get index from collection by name
    func get_index(collection : T.StableCollection, index_name : Text) : ?T.Index {
        Map.get(collection.indexes, Map.thash, index_name);
    };

    // ========== Functions with ByName suffix (lookup by collection + index_name) ==========

    public func clear_index(
        collection : T.StableCollection,
        index_name : Text,
    ) : T.Result<(), Text> {
        let ?index = get_index(collection, index_name) else {
            return #err("Index '" # index_name # "' not found");
        };

        switch (index) {
            case (#composite_index(composite_index)) {
                CompositeIndex.clear(collection, composite_index);
            };
            case (#text_index(text_index)) {
                TextIndex.clear(collection, text_index);
            };
        };

        #ok();
    };

    public func sizeByName(collection : T.StableCollection, index_name : Text) : Nat {
        // Handle DOCUMENT_ID as a pseudo index
        if (index_name == C.DOCUMENT_ID) {
            return DocumentStore.size(collection);
        };

        let ?index = get_index(collection, index_name) else {
            Debug.trap("Index '" # index_name # "' not found in collection");
        };

        size(index);
    };

    public func nameByName(collection : T.StableCollection, index_name : Text) : Text {
        // Handle DOCUMENT_ID as a pseudo index
        if (index_name == C.DOCUMENT_ID) {
            return C.DOCUMENT_ID;
        };

        let ?index = get_index(collection, index_name) else {
            Debug.trap("Index '" # index_name # "' not found in collection");
        };

        name(index);
    };

    // Internal function - use get_config() or get_key_details() for external access
    func get_internal_index(collection : T.StableCollection, index_name : Text) : T.CompositeIndex {
        let ?index = get_index(collection, index_name) else {
            Debug.trap("Index '" # index_name # "' not found in collection");
        };

        get_internal_index_from_index(index);
    };

    public func deallocateByName(
        collection : T.StableCollection,
        index_name : Text,
    ) {
        let ?index = get_index(collection, index_name) else {
            Debug.trap("Index '" # index_name # "' not found in collection");
        };

        deallocate(collection, index);
    };

    public func insertWithCandidMapByName(
        collection : T.StableCollection,
        index_name : Text,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : T.Result<(), Text> {
        let ?index = get_index(collection, index_name) else {
            return #err("Index '" # index_name # "' not found");
        };

        insertWithCandidMap(collection, index, document_id, candid_map);
    };

    public func removeWithCandidMapByName(
        collection : T.StableCollection,
        index_name : Text,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : T.Result<(), Text> {
        let ?index = get_index(collection, index_name) else {
            return #err("Index '" # index_name # "' not found");
        };

        removeWithCandidMap(collection, index, document_id, candid_map);
    };

    public func clearByName(
        collection : T.StableCollection,
        index_name : Text,
    ) {
        let ?index = get_index(collection, index_name) else {
            Debug.trap("Index '" # index_name # "' not found in collection");
        };

        clear(collection, index);
    };

    public func statsByName(
        collection : T.StableCollection,
        index_name : Text,
        entries : Nat,
    ) : T.IndexStats {
        // Handle DOCUMENT_ID as a pseudo index
        if (index_name == C.DOCUMENT_ID) {
            let memory = BTree.getMemoryStats(collection.documents);
            return {
                name = C.DOCUMENT_ID;
                fields = [(C.DOCUMENT_ID, #Ascending)];
                entries = entries;
                memory;
                is_unique = true;
                used_internally = true;
                hidden = false; // Document ID is never hidden
                // For the document ID pseudo index, keys are the document IDs themselves
                avg_index_key_size = if (entries == 0) 0 else (memory.keyBytes / entries);
                total_index_key_size = memory.keyBytes;
                total_index_data_bytes = memory.dataBytes;
            };
        };

        let ?index = get_index(collection, index_name) else {
            Debug.trap("Index '" # index_name # "' not found in collection");
        };

        stats(collection, index, entries);
    };

    public func get_config_by_name(collection : T.StableCollection, index_name : Text) : T.IndexConfig {
        // Handle DOCUMENT_ID as a pseudo index
        if (index_name == C.DOCUMENT_ID) {
            return {
                name = C.DOCUMENT_ID;
                key_details = [(C.DOCUMENT_ID, #Ascending)];
                is_unique = true;
                used_internally = true;
            };
        };

        let ?index = get_index(collection, index_name) else {
            Debug.trap("Index '" # index_name # "' not found in collection");
        };

        get_config(index);
    };

    public func get_key_details_by_name(collection : T.StableCollection, index_name : Text) : [(Text, T.SortDirection)] {
        // Handle DOCUMENT_ID as a pseudo index
        if (index_name == C.DOCUMENT_ID) {
            return [(C.DOCUMENT_ID, #Ascending)];
        };

        let ?index = get_index(collection, index_name) else {
            Debug.trap("Index '" # index_name # "' not found in collection");
        };

        get_key_details(index);
    };

    // ========== Default functions that work with Index objects ==========
    // These are the primary API - they work directly with index objects

    public func hide(collection : T.StableCollection, index_name : Text) : T.Result<(), Text> {

        if (Set.has(collection.hidden_indexes, Set.thash, index_name)) {
            return #err("Index '" # index_name # "' is already hidden");
        };

        if (not Map.has(collection.indexes, Map.thash, index_name)) {
            return #err("Index '" # index_name # "' does not exist");
        };

        ignore Set.add(collection.hidden_indexes, Set.thash, index_name);

        #ok();

    };

    public func unhide(collection : T.StableCollection, index_name : Text) : T.Result<(), Text> {

        if (not Set.has(collection.hidden_indexes, Set.thash, index_name)) {
            return #err("Index '" # index_name # "' is not hidden");
        };

        ignore Set.remove(collection.hidden_indexes, Set.thash, index_name);

        #ok();

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

    public func get_internal_index_from_index(index : T.Index) : T.CompositeIndex {
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

    public func getRankWithCandidMap(
        collection : T.StableCollection,
        index_name : Text,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : T.Result<Nat, Text> {
        let ?index = get_index(collection, index_name) else {
            return #err("Index '" # index_name # "' not found");
        };

        switch (index) {
            case (#text_index(text_index)) {
                return #err("getRankWithCandidMap is not supported for TextIndex");
            };
            case (#composite_index(composite_index)) {
                return CompositeIndex.getRankWithCandidMap(collection, composite_index, document_id, candid_map);
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
        collection : T.StableCollection,
        index : T.Index,
        entries : Nat,
    ) : T.IndexStats {
        let hidden = switch (index) {
            case (#text_index(text_index)) {
                Set.has(collection.hidden_indexes, Map.thash, text_index.internal_index.name);
            };
            case (#composite_index(composite_index)) {
                Set.has(collection.hidden_indexes, Map.thash, composite_index.name);
            };
        };

        switch (index) {
            case (#text_index(text_index)) {
                return CompositeIndex.stats(text_index.internal_index, entries, hidden);
            };
            case (#composite_index(composite_index)) {
                return CompositeIndex.stats(composite_index, entries, hidden);
            };
        };
    };

    public func iterate(
        collection : T.StableCollection,
        include_hidden_indexes : Bool,
    ) : Iter.Iter<(Text, T.Index)> {

        let entries = Map.entries(collection.indexes);
        if (include_hidden_indexes) return entries;

        Iter.filter(
            entries,
            func((index_name, _) : (Text, T.Index)) : Bool {
                not Set.has(collection.hidden_indexes, Set.thash, index_name);
            },
        );
    };

    public func get_config(index : T.Index) : T.IndexConfig {
        let internal_index = get_internal_index_from_index(index);
        {
            name = internal_index.name;
            key_details = internal_index.key_details;
            is_unique = internal_index.is_unique;
            used_internally = internal_index.used_internally;
        };
    };

    public func get_key_details(index : T.Index) : [(Text, T.SortDirection)] {
        let internal_index = get_internal_index_from_index(index);
        internal_index.key_details;
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

        for ((document_id, candid_blob) in DocumentStore.entries(collection)) {
            let candid_map = CollectionUtils.get_candid_map_no_cache(collection, document_id, ?candid_blob);

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

    public func extract_bounds(lower : Map.Map<Text, T.CandidInclusivityQuery>, upper : Map.Map<Text, T.CandidInclusivityQuery>, opt_index_key_details : ?[(Text, T.SortDirection)], opt_fully_covered_equality_and_range_fields : ?Set.Set<Text>) : (T.LowerUpperBounds, T.LowerUpperBounds) {

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

    public func convert_simple_ops_to_bounds(is_and_operation : Bool, simple_operations : [(Text, T.ZqlOperators)], opt_index_key_details : ?[(Text, T.SortDirection)], opt_fully_covered_equality_and_range_fields : ?Set.Set<Text>) : (T.LowerUpperBounds, T.LowerUpperBounds) {

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

        // Base scores for field coverage
        let range_score = num_of_range_fields * 50;
        let sort_score = num_of_sort_fields * 75;
        let equality_score = num_of_equal_fields * 100;

        var score = Float.fromInt(
            range_score + sort_score + equality_score
        );

        // Calculate result set size
        let size = Float.fromInt(interval.1 - interval.0);

        // Size-based scoring calibrated to known safe limits:
        // MAX_IN_MEMORY_FILTER_ENTRIES = 70,000
        // MAX_IN_MEMORY_SORT_ENTRIES = 7,000

        var size_score = 300 - Float.min(Utils.log2(size) * 20, 300);

        // Apply penalties for additional operations using size-based regression
        // Key insight: When both filtering and sorting are needed, filtering happens first
        // and can significantly reduce the dataset. The risk scales with the initial size.
        if (requires_additional_filtering and requires_additional_sorting) {
            // Combined case: penalty scales smoothly from 0.45 (small) to 0.21 (large)
            // Uses logistic-like regression based on filter limit (70,000)
            let filter_limit : Float = 70_000;
            let sort_limit : Float = 7_000;

            // Calculate normalized size relative to sort limit (0 to 1+ range)
            let normalized_size = Float.min(size / filter_limit, 2.0);

            // Penalty function: starts at ~0.45 for small sizes, approaches 0.21 for large sizes
            // Formula: 0.21 + (0.24 * e^(-3 * normalized_size))
            // Approximation using available operations: linear interpolation with decay
            let penalty = if (size <= sort_limit) {
                0.45 // Small size: filtering likely produces sortable result
            } else if (size <= sort_limit * 5) {
                // Gradual transition zone (7K - 35K)
                let t = (size - sort_limit) / (sort_limit * 4);
                0.45 - (t * 0.15) // Interpolate from 0.45 to 0.30
            } else if (size <= filter_limit) {
                // Approaching filter limit (35K - 70K)
                let t = (size - sort_limit * 5) / (filter_limit - sort_limit * 5);
                0.30 - (t * 0.06) // Interpolate from 0.30 to 0.24
            } else {
                // Beyond safe limit: use harshest penalty
                0.21 // Same as original 0.7 * 0.3
            };

            size_score *= penalty;
        } else if (requires_additional_filtering) {
            // Filtering only: must scan entire interval
            size_score *= 0.7;
        } else if (requires_additional_sorting) {
            // Sorting only: dataset size is known and fixed
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

    public func get_best_indexes_to_intersect(collection : T.StableCollection, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) : [T.BestIndexResult] {

        // Memory limits for index intersection
        let MAX_BITMAP_ENTRIES : Nat = 500_000;
        let MAX_SORTED_ENTRIES : Nat = 500_000;

        let results = Buffer.Buffer<T.BestIndexResult>(3);

        // Get the best single index first
        let best_index = switch (get_best_index(collection, operations, sort_field, null, false)) {
            // false is a placeholder here
            case (null) return [];
            case (?index) index;
        };

        results.add(best_index);

        // If the best index fully covers the query, we're done
        if (not best_index.requires_additional_sorting and not best_index.requires_additional_filtering) {
            return Buffer.toArray(results);
        };

        // Track which fields still need coverage
        let uncovered_equal_fields = Set.new<Text>();
        let uncovered_range_fields = Set.new<Text>();
        var needs_sorting = best_index.requires_additional_sorting;

        // Populate uncovered fields from the original query
        for ((field, op) in operations.vals()) {
            switch (op) {
                case (#eq(_)) {
                    if (not Set.has(best_index.fully_covered_equal_fields, Map.thash, field)) {
                        ignore Set.put(uncovered_equal_fields, Map.thash, field);
                    };
                };
                case (_) {
                    if (not Set.has(best_index.fully_covered_range_fields, Map.thash, field)) {
                        ignore Set.put(uncovered_range_fields, Map.thash, field);
                    };
                };
            };
        };

        // Calculate total entries from best_index using interval
        let best_index_size = best_index.interval.1 - best_index.interval.0;
        var total_bitmap_entries : Nat = best_index_size;
        var total_sorted_entries : Nat = if (best_index.requires_additional_sorting) 0 else best_index_size;

        // Helper function to try a combination of operations
        func try_combination(combo_operations : [(Text, T.ZqlOperators)], combo_sort : ?(Text, T.SortDirection)) : ?T.BestIndexResult {
            let candidate = get_best_index(collection, combo_operations, combo_sort, null, false); // false is a placeholder here

            switch (candidate) {
                case (null) null;
                case (?idx) {
                    let idx_size = idx.interval.1 - idx.interval.0;

                    // Check if adding this index would exceed memory limits
                    let fits_in_bitmap = total_bitmap_entries + idx_size <= MAX_BITMAP_ENTRIES;
                    let fits_in_sorted = if (not idx.requires_additional_sorting) {
                        total_sorted_entries + idx_size <= MAX_SORTED_ENTRIES;
                    } else { true };

                    if (fits_in_bitmap and fits_in_sorted) {
                        ?idx;
                    } else {
                        null;
                    };
                };
            };
        };

        // Build list of covered fields from best_index, categorized by selectivity
        let covered_range_fields = Buffer.Buffer<Text>(8);
        let covered_equal_fields = Buffer.Buffer<Text>(8);

        for (field in Set.keys(best_index.fully_covered_range_fields)) {
            covered_range_fields.add(field);
        };

        for (field in Set.keys(best_index.fully_covered_equal_fields)) {
            covered_equal_fields.add(field);
        };

        // Build arrays of uncovered fields
        let uncovered_range_array = Buffer.Buffer<Text>(8);
        let uncovered_equal_array = Buffer.Buffer<Text>(8);

        for (field in Set.keys(uncovered_range_fields)) {
            uncovered_range_array.add(field);
        };

        for (field in Set.keys(uncovered_equal_fields)) {
            uncovered_equal_array.add(field);
        };

        // Helper to build combination operations: covered fields EXCEPT popped_field, PLUS added_field
        func build_combination(popped_field : Text, added_field : Text) : [(Text, T.ZqlOperators)] {
            let combo_ops = Buffer.Buffer<(Text, T.ZqlOperators)>(operations.size());

            // Add all operations for fields that are:
            // 1. Covered by best_index (except the popped_field), OR
            // 2. The added_field
            for ((field, op) in operations.vals()) {
                let is_covered_equal = Set.has(best_index.fully_covered_equal_fields, Map.thash, field);
                let is_covered_range = Set.has(best_index.fully_covered_range_fields, Map.thash, field);
                let is_covered = is_covered_equal or is_covered_range;

                if (field == added_field) {
                    combo_ops.add((field, op));
                } else if (is_covered and field != popped_field) {
                    combo_ops.add((field, op));
                };
            };

            Buffer.toArray(combo_ops);
        };

        // Strategy: Start with least selective (range), swap with uncovered fields
        // Phase 1: Try swapping covered range fields with uncovered fields
        label phase1 for (covered_field in covered_range_fields.vals()) {
            // Try each uncovered range field
            for (uncovered_field in uncovered_range_array.vals()) {
                let combo_ops = build_combination(covered_field, uncovered_field);

                // Try this combination with and without sort requirement
                let candidate = if (needs_sorting) {
                    // First try with sorting to potentially solve both problems
                    switch (try_combination(combo_ops, sort_field)) {
                        case (?idx) {
                            if (not idx.requires_additional_sorting) {
                                needs_sorting := false; // Found sorting solution!
                            };
                            ?idx;
                        };
                        case (null) {
                            // Try without sort requirement
                            try_combination(combo_ops, null);
                        };
                    };
                } else {
                    try_combination(combo_ops, null);
                };

                switch (candidate) {
                    case (null) {};
                    case (?idx) {
                        results.add(idx);
                        let idx_size = idx.interval.1 - idx.interval.0;
                        total_bitmap_entries += idx_size;
                        if (not idx.requires_additional_sorting) {
                            total_sorted_entries += idx_size;
                        };

                        // Mark fields as covered
                        for (field in Set.keys(idx.fully_covered_range_fields)) {
                            Set.delete(uncovered_range_fields, Map.thash, field);
                        };
                        for (field in Set.keys(idx.fully_covered_equal_fields)) {
                            Set.delete(uncovered_equal_fields, Map.thash, field);
                        };

                        // If all covered, we're done
                        if (
                            Set.size(uncovered_range_fields) == 0 and
                            Set.size(uncovered_equal_fields) == 0 and
                            not needs_sorting
                        ) {
                            return Buffer.toArray(results);
                        };
                    };
                };
            };

            // Try each uncovered equality field with this covered range field
            for (uncovered_field in uncovered_equal_array.vals()) {
                let combo_ops = build_combination(covered_field, uncovered_field);

                let candidate = if (needs_sorting) {
                    switch (try_combination(combo_ops, sort_field)) {
                        case (?idx) {
                            if (not idx.requires_additional_sorting) {
                                needs_sorting := false;
                            };
                            ?idx;
                        };
                        case (null) try_combination(combo_ops, null);
                    };
                } else {
                    try_combination(combo_ops, null);
                };

                switch (candidate) {
                    case (null) {};
                    case (?idx) {
                        results.add(idx);
                        let idx_size = idx.interval.1 - idx.interval.0;
                        total_bitmap_entries += idx_size;
                        if (not idx.requires_additional_sorting) {
                            total_sorted_entries += idx_size;
                        };

                        for (field in Set.keys(idx.fully_covered_range_fields)) {
                            Set.delete(uncovered_range_fields, Map.thash, field);
                        };
                        for (field in Set.keys(idx.fully_covered_equal_fields)) {
                            Set.delete(uncovered_equal_fields, Map.thash, field);
                        };

                        if (
                            Set.size(uncovered_range_fields) == 0 and
                            Set.size(uncovered_equal_fields) == 0 and
                            not needs_sorting
                        ) {
                            return Buffer.toArray(results);
                        };
                    };
                };
            };
        };

        // Phase 2: Try swapping covered equality fields with uncovered fields
        label phase2 for (covered_field in covered_equal_fields.vals()) {
            // Try each uncovered range field
            for (uncovered_field in uncovered_range_array.vals()) {
                let combo_ops = build_combination(covered_field, uncovered_field);

                let candidate = if (needs_sorting) {
                    switch (try_combination(combo_ops, sort_field)) {
                        case (?idx) {
                            if (not idx.requires_additional_sorting) {
                                needs_sorting := false;
                            };
                            ?idx;
                        };
                        case (null) try_combination(combo_ops, null);
                    };
                } else {
                    try_combination(combo_ops, null);
                };

                switch (candidate) {
                    case (null) {};
                    case (?idx) {
                        results.add(idx);
                        let idx_size = idx.interval.1 - idx.interval.0;
                        total_bitmap_entries += idx_size;
                        if (not idx.requires_additional_sorting) {
                            total_sorted_entries += idx_size;
                        };

                        for (field in Set.keys(idx.fully_covered_range_fields)) {
                            Set.delete(uncovered_range_fields, Map.thash, field);
                        };
                        for (field in Set.keys(idx.fully_covered_equal_fields)) {
                            Set.delete(uncovered_equal_fields, Map.thash, field);
                        };

                        if (
                            Set.size(uncovered_range_fields) == 0 and
                            Set.size(uncovered_equal_fields) == 0 and
                            not needs_sorting
                        ) {
                            return Buffer.toArray(results);
                        };
                    };
                };
            };

            // Try each uncovered equality field
            for (uncovered_field in uncovered_equal_array.vals()) {
                let combo_ops = build_combination(covered_field, uncovered_field);

                let candidate = if (needs_sorting) {
                    switch (try_combination(combo_ops, sort_field)) {
                        case (?idx) {
                            if (not idx.requires_additional_sorting) {
                                needs_sorting := false;
                            };
                            ?idx;
                        };
                        case (null) try_combination(combo_ops, null);
                    };
                } else {
                    try_combination(combo_ops, null);
                };

                switch (candidate) {
                    case (null) {};
                    case (?idx) {
                        results.add(idx);
                        let idx_size = idx.interval.1 - idx.interval.0;
                        total_bitmap_entries += idx_size;
                        if (not idx.requires_additional_sorting) {
                            total_sorted_entries += idx_size;
                        };

                        for (field in Set.keys(idx.fully_covered_range_fields)) {
                            Set.delete(uncovered_range_fields, Map.thash, field);
                        };
                        for (field in Set.keys(idx.fully_covered_equal_fields)) {
                            Set.delete(uncovered_equal_fields, Map.thash, field);
                        };

                        if (
                            Set.size(uncovered_range_fields) == 0 and
                            Set.size(uncovered_equal_fields) == 0 and
                            not needs_sorting
                        ) {
                            return Buffer.toArray(results);
                        };
                    };
                };
            };
        };

        // Phase 3: If we still need sorting, specifically look for an index that provides it
        if (needs_sorting) {
            switch (sort_field) {
                case (null) {};
                case (?sort_field_info) {
                    let sort_candidate = try_combination([], ?sort_field_info);
                    switch (sort_candidate) {
                        case (null) {};
                        case (?idx) {
                            if (not idx.requires_additional_sorting) {
                                results.add(idx);
                                let idx_size = idx.interval.1 - idx.interval.0;
                                total_sorted_entries += idx_size;
                                needs_sorting := false;
                            };
                        };
                    };
                };
            };
        };

        Buffer.toArray(results);
    };

    public func get_best_index(
        collection : T.StableCollection,
        operations : [(Text, T.ZqlOperators)],
        sort_field : ?(Text, T.SortDirection),
        opt_last_pagination_document : ?T.CandidMap,
        has_nested_and_or_operations : Bool,
    ) : ?T.BestIndexResult {
        let requires_sorting = Option.isSome(sort_field);
        let equal_fields = Set.new<Text>();
        let sort_fields = Buffer.Buffer<(Text, T.SortDirection)>(8);
        let range_fields = Set.new<Text>();
        // let partially_covered_fields = Set.new<Text>();

        fill_field_maps(equal_fields, sort_fields, range_fields, operations, sort_field);

        // the sorting direction of the query and the index can either be a direct match
        // or a direct opposite in order to return the results without additional sorting
        var is_query_and_index_direction_a_match : ?Bool = null;

        let indexes = Buffer.Buffer<IndexCmpDetails>(collection.indexes.size());

        for ((index_name, index) in iterate(collection, false)) {

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

                    // Debug.print("operations: " # debug_show operations);
                    // Debug.print("sort_field: " # debug_show sort_field);
                    // Debug.print("index.name: " # debug_show index.name);
                    // Debug.print("num_of_equal_fields_covered: " # debug_show num_of_equal_fields_covered);
                    // Debug.print("num_of_range_fields_covered: " # debug_show num_of_range_fields_covered);
                    // Debug.print("num_of_sort_fields_covered: " # debug_show num_of_sort_fields_covered);

                    if (num_of_equal_fields_covered > 0 or num_of_range_fields_covered > 0 or num_of_sort_fields_covered > 0) {

                        let (scan_bounds, filter_bounds) = convert_simple_ops_to_bounds(false, operations, ?index.key_details, ?fully_covered_equality_and_range_fields);

                        requires_additional_sorting := requires_additional_sorting or num_of_sort_fields_evaluated < sort_fields.size();

                        let sorted_in_reverse = switch (is_query_and_index_direction_a_match) {
                            case (null) false;
                            case (?is_a_match) not is_a_match;
                        };

                        let should_use_cursor_pagination = (
                            not requires_additional_sorting and (
                                (requires_sorting) or
                                (not has_nested_and_or_operations)
                            )
                        );

                        let interval = CompositeIndex.scan(
                            collection,
                            index,
                            scan_bounds.0,
                            scan_bounds.1,
                            if (should_use_cursor_pagination) opt_last_pagination_document else null,
                            // the reason we need the query to be sorted to use cursor pagination is so that we can safely
                            // filter out the documents that are before the last pagination document
                            // We have 100% guarantee that those documents will not appear later in the results because the index is sorted in the same order as the query
                            should_use_cursor_pagination and not sorted_in_reverse,
                        );

                        let index_details : IndexCmpDetails = {
                            index;

                            num_of_range_fields = num_of_range_fields_covered;
                            num_of_sort_fields = num_of_sort_fields_covered;
                            num_of_equal_fields = num_of_equal_fields_covered;

                            requires_additional_filtering;
                            requires_additional_sorting;
                            fully_covered_equality_and_range_fields;
                            sorted_in_reverse;

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
                    num_of_sort_fields;
                } = best_index_details;

                let fully_covered_range_fields = Set.new<Text>();
                let fully_covered_sort_fields = Set.new<Text>();
                let fully_covered_equal_fields = Set.new<Text>();

                for ((field, _) in index.key_details.vals()) {
                    if (Set.has(range_fields, Set.thash, field) and Set.has(fully_covered_equality_and_range_fields, Set.thash, field)) {
                        ignore Set.put(fully_covered_range_fields, Set.thash, field);
                    };

                    if (Set.has(equal_fields, Set.thash, field) and Set.has(fully_covered_equality_and_range_fields, Set.thash, field)) {
                        ignore Set.put(fully_covered_equal_fields, Set.thash, field);
                    };

                    for (_ in Itertools.range(0, num_of_sort_fields)) {
                        ignore Set.put(fully_covered_sort_fields, Set.thash, field);
                    };
                };

                let best_index_result : T.BestIndexResult = {
                    index;
                    requires_additional_sorting;
                    requires_additional_filtering;
                    sorted_in_reverse = sorted_in_reverse;
                    fully_covered_equality_and_range_fields;
                    score = calculate_score(best_index_details, false);

                    fully_covered_equal_fields;
                    fully_covered_sort_fields;
                    fully_covered_range_fields;

                    interval = best_index_details.interval;
                };

                return ?best_index_result;
            };
        };

    };

};
