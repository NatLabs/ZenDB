import Principal "mo:base@0.16.0/Principal";
import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Nat32 "mo:base@0.16.0/Nat32";
import Result "mo:base@0.16.0/Result";
import Order "mo:base@0.16.0/Order";
import Iter "mo:base@0.16.0/Iter";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Option "mo:base@0.16.0/Option";
import Hash "mo:base@0.16.0/Hash";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.4.0";
import Decoder "mo:serde@3.4.0/Candid/Blob/Decoder";
import Candid "mo:serde@3.4.0/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import SparseBitMap64 "mo:bit-map@0.1.2/SparseBitMap64";

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import C "../Constants";
import Logger "../Logger";
import BTree "../BTree";
import MergeSort "../MergeSort";

import CompositeIndex "Index/CompositeIndex";
import Index "Index";
import Intervals "Intervals";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "CollectionUtils";
import QueryPlan "QueryPlan";
import DocumentStore "DocumentStore";
import CandidMap "../CandidMap";

module {

    let LOGGER_NAMESPACE = "QueryExecution";

    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; nhash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    type QueryBuilder = Query.QueryBuilder;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type CompositeIndex = T.CompositeIndex;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type InternalCandify<A> = T.InternalCandify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;

    let STABLE_MEMORY_BTREE_ORDER = 256;

    type EvalResult = T.EvalResult;

    type IndexDetails = {
        var sorted_in_reverse : ?Bool;
        intervals : Buffer.Buffer<T.Interval>;
    };

    // avoids sorting
    public func get_unique_document_ids_from_query_plan(
        collection : T.StableCollection,
        // only accepts bitmaps directly created from an index scan
        bitmap_cache : Map<Text, T.SparseBitMap64>,
        query_plan : T.QueryPlan,
    ) : EvalResult {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("get_unique_document_ids_from_query_plan");

        log.lazyDebug(
            func() = "Processing query plan with "
            # debug_show query_plan.scans.size() # " scans and "
            # debug_show query_plan.subplans.size() # " subplans"
            # ", operation type: " # (if (query_plan.is_and_operation) "AND" else "OR")
        );

        if (query_plan.scans.size() == 1 and query_plan.subplans.size() == 0) {
            switch (query_plan.scans[0]) {
                case (#IndexScan(index_scan_details)) {
                    let {
                        index_name;
                        requires_additional_filtering;
                        requires_additional_sorting;
                        interval;
                        scan_bounds;
                        filter_bounds;
                    } = index_scan_details;
                    if (not requires_additional_filtering and not requires_additional_sorting) {
                        log.lazyDebug(
                            func() = "Direct interval access on index '"
                            # index_name # "', interval: " # debug_show interval
                        );
                        return #Interval(index_name, [interval], index_scan_details.sorted_in_reverse);
                    };
                };
                case (#FullScan({ filter_bounds; requires_additional_filtering; requires_additional_sorting })) {
                    if (not requires_additional_filtering and not requires_additional_sorting) {
                        log.lazyDebug(func() = "Full scan with no filtering or sorting");
                        return #Interval(C.DOCUMENT_ID, [(0, DocumentStore.size(collection))], false);
                    };
                };
            };
        };

        let bitmaps = Buffer.Buffer<T.SparseBitMap64>(query_plan.scans.size() + query_plan.subplans.size());
        let intervals_by_index = Map.new<Text, IndexDetails>();

        label evaluating_query_plan for (scan_details in query_plan.scans.vals()) {
            let document_ids_iter : Iter<(T.DocumentId, ?[(Text, T.Candid)])> = switch (scan_details) {
                case (#FullScan({ filter_bounds; requires_additional_filtering })) {
                    log.lazyDebug(func() = "Processing full scan");
                    let full_scan_iter = DocumentStore.keys(collection);

                    let filtered_iter = if (requires_additional_filtering) {
                        log.lazyDebug(func() = "Applying filters to full scan");
                        CollectionUtils.multiFilter(collection, full_scan_iter, Buffer.fromArray([(filter_bounds)]), query_plan.is_and_operation);
                    } else {
                        full_scan_iter;
                    };

                    Iter.map<T.DocumentId, (T.DocumentId, ?[(Text, T.Candid)])>(
                        filtered_iter,
                        func(id : T.DocumentId) : (T.DocumentId, ?[(Text, T.Candid)]) {
                            (id, null);
                        },
                    );
                };
                case (#IndexScan(index_scan_details)) {
                    let {
                        index_name;
                        requires_additional_filtering;
                        interval;
                        filter_bounds;
                    } = index_scan_details;

                    log.lazyDebug(
                        func() = "Processing index scan on '" #
                        index_name # "', requires_additional_filtering: " #
                        debug_show requires_additional_filtering
                    );

                    let index_data_utils = CompositeIndex.get_index_data_utils(collection);

                    if (requires_additional_filtering) {
                        log.lazyDebug(func() = "Attempting index-based filtering");

                        // index based filtering improves the worst case scenario of filtering intervas
                        // by using intersecting bitmaps with document ids instead of accessing the
                        // values in the main btree and filtering them
                        //
                        // while the improvement are undeniable for full scans,
                        // its not always the case, as loading the other indexes documents
                        // into bitmaps can be more expensive than filtering the main btree
                        //
                        // todo - add a heuristic to determine when to use index based filtering
                        // can return the intervals from the indexes and compare them before
                        // loading them into bitmaps

                        // let { intervals_by_index; opt_filter_bounds } = get_index_based_filtering_intervals(collection, filter_bounds, index_scan_details.simple_operations);
                        switch (index_based_interval_filtering(collection, bitmap_cache, index_scan_details)) {
                            case (?{ bitmap; opt_filter_bounds }) {
                                log.lazyDebug(func() = "Successfully applied index-based filtering");
                                switch (opt_filter_bounds) {
                                    case (?filter_bounds) {
                                        log.lazyDebug(func() = "Applying additional post-filtering");
                                        let document_ids = Iter.map<Nat, T.DocumentId>(
                                            SparseBitMap64.vals(bitmap),
                                            func(n : Nat) : T.DocumentId {
                                                CollectionUtils.convert_bitmap_8_byte_to_document_id(collection, n);
                                            },
                                        );

                                        let filtered = CollectionUtils.multiFilter(
                                            collection,
                                            document_ids,
                                            Buffer.fromArray([filter_bounds]),
                                            query_plan.is_and_operation,
                                        );

                                        Iter.map<T.DocumentId, (T.DocumentId, ?[(Text, T.Candid)])>(filtered, func(id : T.DocumentId) : (T.DocumentId, ?[(Text, T.Candid)]) { (id, null) });
                                    };
                                    case (null) {
                                        log.lazyDebug(func() = "No additional filtering needed, adding bitmap directly");
                                        bitmaps.add(bitmap);
                                        continue evaluating_query_plan;
                                    };
                                };
                            };
                            case (null) {
                                log.lazyDebug(
                                    func() = "CompositeIndex-based filtering not applicable, falling back to standard approach"
                                );

                                let index_key_details = Index.get_key_details_by_name(collection, index_name);
                                let filter_bounds_buffer = Buffer.fromArray<T.Bounds>([filter_bounds]);

                                let document_ids_with_fields = if (CollectionUtils.can_use_indexed_fields_for_filtering(index_key_details, filter_bounds_buffer)) {
                                    Intervals.document_ids_and_indexed_fields_from_intervals(collection, index_name, [interval], false);
                                } else {
                                    let document_ids = Intervals.document_ids_from_index_intervals(collection, index_name, [interval], false);
                                    Iter.map<T.DocumentId, (T.DocumentId, ?[(Text, T.Candid)])>(
                                        document_ids,
                                        func(id : T.DocumentId) : (T.DocumentId, ?[(Text, T.Candid)]) {
                                            (id, null);
                                        },
                                    );
                                };

                                CollectionUtils.multiFilterWithIndexedFields(collection, document_ids_with_fields, filter_bounds_buffer, query_plan.is_and_operation);

                            };
                        };

                    } else {
                        log.lazyDebug(
                            func() = "Adding direct interval from index '" #
                            index_name # "': " # debug_show interval
                        );
                        add_interval(intervals_by_index, index_name, interval, false);
                        continue evaluating_query_plan;
                    };
                };
            };

            log.lazyDebug(func() = "Creating bitmap from document IDs iterator");
            bitmaps.add(
                SparseBitMap64.fromIter(Iter.map(document_ids_iter, func((id, _)) : Nat { Utils.convert_last_8_bytes_to_nat(id) }))
            );
        };

        log.lazyDebug(
            func() = "Processing " #
            Nat.toText(query_plan.subplans.size()) # " subplans"
        );

        for (or_operation_subplan in query_plan.subplans.vals()) {
            log.lazyDebug(func() = "Recursively processing subplan");
            let eval_result = get_unique_document_ids_from_query_plan(collection, bitmap_cache, or_operation_subplan);

            switch (eval_result) {
                case (#Empty) {
                    log.lazyDebug(func() = "Subplan returned empty result");
                    if (query_plan.is_and_operation) {
                        log.lazyDebug(func() = "Early return with empty result due to AND with empty set");
                        return #Empty;
                    };
                };
                case (#Ids(document_ids_iter)) {
                    log.lazyDebug(func() = "Subplan returned document IDs iterator");
                    bitmaps.add(
                        SparseBitMap64.fromIter(Iter.map(document_ids_iter, func((id, _) : (T.DocumentId, ?[(Text, T.Candid)])) : Nat { Utils.convert_last_8_bytes_to_nat(id) }))
                    );
                };
                case (#BitMap(sub_bitmap)) {
                    log.lazyDebug(
                        func() = "Subplan returned bitmap with " #
                        Nat.toText(SparseBitMap64.size(sub_bitmap)) # " documents"
                    );
                    bitmaps.add(sub_bitmap);
                };
                case (#Interval(index_name, intervals, is_reversed)) {
                    log.lazyDebug(
                        func() = "Subplan returned interval on index '" #
                        index_name # "' with " # Nat.toText(intervals.size()) # " ranges"
                    );
                    add_interval(intervals_by_index, index_name, intervals.get(0), is_reversed);
                };
            };
        };

        log.lazyDebug(
            func() = "Processing " #
            Nat.toText(Map.size(intervals_by_index)) # " interval sets from different indexes"
        );

        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            log.lazyDebug(
                func() = "Processing intervals for index '" #
                index_name # "' with " # Nat.toText(interval_details.intervals.size()) # " intervals"
            );

            if (query_plan.is_and_operation) {
                switch (Intervals.intersect(interval_details.intervals)) {
                    case (?interval) {
                        log.lazyDebug(
                            func() = "Intersected " #
                            Nat.toText(interval_details.intervals.size()) #
                            " intervals to single interval " # debug_show interval
                        );
                        interval_details.intervals.clear();
                        interval_details.intervals.add(interval);
                    };
                    case (null) {
                        log.lazyDebug(
                            func() = "Intervals have empty intersection for index '" #
                            index_name # "', removing from consideration"
                        );
                        ignore Map.remove(intervals_by_index, Map.thash, index_name);
                    };
                };
            } else {
                log.lazyDebug(
                    func() = "Merging overlapping intervals for index '" # index_name # "'"
                );
                Intervals.union(interval_details.intervals);
                log.lazyDebug(
                    func() = "After union operation, index '" # index_name #
                    "' has " # Nat.toText(interval_details.intervals.size()) # " intervals"
                );
            };
        };

        if (Map.size(intervals_by_index) > 1) {
            log.lazyDebug(
                func() = "Converting " #
                Nat.toText(Map.size(intervals_by_index)) # " index intervals to bitmaps"
            );

            let intervals_by_index_array = Map.toArray(intervals_by_index);

            let sorted_intervals_by_index = Array.sort(
                intervals_by_index_array,
                func(a : (Text, IndexDetails), b : (Text, IndexDetails)) : Order {
                    let a_size = a.1.intervals.size();
                    let b_size = b.1.intervals.size();

                    Nat.compare(a_size, b_size);
                },
            );

            let bitmap = if (bitmaps.size() == 0) SparseBitMap64.new() else bitmaps.remove(bitmaps.size() - 1);

            func load_interval_into_bitmap(bitmap : T.SparseBitMap64, index_name : Text, intervals : [T.Interval]) {
                let document_ids_in_interval = Intervals.document_ids_from_index_intervals(collection, index_name, intervals, false);

                for (id in document_ids_in_interval) {
                    let id_as_nat = Utils.convert_last_8_bytes_to_nat(id);
                    SparseBitMap64.add(bitmap, id_as_nat);
                };
            };

            if (not query_plan.is_and_operation) {
                for ((index_name, interval_details) in sorted_intervals_by_index.vals()) {
                    load_interval_into_bitmap(bitmap, index_name, Buffer.toArray(interval_details.intervals));
                };
            } else {

                let sorted_intervals_by_index_iter = sorted_intervals_by_index.vals();

                if (bitmaps.size() == 0) {
                    let ?(index_name, interval_details) = sorted_intervals_by_index_iter.next() else Debug.trap("QueryExecution.get_unique_document_ids_from_query_plan: No elements in intervals_by_index map when size is greater than 0");
                    load_interval_into_bitmap(bitmap, index_name, Buffer.toArray(interval_details.intervals));
                };

                let loading_zone = SparseBitMap64.new();

                for ((index_name, interval_details) in sorted_intervals_by_index_iter) {

                    load_interval_into_bitmap(loading_zone, index_name, Buffer.toArray(interval_details.intervals));
                    SparseBitMap64.intersectInPlace(bitmap, loading_zone);

                    // retains the size but clears the contents
                    SparseBitMap64.clear(loading_zone);
                };
            };

            bitmaps.add(bitmap);

        };

        let result = if (bitmaps.size() == 0 and Map.size(intervals_by_index) == 1) {
            let ?(index_name, interval_details) = Map.entries(intervals_by_index).next() else {
                log.lazyError(
                    func() = "No elements in intervals_by_index map when size is greater than 0"
                );
                log.trap("QueryExecution.get_unique_document_ids_from_query_plan: No elements in map when size is greater than 0");
            };

            let sorted_in_reverse = switch (interval_details.sorted_in_reverse) {
                case (?sorted_in_reverse) sorted_in_reverse;
                case (null) false;
            };

            log.lazyDebug(
                func() = "Using direct interval result from index '" #
                index_name # "' with " # Nat.toText(interval_details.intervals.size()) # " intervals"
            );

            #Interval(index_name, Buffer.toArray(interval_details.intervals), sorted_in_reverse);
        } else if (bitmaps.size() == 0) {
            log.lazyDebug(func() = "No results match the query");
            #Empty;
        } else {
            if (bitmaps.size() == 1) {
                let bitmap = bitmaps.get(0);
                log.lazyDebug(
                    func() = "Using single bitmap with " #
                    Nat.toText(SparseBitMap64.size(bitmap)) # " documents"
                );
                #BitMap(bitmap);
            } else if (query_plan.is_and_operation) {
                log.lazyDebug(
                    func() = "Intersecting " #
                    Nat.toText(bitmaps.size()) # " bitmaps for AND operation"
                );
                #BitMap(SparseBitMap64.multiIntersect(bitmaps.vals()));
            } else {
                log.lazyDebug(
                    func() = "Merging " #
                    Nat.toText(bitmaps.size()) # " bitmaps for OR operation"
                );
                #BitMap(SparseBitMap64.multiUnion(bitmaps.vals()));
            };
        };

        let elapsed = 0;

        switch (result) {
            case (#Empty) {
                log.lazyDebug(
                    func() = "Query returned empty result set in "
                    # debug_show elapsed # " instructions"
                );
            };
            case (#BitMap(bitmap)) {
                log.lazyDebug(
                    func() = "Query returned bitmap with "
                    # debug_show SparseBitMap64.size(bitmap) # " documents in " # debug_show elapsed # " instructions"
                );
            };
            case (#Ids(iter)) {
                log.lazyDebug(
                    func() = "Query returned documents iterator in "
                    # debug_show elapsed # " instructions"
                );
            };
            case (#Interval(index_name, intervals, _)) {
                log.lazyDebug(
                    func() = "Query returned intervals on index '"
                    # index_name # "': " # debug_show (intervals)
                );

                var total_size = 0;
                for (interval in intervals.vals()) {
                    total_size += interval.1 - interval.0;
                };
                log.lazyDebug(
                    func() = "Query returned intervals on index '"
                    # index_name # "' with " # debug_show intervals.size() # " intervals containing "
                    # debug_show total_size # " documents in " # debug_show elapsed # " instructions"
                );
            };
        };

        result;
    };

    public func add_interval(intervals_by_index : Map<Text, IndexDetails>, index_name : Text, interval : T.Interval, is_reversed : Bool) {
        let details = switch (Map.get(intervals_by_index, Map.thash, index_name)) {
            case (?details) {
                switch (details.sorted_in_reverse) {
                    case (?sorted_in_reverse) {
                        if (sorted_in_reverse != is_reversed) {
                            Logger.trap("QueryExecution.add_interval: Inconsistent sorted_in_reverse values for index '" # index_name # "': existing=" # debug_show sorted_in_reverse # ", new=" # debug_show is_reversed);
                        };
                    };
                    case (null) {
                        details.sorted_in_reverse := ?is_reversed;
                    };
                };
                details;
            };
            case (null) {
                let buffer = Buffer.Buffer<T.Interval>(8);

                let details : IndexDetails = {
                    var sorted_in_reverse = ?is_reversed;
                    intervals = buffer;
                };

                ignore Map.put(intervals_by_index, Map.thash, index_name, details);
                details;
            };
        };

        details.intervals.add(interval);
    };

    type IndexIntervalFilterDetails = {
        intervals_map : Map<Text, Buffer.Buffer<T.Interval>>;
        opt_filter_bounds : ?T.Bounds;
    };

    public func get_index_based_filtering_intervals(collection : T.StableCollection, filter_bounds : T.Bounds, operations : [(Text, T.ZqlOperators)]) : IndexIntervalFilterDetails {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("get_index_based_filtering_intervals");

        log.lazyDebug(func() = "Finding best indexes for filtering");
        log.lazyDebug(
            func() = "Initial filter bounds: " # debug_show filter_bounds
        );
        log.lazyDebug(
            func() = "Filter operations: " # debug_show operations
        );

        var prev = filter_bounds;
        var curr = filter_bounds;

        let intervals_map = Map.new<Text, Buffer.Buffer<T.Interval>>();

        loop {
            let fields = Set.new<Text>();

            for ((field, _) in curr.0.vals()) {
                Set.add(fields, Map.thash, field);
            };

            log.lazyDebug(
                func() = "Processing " #
                Nat.toText(Set.size(fields)) # " unique fields"
            );

            let filter_operations = Buffer.Buffer<(Text, T.ZqlOperators)>(8);

            for ((field, value) in operations.vals()) {
                if (Set.has(fields, Map.thash, field)) {
                    filter_operations.add(field, value);
                };
            };

            log.lazyDebug(
                func() = "Found " #
                Nat.toText(filter_operations.size()) # " applicable filter operations"
            );

            let {
                index;
                fully_covered_equality_and_range_fields;
                // true because this function itself is a bitmap intersection
            } = switch (Index.get_best_index(collection, Buffer.toArray(filter_operations), null, null, true)) {
                case (null) {
                    log.lazyDebug(
                        func() = "No suitable index found for filtering"
                    );
                    return {
                        intervals_map;
                        opt_filter_bounds = ?curr;
                    };
                };
                case (?best_index_details) {
                    log.lazyDebug(
                        func() = "Selected index '" #
                        best_index_details.index.name # "' for filtering"
                    );
                    best_index_details;
                };
            };

            let lower_map = Map.new<Text, T.CandidInclusivityQuery>();

            for ((field, opt_state) in curr.0.vals()) {
                switch (opt_state) {
                    case (?state) {
                        ignore Map.put(lower_map, Map.thash, field, state);
                    };
                    case (null) {};
                };
            };

            let upper_map = Map.new<Text, T.CandidInclusivityQuery>();

            for ((field, opt_state) in curr.1.vals()) {
                switch (opt_state) {
                    case (?state) {
                        ignore Map.put(upper_map, Map.thash, field, state);
                    };
                    case (null) {};
                };
            };

            let (scan_bounds, filter_bounds) = Index.convert_simple_ops_to_bounds(
                false,
                Buffer.toArray(filter_operations),
                ?index.key_details,
                ?fully_covered_equality_and_range_fields,
            );

            log.lazyDebug(
                func() = "Extracted scan bounds for index '" #
                index.name # "'"
            );

            let interval = CompositeIndex.scan(collection, index, scan_bounds.0, scan_bounds.1, null, false);

            log.lazyDebug(
                func() = "Generated interval " #
                debug_show interval # " for index '" # index.name # "'"
            );

            switch (Map.get(intervals_map, Map.thash, index.name)) {
                case (?intervals) {
                    log.lazyDebug(
                        func() = "Adding interval to existing set for index '" #
                        index.name # "'"
                    );
                    intervals.add(interval);
                };
                case (null) {
                    log.lazyDebug(
                        func() = "Creating new interval set for index '" #
                        index.name # "'"
                    );
                    ignore Map.put(intervals_map, Map.thash, index.name, Buffer.fromArray<T.Interval>([interval]));
                };
            };

            prev := curr;
            curr := filter_bounds;

            log.lazyDebug(
                func() = "Filter bounds narrowed from " #
                Nat.toText(prev.0.size()) # " to " # Nat.toText(curr.0.size()) # " lower bounds"
            );

        } while (prev.0.size() > curr.0.size() and curr.0.size() > 0);

        let result = {
            intervals_map;
            opt_filter_bounds = if (curr.0.size() == 0) ?curr else null;
        };

        log.lazyDebug(
            func() = "Completed with " #
            Nat.toText(Map.size(intervals_map)) # " index interval sets and " #
            (if (Option.isSome(result.opt_filter_bounds)) "additional" else "no additional") #
            " filter bounds"
        );

        result;
    };

    func retrieve_all_index_interval_iterators(
        collection : T.StableCollection,
        index_intervals : Map<Text, Buffer.Buffer<T.Interval>>,
        sorted_in_reverse : Bool,
        combine_intervals_in_same_index : Bool,
    ) : Buffer<Iter<T.DocumentId>> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("retrieve_all_index_interval_iterators");

        log.lazyDebug(
            func() = "Retrieving iterators for " #
            Nat.toText(Map.size(index_intervals)) # " index interval sets, combine_intervals=" #
            debug_show combine_intervals_in_same_index
        );

        let iterators = Buffer.Buffer<Iter<T.DocumentId>>(8);

        for ((index_name, intervals) in Map.entries(index_intervals)) {
            if (combine_intervals_in_same_index) {
                log.lazyDebug(
                    func() = "Retrieving combined document IDs for " #
                    Nat.toText(intervals.size()) # " intervals on index '" # index_name # "'"
                );

                let document_ids = Intervals.document_ids_from_index_intervals(collection, index_name, Buffer.toArray(intervals), sorted_in_reverse);
                iterators.add(document_ids);

            } else {
                log.lazyDebug(
                    func() = "Retrieving separate iterators for " #
                    Nat.toText(intervals.size()) # " intervals on index '" # index_name # "'"
                );

                for (interval in intervals.vals()) {
                    let document_ids = Intervals.document_ids_from_index_intervals(collection, index_name, [interval], sorted_in_reverse);
                    iterators.add(document_ids);
                };
            };
        };

        log.lazyDebug(
            func() = "Created " #
            Nat.toText(iterators.size()) # " iterators"
        );

        iterators;
    };

    type IndexBasedFilteringResult = {
        bitmap : T.SparseBitMap64;
        opt_filter_bounds : ?T.Bounds;
    };

    public func index_based_interval_filtering(
        collection : T.StableCollection,
        bitmap_cache : Map<Text, T.SparseBitMap64>,
        index_scan_details : T.IndexScanDetails,
    ) : ?IndexBasedFilteringResult {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("index_based_interval_filtering");

        log.lazyDebug(
            func() = "Evaluating index-based filtering options"
        );

        let {
            index_name;
            interval;
            filter_bounds;
            simple_operations = operations;
        } = index_scan_details;

        let original_interval_count = interval.1 - interval.0;

        log.lazyDebug(
            func() = "Original interval has " #
            Nat.toText(original_interval_count) # " documents"
        );

        let {
            intervals_map;
            opt_filter_bounds;
        } = get_index_based_filtering_intervals(collection, filter_bounds, operations);

        var filtering_intervals_count = 0;

        for ((index_name, intervals) in Map.entries(intervals_map)) {
            let cnt = Intervals.count(intervals);

            if (cnt > filtering_intervals_count) {
                filtering_intervals_count := cnt;
            };
        };

        log.lazyDebug(
            func() = "Max filtering interval count: " #
            Nat.toText(filtering_intervals_count) # ", original interval count: " #
            Nat.toText(original_interval_count)
        );

        if (filtering_intervals_count > (original_interval_count * 10)) {
            log.lazyDebug(
                func() = "Filtering intervals too large compared to original, " #
                "falling back to standard filtering approach"
            );
            return null;
        };

        switch (Map.get(intervals_map, Map.thash, index_name)) {
            case (?intervals) {
                log.lazyError(
                    func() = "Filtering index same as scanning index: '" #
                    index_name # "'. This should not happen - it indicates the filtering and scanning operations " #
                    "are using the same index which defeats the purpose of index-based filtering."
                );
                log.trap("QueryExecution.index_based_interval_filtering: filtering index same as scanning index '" # index_name # "'");
                intervals.add(interval);
            };
            case (null) {
                log.lazyDebug(
                    func() = "Adding original scan interval for index '" #
                    index_name # "'"
                );

                ignore Map.put(
                    intervals_map,
                    thash,
                    index_name,
                    Buffer.fromArray<T.Interval>([interval]),
                );
            };
        };

        log.lazyDebug(
            func() = "Intersecting intervals across all indexes"
        );

        for ((index_name, intervals) in Map.entries(intervals_map)) {
            switch (Intervals.intersect(intervals)) {
                case (?interval) {
                    log.lazyDebug(
                        func() = "Intervals for index '" #
                        index_name # "' intersect to " # debug_show interval
                    );
                    intervals.clear();
                    intervals.add(interval);
                };
                case (null) {
                    log.lazyDebug(
                        func() = "Intervals for index '" #
                        index_name # "' have empty intersection, removing index"
                    );
                    ignore Map.remove(intervals_map, Map.thash, index_name);
                };
            };
        };

        let bitmaps = Buffer.Buffer<T.SparseBitMap64>(8);

        log.lazyDebug(
            func() = "Creating bitmaps from " #
            Nat.toText(Map.size(intervals_map)) # " index interval sets"
        );

        for ((index_name, intervals) in Map.entries(intervals_map)) {
            let interval = intervals.get(0);
            let interval_cache_key = index_name # debug_show (Buffer.toArray(intervals));

            log.lazyDebug(
                func() = "Processing interval for index '" #
                index_name # "'"
            );

            let bitmap = switch (Map.get(bitmap_cache, Map.thash, interval_cache_key)) {
                case (?bitmap) {
                    log.lazyDebug(
                        func() = "Using cached bitmap for interval"
                    );
                    bitmap;
                };
                case (null) {
                    log.lazyDebug(
                        func() = "Creating new bitmap for interval"
                    );

                    let document_ids = Intervals.document_ids_from_index_intervals(collection, index_name, [interval], false);
                    let bitmap = SparseBitMap64.fromIter(Iter.map(document_ids, Utils.convert_last_8_bytes_to_nat));

                    ignore Map.put(bitmap_cache, Map.thash, interval_cache_key, bitmap);
                    bitmap;
                };
            };

            bitmaps.add(bitmap);
        };

        log.lazyDebug(
            func() = "Intersecting " #
            Nat.toText(bitmaps.size()) # " bitmaps"
        );

        let bitmap = SparseBitMap64.multiIntersect(bitmaps.vals());

        log.lazyDebug(
            func() = "Final bitmap contains " #
            Nat.toText(SparseBitMap64.size(bitmap)) # " document IDs"
        );

        ?{ bitmap; opt_filter_bounds };
    };

    func load_bitmap(iter : T.Iter<(T.DocumentId, ?[(Text, T.Candid)])>, opt_last_pagination_document_id : ?T.DocumentId) : T.SparseBitMap64 {
        SparseBitMap64.fromIter(
            Iter.map(
                Iter.filter(
                    iter,
                    func(tuple : (T.DocumentId, ?[(Text, T.Candid)])) : Bool {
                        switch (opt_last_pagination_document_id) {
                            case (?last_id) tuple.0 > last_id;
                            case (null) true;
                        };
                    },
                ),
                func(tuple : (T.DocumentId, ?[(Text, T.Candid)])) : Nat {
                    Utils.convert_last_8_bytes_to_nat(tuple.0);
                },
            )
        );
    };

    public func generate_document_ids_for_and_operation(
        collection : T.StableCollection,
        query_plan : T.QueryPlan,
        opt_sort_column : ?(Text, T.SortDirection),
        opt_last_pagination_document_id : ?T.DocumentId,
        sort_documents_by_field_cmp : ((T.DocumentId, ?[(Text, T.Candid)]), (T.DocumentId, ?[(Text, T.Candid)])) -> Order,
    ) : EvalResult {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("generate_document_ids_for_and_operation");

        assert query_plan.is_and_operation;
        let requires_sorting = Option.isSome(opt_sort_column);

        log.lazyDebug(
            func() = "Starting AND operation with " #
            Nat.toText(query_plan.scans.size()) # " scans, " #
            Nat.toText(query_plan.subplans.size()) # " subplans, requires_sorting=" #
            debug_show requires_sorting # ", has_pagination=" # debug_show Option.isSome(opt_last_pagination_document_id)
        );

        if (query_plan.scans.size() == 1 and query_plan.subplans.size() == 0) {

            switch (query_plan.scans[0]) {
                case (#IndexScan({ requires_additional_sorting; requires_additional_filtering; interval; index_name; sorted_in_reverse })) {
                    if (not requires_additional_sorting and not requires_additional_filtering) {
                        log.lazyDebug(
                            func() = "Simple index scan on '" # index_name # "', returning interval directly: " #
                            debug_show interval # ", reversed=" # debug_show sorted_in_reverse
                        );
                        return #Interval(index_name, [interval], sorted_in_reverse);
                    };
                };
                case (#FullScan({ requires_additional_sorting; requires_additional_filtering })) {
                    if (not requires_additional_sorting and not requires_additional_filtering) {
                        let size = DocumentStore.size(collection);
                        log.lazyDebug(
                            func() = "Simple full scan, returning full document range: [0, " #
                            Nat.toText(size) # ")"
                        );
                        return #Interval(C.DOCUMENT_ID, [(0, size)], false);
                    };

                };
            };
        };

        let iterators = Buffer.Buffer<Iter<(T.DocumentId, ?[(Text, T.Candid)])>>(8);
        var scans_sorted_documents_array : [(T.DocumentId, ?[(Text, T.Candid)])] = [];
        let intervals_by_index = Map.new<Text, IndexDetails>();
        let full_scan_details_buffer = Buffer.Buffer<T.FullScanDetails>(8);
        let bitmaps = Buffer.Buffer<T.SparseBitMap64>(8);

        for (scan_details in query_plan.scans.vals()) switch (scan_details) {
            case (#FullScan(full_scan_details)) {
                log.lazyDebug(
                    func() = "Adding full scan to processing buffer, requires_filtering=" #
                    debug_show full_scan_details.requires_additional_filtering #
                    ", requires_sorting=" # debug_show full_scan_details.requires_additional_sorting
                );
                full_scan_details_buffer.add(full_scan_details);
            };
            case (#IndexScan(index_scan_details)) {

                let {
                    index_name;
                    requires_additional_filtering;
                    requires_additional_sorting;
                    sorted_in_reverse;
                    interval;
                    scan_bounds;
                    filter_bounds;
                } = index_scan_details;

                log.lazyDebug(
                    func() = "Processing index scan on '" # index_name # "', interval=" #
                    debug_show interval # ", requires_filtering=" # debug_show requires_additional_filtering #
                    ", requires_sorting=" # debug_show requires_additional_sorting
                );

                if (requires_additional_sorting or requires_additional_filtering) {

                    let index_key_details = Index.get_key_details_by_name(collection, index_name);

                    let filter_bounds_buffer = Buffer.fromArray<T.Bounds>([filter_bounds]);

                    log.lazyDebug(
                        func() : Text = "Determining if indexed fields can be used for filtering"
                    );

                    var document_ids_with_fields = if (
                        CollectionUtils.can_use_indexed_fields_for_filtering(index_key_details, filter_bounds_buffer) or
                        (
                            requires_sorting and
                            Option.isSome(
                                Array.find(index_key_details, func((key, _) : (Text, Any)) : Bool { key == Option.get(opt_sort_column, ("", #Ascending)).0 })
                            )
                        )
                    ) {
                        log.lazyDebug(
                            func() : Text = "Using indexed fields from '" # index_name # "'"
                        );
                        Intervals.document_ids_and_indexed_fields_from_intervals(collection, index_name, [interval], sorted_in_reverse);
                    } else {
                        log.lazyDebug(
                            func() : Text = "Not using indexed fields, retrieving only document IDs from '" # index_name # "'"
                        );
                        let document_ids = Intervals.document_ids_from_index_intervals(collection, index_name, [interval], sorted_in_reverse);
                        Iter.map<T.DocumentId, (T.DocumentId, ?[(Text, T.Candid)])>(
                            document_ids,
                            func(id : T.DocumentId) : (T.DocumentId, ?[(Text, T.Candid)]) {
                                (id, null);
                            },
                        );
                    };

                    if (requires_additional_filtering) {
                        log.lazyDebug(
                            func() = "Applying additional filters to document IDs from index '" # index_name # "'"
                        );
                        document_ids_with_fields := CollectionUtils.multiFilterWithIndexedFields(collection, document_ids_with_fields, filter_bounds_buffer, query_plan.is_and_operation);
                    };

                    if (requires_additional_sorting) {
                        log.lazyDebug(
                            func() = "Preparing documents for sorting from index '" # index_name # "'"
                        );

                        if (scans_sorted_documents_array.size() == 0) {
                            let cursor_pagination_filtered_documents = switch (opt_last_pagination_document_id) {
                                case (null) {
                                    log.lazyDebug(func() = "No pagination cursor, using all documents");
                                    document_ids_with_fields;
                                };
                                case (?last_pagination_document_id) {
                                    log.lazyDebug(
                                        func() = "Applying cursor pagination filter for document: " #
                                        debug_show last_pagination_document_id
                                    );

                                    let last_pagination_document : T.CandidMap = CollectionUtils.get_and_cache_candid_map(
                                        collection,
                                        last_pagination_document_id,
                                    );

                                    // should have a sort column because this block requires additional sorting
                                    let sort_field = Option.get(opt_sort_column, ("", #Ascending)).0;

                                    let opt_sort_field_value = CandidMap.get(
                                        last_pagination_document,
                                        collection.schema_map,
                                        sort_field,
                                    );

                                    let bounds = Buffer.fromArray<T.Bounds>([(
                                        [(
                                            sort_field,
                                            Option.map<T.CandidQuery, T.State<T.CandidQuery>>(opt_sort_field_value, func(candid_query : T.CandidQuery) : T.State<T.CandidQuery> { #Exclusive(candid_query) }),
                                        )],
                                        [(sort_field, ?#Inclusive(#Maximum))],
                                    )]);

                                    CollectionUtils.multiFilterWithIndexedFields(
                                        collection,
                                        document_ids_with_fields,
                                        bounds,
                                        query_plan.is_and_operation,
                                    );

                                };
                            };

                            let arr = Iter.toArray(cursor_pagination_filtered_documents);
                            log.lazyDebug(
                                func() = "Sorting " # Nat.toText(arr.size()) # " documents"
                            );
                            scans_sorted_documents_array := MergeSort.sort(arr, sort_documents_by_field_cmp);
                            document_ids_with_fields := scans_sorted_documents_array.vals();
                        };
                    };

                    if (requires_sorting) {
                        log.lazyDebug(func() = "Adding sorted iterator to iterators buffer");
                        iterators.add(document_ids_with_fields);
                    } else {
                        log.lazyDebug(func() = "Creating bitmap from filtered document IDs");
                        let bitmap = SparseBitMap64.fromIter(Iter.map(document_ids_with_fields, func((id, _) : (T.DocumentId, ?[(Text, T.Candid)])) : Nat { Utils.convert_last_8_bytes_to_nat(id) }));
                        log.lazyDebug(func() = "Bitmap created with " # Nat.toText(SparseBitMap64.size(bitmap)) # " documents");
                        bitmaps.add(bitmap);
                    };

                } else {
                    log.lazyDebug(
                        func() = "No additional processing needed, adding interval directly to intervals_by_index"
                    );
                    add_interval(intervals_by_index, index_name, interval, sorted_in_reverse);
                };
            };
        };

        log.lazyDebug(
            func() = "Processing " # Nat.toText(query_plan.subplans.size()) # " subplans (OR operations)"
        );

        for (or_operation_subplan in query_plan.subplans.vals()) {
            log.lazyDebug(func() = "Evaluating OR subplan");
            let eval_result = generate_document_ids_for_or_operation(collection, or_operation_subplan, opt_sort_column, opt_last_pagination_document_id, sort_documents_by_field_cmp);

            switch (eval_result) {
                case (#Empty) {
                    log.lazyDebug(func() = "OR subplan returned empty, short-circuiting AND operation");
                    return #Empty; // return early if we encounter an empty set
                };
                case (#Ids(iter)) {
                    if (requires_sorting) {
                        iterators.add(iter);
                    } else {
                        bitmaps.add(
                            load_bitmap(iter, opt_last_pagination_document_id)
                        );
                    };
                };
                case (#BitMap(bitmap)) {
                    if (requires_sorting) {
                        log.lazyError(
                            func() : Text {
                                "Received BitMap from OR subplan when sorting is required. Expected sorted iterator instead.";
                            }
                        );
                        log.trap("QueryExecution.generate_document_ids_for_and_operation: BitMap returned when sorting required");
                    };
                    log.lazyDebug(
                        func() : Text {
                            "Adding bitmap from OR subplan with " # Nat.toText(SparseBitMap64.size(bitmap)) # " documents";
                        }
                    );
                    bitmaps.add(bitmap);
                };
                case (#Interval(index_name, intervals, is_reversed)) {
                    let intervalCount = intervals.size();
                    log.lazyDebug(
                        func() : Text {
                            "Adding " # Nat.toText(intervalCount) # " intervals from OR subplan for index " # debug_show index_name;
                        }
                    );
                    for (interval in intervals.vals()) {
                        add_interval(intervals_by_index, index_name, interval, is_reversed);
                    };
                };
            };

        };

        log.lazyDebug(
            func() = "Intersecting intervals for " # Nat.toText(Map.size(intervals_by_index)) # " indexes"
        );

        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            switch (Intervals.intersect(interval_details.intervals)) {
                case (?interval) {
                    log.lazyDebug(
                        func() : Text {
                            "Intervals for index " # debug_show index_name # " intersected to: " # debug_show interval;
                        }
                    );
                    interval_details.intervals.clear();
                    interval_details.intervals.add(interval);
                };
                case (null) {
                    log.lazyDebug(
                        func() : Text {
                            "Empty intersection for index " # debug_show index_name # ", removing from consideration";
                        }
                    );
                    ignore Map.remove(intervals_by_index, Map.thash, index_name);
                };
            };
        };

        if (bitmaps.size() == 0 and full_scan_details_buffer.size() == 0 and iterators.size() == 0 and Map.size(intervals_by_index) <= 1) {
            log.lazyDebug(
                func() = "Simple case: " # (if (Map.size(intervals_by_index) == 1) "single interval" else "empty result")
            );

            let merged_results : EvalResult = if (Map.size(intervals_by_index) == 1) {
                let ?(index_name, interval_details) = Map.entries(intervals_by_index).next() else {
                    log.lazyError(func() = "No elements in intervals_by_index map when size is 1");
                    log.trap("QueryExecution.generate_document_ids_for_and_operation: No elements in map when size is 1");
                };
                let interval = interval_details.intervals.get(0);
                let sorted_in_reverse = switch (interval_details.sorted_in_reverse) {
                    case (?sorted_in_reverse) sorted_in_reverse;
                    case (null) false;
                };
                #Interval(index_name, [interval], sorted_in_reverse);
            } else {
                #Empty;
            };

            return merged_results;

        };

        log.lazyDebug(
            func() = "Converting " # Nat.toText(Map.size(intervals_by_index)) # " index intervals to " #
            (if (requires_sorting) "sorted iterators" else "bitmaps")
        );

        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            let interval = interval_details.intervals.get(0);
            let index_config = Index.get_config_by_name(collection, index_name);

            let sorted_in_reverse = Option.get(interval_details.sorted_in_reverse, false);

            log.lazyDebug(
                func() : Text {
                    "Retrieving documents from index '" # index_name # "' for interval " # debug_show interval # ", reversed=" # debug_show sorted_in_reverse;
                }
            );

            let document_ids_with_fields = Intervals.document_ids_and_indexed_fields_from_intervals(collection, index_config.name, [interval], sorted_in_reverse);

            if (requires_sorting and scans_sorted_documents_array.size() == 0) {
                log.lazyDebug(func() = "Sorting is required, converting to array and sorting");

                let arr = Iter.toArray(document_ids_with_fields);

                if (arr.size() == 0) {
                    log.lazyDebug(func() = "No documents found in interval, returning empty");
                    return #Empty;
                };

                log.lazyDebug(func() = "Sorting " # Nat.toText(arr.size()) # " documents");
                scans_sorted_documents_array := MergeSort.sort(arr, sort_documents_by_field_cmp);
                log.lazyDebug(func() = "Sort completed, adding to iterators");

                iterators.add(scans_sorted_documents_array.vals());

            } else {
                log.lazyDebug(func() = "Loading documents into bitmap");
                bitmaps.add(
                    load_bitmap(document_ids_with_fields, opt_last_pagination_document_id)
                );

            };

        };

        // ! - feature: reduce full scan range by only scanning the intersection with the smallest interval range
        /**
        var smallest_interval_start = 0;
        var smallest_interval_end = 2 ** 64;

        var index_with_smallest_interval_range = "";
                    */

        if (full_scan_details_buffer.size() > 0) {
            log.lazyDebug(
                func() = "Processing " # Nat.toText(full_scan_details_buffer.size()) # " full scan operations"
            );

            var smallest_interval_index = "";
            var smallest_interval_start = 0;
            var smallest_interval_end = 0;

            if (Map.size(intervals_by_index) > 0) {
                log.lazyDebug(func() = "Finding smallest interval to optimize full scan range");

                var smallest_interval_range = 2 ** 64;

                for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
                    let interval = interval_details.intervals.get(0);
                    let range = interval.1 - interval.0 : Nat;

                    if (range < smallest_interval_range) {
                        smallest_interval_range := range;
                        smallest_interval_index := index_name;

                        smallest_interval_start := interval.0;
                        smallest_interval_end := interval.1;
                    };
                };
            };

            let full_scan_filter_bounds = Buffer.Buffer<T.Bounds>(full_scan_details_buffer.size());

            for (full_scan_details in full_scan_details_buffer.vals()) {
                full_scan_filter_bounds.add(full_scan_details.filter_bounds);
            };

            let filtered_ids : Iter<(T.DocumentId, ?[(Text, T.Candid)])> = if (smallest_interval_index == "") {
                let document_ids = DocumentStore.keys(collection);
                let filtered = CollectionUtils.multiFilter(
                    collection,
                    document_ids,
                    full_scan_filter_bounds,
                    query_plan.is_and_operation,
                );
                Iter.map<T.DocumentId, (T.DocumentId, ?[(Text, T.Candid)])>(filtered, func(id : T.DocumentId) : (T.DocumentId, ?[(Text, T.Candid)]) { (id, null) });
            } else {
                let index_key_details = Index.get_key_details_by_name(collection, smallest_interval_index);

                let document_ids_with_fields = if (
                    CollectionUtils.can_use_indexed_fields_for_filtering(index_key_details, full_scan_filter_bounds) or
                    (
                        requires_sorting and
                        Option.isSome(
                            Array.find(index_key_details, func((key, _) : (Text, Any)) : Bool { key == Option.get(opt_sort_column, ("", #Ascending)).0 })
                        )
                    )
                ) {
                    Intervals.document_ids_and_indexed_fields_from_intervals(collection, smallest_interval_index, [(smallest_interval_start, smallest_interval_end)], false);
                } else {
                    let document_ids = Intervals.document_ids_from_index_intervals(collection, smallest_interval_index, [(smallest_interval_start, smallest_interval_end)], false);
                    Iter.map<T.DocumentId, (T.DocumentId, ?[(Text, T.Candid)])>(
                        document_ids,
                        func(id : T.DocumentId) : (T.DocumentId, ?[(Text, T.Candid)]) {
                            (id, null);
                        },
                    );

                };

                CollectionUtils.multiFilterWithIndexedFields(collection, document_ids_with_fields, full_scan_filter_bounds, query_plan.is_and_operation);
            };

            if (requires_sorting and scans_sorted_documents_array.size() == 0) {
                // this code block is only reached if there were no sub #Or queries in the AND operation

                assert iterators.size() == 0;
                assert bitmaps.size() == 0;
                assert Map.size(intervals_by_index) == 0;

                // we need to sort the filtered_ids
                // the other document ids loaded into the array were sorted because they were from nested operations
                // however, a full scan is a new operation that is not sorted by default

                let arr = Iter.toArray(filtered_ids);

                if (arr.size() == 0) return #Empty;

                let sorted = MergeSort.sort(arr, sort_documents_by_field_cmp);

                return #Ids(sorted.vals());

            };

            let bitmap = load_bitmap(filtered_ids, opt_last_pagination_document_id);
            bitmaps.add(bitmap);

        };

        if (iterators.size() == 1) {
            assert bitmaps.size() == 0;
            return #Ids(iterators.get(0));
        };

        if (iterators.size() > 1) {
            assert bitmaps.size() == 0;

            let new_size = if (scans_sorted_documents_array.size() > 0) {
                iterators.size() + 1;
            } else {
                iterators.size();
            };

            let iters = Array.tabulate<Iter<(T.DocumentId, ?[(Text, T.Candid)])>>(
                new_size,
                func(i : Nat) : Iter<(T.DocumentId, ?[(Text, T.Candid)])> {
                    if (i == iterators.size()) return scans_sorted_documents_array.vals();
                    iterators.get(i);
                },
            );

            let merged_iterator = Utils.kmerge_and(iters, sort_documents_by_field_cmp);

            return #Ids(merged_iterator);

        };

        if (bitmaps.size() == 0) {
            return #Empty;
        };

        let bitmap = if (bitmaps.size() == 1) {
            bitmaps.get(0);
        } else { SparseBitMap64.multiIntersect(bitmaps.vals()) };

        #BitMap(bitmap);

    };

    public func generate_document_ids_for_or_operation(
        collection : T.StableCollection,
        query_plan : T.QueryPlan,
        opt_sort_column : ?(Text, T.SortDirection),
        opt_last_pagination_document_id : ?T.DocumentId,
        sort_documents_by_field_cmp : ((T.DocumentId, ?[(Text, T.Candid)]), (T.DocumentId, ?[(Text, T.Candid)])) -> Order,
    ) : EvalResult {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("generate_document_ids_for_or_operation");
        assert not query_plan.is_and_operation;
        let requires_sorting = Option.isSome(opt_sort_column);

        let sort_direction = switch (opt_sort_column) {
            case (?(sort_field, sort_direction)) sort_direction;
            case (null) #Ascending;
        };

        let bitmaps = Buffer.Buffer<T.SparseBitMap64>(8);
        let intervals_by_index = Map.new<Text, IndexDetails>();
        let full_scan_details_buffer = Buffer.Buffer<T.FullScanDetails>(8);

        let iterators = Buffer.Buffer<Iter<(T.DocumentId, ?[(Text, T.Candid)])>>(8);

        for (scan_details in query_plan.scans.vals()) switch (scan_details) {
            case (#FullScan(full_scan_details)) {
                full_scan_details_buffer.add(full_scan_details);
            };
            case (#IndexScan({ index_name; filter_bounds; requires_additional_filtering; requires_additional_sorting; sorted_in_reverse; interval; scan_bounds })) {
                if (index_name != C.DOCUMENT_ID) {
                    log.trap("QueryExecution.generate_document_ids_for_or_operation: OR operations should not have index scans on '" # index_name # "' directly, they should be in subplans");
                };

                full_scan_details_buffer.add({
                    filter_bounds;
                    requires_additional_sorting;
                    scan_bounds;
                    requires_additional_filtering;
                });
            };
        };

        for (and_operation_subplan in query_plan.subplans.vals()) {

            let eval_result = generate_document_ids_for_and_operation(collection, and_operation_subplan, opt_sort_column, opt_last_pagination_document_id, sort_documents_by_field_cmp);

            switch (eval_result) {
                case (#Empty) {}; // do nothing if empty set
                case (#Ids(iter)) {
                    if (requires_sorting) {
                        iterators.add(iter);
                    } else {
                        bitmaps.add(
                            load_bitmap(iter, opt_last_pagination_document_id)
                        );
                    };
                };
                case (#BitMap(bitmap)) {
                    if (requires_sorting) {
                        log.trap("QueryExecution.generate_document_ids_for_or_operation: Should only return sorted iterators when sorting is required");
                    };
                    bitmaps.add(bitmap);
                };
                case (#Interval(index_name, intervals, is_reversed)) {
                    for (interval in intervals.vals()) {
                        add_interval(intervals_by_index, index_name, interval, is_reversed);
                    };
                };
            };
        };

        // requires kmerge sorting between intervals of the same index?
        func requires_additional_sorting_between_intervals(
            collection : T.StableCollection,
            index_name : Text,
            intervals : Buffer.Buffer<T.Interval>,
            opt_sort_column : ?(Text, T.SortDirection),
        ) : Bool {
            if (not requires_sorting) return false;
            if (intervals.size() <= 1) return false;

            let key_details = Index.get_key_details_by_name(collection, index_name);

            let sort_field = switch (opt_sort_column) {
                case (?(sort_field, sort_direction)) sort_field;
                case (null) return false;
            };

            let index_key = key_details.get(0).0;

            // we should retrieve the operation the interval was created for to better determine if it requires sorting
            // this is an approximation that works in many cases, it fails and causes unnecessary sorting when:
            // - the values for the prefix fields are the same across intervals
            //   e.g. index on (age, name), sort by name, intervals:   (20, "A" to "C") or (20, "M" to "Z")
            // - the sort field is an equality field in the index key (e.g. index on (age, name), sort by age, intervals: (20, ="A") or (25, ="A") or (30, ="B"))
            // -

            sort_field != index_key;
        };

        // merge overlapping intervals
        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            let should_call_union = not requires_additional_sorting_between_intervals(collection, index_name, interval_details.intervals, opt_sort_column);

            if (should_call_union) {
                Intervals.union(interval_details.intervals);
            };
        };

        log.lazyDebug(
            func() = "Intervals after union: " # debug_show (
                Array.map<(Text, IndexDetails), (Text, [T.Interval])>(
                    Map.toArray(intervals_by_index),
                    func((index_name, details) : (Text, IndexDetails)) : (Text, [T.Interval]) {
                        (index_name, Buffer.toArray(details.intervals));
                    },
                )
            )
        );

        if (
            bitmaps.size() == 0 and
            full_scan_details_buffer.size() == 0 and
            iterators.size() == 0 and
            Map.size(intervals_by_index) <= 1
        ) {
            if (Map.size(intervals_by_index) == 0) return #Empty;

            let ?(index_name, interval_details) = Map.entries(intervals_by_index).next() else {
                log.trap("QueryExecution.generate_document_ids_for_or_operation: No elements in map when size is greater than 0");
            };

            let intervals = Buffer.toArray(interval_details.intervals);

            let should_return_as_interval = not requires_additional_sorting_between_intervals(collection, index_name, interval_details.intervals, opt_sort_column);

            if (should_return_as_interval) {

                let is_reversed = switch (interval_details.sorted_in_reverse) {
                    case (?sorted_in_reverse) sorted_in_reverse;
                    case (null) false;
                };

                let last_pagination_document_rank = switch (opt_last_pagination_document_id) {
                    case (null) return #Interval(index_name, intervals, is_reversed);
                    case (?last_id) {

                        let last_pagination_document : T.CandidMap = CollectionUtils.get_and_cache_candid_map(
                            collection,
                            last_id,
                        );

                        let rank_res = Index.getRankWithCandidMap(
                            collection,
                            index_name,
                            last_id,
                            last_pagination_document,
                        );

                        let rank = switch (rank_res) {
                            case (#ok(rank)) rank;
                            case (#err(msg)) {
                                log.lazyError(
                                    func() = "Failed to get rank for last pagination document ID: " # msg
                                );
                                log.trap("QueryExecution.generate_document_ids_for_or_operation: " # msg);
                            };
                        };

                        rank;

                    };
                };

                log.lazyDebug(
                    func() = "Last pagination document rank: " # debug_show last_pagination_document_rank
                );

                let filtered_intervals = Buffer.Buffer<T.Interval>(8);

                if (is_reversed) {
                    // For reversed order, we want all intervals before the rank
                    for (interval in intervals.vals()) {
                        if (interval.1 > last_pagination_document_rank) {
                            // Interval extends beyond the rank, truncate it
                            if (interval.0 <= last_pagination_document_rank) {
                                filtered_intervals.add((interval.0, last_pagination_document_rank));
                            };
                        } else {
                            // Interval is completely before the rank, include it
                            filtered_intervals.add(interval);
                        };
                    };
                } else {
                    // For normal order, we want all intervals after the rank
                    for (interval in intervals.vals()) {
                        if (interval.0 <= last_pagination_document_rank) {
                            // Interval starts before or at the rank, truncate it
                            if (interval.1 > last_pagination_document_rank + 1) {
                                filtered_intervals.add((last_pagination_document_rank + 1, interval.1));
                            };
                        } else {
                            // Interval is completely after the rank, include it
                            filtered_intervals.add(interval);
                        };
                    };
                };

                log.lazyDebug(
                    func() = "Pagination filtering - is_reversed: " # debug_show is_reversed #
                    ", original intervals: " # debug_show intervals #
                    ", filtered intervals: " # debug_show Buffer.toArray(filtered_intervals)
                );

                if (filtered_intervals.size() == 0) {
                    log.lazyDebug(func() = "No intervals remaining after pagination filtering");
                    return #Empty;
                } else {
                    return #Interval(index_name, Buffer.toArray(filtered_intervals), is_reversed);
                };

            };

            // moves on to the next block to handle multiple intervals that requir sorting

        };

        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {

            for (interval in interval_details.intervals.vals()) {

                let sorted_in_reverse = Option.get(interval_details.sorted_in_reverse, false);

                if (requires_sorting) {
                    let document_ids_with_fields = Intervals.document_ids_and_indexed_fields_from_intervals(collection, index_name, [interval], sorted_in_reverse);
                    iterators.add(document_ids_with_fields);
                } else {
                    let document_ids = Intervals.document_ids_from_index_intervals(collection, index_name, [interval], sorted_in_reverse);
                    let bitmap = load_bitmap(
                        Iter.map<T.DocumentId, (T.DocumentId, ?[(Text, T.Candid)])>(
                            document_ids,
                            func(id : T.DocumentId) : (T.DocumentId, ?[(Text, T.Candid)]) {
                                (id, null);
                            },
                        ),
                        opt_last_pagination_document_id,
                    );
                    bitmaps.add(bitmap);
                };

            };
        };

        if (full_scan_details_buffer.size() > 0) {
            log.lazyDebug(
                func() = "Processing " # Nat.toText(full_scan_details_buffer.size()) #
                " full scan operations in OR operation"
            );

            let full_scan_filter_bounds = Buffer.Buffer<T.Bounds>(full_scan_details_buffer.size());

            for (full_scan_details in full_scan_details_buffer.vals()) {
                full_scan_filter_bounds.add(full_scan_details.filter_bounds);
            };

            let document_ids = Intervals.document_ids_and_indexed_fields_from_intervals(
                collection,
                C.DOCUMENT_ID,
                [(0, DocumentStore.size(collection))],
                false,
            );

            let filtered_ids = CollectionUtils.multiFilterWithIndexedFields(collection, document_ids, full_scan_filter_bounds, query_plan.is_and_operation);

            if (requires_sorting) {
                log.lazyDebug(func() = "Sorting filtered documents for full scan in OR operation");
                log.lazyDebug(
                    func() = "Pagination document: " # debug_show opt_last_pagination_document_id
                );

                let cursor_pagination_filtered_documents = switch (opt_last_pagination_document_id) {
                    case (null) {
                        log.lazyDebug(func() = "No pagination cursor, using all filtered documents");
                        filtered_ids;
                    };
                    case (?last_pagination_document_id) {
                        log.lazyDebug(func() = "Applying cursor pagination to filtered documents");

                        let last_pagination_document : T.CandidMap = CollectionUtils.get_and_cache_candid_map(
                            collection,
                            last_pagination_document_id,
                        );

                        let sort_field = Option.get(opt_sort_column, ("", #Ascending)).0;
                        log.lazyDebug(func() = "Sort field: " # debug_show sort_field);

                        let opt_sort_field_value = CandidMap.get(
                            last_pagination_document,
                            collection.schema_map,
                            sort_field,
                        );

                        log.lazyDebug(
                            func() = "Sort field value for last pagination document: " #
                            debug_show opt_sort_field_value
                        );

                        let bounds = if (sort_direction == #Ascending) {
                            Buffer.fromArray<T.Bounds>([(
                                [
                                    (
                                        sort_field,
                                        Option.map<T.CandidQuery, T.State<T.CandidQuery>>(opt_sort_field_value, func(candid_query) { #Inclusive(candid_query) }),
                                    ),
                                    (C.DOCUMENT_ID, ?#Exclusive(#Blob(last_pagination_document_id))),
                                ],
                                [(sort_field, ?#Inclusive(#Maximum)), (C.DOCUMENT_ID, ?#Inclusive(#Maximum))],
                            )]);
                        } else {
                            Buffer.fromArray<T.Bounds>([(
                                [(sort_field, ?#Inclusive(#Minimum)), (C.DOCUMENT_ID, ?#Inclusive(#Minimum))],
                                [
                                    (
                                        sort_field,
                                        Option.map<T.CandidQuery, T.State<T.CandidQuery>>(opt_sort_field_value, func(candid_query) { #Inclusive(candid_query) }),
                                    ),
                                    (C.DOCUMENT_ID, ?#Exclusive(#Blob(last_pagination_document_id))),
                                ],
                            )]);

                        };

                        CollectionUtils.multiFilterWithIndexedFields(
                            collection,
                            filtered_ids,
                            bounds,
                            query_plan.is_and_operation,
                        );

                    };
                };

                let arr = Iter.toArray(cursor_pagination_filtered_documents);
                let sorted = MergeSort.sort(arr, sort_documents_by_field_cmp);

                iterators.add(sorted.vals());

            } else {
                let bitmap = load_bitmap(filtered_ids, opt_last_pagination_document_id);
                bitmaps.add(bitmap);
            };

        };

        if (requires_sorting) {
            assert bitmaps.size() == 0;

            if (iterators.size() == 0) {
                log.lazyDebug(func() = "No iterators available, returning empty");
                return #Empty;
            };

            log.lazyDebug(
                func() = "Merging " # Nat.toText(iterators.size()) # " sorted iterators using k-way merge"
            );
            var merged_iterators = Utils.kmerge_or<(T.DocumentId, ?[(Text, T.Candid)])>(Buffer.toArray(iterators), sort_documents_by_field_cmp);

            return #Ids(merged_iterators);

        };

        assert iterators.size() == 0;

        if (bitmaps.size() == 0) {
            log.lazyDebug(func() = "No bitmaps to merge, returning empty");
            #Empty;
        } else {
            log.lazyDebug(
                func() = "Performing union on " # Nat.toText(bitmaps.size()) # " bitmaps"
            );
            let bitmap = SparseBitMap64.multiUnion(bitmaps.vals());
            log.lazyDebug(func() = "Union completed, resulting bitmap has " # Nat.toText(SparseBitMap64.size(bitmap)) # " documents");
            #BitMap(bitmap);
        };

    };

    public func generate_document_ids_for_query_plan(
        collection : T.StableCollection,
        { query_plan; opt_last_pagination_document_id } : T.QueryPlanResult,
        opt_sort_column : ?(Text, T.SortDirection),
        sort_documents_by_field_cmp : ((T.DocumentId, ?[(Text, T.Candid)]), (T.DocumentId, ?[(Text, T.Candid)])) -> Order,
    ) : EvalResult {

        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("generate_document_ids_for_query_plan");

        log.lazyInfo(func() = "Generating document IDs for query plan");
        log.logDebug("QueryExecution.generate_document_ids_for_query_plan(): Query plan: " # debug_show query_plan);

        let result = if (query_plan.is_and_operation) {
            generate_document_ids_for_and_operation(collection, query_plan, opt_sort_column, opt_last_pagination_document_id, sort_documents_by_field_cmp);
        } else {
            generate_document_ids_for_or_operation(collection, query_plan, opt_sort_column, opt_last_pagination_document_id, sort_documents_by_field_cmp);
        };

        let elapsed = 0;

        switch (result) {
            case (#Empty) {
                log.lazyInfo(
                    func() = "Query returned empty result in "
                    # debug_show elapsed # " instructions"
                );
            };
            case (#BitMap(bitmap)) {
                log.lazyInfo(
                    func() = "Query returned "
                    # debug_show SparseBitMap64.size(bitmap) # " documents in bitmap in " # debug_show elapsed # " instructions"
                );
            };
            case (#Ids(iter)) {
                log.lazyInfo(
                    func() = "Query returned iterator in "
                    # debug_show elapsed # " instructions"
                );
            };
            case (#Interval(index_name, intervals, is_reversed)) {
                var total = 0;
                for (interval in intervals.vals()) {
                    total += interval.1 - interval.0;
                };

                log.lazyInfo(
                    func() = "Query returned "
                    # debug_show total # " documents from " # debug_show intervals
                    # " intervals on index '" # index_name # "'"
                    # (if (is_reversed) " (reversed order)" else "")
                    # " in " # debug_show elapsed # " instructions"
                );

            };
        };

        result;
    };
};
