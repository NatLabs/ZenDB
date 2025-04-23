import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Result "mo:base/Result";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Hash "mo:base/Hash";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import BitMap "mo:bit-map";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import ByteUtils "../ByteUtils";
import C "../Constants";
import Logger "../Logger";

import Index "Index";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "Utils";
import QueryPlan "QueryPlan";
import Intervals "Intervals";

module {
    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; nhash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    type QueryBuilder = Query.QueryBuilder;

    public type BTreeUtils<K, V> = MemoryBTree.BTreeUtils<K, V>;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type InternalCandify<A> = T.InternalCandify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;

    let DEFAULT_BTREE_ORDER = 256;

    type EvalResult = T.EvalResult;

    type IndexDetails = {
        var sorted_in_reverse : ?Bool;
        intervals : Buffer.Buffer<(Nat, Nat)>;
    };

    // avoids sorting
    public func get_unique_record_ids_from_query_plan(
        collection : T.StableCollection,
        // only accepts bitmaps directly created from an index scan
        bitmap_cache : Map<Text, BitMap.BitMap>,
        query_plan : T.QueryPlan,
    ) : EvalResult {
        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.get_unique_record_ids(): Processing query plan with "
            # debug_show query_plan.scans.size() # " scans and "
            # debug_show query_plan.subplans.size() # " subplans"
            # ", operation type: " # (if (query_plan.is_and_operation) "AND" else "OR"),
        );

        if (query_plan.scans.size() == 1 and query_plan.subplans.size() == 0) {
            switch (query_plan.scans[0]) {
                case (#IndexScan(index_scan_details)) {
                    let {
                        index;
                        requires_additional_filtering;
                        requires_additional_sorting;
                        interval;
                        scan_bounds;
                        filter_bounds;
                    } = index_scan_details;
                    if (not requires_additional_filtering and not requires_additional_sorting) {
                        Logger.lazyLog(
                            collection.logger,
                            func() = "QueryExecution.get_unique_record_ids(): Direct interval access on index '"
                            # index.name # "', interval: " # debug_show interval,
                        );
                        return #Interval(index.name, [interval], index_scan_details.sorted_in_reverse);
                    };
                };
                case (#FullScan({ filter_bounds; requires_additional_filtering; requires_additional_sorting })) {
                    if (not requires_additional_filtering and not requires_additional_sorting) {
                        Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Full scan with no filtering or sorting");
                        return #Interval(C.RECORD_ID_FIELD, [(0, MemoryBTree.size(collection.main))], false);
                    };
                };
            };
        };

        let bitmaps = Buffer.Buffer<T.BitMap>(query_plan.scans.size() + query_plan.subplans.size());
        let intervals_by_index = Map.new<Text, IndexDetails>();

        label evaluating_query_plan for (scan_details in query_plan.scans.vals()) {
            let record_ids_iter = switch (scan_details) {
                case (#FullScan({ filter_bounds; requires_additional_filtering })) {
                    Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Processing full scan");
                    let main_btree_utils = CollectionUtils.get_main_btree_utils();
                    let full_scan_iter = MemoryBTree.keys(collection.main, main_btree_utils);

                    if (requires_additional_filtering) {
                        Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Applying filters to full scan");
                        CollectionUtils.multi_filter(collection, full_scan_iter, Buffer.fromArray([(filter_bounds)]), query_plan.is_and_operation);
                    } else {
                        full_scan_iter;
                    };
                };
                case (#IndexScan(index_scan_details)) {
                    let {
                        index;
                        requires_additional_filtering;
                        interval;
                        filter_bounds;
                    } = index_scan_details;

                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.get_unique_record_ids(): Processing index scan on '" #
                        index.name # "', requires_additional_filtering: " #
                        debug_show requires_additional_filtering,
                    );

                    let index_data_utils = CollectionUtils.get_index_data_utils();

                    if (requires_additional_filtering) {
                        Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Attempting index-based filtering");

                        // index based filtering improves the worst case scenario of filtering intervas
                        // by using intersecting bitmaps with record ids instead of accessing the
                        // values in the main btree and filtering them
                        //
                        // while the improvement are undeniable for full scans,
                        // its not always the case, as loading the other indexes records
                        // into bitmaps can be more expensive than filtering the main btree
                        //
                        // todo - add a heuristic to determine when to use index based filtering
                        // can return the intervals from the indexes and compare them before
                        // loading them into bitmaps

                        // let { intervals_by_index; opt_filter_bounds } = get_index_based_filtering_intervals(collection, filter_bounds, index_scan_details.simple_operations);
                        switch (index_based_interval_filtering(collection, bitmap_cache, index_scan_details)) {
                            case (?{ bitmap; opt_filter_bounds }) {
                                Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Successfully applied index-based filtering");
                                switch (opt_filter_bounds) {
                                    case (?filter_bounds) {
                                        Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Applying additional post-filtering");
                                        CollectionUtils.multi_filter(collection, bitmap.vals(), Buffer.fromArray([filter_bounds]), query_plan.is_and_operation);
                                    };
                                    case (null) {
                                        Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): No additional filtering needed, adding bitmap directly");
                                        bitmaps.add(bitmap);
                                        continue evaluating_query_plan;
                                    };
                                };
                            };
                            case (null) {
                                Logger.lazyLog(
                                    collection.logger,
                                    func() = "QueryExecution.get_unique_record_ids(): Index-based filtering not applicable, falling back to standard approach",
                                );
                                let record_ids = CollectionUtils.record_ids_from_index_intervals(collection, index.name, [interval], false);
                                CollectionUtils.multi_filter(collection, record_ids, Buffer.fromArray([filter_bounds]), query_plan.is_and_operation);
                            };
                        };

                    } else {
                        Logger.lazyLog(
                            collection.logger,
                            func() = "QueryExecution.get_unique_record_ids(): Adding direct interval from index '" #
                            index.name # "': " # debug_show interval,
                        );
                        add_interval(intervals_by_index, index.name, interval, false);
                        continue evaluating_query_plan;
                    };
                };
            };

            Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Creating bitmap from record IDs iterator");
            bitmaps.add(
                BitMap.fromIter(record_ids_iter)
            );
        };

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.get_unique_record_ids(): Processing " #
            Nat.toText(query_plan.subplans.size()) # " subplans",
        );

        for (or_operation_subplan in query_plan.subplans.vals()) {
            Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Recursively processing subplan");
            let eval_result = get_unique_record_ids_from_query_plan(collection, bitmap_cache, or_operation_subplan);

            switch (eval_result) {
                case (#Empty) {
                    Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Subplan returned empty result");
                    if (query_plan.is_and_operation) {
                        Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Early return with empty result due to AND with empty set");
                        return #Empty;
                    };
                };
                case (#Ids(record_ids_iter)) {
                    Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): Subplan returned record IDs iterator");
                    bitmaps.add(
                        BitMap.fromIter(record_ids_iter)
                    );
                };
                case (#BitMap(sub_bitmap)) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.get_unique_record_ids(): Subplan returned bitmap with " #
                        Nat.toText(sub_bitmap.size()) # " records",
                    );
                    bitmaps.add(sub_bitmap);
                };
                case (#Interval(index_name, intervals, is_reversed)) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.get_unique_record_ids(): Subplan returned interval on index '" #
                        index_name # "' with " # Nat.toText(intervals.size()) # " ranges",
                    );
                    add_interval(intervals_by_index, index_name, intervals.get(0), is_reversed);
                };
            };
        };

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.get_unique_record_ids(): Processing " #
            Nat.toText(Map.size(intervals_by_index)) # " interval sets from different indexes",
        );

        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.get_unique_record_ids(): Processing intervals for index '" #
                index_name # "' with " # Nat.toText(interval_details.intervals.size()) # " intervals",
            );

            if (query_plan.is_and_operation) {
                switch (Intervals.intervals_intersect(interval_details.intervals)) {
                    case (?interval) {
                        Logger.lazyLog(
                            collection.logger,
                            func() = "QueryExecution.get_unique_record_ids(): Intersected " #
                            Nat.toText(interval_details.intervals.size()) #
                            " intervals to single interval " # debug_show interval,
                        );
                        interval_details.intervals.clear();
                        interval_details.intervals.add(interval);
                    };
                    case (null) {
                        Logger.lazyLog(
                            collection.logger,
                            func() = "QueryExecution.get_unique_record_ids(): Intervals have empty intersection for index '" #
                            index_name # "', removing from consideration",
                        );
                        ignore Map.remove(intervals_by_index, thash, index_name);
                    };
                };
            } else {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Merging overlapping intervals for index '" # index_name # "'",
                );
                Intervals.intervals_union(interval_details.intervals);
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): After union operation, index '" # index_name #
                    "' has " # Nat.toText(interval_details.intervals.size()) # " intervals",
                );
            };
        };

        if (Map.size(intervals_by_index) > 1) {
            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.get_unique_record_ids(): Converting " #
                Nat.toText(Map.size(intervals_by_index)) # " index intervals to bitmaps",
            );

            for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
                let ?index = Map.get(collection.indexes, thash, index_name) else {
                    Logger.lazyError(
                        collection.logger,
                        func() = "QueryExecution.get_unique_record_ids(): Index not found: " # index_name,
                    );
                    Debug.trap("Unreachable: IndexMap not found for index: " # index_name);
                };

                if (query_plan.is_and_operation) {
                    assert interval_details.intervals.size() == 1;
                };

                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Creating bitmap from intervals on index '" #
                    index_name # "'",
                );

                let bitmap = BitMap.BitMap(1024);

                for (interval in interval_details.intervals.vals()) {
                    let record_ids = CollectionUtils.record_ids_from_index_intervals(collection, index.name, [interval], false);

                    for (id in record_ids) {
                        bitmap.set(id, true);
                    };
                };

                bitmaps.add(bitmap);
            };
        };

        let result = if (bitmaps.size() == 0 and Map.size(intervals_by_index) == 1) {
            let ?(index_name, interval_details) = Map.entries(intervals_by_index).next() else {
                Logger.lazyError(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): No elements in map when size is greater than 0",
                );
                Debug.trap("No elements in map when size is greater than 0");
            };

            let sorted_in_reverse = switch (interval_details.sorted_in_reverse) {
                case (?sorted_in_reverse) sorted_in_reverse;
                case (null) false;
            };

            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.get_unique_record_ids(): Using direct interval result from index '" #
                index_name # "' with " # Nat.toText(interval_details.intervals.size()) # " intervals",
            );

            #Interval(index_name, Buffer.toArray(interval_details.intervals), sorted_in_reverse);
        } else if (bitmaps.size() == 0) {
            Logger.lazyLog(collection.logger, func() = "QueryExecution.get_unique_record_ids(): No results match the query");
            #Empty;
        } else {
            if (bitmaps.size() == 1) {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Using single bitmap with " #
                    Nat.toText(bitmaps.get(0).size()) # " records",
                );
                #BitMap(bitmaps.get(0));
            } else if (query_plan.is_and_operation) {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Intersecting " #
                    Nat.toText(bitmaps.size()) # " bitmaps for AND operation",
                );
                #BitMap(BitMap.multiIntersect(bitmaps.vals()));
            } else {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Merging " #
                    Nat.toText(bitmaps.size()) # " bitmaps for OR operation",
                );
                #BitMap(BitMap.multiUnion(bitmaps.vals()));
            };
        };

        let elapsed = 0;

        switch (result) {
            case (#Empty) {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Query returned empty result set in "
                    # debug_show elapsed # " instructions",
                );
            };
            case (#BitMap(bitmap)) {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Query returned bitmap with "
                    # debug_show bitmap.size() # " records in " # debug_show elapsed # " instructions",
                );
            };
            case (#Ids(iter)) {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Query returned records iterator in "
                    # debug_show elapsed # " instructions",
                );
            };
            case (#Interval(index_name, intervals, _)) {
                var total_size = 0;
                for (interval in intervals.vals()) {
                    total_size += interval.1 - interval.0;
                };
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.get_unique_record_ids(): Query returned intervals on index '"
                    # index_name # "' with " # debug_show intervals.size() # " intervals containing "
                    # debug_show total_size # " records in " # debug_show elapsed # " instructions",
                );
            };
        };

        result;
    };

    public func add_interval(intervals_by_index : Map<Text, IndexDetails>, index : Text, interval : (Nat, Nat), is_reversed : Bool) {
        let details = switch (Map.get(intervals_by_index, thash, index)) {
            case (?details) {
                switch (details.sorted_in_reverse) {
                    case (?sorted_in_reverse) {
                        if (sorted_in_reverse != is_reversed) {
                            Debug.trap("Inconsistent sorted_in_reverse values");
                        };
                    };
                    case (null) {
                        details.sorted_in_reverse := ?is_reversed;
                    };
                };
                details;
            };
            case (null) {
                let buffer = Buffer.Buffer<(Nat, Nat)>(8);

                let details : IndexDetails = {
                    var sorted_in_reverse = ?is_reversed;
                    intervals = buffer;
                };

                ignore Map.put(intervals_by_index, thash, index, details);
                details;
            };
        };

        details.intervals.add(interval);
    };

    type IndexIntervalFilterDetails = {
        intervals_map : Map<Text, Buffer.Buffer<(Nat, Nat)>>;
        opt_filter_bounds : ?T.Bounds;
    };

    public func get_index_based_filtering_intervals(collection : T.StableCollection, filter_bounds : T.Bounds, operations : [(Text, T.ZqlOperators)]) : IndexIntervalFilterDetails {
        Logger.lazyLog(collection.logger, func() = "QueryExecution.get_index_based_filtering_intervals(): Finding best indexes for filtering");

        var prev = filter_bounds;
        var curr = filter_bounds;

        let intervals_map = Map.new<Text, Buffer.Buffer<(Nat, Nat)>>();

        loop {
            let fields = Set.new<Text>();

            for ((field, _) in curr.0.vals()) {
                Set.add(fields, thash, field);
            };

            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.get_index_based_filtering_intervals(): Processing " #
                Nat.toText(Set.size(fields)) # " unique fields",
            );

            let filter_operations = Buffer.Buffer<(Text, T.ZqlOperators)>(8);

            for ((field, value) in operations.vals()) {
                if (Set.has(fields, thash, field)) {
                    filter_operations.add(field, value);
                };
            };

            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.get_index_based_filtering_intervals(): Found " #
                Nat.toText(filter_operations.size()) # " applicable filter operations",
            );

            let {
                index;
                fully_covered_equality_and_range_fields;
            } = switch (Index.get_best_index(collection, Buffer.toArray(filter_operations), null)) {
                case (null) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.get_index_based_filtering_intervals(): No suitable index found for filtering",
                    );
                    return {
                        intervals_map;
                        opt_filter_bounds = ?curr;
                    };
                };
                case (?best_index_details) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.get_index_based_filtering_intervals(): Selected index '" #
                        best_index_details.index.name # "' for filtering",
                    );
                    best_index_details;
                };
            };

            let lower_map = Map.new<Text, T.CandidInclusivityQuery>();

            for ((field, opt_state) in curr.0.vals()) {
                switch (opt_state) {
                    case (?state) {
                        ignore Map.put(lower_map, thash, field, state);
                    };
                    case (null) {};
                };
            };

            let upper_map = Map.new<Text, T.CandidInclusivityQuery>();

            for ((field, opt_state) in curr.1.vals()) {
                switch (opt_state) {
                    case (?state) {
                        ignore Map.put(upper_map, thash, field, state);
                    };
                    case (null) {};
                };
            };

            let (scan_bounds, filter_bounds) = Index.extract_scan_and_filter_bounds(lower_map, upper_map, ?index.key_details, ?fully_covered_equality_and_range_fields);

            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.get_index_based_filtering_intervals(): Extracted scan bounds for index '" #
                index.name # "'",
            );

            let interval = Index.scan(collection, index, scan_bounds.0, scan_bounds.1, null);

            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.get_index_based_filtering_intervals(): Generated interval " #
                debug_show interval # " for index '" # index.name # "'",
            );

            switch (Map.get(intervals_map, thash, index.name)) {
                case (?intervals) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.get_index_based_filtering_intervals(): Adding interval to existing set for index '" #
                        index.name # "'",
                    );
                    intervals.add(interval);
                };
                case (null) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.get_index_based_filtering_intervals(): Creating new interval set for index '" #
                        index.name # "'",
                    );
                    ignore Map.put(intervals_map, thash, index.name, Buffer.fromArray<(Nat, Nat)>([interval]));
                };
            };

            prev := curr;
            curr := filter_bounds;

            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.get_index_based_filtering_intervals(): Filter bounds narrowed from " #
                Nat.toText(prev.0.size()) # " to " # Nat.toText(curr.0.size()) # " lower bounds",
            );

        } while (prev.0.size() > curr.0.size() and curr.0.size() > 0);

        let result = {
            intervals_map;
            opt_filter_bounds = if (curr.0.size() == 0) ?curr else null;
        };

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.get_index_based_filtering_intervals(): Completed with " #
            Nat.toText(Map.size(intervals_map)) # " index interval sets and " #
            (if (Option.isSome(result.opt_filter_bounds)) "additional" else "no additional") #
            " filter bounds",
        );

        result;
    };

    func retrieve_all_index_interval_iterators(
        collection : T.StableCollection,
        index_intervals : Map<Text, Buffer.Buffer<(Nat, Nat)>>,
        sorted_in_reverse : Bool,
        combine_intervals_in_same_index : Bool,
    ) : Buffer<Iter<Nat>> {
        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.retrieve_all_index_interval_iterators(): Retrieving iterators for " #
            Nat.toText(Map.size(index_intervals)) # " index interval sets, combine_intervals=" #
            debug_show combine_intervals_in_same_index,
        );

        let iterators = Buffer.Buffer<Iter<Nat>>(8);

        for ((index_name, intervals) in Map.entries(index_intervals)) {
            if (combine_intervals_in_same_index) {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.retrieve_all_index_interval_iterators(): Retrieving combined record IDs for " #
                    Nat.toText(intervals.size()) # " intervals on index '" # index_name # "'",
                );

                let record_ids = CollectionUtils.record_ids_from_index_intervals(collection, index_name, Buffer.toArray(intervals), sorted_in_reverse);
                iterators.add(record_ids);

            } else {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.retrieve_all_index_interval_iterators(): Retrieving separate iterators for " #
                    Nat.toText(intervals.size()) # " intervals on index '" # index_name # "'",
                );

                for (interval in intervals.vals()) {
                    let record_ids = CollectionUtils.record_ids_from_index_intervals(collection, index_name, [interval], sorted_in_reverse);
                    iterators.add(record_ids);
                };
            };
        };

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.retrieve_all_index_interval_iterators(): Created " #
            Nat.toText(iterators.size()) # " iterators",
        );

        iterators;
    };

    type IndexBasedFilteringResult = {
        bitmap : BitMap.BitMap;
        opt_filter_bounds : ?T.Bounds;
    };

    public func index_based_interval_filtering(
        collection : T.StableCollection,
        bitmap_cache : Map<Text, BitMap.BitMap>,
        index_scan_details : T.IndexScanDetails,
    ) : ?IndexBasedFilteringResult {
        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.index_based_interval_filtering(): Evaluating index-based filtering options",
        );

        let {
            index;
            interval;
            filter_bounds;
            simple_operations = operations;
        } = index_scan_details;

        let original_interval_count = interval.1 - interval.0;

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.index_based_interval_filtering(): Original interval has " #
            Nat.toText(original_interval_count) # " records",
        );

        let { intervals_map; opt_filter_bounds } = get_index_based_filtering_intervals(collection, filter_bounds, operations);

        var filtering_intervals_count = 0;

        for ((index_name, intervals) in Map.entries(intervals_map)) {
            let cnt = Intervals.count(intervals);

            if (cnt > filtering_intervals_count) {
                filtering_intervals_count := cnt;
            };
        };

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.index_based_interval_filtering(): Max filtering interval count: " #
            Nat.toText(filtering_intervals_count) # ", original interval count: " #
            Nat.toText(original_interval_count),
        );

        if (filtering_intervals_count > (original_interval_count * 10)) {
            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.index_based_interval_filtering(): Filtering intervals too large compared to original, " #
                "falling back to standard filtering approach",
            );
            return null;
        };

        switch (Map.get(intervals_map, thash, index.name)) {
            case (?intervals) {
                Logger.lazyError(
                    collection.logger,
                    func() = "QueryExecution.index_based_interval_filtering(): Filtering index same as scanning index: " #
                    index.name,
                );
                Debug.trap("QueryExecution.index_based_interval_filtering: this is interesting, why would the filtering index be the same as the scanning index?");
                intervals.add(interval);
            };
            case (null) {
                Logger.lazyLog(
                    collection.logger,
                    func() = "QueryExecution.index_based_interval_filtering(): Adding original scan interval for index '" #
                    index.name # "'",
                );

                ignore Map.put(
                    intervals_map,
                    thash,
                    index.name,
                    Buffer.fromArray<(Nat, Nat)>([interval]),
                );
            };
        };

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.index_based_interval_filtering(): Intersecting intervals across all indexes",
        );

        for ((index_name, intervals) in Map.entries(intervals_map)) {
            switch (Intervals.intervals_intersect(intervals)) {
                case (?interval) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.index_based_interval_filtering(): Intervals for index '" #
                        index_name # "' intersect to " # debug_show interval,
                    );
                    intervals.clear();
                    intervals.add(interval);
                };
                case (null) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.index_based_interval_filtering(): Intervals for index '" #
                        index_name # "' have empty intersection, removing index",
                    );
                    ignore Map.remove(intervals_map, thash, index_name);
                };
            };
        };

        let bitmaps = Buffer.Buffer<T.BitMap>(8);

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.index_based_interval_filtering(): Creating bitmaps from " #
            Nat.toText(Map.size(intervals_map)) # " index interval sets",
        );

        for ((index_name, intervals) in Map.entries(intervals_map)) {
            let interval = intervals.get(0);
            let interval_cache_key = index_name # debug_show (Buffer.toArray(intervals));

            Logger.lazyLog(
                collection.logger,
                func() = "QueryExecution.index_based_interval_filtering(): Processing interval for index '" #
                index_name # "'",
            );

            let bitmap = switch (Map.get(bitmap_cache, thash, interval_cache_key)) {
                case (?bitmap) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.index_based_interval_filtering(): Using cached bitmap for interval",
                    );
                    bitmap;
                };
                case (null) {
                    Logger.lazyLog(
                        collection.logger,
                        func() = "QueryExecution.index_based_interval_filtering(): Creating new bitmap for interval",
                    );

                    let record_ids = CollectionUtils.record_ids_from_index_intervals(collection, index_name, [interval], false);
                    let bitmap = BitMap.fromIter(record_ids);

                    ignore Map.put(bitmap_cache, thash, interval_cache_key, bitmap);
                    bitmap;
                };
            };

            bitmaps.add(bitmap);
        };

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.index_based_interval_filtering(): Intersecting " #
            Nat.toText(bitmaps.size()) # " bitmaps",
        );

        let bitmap = BitMap.multiIntersect(bitmaps.vals());

        Logger.lazyLog(
            collection.logger,
            func() = "QueryExecution.index_based_interval_filtering(): Final bitmap contains " #
            Nat.toText(bitmap.size()) # " record IDs",
        );

        ?{ bitmap; opt_filter_bounds };
    };

    public func generate_record_ids_for_query_plan_with_and_operation(
        collection : T.StableCollection,
        query_plan : T.QueryPlan,
        opt_sort_column : ?(Text, T.SortDirection),
        sort_records_by_field_cmp : (Nat, Nat) -> Order,
    ) : EvalResult {
        assert query_plan.is_and_operation;
        let requires_sorting = Option.isSome(opt_sort_column);

        if (query_plan.scans.size() == 1 and query_plan.subplans.size() == 0) {

            switch (query_plan.scans[0]) {
                case (#IndexScan({ requires_additional_sorting; requires_additional_filtering; interval; index; sorted_in_reverse })) {
                    if (not requires_additional_sorting and not requires_additional_filtering) {
                        return #Interval(index.name, [interval], sorted_in_reverse);
                    };
                };
                case (#FullScan({ requires_additional_sorting; requires_additional_filtering })) {
                    if (not requires_additional_sorting and not requires_additional_filtering) {
                        return #Interval(C.RECORD_ID_FIELD, [(0, MemoryBTree.size(collection.main))], false);
                    };

                };
            };
        };

        let iterators = Buffer.Buffer<Iter<Nat>>(8);
        let sorted_records_from_iter = Buffer.Buffer<Nat>(8);
        let intervals_by_index = Map.new<Text, IndexDetails>();
        let full_scan_details_buffer = Buffer.Buffer<T.FullScanDetails>(8);
        let bitmaps = Buffer.Buffer<T.BitMap>(8);

        for (scan_details in query_plan.scans.vals()) switch (scan_details) {
            case (#FullScan(full_scan_details)) {
                full_scan_details_buffer.add(full_scan_details);
            };
            case (#IndexScan(index_scan_details)) {

                let {
                    index;
                    requires_additional_filtering;
                    requires_additional_sorting;
                    sorted_in_reverse;
                    interval;
                    scan_bounds;
                    filter_bounds;
                } = index_scan_details;

                if (requires_additional_sorting or requires_additional_filtering) {

                    var record_ids : Iter<Nat> = CollectionUtils.record_ids_from_index_intervals(collection, index.name, [interval], sorted_in_reverse);

                    if (requires_additional_filtering) {
                        record_ids := CollectionUtils.multi_filter(collection, record_ids, Buffer.fromArray([filter_bounds]), query_plan.is_and_operation);
                    };

                    if (requires_additional_sorting) {
                        if (sorted_records_from_iter.size() == 0) {
                            Utils.buffer_add_all(sorted_records_from_iter, record_ids);

                            sorted_records_from_iter.sort(sort_records_by_field_cmp);
                            record_ids := sorted_records_from_iter.vals();
                        };

                    };

                    iterators.add(record_ids);

                } else {
                    add_interval(intervals_by_index, index.name, interval, sorted_in_reverse);
                };
            };
        };

        for (or_operation_subplan in query_plan.subplans.vals()) {
            let eval_result = generate_record_ids_for_query_plan_with_or_operation(collection, or_operation_subplan, opt_sort_column, sort_records_by_field_cmp);

            switch (eval_result) {
                case (#Empty) return #Empty; // return early if we encounter an empty set
                case (#Ids(iter)) {
                    if (requires_sorting) {
                        iterators.add(iter);
                    } else {
                        let bitmap = BitMap.fromIter(iter);
                        bitmaps.add(bitmap);
                    };
                };
                case (#BitMap(bitmap)) {
                    if (requires_sorting) Debug.trap("Should only return sorted iterators when sorting is required");

                    bitmaps.add(bitmap);
                };
                case (#Interval(index, intervals, is_reversed)) {
                    for (interval in intervals.vals()) {
                        add_interval(intervals_by_index, index, interval, is_reversed);
                    };
                };
            };

        };

        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            switch (Intervals.intervals_intersect(interval_details.intervals)) {
                case (?interval) {
                    interval_details.intervals.clear();
                    interval_details.intervals.add(interval);
                };
                case (null) ignore Map.remove(intervals_by_index, thash, index_name);
            };
        };

        // Debug.print("intervals_by_index: " # debug_show Map.size(intervals_by_index));
        // Debug.print("iterators: " # debug_show iterators.size());
        // Debug.print("full_scan_details_buffer: " # debug_show full_scan_details_buffer.size());
        // Debug.print("bitmaps: " # debug_show bitmaps.size());
        // Debug.print("sorted_records_from_iter: " # debug_show sorted_records_from_iter.size());

        if (bitmaps.size() == 0 and full_scan_details_buffer.size() == 0 and iterators.size() == 0 and Map.size(intervals_by_index) <= 1) {

            let merged_results : EvalResult = if (Map.size(intervals_by_index) == 1) {
                let ?(index_name, interval_details) = Map.entries(intervals_by_index).next() else Debug.trap("No elements in map when size is greater than 0");
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

        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            let interval = interval_details.intervals.get(0);
            let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);

            let sorted_in_reverse = Option.get(interval_details.sorted_in_reverse, false);

            let record_ids = CollectionUtils.record_ids_from_index_intervals(collection, index.name, [interval], sorted_in_reverse);

            if (requires_sorting and sorted_records_from_iter.size() == 0) {

                for (id in record_ids) {
                    sorted_records_from_iter.add(id);
                };

                if (sorted_records_from_iter.size() == 0) return #Empty;

                iterators.add(sorted_records_from_iter.vals());

            } else {
                let bitmap = BitMap.fromIter(record_ids);
                bitmaps.add(bitmap);

            };

        };

        // ! - feature: reduce full scan range by only scanning the intersection with the smallest interval range
        /**
        var smallest_interval_start = 0;
        var smallest_interval_end = 2 ** 64;

        var index_with_smallest_interval_range = "";
                    */

        if (full_scan_details_buffer.size() > 0) {

            var smallest_interval_index = "";
            var smallest_interval_start = 0;
            var smallest_interval_end = 0;

            if (Map.size(intervals_by_index) > 0) {

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

            let filtered_ids = if (smallest_interval_index == "") {
                let main_btree_utils = CollectionUtils.get_main_btree_utils();
                let filtered_ids = CollectionUtils.multi_filter(
                    collection,
                    MemoryBTree.keys(collection.main, main_btree_utils),
                    full_scan_filter_bounds,
                    query_plan.is_and_operation,
                );
            } else {
                let record_ids_in_interval = CollectionUtils.record_ids_from_index_intervals(collection, smallest_interval_index, [(smallest_interval_start, smallest_interval_end)], false);

                let filtered_ids = CollectionUtils.multi_filter(collection, record_ids_in_interval, full_scan_filter_bounds, query_plan.is_and_operation);
            };

            if (requires_sorting and sorted_records_from_iter.size() == 0) {
                assert iterators.size() == 0;
                assert bitmaps.size() == 0;
                assert Map.size(intervals_by_index) == 0;

                // we need to sort the filtered_ids
                // the other record ids loaded into the buffer were sorted because they were from nested operations
                // however, a full scan is a new operation that is not sorted by default

                for (id in filtered_ids) {
                    sorted_records_from_iter.add(id);
                };

                if (sorted_records_from_iter.size() == 0) return #Empty;

                sorted_records_from_iter.sort(sort_records_by_field_cmp);

                return #Ids(sorted_records_from_iter.vals());

            };

            // Debug.print("added full scan bounds to bitmaps");
            // Debug.print("too bad it requires sorting: " # debug_show requires_sorting);
            // Debug.print("query_plan.subplans.size() : " # debug_show query_plan.subplans.size());
            // Debug.print("query_plan.scans.size() : " # debug_show query_plan.scans.size());

            let bitmap = BitMap.fromIter(filtered_ids);
            bitmaps.add(bitmap);

        };

        if (iterators.size() == 1) {
            return #Ids(iterators.get(0));
        };

        if (iterators.size() > 1) {
            var fill_sorted_records_from_iter = if (sorted_records_from_iter.size() > 0) {
                false;
            } else { true };

            for (_iter in iterators.vals()) {
                let iter = if (fill_sorted_records_from_iter) {
                    for (id in _iter) {
                        sorted_records_from_iter.add(id);
                    };
                    sorted_records_from_iter.vals();
                } else { _iter };

                let bitmap = BitMap.fromIter(iter);
                bitmaps.add(bitmap);

                fill_sorted_records_from_iter := false;
            };

        };

        if (bitmaps.size() == 0) {
            return #Empty;
        };

        let bitmap = if (bitmaps.size() == 1) {
            bitmaps.get(0);
        } else { BitMap.multiIntersect(bitmaps.vals()) };

        if (sorted_records_from_iter.size() > 0) {
            let sorted_bitmap_vals = Iter.filter<Nat>(
                sorted_records_from_iter.vals(),
                func(id : Nat) : Bool = bitmap.get(id),
            );

            #Ids(sorted_bitmap_vals);

        } else {
            #BitMap(bitmap);
        };

    };

    public func generate_record_ids_for_query_plan_with_or_operation(
        collection : T.StableCollection,
        query_plan : T.QueryPlan,
        opt_sort_column : ?(Text, T.SortDirection),
        sort_records_by_field_cmp : (Nat, Nat) -> Order,
    ) : EvalResult {
        assert not query_plan.is_and_operation;
        let requires_sorting = Option.isSome(opt_sort_column);

        let bitmaps = Buffer.Buffer<T.BitMap>(8);
        let intervals_by_index = Map.new<Text, IndexDetails>();

        let iterators = Buffer.Buffer<Iter<Nat>>(8);
        let full_scan_details_buffer = Buffer.Buffer<T.FullScanDetails>(8);

        for (scan_details in query_plan.scans.vals()) switch (scan_details) {
            case (#FullScan(full_scan_details)) {
                full_scan_details_buffer.add(full_scan_details);
            };
            case (#IndexScan(index_scan_details)) {

                let {
                    index;
                    requires_additional_filtering;
                    requires_additional_sorting;
                    sorted_in_reverse;
                    interval;
                    scan_bounds;
                    filter_bounds;
                } = index_scan_details;

                if (not requires_additional_filtering and not requires_additional_sorting) {
                    add_interval(intervals_by_index, index.name, interval, sorted_in_reverse);

                } else {
                    var record_ids : Iter<Nat> = CollectionUtils.record_ids_from_index_intervals(collection, index.name, [interval], sorted_in_reverse);

                    if (requires_additional_filtering) {
                        record_ids := CollectionUtils.multi_filter(collection, record_ids, Buffer.fromArray([filter_bounds]), query_plan.is_and_operation);
                    };

                    if (requires_additional_sorting) {

                        let buffer = Buffer.Buffer<Nat>(8);

                        for (id in record_ids) {
                            buffer.add(id);
                        };

                        buffer.sort(sort_records_by_field_cmp);
                        record_ids := buffer.vals();

                    };

                    iterators.add(record_ids);

                };

            };
        };

        for (and_operation_subplan in query_plan.subplans.vals()) {
            let eval_result = generate_record_ids_for_query_plan_with_and_operation(collection, and_operation_subplan, opt_sort_column, sort_records_by_field_cmp);

            switch (eval_result) {
                case (#Empty) {}; // do nothing if empty set
                case (#Ids(iter)) {
                    if (requires_sorting) {
                        iterators.add(iter);
                    } else {
                        let bitmap = BitMap.fromIter(iter);
                        bitmaps.add(bitmap);
                    };
                };
                case (#BitMap(bitmap)) {
                    if (requires_sorting) Debug.trap("Should only return sorted iterators when sorting is required");
                    bitmaps.add(bitmap);
                };
                case (#Interval(index, intervals, is_reversed)) {
                    for (interval in intervals.vals()) {
                        add_interval(intervals_by_index, index, interval, is_reversed);
                    };
                };
            };
        };

        func requires_additional_sorting_between_intervals(
            collection : T.StableCollection,
            index_name : Text,
            intervals : Buffer.Buffer<(Nat, Nat)>,
            opt_sort_column : ?(Text, T.SortDirection),
        ) : Bool {
            if (intervals.size() <= 1) return false;

            let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);

            let sort_field = switch (opt_sort_column) {
                case (?(sort_field, sort_direction)) sort_field;
                case (null) return false;
            };

            let index_key = index.key_details.get(0).0;

            sort_field != index_key;
        };

        // merge overlapping intervals
        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            let should_call_union = not requires_additional_sorting_between_intervals(collection, index_name, interval_details.intervals, opt_sort_column);

            if (should_call_union) {
                Intervals.intervals_union(interval_details.intervals);
            };
        };

        if (bitmaps.size() == 0 and full_scan_details_buffer.size() == 0 and iterators.size() == 0 and Map.size(intervals_by_index) <= 1) {
            if (Map.size(intervals_by_index) == 0) return #Empty;

            let ?(index_name, interval_details) = Map.entries(intervals_by_index).next() else Debug.trap("No elements in map when size is greater than 0");

            let intervals = Buffer.toArray(interval_details.intervals);

            let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);

            let should_return_as_interval = not requires_additional_sorting_between_intervals(collection, index_name, interval_details.intervals, opt_sort_column);

            if (should_return_as_interval) {

                let is_reversed = switch (interval_details.sorted_in_reverse) {
                    case (?sorted_in_reverse) sorted_in_reverse;
                    case (null) false;
                };

                return #Interval(index_name, intervals, is_reversed);
            };

            // moves on to the next block to handle multiple intervals that require sorting

        };

        for ((index_name, interval_details) in Map.entries(intervals_by_index)) {
            let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);
            let index_data_utils = CollectionUtils.get_index_data_utils();

            for (interval in interval_details.intervals.vals()) {

                let sorted_in_reverse = Option.get(interval_details.sorted_in_reverse, false);

                let record_ids = CollectionUtils.record_ids_from_index_intervals(collection, index_name, [interval], sorted_in_reverse);

                if (requires_sorting) {
                    iterators.add(record_ids);
                } else {
                    let bitmap = BitMap.fromIter(record_ids);
                    bitmaps.add(bitmap);
                };

            };
        };

        if (full_scan_details_buffer.size() > 0) {

            let full_scan_filter_bounds = Buffer.Buffer<T.Bounds>(full_scan_details_buffer.size());

            for (full_scan_details in full_scan_details_buffer.vals()) {
                full_scan_filter_bounds.add(full_scan_details.filter_bounds);
            };

            let main_btree_utils = CollectionUtils.get_main_btree_utils();
            let record_ids = MemoryBTree.keys(collection.main, main_btree_utils);
            let filtered_ids = CollectionUtils.multi_filter(collection, record_ids, full_scan_filter_bounds, query_plan.is_and_operation);

            if (requires_sorting) {
                let buffer = Buffer.Buffer<Nat>(8);
                for (id in filtered_ids) {
                    buffer.add(id);
                };

                buffer.sort(sort_records_by_field_cmp);
                iterators.add(buffer.vals());

            } else {
                let bitmap = BitMap.fromIter(filtered_ids);
                bitmaps.add(bitmap);
            };

        };

        func deduplicate_record_ids_iter(
            record_ids_iter : Iter<Nat>
        ) : Iter<Nat> {
            let dedup_bitmap = BitMap.BitMap(1024);

            object {
                public func next() : ?Nat {
                    loop switch (record_ids_iter.next()) {
                        case (null) return null;
                        case (?id) {
                            if (not dedup_bitmap.get(id)) {
                                dedup_bitmap.set(id, true);
                                return ?id;
                            };
                        };
                    };
                };
            };
        };

        if (requires_sorting) {
            assert bitmaps.size() == 0;

            if (iterators.size() == 0) return #Empty;

            let merged_iterators = Itertools.kmerge<Nat>(Buffer.toArray(iterators), sort_records_by_field_cmp);

            let deduped_iter = deduplicate_record_ids_iter(merged_iterators);

            return #Ids(deduped_iter);

        };

        assert iterators.size() == 0;

        if (bitmaps.size() == 0) {
            #Empty;
        } else {
            let bitmap = BitMap.multiUnion(bitmaps.vals());
            #BitMap(bitmap);
        };

    };

    public func generate_record_ids_for_query_plan(
        collection : StableCollection,
        query_plan : T.QueryPlan,
        opt_sort_column : ?(Text, T.SortDirection),
        sort_records_by_field_cmp : (Nat, Nat) -> Order,
    ) : EvalResult {
        Logger.lazyInfo(collection.logger, func() = "QueryExecution.generate_record_ids_for_query_plan(): Generating record IDs for query plan");

        let result = if (query_plan.is_and_operation) {
            generate_record_ids_for_query_plan_with_and_operation(collection, query_plan, opt_sort_column, sort_records_by_field_cmp);
        } else {
            generate_record_ids_for_query_plan_with_or_operation(collection, query_plan, opt_sort_column, sort_records_by_field_cmp);
        };

        let elapsed = 0;

        switch (result) {
            case (#Empty) {
                Logger.lazyInfo(
                    collection.logger,
                    func() = "QueryExecution.generate_record_ids_for_query_plan(): Query returned empty result in "
                    # debug_show elapsed # " instructions",
                );
            };
            case (#BitMap(bitmap)) {
                Logger.lazyInfo(
                    collection.logger,
                    func() = "QueryExecution.generate_record_ids_for_query_plan(): Query returned "
                    # debug_show bitmap.size() # " records in bitmap in " # debug_show elapsed # " instructions",
                );
            };
            case (#Ids(iter)) {
                Logger.lazyInfo(
                    collection.logger,
                    func() = "QueryExecution.generate_record_ids_for_query_plan(): Query returned iterator in "
                    # debug_show elapsed # " instructions",
                );
            };
            case (#Interval(index_name, intervals, is_reversed)) {
                var total = 0;
                for (interval in intervals.vals()) {
                    total += interval.1 - interval.0;
                };
                Logger.lazyInfo(
                    collection.logger,
                    func() = "QueryExecution.generate_record_ids_for_query_plan(): Query returned "
                    # debug_show total # " records from " # debug_show intervals.size()
                    # " intervals on index '" # index_name # "'"
                    # (if (is_reversed) " (reversed order)" else "")
                    # " in " # debug_show elapsed # " instructions",
                );
            };
        };

        result;
    };
};
