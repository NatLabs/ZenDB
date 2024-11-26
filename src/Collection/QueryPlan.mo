import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Option "mo:base/Option";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Order "mo:base/Order";
import Text "mo:base/Text";

import Candid "mo:serde/Candid";
import Map "mo:map/Map";
import Set "mo:map/Set";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "../Types";
import CandidMap "../CandidMap";
import Utils "../Utils";

import Index "Index";
import CollectionUtils "Utils";
import C "Constants";
import Schema "Schema";

module {

    public type ZenQueryLang = T.ZenQueryLang;

    public type ScanDetails = T.ScanDetails;

    public type QueryPlan = T.QueryPlan;

    let { nhash; thash } = Map;
    type Map<A, B> = Map.Map<A, B>;
    type Buffer<A> = Buffer.Buffer<A>;
    type Iter<A> = Iter.Iter<A>;
    type State<A> = T.State<A>;
    type Candid = T.Candid;
    type Bounds = T.Bounds;
    type SortDirection = T.SortDirection;
    type FieldLimit = T.FieldLimit;
    type Index = T.Index;
    type Order = Order.Order;

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
                ignore Map.put(lower, thash, field, #True(candid));
            };
            case (#lte(candid)) {
                ignore Map.put(upper, thash, field, #True(candid));
            };
            case (#lt(candid)) {
                ignore Map.put(upper, thash, field, #False(candid));
            };
            case (#gt(candid)) {
                ignore Map.put(lower, thash, field, #False(candid));
            };
            case (#In(_) or #Not(_)) {
                Debug.trap(debug_show op # " not allowed in this context. Should have been expanded by the query builder");
            };
        };
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

        Debug.print("Index_key_details: " # debug_show index.key_details);
        Debug.print("full_start_query: " # debug_show full_start_query);
        Debug.print("full_end_query: " # debug_show full_end_query);

        let scans = CollectionUtils.memorybtree_scan_interval(index.data, index_data_utils, ?full_start_query, ?full_end_query);
        Debug.print("scan_intervals: " # debug_show scans);
        scans

        // let records_iter = MemoryBTree.scan(index.data, index_data_utils, ?full_start_query, ?full_end_query);

        // let record_ids_iter = Iter.map<([Candid], Nat), Nat>(
        //     records_iter,
        //     func((_, id) : ([Candid], Nat)) : (Nat) { id },
        // );

        // record_ids_iter;

        // return id_to_record_iter(collection, blobify,record_ids_iter );

    };

    public func query_plan_from_and_operation(
        collection : T.StableCollection,
        query_statements : [T.ZenQueryLang],
        sort_column : ?(Text, T.SortDirection),
        cursor_record : ?(Nat, Candid.Candid),
        cursor_map : CandidMap.CandidMap,
    ) : QueryPlan {

        let requires_sorting : Bool = Option.isSome(sort_column);

        let lower_bound = Map.new<Text, T.State<T.Candid>>();
        let upper_bound = Map.new<Text, T.State<T.Candid>>();

        let fields_with_equality_ops = Set.new<Text>();

        let bitmaps = Buffer.Buffer<T.BitMap>(8);
        let iterators = Buffer.Buffer<Iter<Nat>>(8);
        let sorted_records_from_iter = Buffer.Buffer<Nat>(8);
        let full_scan_bounds = Buffer.Buffer<([(Text, ?State<Candid>)], [(Text, ?State<Candid>)])>(8);

        var operations = Buffer.Buffer<(Text, T.ZqlOperators)>(8);

        let sub_query_plans = Buffer.Buffer<QueryPlan>(8);

        var num_of_nested_or_operations = 0;

        if (query_statements.size() == 0 and not requires_sorting) {

            return {
                is_and_operation = true;
                subplans = [];
                simple_operations = [];
                scans = [
                    #FullScan({
                        requires_additional_sorting = false;
                        requires_additional_filtering = false;
                        scan_bounds = ([], []);
                        filter_bounds = ([], []);
                    })
                ];
            };
        };

        for (query_statement in query_statements.vals()) {
            switch (query_statement) {
                case (#Operation(field, op)) {

                    // if the field is already in the lower or upper bounds, then we can't add it again
                    // because it would be a contradiction
                    // for example, if we have an equal operation on a field (x = 5), we can't have another operation on the same field (like x > 5 or x < 5 or x = 8)

                    switch (op) {
                        case (#eq(_)) {
                            let opt_exists_in_lower = Map.get(lower_bound, thash, field);
                            let opt_exists_in_upper = Map.get(upper_bound, thash, field);
                            let has_equality = Set.has(fields_with_equality_ops, thash, field);

                            if (Option.isSome(opt_exists_in_lower) or Option.isSome(opt_exists_in_upper) or has_equality) {
                                Debug.trap("Contradictory operations on the same field");
                            };

                            Set.add(fields_with_equality_ops, thash, field);
                        };
                        case (_) {};
                    };

                    operations.add(field, op);
                    operation_eval(field, op, lower_bound, upper_bound);
                };
                case (#And(_)) Debug.trap("And not allowed in this context");
                case (#Or(nested_or_operations)) {

                    num_of_nested_or_operations += 1;

                    let sub_query_plan = query_plan_from_or_operation(
                        collection,
                        nested_or_operations,
                        sort_column,
                        cursor_record,
                        cursor_map,
                    );

                    sub_query_plans.add(sub_query_plan);

                };
            };
        };

        // if there where #Operation types in the operations

        let best_index_result = switch (Index.get_best_index(collection, Buffer.toArray(operations), sort_column)) {
            case (null) {
                Debug.print("no index found so adding to full scan bounds");
                let (scan_bounds, filter_bounds) = extract_scan_and_filter_bounds(lower_bound, upper_bound, null, null);

                return {
                    is_and_operation = true;
                    subplans = [];
                    simple_operations = Buffer.toArray(operations);
                    scans = [
                        #FullScan({
                            requires_additional_sorting = requires_sorting;
                            requires_additional_filtering = operations.size() > 0;
                            scan_bounds = scan_bounds;
                            filter_bounds = filter_bounds;
                        })
                    ];
                };

            };
            case (?best_index_result) best_index_result;
        };

        let index = best_index_result.index;
        let requires_additional_filtering = best_index_result.requires_additional_filtering;
        let requires_additional_sorting = best_index_result.requires_additional_sorting;
        let sorted_in_reverse = best_index_result.sorted_in_reverse;

        let operations_array = Buffer.toArray(operations);

        let (scan_bounds, filter_bounds) = extract_scan_and_filter_bounds(lower_bound, upper_bound, ?index.key_details, ?best_index_result.fully_covered_equality_and_range_fields);

        Debug.print("lower bound: " # debug_show (lower_bound));
        Debug.print("upper bound: " # debug_show (upper_bound));

        Debug.print("scan lower bound: " # debug_show (scan_bounds.0));
        Debug.print("scan upper bound: " # debug_show (scan_bounds.1));

        var interval = scan(collection, index, scan_bounds.0, scan_bounds.1, cursor_record);

        Debug.print("best interval: " # debug_show ({ index = index.name; requires_additional_filtering; requires_additional_sorting; sorted_in_reverse; interval }));
        // Debug.print("index entries: " # debug_show (Iter.toArray(MemoryBTree.keys(index.data, get_index_data_utils(collection, index.key_details)))));

        Debug.print("interval: " # debug_show interval);
        Debug.print("requires_additional_filtering: " # debug_show requires_additional_filtering);
        Debug.print("requires_additional_sorting: " # debug_show requires_additional_sorting);

        if (requires_additional_filtering) {
            // we need to do index interval intersection with the filter bounds

        };

        let query_plan : QueryPlan = {
            is_and_operation = true;
            subplans = Buffer.toArray(sub_query_plans);
            simple_operations = operations_array;
            scans = [
                #IndexScan({
                    index;
                    requires_additional_filtering;
                    requires_additional_sorting;
                    sorted_in_reverse;
                    interval;
                    scan_bounds;
                    filter_bounds;
                    simple_operations = operations_array;
                })
            ];
        };

        return query_plan;

    };

    // // update the bounds to shift to the pagination cursor
    // func shift_bounds_to_pagination_cursor(
    //     collection : T.StableCollection,
    //     lower : Map<Text, T.State<Candid>>,
    //     upper : Map<Text, T.State<Candid>>,
    //     operations : [(Text, T.ZqlOperators)],
    //     index_key_details : [(Text, T.SortDirection)],
    //     cursor_id : Nat,
    //     cursor_map : CandidMap.CandidMap,
    //     pagination_direction : T.PaginationDirection,
    // ) {

    //     Debug.print("It should be shifting!!!!");

    //     let equality_set = Set.new<Text>();

    //     for ((field, op) in operations.vals()) {
    //         switch (op) {
    //             case (#eq(candid)) ignore Set.put(equality_set, thash, field);
    //             case (_) {};
    //         };
    //     };

    //     for ((field, sort_direction) in index_key_details.vals()) {
    //         if (Set.has(equality_set, thash, field)) {
    //             switch (Map.get(lower, thash, field)) {
    //                 case (? #True(val) or ? #False(val)) if (?val != cursor_map.get(field)) {
    //                     // can't shift bounds if the equality constraint does not match the cursor

    //                     return;
    //                 };
    //                 case (null) Debug.trap("cursor value not found in bounds");
    //             };
    //         } else {
    //             if (field == C.RECORD_ID_FIELD) {
    //                 switch (sort_direction) {
    //                     case (#Ascending) {
    //                         ignore Map.put(lower, thash, field, #True(#Nat(cursor_id + 1)));
    //                     };
    //                     case (#Descending) {
    //                         ignore Map.put(upper, thash, field, #True(#Nat(if (cursor_id > 0) cursor_id - 1 else 0)));
    //                     };
    //                 };
    //             } else {
    //                 switch (cursor_map.get(field)) {
    //                     case (?cursor_value) {
    //                         let should_replace = switch (sort_direction) {
    //                             case (#Ascending) {
    //                                 switch (Map.get(lower, thash, field)) {
    //                                     case (? #True(val) or ? #False(val)) Schema.cmp_candid(collection.schema, cursor_value, val) == 1;
    //                                     case (null) true;
    //                                 };
    //                             };
    //                             case (#Descending) {
    //                                 switch (Map.get(upper, thash, field)) {
    //                                     case (? #True(val) or ? #False(val)) Schema.cmp_candid(collection.schema, cursor_value, val) == -1;
    //                                     case (null) true;
    //                                 };
    //                             };
    //                         };

    //                         if (should_replace) {
    //                             switch (sort_direction) {
    //                                 case (#Ascending) {
    //                                     ignore Map.put(lower, thash, field, #True(cursor_value));
    //                                 };
    //                                 case (#Descending) {
    //                                     ignore Map.put(upper, thash, field, #True(cursor_value));
    //                                 };
    //                             };
    //                         };
    //                     };
    //                     case (null) {};
    //                 };
    //             };

    //         };
    //     };
    // };

    public func query_plan_from_or_operation(
        collection : T.StableCollection,
        query_statements : [T.ZenQueryLang],
        sort_column : ?(Text, T.SortDirection),
        cursor_record : ?(Nat, Candid.Candid),
        cursor_map : CandidMap.CandidMap,
    ) : QueryPlan {

        // Debug.print("Or operations: " # debug_show buffer);
        let bitmaps = Buffer.Buffer<T.BitMap>(8);
        let full_scan_bounds = Buffer.Buffer<([(Text, ?State<Candid>)], [(Text, ?State<Candid>)])>(8);

        let requires_sorting : Bool = Option.isSome(sort_column);
        let iterators = Buffer.Buffer<Iter<Nat>>(8);

        let scans = Buffer.Buffer<ScanDetails>(8);
        let sub_query_plans = Buffer.Buffer<QueryPlan>(8);

        label resolving_or_operations for (query_statement in query_statements.vals()) {
            let lower_bound = Map.new<Text, State<Candid>>();
            let upper_bound = Map.new<Text, State<Candid>>();

            switch (query_statement) {
                case (#Or(_)) Debug.trap("Directly nested #Or not allowed in this context");
                case (#Operation(field, op)) {
                    operation_eval(field, op, lower_bound, upper_bound);

                    let opt_index = Index.get_best_index(collection, [(field, op)], sort_column);

                    let scan_details = switch (opt_index) {
                        case (null) {
                            let (scan_bounds, filter_bounds) = extract_scan_and_filter_bounds(lower_bound, upper_bound, null, null);

                            let scan_details : ScanDetails = #FullScan({
                                requires_additional_sorting = requires_sorting;
                                requires_additional_filtering = true;
                                scan_bounds;
                                filter_bounds;
                            });

                        };
                        case (?best_index_info) {

                            let index = best_index_info.index;
                            let requires_additional_filtering = best_index_info.requires_additional_filtering;
                            let requires_additional_sorting = best_index_info.requires_additional_sorting;
                            let sorted_in_reverse = best_index_info.sorted_in_reverse;

                            let (scan_bounds, filter_bounds) = extract_scan_and_filter_bounds(lower_bound, upper_bound, ?index.key_details, ?best_index_info.fully_covered_equality_and_range_fields);

                            let interval = scan(collection, index, scan_bounds.0, scan_bounds.1, cursor_record);

                            Debug.print("best interval: " # debug_show ({ index = index.name; requires_additional_filtering; requires_additional_sorting; sorted_in_reverse; interval }));
                            Debug.print("interval: " # debug_show interval);
                            Debug.print("requires_additional_filtering: " # debug_show requires_additional_filtering);
                            Debug.print("requires_additional_sorting: " # debug_show requires_additional_sorting);

                            let scan_details : ScanDetails = #IndexScan({
                                index;
                                requires_additional_filtering;
                                requires_additional_sorting;
                                sorted_in_reverse;
                                interval;
                                scan_bounds;
                                filter_bounds;
                                simple_operations = [(field, op)];
                            });

                            // Debug.print("index entries: " # debug_show (Iter.toArray(MemoryBTree.keys(index.data, get_index_data_utils(collection, index.key_details)))));

                        };
                    };

                    scans.add(scan_details);
                };
                case (#And(nested_query_statements)) {

                    let sub_query_plan = query_plan_from_and_operation(
                        collection,
                        nested_query_statements,
                        sort_column,
                        cursor_record,
                        cursor_map,
                    );

                    sub_query_plans.add(sub_query_plan);

                };

            };

        };

        let query_plan : QueryPlan = {
            is_and_operation = false;
            subplans = Buffer.toArray(sub_query_plans);
            simple_operations = [];
            scans = Buffer.toArray(scans);
        };

    };

    public func create_query_plan(
        collection : T.StableCollection,
        db_query : ZenQueryLang,
        sort_column : ?(Text, T.SortDirection),
        cursor_record : ?(Nat, Candid.Candid),
        cursor_map : CandidMap.CandidMap,
    ) : QueryPlan {

        switch (db_query) {
            case (#And(operations)) query_plan_from_and_operation(
                collection,
                operations,
                sort_column,
                cursor_record,
                cursor_map,
            );
            case (#Or(operations)) query_plan_from_or_operation(
                collection,
                operations,
                sort_column,
                cursor_record,
                cursor_map,
            );
            case (_) Debug.trap("create_query_plan(): Unsupported query type");
        };
    };

};
