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

import T "../Types";
import CandidMap "../CandidMap";
import Utils "../Utils";

import Index "Index";
import CollectionUtils "Utils";
import C "../Constants";
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

    public func query_plan_from_and_operation(
        collection : T.StableCollection,
        query_statements : [T.ZenQueryLang],
        sort_column : ?(Text, T.SortDirection),
        cursor_record : ?(Nat, Candid.Candid),
        cursor_map : CandidMap.CandidMap,
    ) : QueryPlan {

        let requires_sorting : Bool = Option.isSome(sort_column);

        let simple_operations = Buffer.Buffer<(Text, T.ZqlOperators)>(8);

        let sorted_records_from_iter = Buffer.Buffer<Nat>(8);

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
                    simple_operations.add((field, op));
                    operations.add(field, op);
                };
                case (#And(_)) Debug.trap("And not allowed in this context");
                case ((_)) {};
            };
        };

        for (query_statement in query_statements.vals()) {
            switch (query_statement) {

                case (#Or(nested_or_operations)) {

                    num_of_nested_or_operations += 1;

                    let sub_query_plan = query_plan_from_or_operation(
                        collection,
                        nested_or_operations,
                        sort_column,
                        cursor_record,
                        cursor_map,
                        Buffer.toArray(operations),
                    );

                    sub_query_plans.add(sub_query_plan);

                };
                case ((_)) {};
            };
        };

        // consider query: (0 < x < 5 or y = 6) and (x = 3)
        // we want to reduce the query so the scan size of the #Or operations are smaller
        // so apply the #And operations on the #Or operations to get:
        //  -> (0 < x < 5 and x = 3) or (y = 6 and x = 3)
        // which then can be reduced to:
        //  -> (x = 3) or (y = 6 and x = 3 )
        //
        // in terms of size of each operation, the first statement has been reduced from scanning a range of 5 values to only scanning 1 value in the btree
        //
        // the actual feature for reducing the query is implemented in the query_plan_from_or_operation function where the parent_simple_and_operations from this function is passed in. Here we just remove dangling #And operations that will be applied to the #Or operations, by leaving scans empty
        if (sub_query_plans.size() > 0) {
            if (sub_query_plans.size() == 1) {
                return sub_query_plans.get(0);
            };

            return {
                is_and_operation = true;
                subplans = Buffer.toArray(sub_query_plans);
                simple_operations = [];
                scans = [];
            };
        };

        // if there where #Operation types in the operations

        let best_index_result = switch (Index.get_best_index(collection, Buffer.toArray(operations), sort_column)) {
            case (null) {
                // Debug.print("no index found so adding to full scan bounds");
                let (scan_bounds, filter_bounds) = Index.convert_simple_operations_to_scan_and_filter_bounds(true, Buffer.toArray(simple_operations), null, null);

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

        let (scan_bounds, filter_bounds) = Index.convert_simple_operations_to_scan_and_filter_bounds(true, Buffer.toArray(simple_operations), ?index.key_details, ?best_index_result.fully_covered_equality_and_range_fields);

        // Debug.print("scan lower bound: " # debug_show (scan_bounds.0));
        // Debug.print("scan upper bound: " # debug_show (scan_bounds.1));

        var interval = Index.scan(collection, index, scan_bounds.0, scan_bounds.1, cursor_record);

        // Debug.print("best interval: " # debug_show ({ index = index.name; requires_additional_filtering; requires_additional_sorting; sorted_in_reverse; interval }));
        // Debug.print("index entries: " # debug_show (Iter.toArray(MemoryBTree.keys(index.data, get_index_data_utils(collection, index.key_details)))));

        // Debug.print("interval: " # debug_show interval);
        // Debug.print("requires_additional_filtering: " # debug_show requires_additional_filtering);
        // Debug.print("requires_additional_sorting: " # debug_show requires_additional_sorting);

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

    public func query_plan_from_or_operation(
        collection : T.StableCollection,
        query_statements : [T.ZenQueryLang],
        sort_column : ?(Text, T.SortDirection),
        cursor_record : ?(Nat, Candid.Candid),
        cursor_map : CandidMap.CandidMap,
        parent_simple_and_operations : [(Text, T.ZqlOperators)],
    ) : QueryPlan {

        // Debug.print("Or operations: " # debug_show buffer);

        let requires_sorting : Bool = Option.isSome(sort_column);

        let scans = Buffer.Buffer<ScanDetails>(8);
        let sub_query_plans = Buffer.Buffer<QueryPlan>(8);

        label resolving_or_operations for (query_statement in query_statements.vals()) {

            switch (query_statement) {
                case (#Or(_)) Debug.trap("Directly nested #Or not allowed in this context");
                case (#Operation(field, op)) {

                    let operations = Array.append(parent_simple_and_operations, [(field, op)]);
                    let opt_index = Index.get_best_index(collection, operations, sort_column);

                    let scan_details = switch (opt_index) {
                        case (null) {
                            let (scan_bounds, filter_bounds) = Index.convert_simple_operations_to_scan_and_filter_bounds(false, operations, null, null);

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

                            let (scan_bounds, filter_bounds) = Index.convert_simple_operations_to_scan_and_filter_bounds(false, operations, ?index.key_details, ?best_index_info.fully_covered_equality_and_range_fields);

                            let interval = Index.scan(collection, index, scan_bounds.0, scan_bounds.1, cursor_record);

                            // Debug.print("best interval: " # debug_show ({ index = index.name; requires_additional_filtering; requires_additional_sorting; sorted_in_reverse; interval }));
                            // Debug.print("interval: " # debug_show interval);
                            // Debug.print("requires_additional_filtering: " # debug_show requires_additional_filtering);
                            // Debug.print("requires_additional_sorting: " # debug_show requires_additional_sorting);

                            let scan_details : ScanDetails = #IndexScan({
                                index;
                                requires_additional_filtering;
                                requires_additional_sorting;
                                sorted_in_reverse;
                                interval;
                                scan_bounds;
                                filter_bounds;
                                simple_operations = operations;
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

        let query_plan = switch (db_query) {
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
                [],
            );
            case (_) Debug.trap("create_query_plan(): Unsupported query type");
        };

        query_plan;
    };

};
