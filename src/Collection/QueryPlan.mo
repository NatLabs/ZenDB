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
import Logger "../Logger";

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

    public func fromAndOperation(
        collection : T.StableCollection,
        query_statements : [T.ZenQueryLang],
        sort_column : ?(Text, T.SortDirection),
        cursor_record : ?(Nat, Candid.Candid),
    ) : QueryPlan {
        Logger.lazyDebug(
            collection.logger,
            func() = "QueryPlan.fromAndOperation(): Creating query plan for AND operation with " #
            Nat.toText(query_statements.size()) # " statements",
        );

        let requires_sorting : Bool = Option.isSome(sort_column);
        if (requires_sorting) {
            let (sort_field, direction) = Option.unwrap(sort_column);
            Logger.lazyDebug(
                collection.logger,
                func() = "QueryPlan.fromAndOperation(): Sorting required on field '" #
                sort_field # "' in " # debug_show direction # " order",
            );
        };

        let simple_operations = Buffer.Buffer<(Text, T.ZqlOperators)>(8);

        let sorted_records_from_iter = Buffer.Buffer<Nat>(8);

        var operations = Buffer.Buffer<(Text, T.ZqlOperators)>(8);

        let sub_query_plans = Buffer.Buffer<QueryPlan>(8);

        var num_of_nested_or_operations = 0;

        if (query_statements.size() == 0 and not requires_sorting) {
            Logger.lazyDebug(collection.logger, func() = "QueryPlan.fromAndOperation(): Empty query with no sorting, using full scan");

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
                    Logger.lazyDebug(
                        collection.logger,
                        func() = "QueryPlan.fromAndOperation(): Adding simple operation on field '" #
                        field # "': " # debug_show op,
                    );
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
                    Logger.lazyDebug(
                        collection.logger,
                        func() = "QueryPlan.fromAndOperation(): Processing nested OR operation with " #
                        Nat.toText(nested_or_operations.size()) # " statements",
                    );

                    num_of_nested_or_operations += 1;

                    let sub_query_plan = fromOrOperation(
                        collection,
                        nested_or_operations,
                        sort_column,
                        cursor_record,
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
        // the actual feature for reducing the query is implemented in the fromOrOperation function where the parent_simple_and_operations from this function is passed in. Here we just remove dangling #And operations that will be applied to the #Or operations, by leaving scans empty
        // consider query: (0 < x < 5 or y = 6) and (x = 3)
        // we want to reduce the query so the scan size of the #Or operations are smaller
        // so apply the #And operations on the #Or operations to get:
        //  -> (0 < x < 5 and x = 3) or (y = 6 and x = 3)
        // which then can be reduced to:
        //  -> (x = 3) or (y = 6 and x = 3 )
        //
        // in terms of size of each operation, the first statement has been reduced from scanning a range of 5 values to only scanning 1 value in the btree
        //
        // the actual feature for reducing the query is implemented in the fromOrOperation function where the parent_simple_and_operations from this function is passed in. Here we just remove dangling #And operations that will be applied to the #Or operations, by leaving scans empty
        if (sub_query_plans.size() > 0) {
            Logger.lazyDebug(
                collection.logger,
                func() = "QueryPlan.fromAndOperation(): Query has " #
                Nat.toText(sub_query_plans.size()) # " nested OR subplans",
            );

            if (sub_query_plans.size() == 1) {
                Logger.lazyDebug(collection.logger, func() = "QueryPlan.fromAndOperation(): Single subplan, returning it directly");
                return sub_query_plans.get(0);
            };

            Logger.lazyDebug(
                collection.logger,
                func() = "QueryPlan.fromAndOperation(): Returning combined AND plan with " #
                Nat.toText(sub_query_plans.size()) # " OR subplans",
            );
            return {
                is_and_operation = true;
                subplans = Buffer.toArray(sub_query_plans);
                simple_operations = [];
                scans = [];

                // if there where #Operation types in the operations
            };
        };

        // if there where #Operation types in the operations

        let best_index_result = switch (Index.getBestIndex(collection, Buffer.toArray(operations), sort_column)) {
            case (null) {
                Logger.lazyDebug(collection.logger, func() = "QueryPlan.fromAndOperation(): No suitable index found, using full scan");
                let (scan_bounds, filter_bounds) = Index.convertSimpleOpsToBounds(true, Buffer.toArray(simple_operations), null, null);

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
            case (?best_index_result) {
                Logger.lazyDebug(
                    collection.logger,
                    func() = "QueryPlan.fromAndOperation(): Found best index: '" #
                    best_index_result.index.name # "'",
                );
                best_index_result;
            };
        };

        let index = best_index_result.index;
        let requires_additional_filtering = best_index_result.requires_additional_filtering;
        let requires_additional_sorting = best_index_result.requires_additional_sorting;
        let sorted_in_reverse = best_index_result.sorted_in_reverse;

        Logger.lazyDebug(
            collection.logger,
            func() = "QueryPlan.fromAndOperation(): Index details - " #
            "requires_additional_filtering: " # debug_show requires_additional_filtering # ", " #
            "requires_additional_sorting: " # debug_show requires_additional_sorting # ", " #
            "sorted_in_reverse: " # debug_show sorted_in_reverse,
        );

        let operations_array = Buffer.toArray(operations);

        let (scan_bounds, filter_bounds) = Index.convertSimpleOpsToBounds(true, Buffer.toArray(simple_operations), ?index.key_details, ?best_index_result.fully_covered_equality_and_range_fields);

        Logger.lazyDebug(
            collection.logger,
            func() = "QueryPlan.fromAndOperation(): Scan bounds - " #
            "lower: " # debug_show scan_bounds.0 # ", " #
            "upper: " # debug_show scan_bounds.1,
        );

        var interval = Index.scan(collection, index, scan_bounds.0, scan_bounds.1, cursor_record);

        Logger.lazyDebug(
            collection.logger,
            func() {
                "QueryPlan.fromOrOperation(): Index scan intervals: " # debug_show interval;
            },
        );

        // Debug.print("Index entries: " # debug_show (Iter.toArray(Index.entries(collection, index))));

        if (requires_additional_filtering) {
            // we need to do index interval intersection with the filter bounds
            Logger.debugMsg(collection.logger, "QueryPlan.fromAndOperation(): Additional filtering required with filter bounds");
        };

        let query_plan : QueryPlan = {
            is_and_operation = true;
            subplans = Buffer.toArray(sub_query_plans);
            simple_operations = operations_array;
            scans = [
                #IndexScan({
                    index_name = index.name;
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

        Logger.lazyDebug(collection.logger, func() = "QueryPlan.fromAndOperation(): Created index scan query plan using index '" # index.name # "'");
        return query_plan;

    };

    public func fromOrOperation(
        collection : T.StableCollection,
        query_statements : [T.ZenQueryLang],
        sort_column : ?(Text, T.SortDirection),
        cursor_record : ?(Nat, Candid.Candid),
        parent_simple_and_operations : [(Text, T.ZqlOperators)],
    ) : QueryPlan {
        Logger.lazyDebug(
            collection.logger,
            func() = "QueryPlan.fromOrOperation(): Creating query plan for OR operation with " #
            Nat.toText(query_statements.size()) # " statements and " # Nat.toText(parent_simple_and_operations.size()) # " parent AND operations",
        );

        let requires_sorting : Bool = Option.isSome(sort_column);
        if (requires_sorting) {
            let (sort_field, direction) = Option.unwrap(sort_column);
            Logger.lazyDebug(
                collection.logger,
                func() = "QueryPlan.fromOrOperation(): Sorting required on field '" #
                sort_field # "' in " # debug_show direction # " order",
            );
        };

        let scans = Buffer.Buffer<ScanDetails>(8);
        let sub_query_plans = Buffer.Buffer<QueryPlan>(8);

        label resolving_or_operations for (query_statement in query_statements.vals()) {

            switch (query_statement) {
                case (#Or(_)) Debug.trap("Directly nested #Or not allowed in this context");
                case (#Operation(field, op)) {
                    Logger.lazyDebug(
                        collection.logger,
                        func() = "QueryPlan.fromOrOperation(): Processing operation on field '" #
                        field # "': " # debug_show op,
                    );

                    let operations = Array.append(parent_simple_and_operations, [(field, op)]);
                    let opt_index = Index.getBestIndex(collection, operations, sort_column);

                    let scan_details = switch (opt_index) {
                        case (null) {
                            Logger.lazyDebug(collection.logger, func() = "QueryPlan.fromOrOperation(): No suitable index found for operation, using full scan");
                            let (scan_bounds, filter_bounds) = Index.convertSimpleOpsToBounds(false, operations, null, null);

                            let scan_details : ScanDetails = #FullScan({
                                requires_additional_sorting = requires_sorting;
                                requires_additional_filtering = true;
                                scan_bounds;
                                filter_bounds;
                            });

                        };
                        case (?best_index_info) {
                            Logger.lazyDebug(
                                collection.logger,
                                func() = "QueryPlan.fromOrOperation(): Found best index for operation: '" #
                                best_index_info.index.name # "'",
                            );

                            let index = best_index_info.index;
                            let requires_additional_filtering = best_index_info.requires_additional_filtering;
                            let requires_additional_sorting = best_index_info.requires_additional_sorting;
                            let sorted_in_reverse = best_index_info.sorted_in_reverse;

                            Logger.lazyDebug(
                                collection.logger,
                                func() = "QueryPlan.fromOrOperation(): Index details - " #
                                "requires_additional_filtering: " # debug_show requires_additional_filtering # ", " #
                                "requires_additional_sorting: " # debug_show requires_additional_sorting # ", " #
                                "sorted_in_reverse: " # debug_show sorted_in_reverse,
                            );

                            let (scan_bounds, filter_bounds) = Index.convertSimpleOpsToBounds(
                                false,
                                operations,
                                ?index.key_details,
                                ?best_index_info.fully_covered_equality_and_range_fields,
                            );

                            let interval = Index.scan(collection, index, scan_bounds.0, scan_bounds.1, cursor_record);
                            Logger.lazyDebug(
                                collection.logger,
                                func() {
                                    "QueryPlan.fromOrOperation(): Index scan intervals: " # debug_show interval;
                                },
                            );

                            let scan_details : ScanDetails = #IndexScan({
                                index_name = index.name;
                                requires_additional_filtering;
                                requires_additional_sorting;
                                sorted_in_reverse;
                                interval;
                                scan_bounds;
                                filter_bounds;
                                simple_operations = operations;
                            });
                        };
                    };

                    scans.add(scan_details);
                };
                case (#And(nested_query_statements)) {
                    Logger.lazyDebug(
                        collection.logger,
                        func() = "QueryPlan.fromOrOperation(): Processing nested AND operation with " #
                        Nat.toText(nested_query_statements.size()) # " statements",
                    );

                    let sub_query_plan = fromAndOperation(
                        collection,
                        nested_query_statements,
                        sort_column,
                        cursor_record,
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

        Logger.lazyDebug(
            collection.logger,
            func() = "QueryPlan.fromOrOperation(): Created OR query plan with " #
            Nat.toText(sub_query_plans.size()) # " AND subplans and " # Nat.toText(scans.size()) # " scans",
        );

        return query_plan;
    };

    public func createQueryPlan(
        collection : T.StableCollection,
        db_query : ZenQueryLang,
        sort_column : ?(Text, T.SortDirection),
        cursor_record : ?(Nat, Candid.Candid),
    ) : QueryPlan {
        Logger.lazyInfo(
            collection.logger,
            func() = "QueryPlan.createQueryPlan(): Creating query plan",
        );

        let query_plan = switch (db_query) {
            case (#And(operations)) {
                Logger.lazyDebug(
                    collection.logger,
                    func() = "QueryPlan.createQueryPlan(): Processing top-level AND query with " #
                    Nat.toText(operations.size()) # " operations",
                );

                fromAndOperation(
                    collection,
                    operations,
                    sort_column,
                    cursor_record,
                );
            };
            case (#Or(operations)) {
                Logger.lazyDebug(
                    collection.logger,
                    func() = "QueryPlan.createQueryPlan(): Processing top-level OR query with " #
                    Nat.toText(operations.size()) # " operations",
                );

                fromOrOperation(
                    collection,
                    operations,
                    sort_column,
                    cursor_record,
                    [],
                );
            };
            case (_) {
                Logger.lazyError(
                    collection.logger,
                    func() = "QueryPlan.createQueryPlan(): Unsupported query type: " # debug_show db_query,
                );
                Debug.trap("createQueryPlan(): Unsupported query type");
            };
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "QueryPlan.createQueryPlan(): Query plan created successfully -> " # debug_show query_plan,
        );
        query_plan;
    };

};
