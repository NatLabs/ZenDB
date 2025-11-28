import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Option "mo:base@0.16.0/Option";
import Iter "mo:base@0.16.0/Iter";
import Nat "mo:base@0.16.0/Nat";
import Order "mo:base@0.16.0/Order";
import Text "mo:base@0.16.0/Text";

import Candid "mo:serde@3.4.0/Candid";
import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import SparseBitMap64 "mo:bit-map@1.1.0/SparseBitMap64";
import Itertools "mo:itertools@0.2.2/Iter";

import T "../Types";
import CandidMap "../CandidMap";
import Utils "../Utils";

import CompositeIndex "Index/CompositeIndex";
import Index "Index";
import CollectionUtils "CollectionUtils";
import C "../Constants";
import Schema "Schema";
import Logger "../Logger";
import DocumentStore "DocumentStore";
import Query "../Query";

module {
    let LOGGER_NAMESPACE = "QueryPlan";

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
    type CompositeIndex = T.CompositeIndex;
    type Order = Order.Order;

    public func from_and_operation(
        collection : T.StableCollection,
        query_statements : [T.ZenQueryLang],
        sort_column : ?(Text, T.SortDirection),
        last_pagination_document : ?T.CandidMap,
        parent_has_nested_operations : Bool, // this helps us indicate if the results from this operation will be fed into an outer operation via a bitmap intersection (if requires_sorting is false) or a kmerge (if requires_sorting is true)
    ) : QueryPlan {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("from_and_operation");
        log.lazyDebug(
            func() = "Creating query plan for AND operation with " #
            Nat.toText(query_statements.size()) # " statements"
        );

        let requires_sorting : Bool = Option.isSome(sort_column);
        if (requires_sorting) {
            let (sort_field, direction) = Option.unwrap(sort_column);
            log.lazyDebug(
                func() = "Sorting required on field '" #
                sort_field # "' in " # debug_show direction # " order"
            );
        };

        let simple_operations = Buffer.Buffer<(Text, T.ZqlOperators)>(8);

        let sorted_records_from_iter = Buffer.Buffer<Nat>(8);

        var operations = Buffer.Buffer<(Text, T.ZqlOperators)>(8);

        let sub_query_plans = Buffer.Buffer<QueryPlan>(8);

        var num_of_nested_or_operations = 0;

        let opt_sort_direction = do ? { sort_column!.1 };
        let sort_direction = Option.get(opt_sort_direction, #Ascending);

        if (query_statements.size() == 0 and not requires_sorting) {
            log.lazyDebug(func() = "Empty query with no sorting, using full scan");

            return {
                is_and_operation = true;
                subplans = [];
                simple_operations = [];
                scans = [
                    switch (last_pagination_document) {
                        case (?cursor_document) {

                            let ?#Blob(id) = CandidMap.get(cursor_document, collection.schema_map, C.DOCUMENT_ID) else Debug.trap("Pagination cursor document is missing document ID field");

                            let last_document_pos = DocumentStore.get_expected_index(collection, id);
                            let pos = switch (last_document_pos) {
                                case (#Found(pos)) pos;
                                case (#NotFound(pos)) pos;
                            };

                            let interval = switch (sort_direction) {
                                case (#Ascending) (pos + 1, DocumentStore.size(collection));
                                case (#Descending) (0, (if (pos > 0) pos - 1 else 0));
                            };

                            #IndexScan({
                                index_name = C.DOCUMENT_ID;
                                requires_additional_filtering = false;
                                requires_additional_sorting = false;
                                sorted_in_reverse = sort_direction == #Descending;
                                interval;
                                filter_bounds = ([], []);
                                scan_bounds = ([], []);
                                simple_operations = [];

                            });

                        };
                        case (null) {
                            #FullScan({
                                requires_additional_sorting = false;
                                requires_additional_filtering = false;
                                scan_bounds = ([], []);
                                filter_bounds = ([], []);
                            });
                        };

                    },

                ];
            };
        };

        var this_query_has_nested_or_operations = false;

        for (query_statement in query_statements.vals()) {
            switch (query_statement) {
                case (#Operation(field, op)) {
                    log.lazyDebug(
                        func() = "Adding simple operation on field '" #
                        field # "': " # debug_show op
                    );
                    // ?: what's the difference between these two
                    simple_operations.add((field, op));
                    operations.add(field, op);
                };
                case (#And(_)) Debug.trap("And not allowed in this context");
                case (#Or(_)) {
                    this_query_has_nested_or_operations := true;
                };
            };
        };

        for (query_statement in query_statements.vals()) {
            switch (query_statement) {

                case (#Or(nested_or_operations)) {
                    log.lazyDebug(
                        func() = "Processing nested OR operation with " #
                        Nat.toText(nested_or_operations.size()) # " statements"
                    );

                    num_of_nested_or_operations += 1;

                    let sub_query_plan = from_or_operation(
                        collection,
                        nested_or_operations,
                        sort_column,
                        last_pagination_document,
                        Buffer.toArray(operations),
                        true, // since we are in a nested OR operation, the parent has nested operations
                    );

                    sub_query_plans.add(sub_query_plan);

                };
                case ((_)) {};
            };
        };

        // the actual feature for reducing the query is implemented in the from_or_operation function where the parent_simple_and_operations from this function is passed in. Here we just remove dangling #And operations that will be applied to the #Or operations, by leaving scans empty
        // consider query the query below. we want to reduce the query so the scan size of the #Or operations are smaller
        //  -> (0 < x < 5 or y = 6) and (x = 3)
        // so apply the #And operations to the #Or operations to get:
        //  -> (0 < x < 5 and x = 3) or (y = 6 and x = 3)
        // which then can be reduced to:
        //  -> (x = 3) or (y = 6 and x = 3 )
        //
        // in terms of size of each operation, the first statement has been reduced from scanning a range of 5 values to only scanning 1 value in the btree
        //
        // the actual feature for reducing the query is implemented in the from_or_operation function where the parent_simple_and_operations from this function is passed in. Here we just remove dangling #And operations that will be applied to the #Or operations, by leaving scans empty
        if (sub_query_plans.size() > 0) {
            log.lazyDebug(
                func() = "Query has " #
                Nat.toText(sub_query_plans.size()) # " nested OR subplans"
            );

            if (sub_query_plans.size() == 1) {
                log.lazyDebug(func() = "Single subplan, returning it directly");
                return sub_query_plans.get(0);
            };

            log.lazyDebug(
                func() = "Returning combined AND plan with " #
                Nat.toText(sub_query_plans.size()) # " OR subplans"
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

        let has_nested_and_or_operations_in_path = parent_has_nested_operations or this_query_has_nested_or_operations;

        let best_index_result = switch (Index.get_best_index(collection, Buffer.toArray(operations), sort_column, last_pagination_document, has_nested_and_or_operations_in_path)) {
            case (null) {
                log.lazyDebug(func() = "No suitable index found, using full scan");

                switch (last_pagination_document) {
                    case (?cursor_document) {

                        switch (sort_column) {
                            case (?(sort_field, sort_direction)) {

                                let ?cursor_value = CandidMap.get(cursor_document, collection.schema_map, sort_field) else Debug.trap("Pagination cursor document is missing sort field");
                                let ?#Blob(id) = CandidMap.get(cursor_document, collection.schema_map, C.DOCUMENT_ID) else Debug.trap("Pagination cursor document is missing document ID field");

                                switch (sort_direction) {
                                    case (#Ascending) {
                                        operations.add(sort_field, #gte(cursor_value));
                                        // operations.add(C.DOCUMENT_ID, #gt(#Blob(id)));
                                        // the above operation should be added so we filter out all the entries that are equal to the cursor value but have a document ID less than the cursor document ID.
                                        // However, this assumes the secondary sort order is always document ID which is not the case.
                                        // We would need to get the results from this query and then skip all the entries until we reach the document ID of the cursor document.
                                    };
                                    case (#Descending) {
                                        operations.add(sort_field, #lte(cursor_value));
                                        // operations.add(C.DOCUMENT_ID, #lt(#Blob(id)));
                                    };
                                };
                            };
                            case (null) {

                                let ?#Blob(id) = CandidMap.get(cursor_document, collection.schema_map, C.DOCUMENT_ID) else Debug.trap("Pagination cursor document is missing document ID field");

                                let pos = switch (DocumentStore.get_expected_index(collection, id)) {
                                    case (#Found(pos)) pos;
                                    case (#NotFound(pos)) pos;
                                };

                                let interval = switch (sort_direction) {
                                    case (#Ascending) (pos + 1, DocumentStore.size(collection));
                                    case (#Descending) (0, (if (pos > 0) pos - 1 else 0));
                                };

                                let (scan_bounds, filter_bounds) = Index.convert_simple_ops_to_bounds(true, Buffer.toArray(operations), null, null);
                                assert not requires_sorting;

                                return {
                                    is_and_operation = true;
                                    subplans = [];
                                    simple_operations = Buffer.toArray(operations);
                                    scans = [

                                        #IndexScan({
                                            index_name = C.DOCUMENT_ID;
                                            requires_additional_sorting = requires_sorting;
                                            requires_additional_filtering = operations.size() > 0;
                                            sorted_in_reverse = sort_direction == #Descending;
                                            interval;
                                            scan_bounds;
                                            filter_bounds;
                                            simple_operations = Buffer.toArray(operations);
                                        })

                                    ];
                                };
                            };
                        };
                    };
                    case (null) {};
                };

                let (scan_bounds, filter_bounds) = Index.convert_simple_ops_to_bounds(true, Buffer.toArray(operations), null, null);

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
                        }),

                    ];
                };

            };
            case (?best_index_result) {
                log.lazyDebug(
                    func() = "Found best index: '" #
                    best_index_result.index.name # "'"
                );
                best_index_result;
            };
        };

        let {
            index;
            requires_additional_filtering;
            requires_additional_sorting;
            sorted_in_reverse;
            interval;
        } = best_index_result;

        log.lazyDebug(
            func() = "CompositeIndex details - " #
            "requires_additional_filtering: " # debug_show requires_additional_filtering # ", " #
            "requires_additional_sorting: " # debug_show requires_additional_sorting # ", " #
            "sorted_in_reverse: " # debug_show sorted_in_reverse
        );

        let operations_array = Buffer.toArray(operations);

        let (scan_bounds, filter_bounds) = Index.convert_simple_ops_to_bounds(true, Buffer.toArray(simple_operations), ?index.key_details, ?best_index_result.fully_covered_equality_and_range_fields);

        log.lazyDebug(
            func() = "Scan bounds - " #
            "lower: " # debug_show scan_bounds.0 # ", " #
            "upper: " # debug_show scan_bounds.1
        );

        // var interval = CompositeIndex.scan(collection, index, scan_bounds.0, scan_bounds.1, last_pagination_document, not sorted_in_reverse); // pagination_token

        log.lazyDebug(
            func() {
                "CompositeIndex scan intervals: " # debug_show interval;
            }
        );

        if (requires_additional_filtering) {
            // we need to do index interval intersection with the filter bounds
            log.logDebug("Additional filtering required with filter bounds");
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

        log.lazyDebug(func() = "Created index scan query plan using index '" # index.name # "'");
        return query_plan;

    };

    public func from_or_operation(
        collection : T.StableCollection,
        query_statements : [T.ZenQueryLang],
        sort_column : ?(Text, T.SortDirection),
        last_pagination_document : ?T.CandidMap,
        parent_simple_and_operations : [(Text, T.ZqlOperators)],
        parent_has_nested_operations : Bool,
    ) : QueryPlan {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("from_or_operation");
        log.lazyDebug(
            func() = "Creating query plan for OR operation with " #
            debug_show (query_statements.size()) # " statements and " # debug_show (parent_simple_and_operations.size()) # " parent AND operations"
        );

        let requires_sorting : Bool = Option.isSome(sort_column);
        if (requires_sorting) {
            let (sort_field, direction) = Option.unwrap(sort_column);
            log.lazyDebug(
                func() = "Sorting required on field '" #
                sort_field # "' in " # debug_show direction # " order"
            );
        };

        let opt_sort_direction = do ? { sort_column!.1 };
        let sort_direction = Option.get(opt_sort_direction, #Ascending);

        let scans = Buffer.Buffer<ScanDetails>(8);
        let sub_query_plans = Buffer.Buffer<QueryPlan>(8);

        let this_query_has_nested_or_operations = Itertools.any(
            query_statements.vals(),
            func(qs : T.ZenQueryLang) : Bool {
                switch (qs) {
                    case (#Or(_)) Debug.trap("Directly nested #Or not allowed in this context");
                    case (#And(nested_qs)) true;
                    case (_) false;
                };
            },
        );

        let has_nested_and_or_operations_in_path = parent_has_nested_operations or this_query_has_nested_or_operations and query_statements.size() <= 1;

        label resolving_or_operations for (query_statement in query_statements.vals()) {

            switch (query_statement) {
                case (#Or(_)) Debug.trap("Directly nested #Or not allowed in this context");
                case (#Operation(field, op)) {
                    log.lazyDebug(
                        func() = "Processing operation on field '" #
                        field # "': " # debug_show op
                    );

                    let operations = Array.append(parent_simple_and_operations, [(field, op)]);
                    let opt_index = Index.get_best_index(collection, operations, sort_column, last_pagination_document, has_nested_and_or_operations_in_path);

                    let scan_details = switch (opt_index) {
                        case (null) {
                            log.lazyDebug(func() = "No suitable index found for operation, using full scan");
                            let (scan_bounds, filter_bounds) = Index.convert_simple_ops_to_bounds(false, operations, null, null);

                            // Debug.print("Or operation filter bounds: " # debug_show filter_bounds);

                            var scan_details : ScanDetails = #FullScan({
                                requires_additional_sorting = requires_sorting;
                                requires_additional_filtering = true;
                                scan_bounds;
                                filter_bounds;
                            });

                            switch (last_pagination_document) {
                                case (null) {};
                                case (?cursor_document) {

                                    switch (sort_column) {
                                        case (?(sort_field, sort_direction)) {

                                        };
                                        case (null) {

                                            let ?#Blob(id) = CandidMap.get(cursor_document, collection.schema_map, C.DOCUMENT_ID) else Debug.trap("Pagination cursor document is missing document ID field");

                                            let pos = switch (DocumentStore.get_expected_index(collection, id)) {
                                                case (#Found(pos)) pos;
                                                case (#NotFound(pos)) pos;
                                            };

                                            let interval = switch (sort_direction) {
                                                case (#Ascending) (pos + 1, DocumentStore.size(collection));
                                                case (#Descending) (0, (if (pos > 0) pos - 1 else 0));
                                            };

                                            assert not requires_sorting;

                                            scan_details := #IndexScan({
                                                index_name = C.DOCUMENT_ID;
                                                requires_additional_sorting = false;
                                                requires_additional_filtering = true;
                                                sorted_in_reverse = sort_direction == #Descending;
                                                interval;
                                                scan_bounds;
                                                filter_bounds;
                                                simple_operations = operations;
                                            });

                                        };
                                    };
                                };
                            };

                            scans.add(scan_details);

                        };
                        case (?index) {
                            let sub_query_plan = from_and_operation(
                                collection,
                                Array.map<(Text, T.ZqlOperators), T.ZenQueryLang>(
                                    operations,
                                    func((f, o) : (Text, T.ZqlOperators)) : T.ZenQueryLang {
                                        #Operation(f, o);
                                    },
                                ),
                                sort_column,
                                last_pagination_document,
                                true,
                            );

                            sub_query_plans.add(sub_query_plan);
                        };
                    };

                };
                case (#And(nested_query_statements)) {
                    log.lazyDebug(
                        func() = "Processing nested AND operation with " #
                        Nat.toText(nested_query_statements.size()) # " statements"
                    );

                    let sub_query_plan = from_and_operation(
                        collection,
                        nested_query_statements,
                        sort_column,
                        last_pagination_document,
                        true,
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

        log.lazyDebug(
            func() = "Created OR query plan with " #
            Nat.toText(sub_query_plans.size()) # " AND subplans and " # Nat.toText(scans.size()) # " scans"
        );

        return query_plan;
    };

    public func create_query_plan(
        collection : T.StableCollection,
        db_query : ZenQueryLang,
        sort_column : ?(Text, T.SortDirection),
        pagination_token : ?T.PaginationToken,
    ) : T.QueryPlanResult {
        let LOGGER_SUB_NAMESPACE = "create_query_plan";
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace(LOGGER_SUB_NAMESPACE);

        log.lazyInfo(func() = "Creating query plan");

        let opt_last_pagination_document_id = do ? {
            pagination_token!.last_document_id!;
        };

        let opt_last_pagination_document = do ? {
            CollectionUtils.get_and_cache_candid_map(
                collection,
                opt_last_pagination_document_id!,
            );
        };

        let query_plan = switch (db_query) {
            case (#And(operations)) {
                log.lazyDebug(
                    func() = "Processing top-level AND query with " #
                    Nat.toText(operations.size()) # " operations"
                );

                from_and_operation(
                    collection,
                    operations,
                    sort_column,
                    opt_last_pagination_document,
                    false,
                );
            };
            case (#Or(operations)) {
                log.lazyDebug(
                    func() = "Processing top-level OR query with " #
                    Nat.toText(operations.size()) # " operations"
                );

                from_or_operation(
                    collection,
                    operations,
                    sort_column,
                    opt_last_pagination_document,
                    [],
                    false,
                );
            };
            case (_) {
                log.lazyError(
                    func() = "Unsupported query type: " # debug_show db_query
                );
                Debug.trap("createQueryPlan(): Unsupported query type");
            };
        };

        log.lazyInfo(
            func() = "Query plan created successfully -> " # debug_show query_plan
        );

        {
            query_plan;
            opt_last_pagination_document_id;
        };
    };

};
