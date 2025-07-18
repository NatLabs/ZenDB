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
import BitMap "mo:bit-map";

import T "../Types";
import CandidMap "../CandidMap";
import Utils "../Utils";
import C "../Constants";
import Logger "../Logger";
import CandidUtils "../CandidUtils";

import { Orchid } "Orchid";

import CollectionUtils "Utils";
import Schema "Schema";
import BTree "../BTree";
import SchemaMap "SchemaMap";
import DocumentStore "DocumentStore";

module Index {

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

    public func new(
        collection : StableCollection,
        name : Text,
        index_key_details : [(Text, SortDirection)],
        is_unique : Bool, // if true, the index is unique and the document ids are not concatenated with the index key values to make duplicate values appear unique
        used_internally : Bool, // cannot be deleted by user if true
    ) : T.Index {

        let key_details : [(Text, SortDirection)] = if (is_unique) {
            let contains_option_type = Itertools.any(
                index_key_details.vals(),
                func(index_key_detail : (Text, SortDirection)) : Bool {
                    switch (SchemaMap.get(collection.schema_map, index_key_detail.0)) {
                        case (?#Option(_)) true;
                        case (null) Debug.trap("Index key details must be a valid field in the schema map");
                        case (_) false;
                    };
                },
            );

            if (contains_option_type) {
                Array.append(
                    index_key_details,
                    [(C.UNIQUE_INDEX_NULL_EXEMPT_ID, #Ascending)],
                );
            } else {
                index_key_details;
            }

        } else {
            Array.append(
                index_key_details,
                [(C.DOCUMENT_ID, #Ascending)],
            );
        };

        let index : Index = {
            name;
            key_details;
            data = CollectionUtils.newBtree(collection);
            used_internally;
            is_unique;
        };

        index;

    };

    public func size(index : Index) : Nat {
        BTree.size(index.data);
    };

    public func insert(
        collection : StableCollection,
        index : Index,
        id : Nat,
        candid_map : T.CandidMap,
    ) : T.Result<(), Text> {

        let index_data_utils = CollectionUtils.getIndexDataUtils(collection);

        let index_key_values = switch (CollectionUtils.getIndexColumns(collection, index.key_details, id, candid_map)) {
            case (?index_key_values) index_key_values;
            case (null) {
                Logger.lazyDebug(
                    collection.logger,
                    func() = "Skipping indexing for document with id " # debug_show id # " because it does not have any values in the index",
                );

                return #ok();
            };
        };

        switch (BTree.put(index.data, index_data_utils, index_key_values, id)) {
            case (null) {};
            case (?prev_id) {
                ignore BTree.put(index.data, index_data_utils, index_key_values, prev_id);

                return #err(
                    "Failed to insert document with id " # debug_show id # " into index " # index.name # ", because a duplicate entry with id " # debug_show prev_id # " already exists"
                );
            };
        };

        Logger.lazyDebug(
            collection.logger,
            func() = "Storing document with id " # debug_show id # " in index " # index.name # ", originally "
            # debug_show (index_key_values) # ", now encoded as " # (
                switch (index_data_utils) {
                    case (#stableMemory(utils)) debug_show utils.key.blobify.to_blob(index_key_values);
                    case (#heap(utils)) debug_show utils.blobify.to_blob(index_key_values);
                }
            ),
        );

        #ok();
    };

    public func remove(collection : StableCollection, index : Index, id : Nat, prev_candid_map : T.CandidMap) : T.Result<(), Text> {
        let index_data_utils = CollectionUtils.getIndexDataUtils(collection);

        let index_key_values = switch (CollectionUtils.getIndexColumns(collection, index.key_details, id, prev_candid_map)) {
            case (?index_key_values) index_key_values;
            case (null) {
                Logger.lazyDebug(
                    collection.logger,
                    func() = "Skipping indexing for document with id " # debug_show id # " because it does not have any values in the index",
                );

                return #ok();
            };
        };

        switch (BTree.remove(index.data, index_data_utils, index_key_values)) {
            case (null) return #err(
                "Failed to remove document with id " # debug_show id # " from index " # index.name # ", because it does not exist"
            );
            case (?prev_id) {
                if (prev_id != id) {

                    ignore BTree.put(index.data, index_data_utils, index_key_values, prev_id);

                    return #err(
                        "Failed to remove document with id " # debug_show id # " from index " # index.name # ", because it was not found in the index. The document with id " # debug_show prev_id # " was found instead"
                    );
                };
            };
        };

        #ok();
    };

    public func clear(collection : StableCollection, index : Index) {
        BTree.clear(index.data);
    };

    public func populateIndex(
        collection : StableCollection,
        index : Index,
    ) : T.Result<(), Text> {
        assert Index.size(index) == 0;

        let doc_store_utils = DocumentStore.getBtreeUtils(collection.documents);

        for ((id, candid_blob) in DocumentStore.entries(collection.documents, doc_store_utils)) {
            let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);
            let candid_map = CandidMap.new(collection.schema_map, id, candid);

            switch (Index.insert(collection, index, id, candid_map)) {
                case (#err(err)) {
                    return #err("populate_index() failed on index '" # index.name # "': " # err);
                };
                case (#ok(_)) {};
            };

        };

        #ok();

    };

    /// Clears the index and repopulates it with all documents from the collection.
    public func repopulateIndex(collection : StableCollection, index : Index) : T.Result<(), Text> {
        // clear the index first
        Index.clear(collection, index);
        populateIndex(collection, index);
    };

    func populateIndexesFromCandidMapDocumentEntries(
        collection : StableCollection,
        indexes : [T.Index],
        document_entries : T.Iter<(Nat, T.CandidMap)>,
        on_start : (T.Index) -> (),
    ) : T.Result<(), Text> {

        for ((id, candid_map) in document_entries) {

            for (index in indexes.vals()) {
                on_start(index);
                switch (Index.insert(collection, index, id, candid_map)) {
                    case (#err(err)) {
                        return #err("populate_index() failed on index '" # index.name # "': " # err);
                    };
                    case (#ok(_)) {};
                };
            };

        };

        #ok();
    };

    public func populateIndexes(
        collection : StableCollection,
        indexes : [T.Index],
    ) : T.Result<(), Text> {

        let candid_map_document_entries = Iter.map<(Nat, Blob), (Nat, T.CandidMap)>(
            DocumentStore.entries(collection.documents, DocumentStore.getBtreeUtils(collection.documents)),
            func((id, candid_blob) : (Nat, Blob)) : (Nat, T.CandidMap) {
                let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);
                (id, CandidMap.new(collection.schema_map, id, candid));
            },
        );

        populateIndexesFromCandidMapDocumentEntries(
            collection,
            indexes,
            candid_map_document_entries,
            func(_index : T.Index) {},
        );

    };

    public func repopulateIndexes(
        collection : StableCollection,
        indexes : [T.Index],
    ) : T.Result<(), Text> {

        let candid_map_document_entries = Iter.map<(Nat, Blob), (Nat, T.CandidMap)>(
            DocumentStore.entries(collection.documents, DocumentStore.getBtreeUtils(collection.documents)),
            func((id, candid_blob) : (Nat, Blob)) : (Nat, T.CandidMap) {
                let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);
                (id, CandidMap.new(collection.schema_map, id, candid));
            },
        );

        populateIndexesFromCandidMapDocumentEntries(
            collection,
            indexes,
            candid_map_document_entries,
            func(index : T.Index) {
                Index.clear(collection, index);
            },
        );
    };

    // public func exists(
    //     collection : StableCollection,
    //     index : Index,
    //     id : Nat,
    //     candid_map : T.CandidMap,
    // ) : Bool {
    //     let index_data_utils = CollectionUtils.getIndexDataUtils(collection);

    //     let index_key_values = switch (CollectionUtils.getIndexColumns(collection, index.key_details, id, candid_map)) {
    //         case (?index_key_values) index_key_values;
    //         case (null) {
    //             Logger.lazyDebug(
    //                 collection.logger,
    //                 func() = "Skipping indexing for document with id " # debug_show id # " because it does not have any values in the index",
    //             );

    //             return false;
    //         };
    //     };

    //     switch (BTree.get(index.data, index_data_utils, index_key_values)) {
    //         case (null) false;
    //         case (?prev_id) {
    //             return true;

    //         };
    //     };
    // };

    let EQUALITY_SCORE = 4;
    let SORT_SCORE = 2;
    let RANGE_SCORE = 1;

    let ADDITIONAL_FILTER_SCORE = 1;
    let ADDITIONAL_SORT_SCORE = 1;

    func operation_eval(
        field : Text,
        op : T.ZqlOperators,
        lower : Map<Text, T.CandidInclusivityQuery>,
        upper : Map<Text, T.CandidInclusivityQuery>,
    ) {
        switch (op) {
            case (#eq(candid)) {
                ignore Map.put(lower, thash, field, #Inclusive(candid));
                ignore Map.put(upper, thash, field, #Inclusive(candid));
            };
            case (#gte(candid)) {
                switch (Map.get(lower, thash, field)) {
                    case (?#Inclusive(val) or ?#Exclusive(val)) {
                        if (Schema.cmpCandid(#Empty, candid, val) == 1) {
                            ignore Map.put(lower, thash, field, #Inclusive(candid));
                        };
                    };
                    case (null) ignore Map.put(lower, thash, field, #Inclusive(candid));
                };
            };
            case (#lte(candid)) {
                switch (Map.get(upper, thash, field)) {
                    case (?#Inclusive(val) or ?#Exclusive(val)) {
                        if (Schema.cmpCandid(#Empty, candid, val) == -1) {
                            ignore Map.put(upper, thash, field, #Inclusive(candid));
                        };
                    };
                    case (null) ignore Map.put(upper, thash, field, #Inclusive(candid));
                };
            };
            case (#lt(candid)) {
                switch (Map.get(upper, thash, field)) {
                    case (?#Inclusive(val) or ?#Exclusive(val)) {
                        let cmp = Schema.cmpCandid(#Empty, candid, val);
                        if (cmp == -1 or cmp == 0) {
                            ignore Map.put(upper, thash, field, #Exclusive(candid));
                        };
                    };
                    case (null) ignore Map.put(upper, thash, field, #Exclusive(candid));
                };
            };
            case (#gt(candid)) {
                switch (Map.get(lower, thash, field)) {
                    case (?#Inclusive(val) or ?#Exclusive(val)) {
                        let cmp = Schema.cmpCandid(#Empty, candid, val);
                        if (cmp == 1 or cmp == 0) {
                            ignore Map.put(lower, thash, field, #Exclusive(candid));
                        };
                    };
                    case (null) ignore Map.put(lower, thash, field, #Exclusive(candid));
                };
            };

            case (#exists) {
                ignore Map.put(lower, thash, field, #Inclusive(#Minimum));
                ignore Map.put(upper, thash, field, #Inclusive(#Maximum));
            };

            // aliases should be handled by the query builder
            case (#anyOf(_) or #between(_, _) or #startsWith(_) or #not_(_)) {
                Debug.trap(debug_show op # " not allowed in this context. Should have been expanded by the query builder");
            };
        };
    };

    public func scan<Document>(
        collection : T.StableCollection,
        index : T.Index,
        start_query : [(Text, ?T.CandidInclusivityQuery)],
        end_query : [(Text, ?T.CandidInclusivityQuery)],
        opt_cursor : ?(Nat, Candid.Candid),
    ) : (Nat, Nat) {
        // Debug.print("start_query: " # debug_show start_query);
        // Debug.print("end_query: " # debug_show end_query);

        let index_data_utils = CollectionUtils.getIndexDataUtils(collection);

        func sort_by_key_details(a : (Text, Any), b : (Text, Any)) : Order {
            let pos_a = switch (Array.indexOf<(Text, SortDirection)>((a.0, #Ascending), index.key_details, Utils.tupleEq(Text.equal))) {
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            let pos_b = switch (Array.indexOf<(Text, SortDirection)>((b.0, #Ascending), index.key_details, Utils.tupleEq(Text.equal))) {
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            if (pos_a > pos_b) return #greater;
            if (pos_a < pos_b) return #less;
            #equal;
        };

        func sort_and_fill_query_entries(
            query_entries : [(Text, ?T.CandidInclusivityQuery)],
            opt_cursor : ?(Nat, T.PaginationDirection),
            is_lower_bound : Bool,
        ) : [(Text, ?T.CandidInclusivityQuery)] {
            let sorted = Array.sort(query_entries, sort_by_key_details);

            Array.tabulate<(Text, ?T.CandidInclusivityQuery)>(
                index.key_details.size(),
                func(i : Nat) : (Text, ?T.CandidInclusivityQuery) {

                    let index_key_tuple = index.key_details[i];

                    switch (opt_cursor) {
                        case (?(id, pagination_direction)) if (index.key_details[i].0 == C.DOCUMENT_ID) {
                            // DOCUMENT_ID is only added in the query if it is a cursor
                            // todo: update based on pagination_direction and is_lower_bound
                            return (C.DOCUMENT_ID, ?#Inclusive(#Nat(id + 1)));
                        };
                        case (null) {};
                    };

                    if (i >= query_entries.size()) {
                        return (index_key_tuple.0, null);
                    };

                    sorted[i];
                },
            );

        };

        // filter null entries and update the last entry to be inclusive by replacing it with the next or previous value
        func format_query_entries(query_entries : [(Text, ?T.CandidInclusivityQuery)], is_lower_bound : Bool) : [T.CandidQuery] {
            if (query_entries.size() == 0) return [];

            let opt_index_of_first_null = Itertools.findIndex(
                query_entries.vals(),
                func(entry : (Text, ?T.CandidInclusivityQuery)) : Bool {
                    entry.1 == null;
                },
            );

            switch (opt_index_of_first_null) {
                case (?0) {
                    // the first null value was found at index 0
                    return [if (is_lower_bound) #Minimum else #Maximum];
                };
                case (_) {};

            };

            let index_of_first_null = Option.get(opt_index_of_first_null, 0);

            func get_new_value_at_index(i : Nat) : T.CandidQuery {
                switch (query_entries[i]) {
                    case ((_field, ?#Inclusive(#Minimum) or ?#Exclusive(#Minimum))) #Minimum;
                    case ((_field, ?#Inclusive(#Maximum) or ?#Exclusive(#Maximum))) #Maximum;
                    case ((_field, ?#Inclusive(value))) value;
                    case ((_field, ?#Exclusive(value))) if (is_lower_bound) {
                        let next = CandidUtils.getNextValue(value);
                        // Debug.print("Retrieving the next value for " # debug_show value);
                        // Debug.print("Next value: " # debug_show next);
                        next;
                    } else {
                        CandidUtils.getPrevValue(value);
                    };
                    case (_) Debug.trap("filter_null_entries_in_query: received null value, should not happen");
                };
            };

            Array.tabulate<T.CandidQuery>(
                Nat.min(index_of_first_null + 2, query_entries.size()),
                func(i : Nat) : T.CandidQuery {
                    let (_field, inclusivity_query) = query_entries[i];

                    return get_new_value_at_index(i);

                },
            );

        };

        let opt_cursor_with_direction : ?(Nat, T.PaginationDirection) = switch (opt_cursor) {
            case (null) null;
            case (?(id, cursor)) {
                ?(id, #Forward);
            };
        };

        let sorted_start_query = sort_and_fill_query_entries(start_query, opt_cursor_with_direction, true);
        let sorted_end_query = sort_and_fill_query_entries(end_query, opt_cursor_with_direction, false);

        Logger.lazyDebug(
            collection.logger,
            func() = "scan after sort and fill: " # debug_show (sorted_start_query, sorted_end_query),
        );

        let start_query_values = format_query_entries(sorted_start_query, true);
        let end_query_values = format_query_entries(sorted_end_query, false);

        Logger.lazyDebug(
            collection.logger,
            func() = "scan after format: " # debug_show (start_query_values, end_query_values),
        );

        Logger.lazyDebug(
            collection.logger,
            func() = "encoded start_query_values: " # debug_show (Orchid.blobify.to_blob(start_query_values)),
        );
        Logger.lazyDebug(
            collection.logger,
            func() = "encoded end_query_values: " # debug_show (Orchid.blobify.to_blob(end_query_values)),
        );

        let scans = BTree.getScanAsInterval(index.data, index_data_utils, ?start_query_values, ?end_query_values);

        Logger.lazyDebug(
            collection.logger,
            func() = "scan interval results: " # debug_show scans # "\nindex size: " # debug_show BTree.size(index.data),
        );

        scans;
    };

    public func fromInterval(
        collection : T.StableCollection,
        index : Index,
        interval : (Nat, Nat),
    ) : [([T.CandidQuery], T.DocumentId)] {
        let index_data_utils = CollectionUtils.getIndexDataUtils(collection);

        let (start, end) = interval;

        Iter.toArray<([T.CandidQuery], T.DocumentId)>(
            BTree.range<[T.CandidQuery], T.DocumentId>(index.data, index_data_utils, start, end)
        );
    };

    public func entries(
        collection : T.StableCollection,
        index : Index,
    ) : T.RevIter<([T.CandidQuery], T.DocumentId)> {
        let index_data_utils = CollectionUtils.getIndexDataUtils(collection);

        BTree.entries<[T.CandidQuery], T.DocumentId>(index.data, index_data_utils);
    };

    // func unwrap_candid_option_value(option : T.CandidQuery) : T.CandidQuery {
    //     switch (option_type) {
    //         case (#Option(inner)) {
    //             unwrap_candid_option_value(inner);
    //         };
    //         case (unwrapped) { unwrapped };
    //     };
    // };

    // func wrap_index_key_with_n_options_from_type(index_key_type : T.CandidType, index_key : T.CandidQuery) : T.CandidQuery {
    //     func helper(index_key_type : T.CandidType) : T.CandidQuery {
    //         switch (index_key_type) {
    //             case (#Option) {
    //                 #Option(index_key);
    //             };
    //             case (other_types) index_key;
    //         };
    //     };

    //     helper(unwrap_candid_option_value(index_key));

    // };

    public func extractBounds(lower : Map<Text, T.CandidInclusivityQuery>, upper : Map<Text, T.CandidInclusivityQuery>, opt_index_key_details : ?[(Text, T.SortDirection)], opt_fully_covered_equality_and_range_fields : ?Set.Set<Text>) : (Bounds, Bounds) {

        assert Option.isSome(opt_index_key_details) == Option.isSome(opt_fully_covered_equality_and_range_fields);

        let scan_bounds = switch (opt_index_key_details) {
            case (null) ([], []);
            case (?index_key_details) {

                let scan_lower_bound = Array.map(
                    index_key_details,
                    func((field, _) : (Text, SortDirection)) : FieldLimit {
                        let lower_bound = switch (Map.get(lower, thash, field)) {
                            case (?lower_bound) lower_bound;
                            case (null) #Inclusive(#Minimum);
                        };

                        (field, ?lower_bound);
                    },
                );

                let scan_upper_bound = Array.map(
                    index_key_details,
                    func((field, _) : (Text, SortDirection)) : FieldLimit {
                        let upper_bound = switch (Map.get(upper, thash, field)) {
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
        let arr1 = Array.tabulate<(Text, ?State<T.CandidQuery>)>(
            max_size,
            func(i : Nat) : (Text, ?State<T.CandidQuery>) {
                let ?(key, value) = iter.next();
                (key, ?value);
            },
        );

        let iter_2 = Map.entries(a);
        let arr2 = Array.tabulate<(Text, ?State<T.CandidQuery>)>(
            max_size,
            func(i : Nat) : (Text, ?State<T.CandidQuery>) {
                let ?(key, _) = iter_2.next();
                let value = Map.get(b, thash, key);
                (key, value);
            },
        );

        let filter_bounds = if (is_lower_bound_larger) (arr1, arr2) else (arr2, arr1);

        (scan_bounds, filter_bounds)

    };

    public func convertSimpleOpsToBounds(is_and_operation : Bool, simple_operations : [(Text, T.ZqlOperators)], opt_index_key_details : ?[(Text, T.SortDirection)], opt_fully_covered_equality_and_range_fields : ?Set.Set<Text>) : (T.Bounds, T.Bounds) {

        let lower_bound = Map.new<Text, T.State<T.CandidQuery>>();
        let upper_bound = Map.new<Text, T.State<T.CandidQuery>>();

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

        extractBounds(lower_bound, upper_bound, opt_index_key_details, opt_fully_covered_equality_and_range_fields);

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

    public func fillFieldMaps(equal_fields : Set.Set<Text>, sort_fields : Buffer<(Text, T.SortDirection)>, range_fields : Set.Set<Text>, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) {

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

    public func getBestIndex(collection : StableCollection, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) : ?BestIndexResult {
        let equal_fields = Set.new<Text>();
        let sort_fields = Buffer.Buffer<(Text, T.SortDirection)>(8);
        let range_fields = Set.new<Text>();
        // let partially_covered_fields = Set.new<Text>();

        fillFieldMaps(equal_fields, sort_fields, range_fields, operations, sort_field);

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

            // Debug.print("scoring indexes");
            label scoring_indexes for ((index_key, direction) in index.key_details.vals()) {
                index_key_details_position += 1;

                // if (index_key == C.DOCUMENT_ID) break scoring_indexes;

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

            // Debug.print("searching_for_holes");
            label searching_for_holes for ((prev, current) in Itertools.slidingTuples(Set.keys(positions_matching_equality_or_range))) {
                if (current - prev > 1) {
                    requires_additional_filtering := true;
                    break searching_for_holes;
                };
            };

            if (num_of_equal_fields_covered > 0 or num_of_range_fields_covered > 0 or num_of_sort_fields_covered > 0) {

                let (scan_bounds, filter_bounds) = convertSimpleOpsToBounds(false, operations, ?index.key_details, ?fully_covered_equality_and_range_fields);

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

    // public func getIndexDataUtils_v1(collection : StableCollection, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) : ?BestIndexResult {
    //     let equal_fields = Set.new<Text>();
    //     let sort_fields = Buffer.Buffer<(Text, T.SortDirection)>(8);
    //     let range_fields = Set.new<Text>();
    //     // let partially_covered_fields = Set.new<Text>();

    //     func fillFieldMaps(equal_fields : Set.Set<Text>, sort_fields : Buffer<(Text, T.SortDirection)>, range_fields : Set.Set<Text>, operations : [(Text, T.ZqlOperators)], sort_field : ?(Text, T.SortDirection)) {

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

    //     fillFieldMaps(equal_fields, sort_fields, range_fields, operations, sort_field);

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

    //             if (index_key == C.DOCUMENT_ID) break scoring_indexes;

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
