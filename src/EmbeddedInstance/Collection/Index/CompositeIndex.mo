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
import BitMap "mo:bit-map@0.1.2";
import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";
import Vector "mo:vector@0.4.2";

import T "../../Types";
import CandidMap "../../CandidMap";
import Utils "../../Utils";
import C "../../Constants";
import Logger "../../Logger";
import CandidUtils "../../CandidUtils";
import BTree "../../BTree";

import { Orchid } "../Orchid";

import CollectionUtils "../CollectionUtils";
import Schema "../Schema";

import SchemaMap "../SchemaMap";
import DocumentStore "../DocumentStore";
import MergeSort "../../MergeSort";

module CompositeIndex {

    let LOGGER_NAMESPACE = "CompositeIndex";

    type BestIndexResult = T.BestIndexResult;

    type StableCollection = T.StableCollection;
    type Buffer<A> = Buffer.Buffer<A>;
    type CompositeIndex = T.CompositeIndex;
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
        collection : T.StableCollection,
        name : Text,
        index_key_details : [(Text, SortDirection)],
        is_unique : Bool, // if true, the index is unique and the document ids are not concatenated with the index key values to make duplicate values appear unique
        used_internally : Bool, // cannot be deleted by user if true
    ) : T.CompositeIndex {

        let key_details : [(Text, SortDirection)] = if (is_unique) {
            let contains_option_type = Itertools.any(
                index_key_details.vals(),
                func(index_key_detail : (Text, SortDirection)) : Bool {
                    switch (SchemaMap.get(collection.schema_map, index_key_detail.0)) {
                        case (?#Option(_)) true;
                        case (null) Debug.trap("CompositeIndex key details must be a valid field in the schema map");
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

        let index : CompositeIndex = {
            name;
            key_details;
            data = CollectionUtils.new_btree(collection);
            used_internally;
            is_unique;
        };

        index;

    };

    public func deallocate(collection : T.StableCollection, index : CompositeIndex) {
        BTree.clear(index.data);

        // deallocate the btree if using stable memory
        switch (index.data) {
            case (#stableMemory(memory_btree)) {
                Vector.add(collection.freed_btrees, memory_btree);
            };
            case (#heap(_)) {};
        };

    };

    public func size(index : CompositeIndex) : Nat {
        BTree.size(index.data);
    };

    public func get_index_key_utils() : TypeUtils.TypeUtils<[T.CandidQuery]> {
        Orchid;
    };

    public func get_index_data_utils(collection : T.StableCollection) : T.BTreeUtils<[T.CandidQuery], T.DocumentId> {
        switch (collection.memory_type) {
            case (#stableMemory(_)) {
                #stableMemory(MemoryBTree.createUtils<[T.CandidQuery], T.DocumentId>(Orchid, TypeUtils.Blob));
            };
            case (#heap(_)) {
                #heap({
                    blobify = Orchid.blobify;
                    cmp = Orchid.btree_cmp;
                });
            };
        };

    };

    public func insert(
        collection : T.StableCollection,
        index : CompositeIndex,
        id : Blob,
        values : [T.CandidQuery],
    ) : T.Result<(), Text> {

        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("insert");
        let index_data_utils = get_index_data_utils(collection);

        switch (BTree.put(index.data, index_data_utils, values, id)) {
            case (null) {};
            case (?prev_id) {
                ignore BTree.put(index.data, index_data_utils, values, prev_id);

                return #err(
                    "Failed to insert document with id " # debug_show id # " into index " # index.name # ", because a duplicate entry with id " # debug_show prev_id # " already exists"
                );
            };
        };

        log.lazyDebug(
            func() = "Storing document with id " # debug_show id # " in index " # index.name # ", originally "
            # debug_show (values) # ", now encoded as " # (
                switch (index_data_utils) {
                    case (#stableMemory(utils)) debug_show utils.key.blobify.to_blob(values);
                    case (#heap(utils)) debug_show utils.blobify.to_blob(values);
                }
            )
        );

        #ok()

    };

    public func insertWithCandidMap(
        collection : T.StableCollection,
        index : CompositeIndex,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : T.Result<(), Text> {

        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("insertWithCandidMap");

        let index_key_values = switch (CollectionUtils.getIndexColumns(collection, index.key_details, document_id, candid_map)) {
            case (?index_key_values) index_key_values;
            case (null) {
                log.lazyDebug(
                    func() = "Skipping indexing for document with id " # debug_show document_id # " because it does not have any values in the index"
                );

                return #ok();
            };
        };

        CompositeIndex.insert(collection, index, document_id, index_key_values);

    };

    public func removeWithCandidMap(collection : T.StableCollection, index : CompositeIndex, document_id : T.DocumentId, prev_candid_map : T.CandidMap) : T.Result<(), Text> {

        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("removeWithCandidMap");

        let index_key_values = switch (CollectionUtils.getIndexColumns(collection, index.key_details, document_id, prev_candid_map)) {
            case (?index_key_values) index_key_values;
            case (null) {
                log.lazyDebug(
                    func() = "Skipping indexing for document with id " # debug_show document_id # " because it does not have any values in the index"
                );

                return #ok();
            };
        };

        CompositeIndex.remove(collection, index, document_id, index_key_values);
    };

    public func remove(collection : T.StableCollection, index : CompositeIndex, document_id : T.DocumentId, prev_candid_values : [T.CandidQuery]) : T.Result<(), Text> {
        let index_data_utils = get_index_data_utils(collection);

        switch (BTree.remove(index.data, index_data_utils, prev_candid_values)) {
            case (null) return #err(
                "Failed to remove document with id " # debug_show document_id # " from index " # index.name # ", because it does not exist"
            );
            case (?prev_id) {
                if (prev_id != document_id) {

                    ignore BTree.put(index.data, index_data_utils, prev_candid_values, prev_id);

                    return #err(
                        "Failed to remove document with id " # debug_show document_id # " from index " # index.name # ", because it was not found in the index. The document with id " # debug_show prev_id # " was found instead"
                    );
                };
            };
        };

        #ok();
    };

    public func clear(collection : T.StableCollection, index : CompositeIndex) {
        BTree.clear(index.data);
    };

    public func populate_index(
        collection : T.StableCollection,
        index : CompositeIndex,
    ) : T.Result<(), Text> {
        assert CompositeIndex.size(index) == 0;

        let doc_store_utils = DocumentStore.getBtreeUtils(collection.documents);

        for ((document_id, candid_blob) in DocumentStore.entries(collection.documents, doc_store_utils)) {
            let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);
            let candid_map = CandidMap.new(collection.schema_map, document_id, candid);

            switch (CompositeIndex.insertWithCandidMap(collection, index, document_id, candid_map)) {
                case (#err(err)) {
                    return #err("populate_index() failed on index '" # index.name # "': " # err);
                };
                case (#ok(_)) {};
            };

        };

        #ok();

    };

    /// Clears the index and repopulates it with all documents from the collection.
    public func repopulate_index(collection : T.StableCollection, index : CompositeIndex) : T.Result<(), Text> {
        // clear the index first
        CompositeIndex.clear(collection, index);
        populate_index(collection, index);
    };

    func populate_indexes_From_candid_map_document_entries(
        collection : T.StableCollection,
        indexes : [T.CompositeIndex],
        document_entries : T.Iter<(T.DocumentId, T.CandidMap)>,
        on_start : (T.CompositeIndex) -> (),
    ) : T.Result<(), Text> {

        for ((id, candid_map) in document_entries) {

            for (index in indexes.vals()) {
                on_start(index);
                switch (CompositeIndex.insertWithCandidMap(collection, index, id, candid_map)) {
                    case (#err(err)) {
                        return #err("populate_index() failed on index '" # index.name # "': " # err);
                    };
                    case (#ok(_)) {};
                };
            };

        };

        #ok();
    };

    public func populate_indexes(
        collection : T.StableCollection,
        indexes : [T.CompositeIndex],
    ) : T.Result<(), Text> {

        let candid_map_document_entries = Iter.map<(T.DocumentId, T.CandidBlob), (T.DocumentId, T.CandidMap)>(
            DocumentStore.entries(collection.documents, DocumentStore.getBtreeUtils(collection.documents)),
            func((id, candid_blob) : (T.DocumentId, T.CandidBlob)) : (T.DocumentId, T.CandidMap) {
                let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);
                (id, CandidMap.new(collection.schema_map, id, candid));
            },
        );

        populate_indexes_From_candid_map_document_entries(
            collection,
            indexes,
            candid_map_document_entries,
            func(_index : T.CompositeIndex) {},
        );

    };

    public func repopulate_indexes(
        collection : T.StableCollection,
        indexes : [T.CompositeIndex],
    ) : T.Result<(), Text> {

        let candid_map_document_entries = Iter.map<(T.DocumentId, T.CandidBlob), (T.DocumentId, T.CandidMap)>(
            DocumentStore.entries(collection.documents, DocumentStore.getBtreeUtils(collection.documents)),
            func((id, candid_blob) : (T.DocumentId, T.CandidBlob)) : (T.DocumentId, T.CandidMap) {
                let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);
                (id, CandidMap.new(collection.schema_map, id, candid));
            },
        );

        populate_indexes_From_candid_map_document_entries(
            collection,
            indexes,
            candid_map_document_entries,
            func(index : T.CompositeIndex) {
                CompositeIndex.clear(collection, index);
            },
        );
    };

    // public func exists(
    //     collection : T.StableCollection,
    //     index : CompositeIndex,
    //     id : Blob,
    //     candid_map : T.CandidMap,
    // ) : Bool {
    //     let index_data_utils = get_index_data_utils(collection);

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

    public func scan(
        collection : T.StableCollection,
        index : T.CompositeIndex,
        start_query : [(Text, ?T.CandidInclusivityQuery)],
        end_query : [(Text, ?T.CandidInclusivityQuery)],
        opt_cursor : ?(T.DocumentId, Candid.Candid),
    ) : T.Interval {
        // Debug.print("start_query: " # debug_show start_query);
        // Debug.print("end_query: " # debug_show end_query);

        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("scan");
        let index_data_utils = get_index_data_utils(collection);

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

        func sort_and_fill_query_entries(
            query_entries : [(Text, ?T.CandidInclusivityQuery)],
            opt_cursor : ?(T.DocumentId, T.PaginationDirection),
            is_lower_bound : Bool,
        ) : [(Text, ?T.CandidInclusivityQuery)] {
            let sorted = MergeSort.sort(query_entries, sort_by_key_details);

            Array.tabulate<(Text, ?T.CandidInclusivityQuery)>(
                index.key_details.size(),
                func(i : Nat) : (Text, ?T.CandidInclusivityQuery) {

                    let index_key_tuple = index.key_details[i];

                    switch (opt_cursor) {
                        case (?(id, pagination_direction)) if (index.key_details[i].0 == C.DOCUMENT_ID) {
                            // DOCUMENT_ID is only added in the query if it is a cursor
                            // todo: update based on pagination_direction and is_lower_bound
                            return (
                                C.DOCUMENT_ID,
                                ?#Inclusive(CandidUtils.getNextValue(#Blob(id))),
                            );
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

        // filter null entries and update the last entry to be inclusive or exclusive by keeping it's value or replacing it with the next or previous value respectively
        func format_query_entries(query_entries : [(Text, ?T.CandidInclusivityQuery)], is_lower_bound : Bool) : [T.CandidQuery] {
            if (query_entries.size() == 0) return [];

            let opt_index_of_first_null = Itertools.findIndex(
                query_entries.vals(),
                func(entry : (Text, ?T.CandidInclusivityQuery)) : Bool {
                    entry.1 == null;
                },
            );

            let index_of_first_null = switch (opt_index_of_first_null) {
                case (?0) {
                    // the first null value was found at index 0
                    return [if (is_lower_bound) #Minimum else #Maximum];
                };
                case (?n) n;
                case (null) query_entries.size();
            };

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
                Nat.min(index_of_first_null, query_entries.size()),
                func(i : Nat) : T.CandidQuery {
                    let (_field, inclusivity_query) = query_entries[i];

                    return get_new_value_at_index(i);

                },
            );

        };

        let opt_cursor_with_direction : ?(Blob, T.PaginationDirection) = switch (opt_cursor) {
            case (null) null;
            case (?(id, cursor)) {
                ?(id, #Forward);
            };
        };

        let sorted_start_query = sort_and_fill_query_entries(start_query, opt_cursor_with_direction, true);
        let sorted_end_query = sort_and_fill_query_entries(end_query, opt_cursor_with_direction, false);

        log.lazyDebug(
            func() = "scan after sort and fill: " # debug_show (sorted_start_query, sorted_end_query)
        );

        let start_query_values = format_query_entries(sorted_start_query, true);
        let end_query_values = format_query_entries(sorted_end_query, false);

        log.lazyDebug(
            func() = "scan after format: " # debug_show (start_query_values, end_query_values)
        );

        log.lazyDebug(
            func() = "encoded start_query_values: " # debug_show (Orchid.blobify.to_blob(start_query_values))
        );
        log.lazyDebug(
            func() = "encoded end_query_values: " # debug_show (Orchid.blobify.to_blob(end_query_values))
        );

        let scans = CompositeIndex.scan_with_bounds(collection, index, start_query_values, end_query_values);

        log.lazyDebug(
            func() = "scan interval results: " # debug_show scans # "\nindex size: " # debug_show BTree.size(index.data)
        );

        scans;
    };

    public func scan_with_bounds(
        collection : T.StableCollection,
        index : CompositeIndex,
        lower : [T.CandidQuery],
        upper : [T.CandidQuery],
    ) : (Nat, Nat) {

        let index_data_utils = get_index_data_utils(collection);

        BTree.getScanAsInterval(
            index.data,
            index_data_utils,
            if (lower.size() == 0) null else ?lower,
            if (upper.size() == 0) null else ?upper,
        );
    };

    public func from_interval(
        collection : T.StableCollection,
        index : CompositeIndex,
        interval : T.Interval,
    ) : [([T.CandidQuery], T.DocumentId)] {
        let index_data_utils = get_index_data_utils(collection);

        let (start, end) = interval;

        Iter.toArray<([T.CandidQuery], T.DocumentId)>(
            BTree.range<[T.CandidQuery], T.DocumentId>(index.data, index_data_utils, start, end)
        );
    };

    public func entries(
        collection : T.StableCollection,
        index : CompositeIndex,
    ) : T.RevIter<([T.CandidQuery], T.DocumentId)> {
        let index_data_utils = get_index_data_utils(collection);
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

    public func stats(index : CompositeIndex, collection_entries : Nat) : T.IndexStats {

        let memory = BTree.getMemoryStats(index.data);
        let index_entries = CompositeIndex.size(index); // could be less or more than collection entries depending on the index type and if there are duplicate values

        {
            name = index.name;
            fields = index.key_details;
            entries = index_entries;
            memory;
            is_unique = index.is_unique;
            used_internally = index.used_internally;

            // the index fields values are stored as the keys
            avg_index_key_size = if (collection_entries == 0) 0 else (memory.keyBytes / collection_entries);
            total_index_key_size = memory.keyBytes;

            // includes the key and the document id as the value
            total_index_data_bytes = memory.dataBytes;
        };
    };

};
