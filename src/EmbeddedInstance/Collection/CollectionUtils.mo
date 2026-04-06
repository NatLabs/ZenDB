import Principal "mo:core@2.4/Principal";
import Array "mo:core@2.4/Array";
import Debug "mo:core@2.4/Debug";
import Text "mo:core@2.4/Text";
import Char "mo:core@2.4/Char";
import Nat32 "mo:core@2.4/Nat32";
import Result "mo:core@2.4/Result";
import Order "mo:core@2.4/Order";
import Iter "mo:core@2.4/Iter";
import Buffer "mo:base@0.16/Buffer";
import Nat "mo:core@2.4/Nat";
import Option "mo:core@2.4/Option";
import Hash "mo:base@0.16/Hash";
import Float "mo:core@2.4/Float";
import Int "mo:core@2.4/Int";
import Int32 "mo:core@2.4/Int32";
import Blob "mo:core@2.4/Blob";
import Nat64 "mo:core@2.4/Nat64";
import Int16 "mo:core@2.4/Int16";
import Int64 "mo:core@2.4/Int64";
import Int8 "mo:core@2.4/Int8";
import Nat16 "mo:core@2.4/Nat16";
import Nat8 "mo:core@2.4/Nat8";
import InternetComputer "mo:core@2.4/InternetComputer";

import Map "mo:map@9.0/Map";
import Set "mo:map@9.0/Set";
import Serde "mo:serde@3.5";
import Decoder "mo:serde@3.5/Candid/Blob/Decoder";
import Candid "mo:serde@3.5/Candid";
import Itertools "mo:itertools@0.2/Iter";
import RevIter "mo:itertools@0.2/RevIter";
import ByteUtils "mo:byte-utils@0.2";

import MemoryBTree "mo:memory-collection@0.4/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection@0.4/TypeUtils";
import Int8Cmp "mo:memory-collection@0.4/TypeUtils/Int8Cmp";
import Cmp "mo:augmented-btrees@0.9/Cmp";
import Vector "mo:vector@0.4";

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";

import Orchid "Orchid";
import Schema "Schema";
import C "../Constants";
import Logger "../Logger";
import SchemaMap "SchemaMap";
import BTree "../BTree";
import DocumentStore "DocumentStore";
import TwoQueueCache "../TwoQueueCache";
import Runtime "mo:core@2.4/Runtime";

module CollectionUtils {
    let LOGGER_NAMESPACE = "CollectionUtils";

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    // public type MemoryBTree = MemoryBTree.VersionedMemoryBTree;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

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

    public let { thash; bhash } = Map;

    public func new_btree<K, V>(collection : T.StableCollection) : T.BTree<K, V> {
        switch (collection.memory_type) {
            case (#heap) { BTree.newHeap() };
            case (#stableMemory) {
                switch (Vector.removeLast(collection.freed_btrees)) {
                    case (?memory_btree) {
                        MemoryBTree.clear(memory_btree);
                        #stableMemory(memory_btree);
                    };
                    case (null) {
                        if (Option.get(collection.is_compression_enabled, true)) {
                            BTree.newStableMemory();
                        } else {
                            BTree.newStableMemoryUncompressed();
                        };
                    };
                };
            };
        };
    };

    public func convert_bitmap_8_byte_to_document_id(collection : T.StableCollection, bitmap_value : Nat) : T.DocumentId {
        let n64 = Nat64.fromNat(bitmap_value);

        let n_bytes = ByteUtils.BigEndian.fromNat64(n64);

        Blob.fromArray([
            collection.instance_id[0],
            collection.instance_id[1],
            collection.instance_id[2],
            collection.instance_id[3],
            n_bytes[0],
            n_bytes[1],
            n_bytes[2],
            n_bytes[3],
            n_bytes[4],
            n_bytes[5],
            n_bytes[6],
            n_bytes[7],
        ]);

    };

    public func getMainBtreeUtils(collection : T.StableCollection) : T.BTreeUtils<T.DocumentId, T.Document> {
        switch (collection.documents) {
            case (#stableMemory(_)) DocumentStore.StableMemoryUtils;
            case (#heap(_)) DocumentStore.HeapUtils;
        };
    };

    public func getIndexColumns(collection : T.StableCollection, index_key_details : [(Text, SortDirection)], document_id : T.DocumentId, candid_map : T.CandidMap) : ?[Candid] {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("getIndexColumns");
        let buffer = Buffer.Buffer<Candid>(8);

        var field_columns_excluding_document_id = 0;
        var field_columns_with_missing_value_at_path = 0;

        var option_field_type_count = 0;
        var null_option_field_value_count = 0;

        for ((index_key, dir) in index_key_details.vals()) {
            if (index_key == C.UNIQUE_INDEX_NULL_EXEMPT_ID) {
                let val = if (null_option_field_value_count == option_field_type_count) {
                    #Blob(document_id); // use the document id to ensure the key is unique in the index
                } else {
                    // if at least one optional field has a value, we don't need to exempt the key from the btree's uniqueness restriction
                    // so we can set the value to a dummy value
                    #Blob("" : Blob);
                };

                buffer.add(val)

            } else if (index_key == C.DOCUMENT_ID) {
                buffer.add(#Blob(document_id));
            } else {
                field_columns_excluding_document_id += 1;

                let candidValue = switch (CandidMap.get(candid_map, collection.schema_map, index_key)) {
                    case (?val) {
                        switch (val) {
                            case (#Option(_)) {
                                option_field_type_count += 1;
                            };
                            case (#Null) {
                                option_field_type_count += 1;
                                null_option_field_value_count += 1;
                            };
                            case (_) {};
                        };

                        val;
                    };
                    case (null) {
                        field_columns_with_missing_value_at_path += 1;
                        #Null;
                    };
                };

                buffer.add(candidValue);
            };
        };

        if (field_columns_excluding_document_id == field_columns_with_missing_value_at_path) {
            // In this case, all the index key values for the fields are missing, so we will return a null value so this document is not indexed

            return null;
        };

        let indexKeyValues = Buffer.toArray(buffer);

        log.lazyDebug(
            func() : Text {
                "Retrieved index key values (" # debug_show (indexKeyValues) # ") for index key details (" # debug_show (index_key_details) # ") for id [" # debug_show document_id # "] in collection (" # debug_show collection.name # ")";
            }
        );

        ?indexKeyValues;

    };

    public func stripCandidBlobTypeHeader(collection : T.StableCollection, candid_blob : Blob) : Blob {
        let header_size = Candid.TypedSerializer.getTypeHeader(collection.candid_serializer).size();
        Utils.slice_blob(candid_blob, header_size, candid_blob.size());
    };

    public func appendTypeHeaderToCandidBlob(collection : T.StableCollection, candid_blob : Blob) : Blob {
        let candid_blob_type_header = Candid.TypedSerializer.getTypeHeader(collection.candid_serializer);

        Blob.fromArray(
            Array.concat(
                candid_blob_type_header,
                Blob.toArray(candid_blob),
            )
        );
    };


    public func getOptPartialCandidBlob(collection : T.StableCollection, id : T.DocumentId) : ?Blob {
        DocumentStore.get(collection, id); 
    };

    public func getOptFullCandidBlob(collection : T.StableCollection, id : T.DocumentId) : ?Blob {
        let ?partial_candid_blob = getOptPartialCandidBlob(collection, id) else return null;
        let candid_blob_type_header = Candid.TypedSerializer.getTypeHeader(collection.candid_serializer);

        ?(appendTypeHeaderToCandidBlob(collection, partial_candid_blob));
    };

    public func lookupPartialCandidBlob(collection : T.StableCollection, id : T.DocumentId) : Blob {
        let ?documentCandidBlob : ?Blob = getOptPartialCandidBlob(collection, id) else Runtime.trap("lookupPartialCandidBlob: document not found for id: " # debug_show id);
        
        documentCandidBlob;
    };

    public func lookupFullCandidBlob(collection : T.StableCollection, id : T.DocumentId) : Blob {
        let ?candid_blob = getOptFullCandidBlob(collection, id) else Runtime.trap("lookupFullCandidBlob: document not found for id: " # debug_show id);
        candid_blob;
    };

    public func lookupMotokoDocument<Record>(collection : T.StableCollection, blobify : T.InternalCandify<Record>, id : T.DocumentId) : Record {
        let documentCandidBlob = lookupFullCandidBlob(collection, id);
        let document = blobify.from_blob(documentCandidBlob);
        document;
    };

    public func decodePartialCandidBlob(collection : T.StableCollection, candid_blob : Blob) : Candid.Candid {
        let candid_result = Candid.TypedSerializer.decodeWithNoTypeHeader(collection.candid_serializer, candid_blob);
        let #ok(candid_values) = candid_result else Runtime.trap("decodePartialCandidBlob: decoding candid blob failed: " # debug_show candid_result);
        let candid = candid_values[0];
        candid;
    };


    /// Only used for testing purposes, internally we should be using decodePartialCandidBlob to avoid the overhead of encoding/decoding the type header 
    public func decodeFullCandidBlob(collection : T.StableCollection, candid_blob : Blob) : Candid.Candid {
        let candid_result = Candid.TypedSerializer.decode(collection.candid_serializer, candid_blob);
        let #ok(candid_values) = candid_result else Runtime.trap("decodeFullCandidBlob: decoding candid blob failed: " # debug_show candid_result);
        let candid = candid_values[0];
        candid;
    };

    public func lookupCandidDocument(collection : T.StableCollection, id : T.DocumentId) : ?Candid.Candid {
        let document_details = lookupPartialCandidBlob(collection, id);
        let candid = decodePartialCandidBlob(collection, document_details);

        ?candid;
    };

    public func removeCandidBlob(collection : T.StableCollection, id : T.DocumentId) : ?Blob {
        let ?partial_candid_blob = DocumentStore.remove(collection, id) else return null;
        ?appendTypeHeaderToCandidBlob(collection, partial_candid_blob);
    };

    public func candidMapFilterCondition(collection : T.StableCollection, id : T.DocumentId, candid_map : T.CandidMap, lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)]) : Bool {

        for (((key, opt_lower_val), (upper_key, opt_upper_val)) in Itertools.zip(lower.vals(), upper.vals())) {
            assert key == upper_key;

            //    Debug.print("candid_map: " # debug_show candid_map.extract_candid());

            let field_value = switch (CandidMap.get(candid_map, collection.schema_map, key)) {
                case (?val) val;
                case (null) return false; // nested field is missing
            };

            var res = true;

            switch (opt_lower_val) {
                case (?(#Inclusive(lower_val))) {
                    if (Schema.cmp_candid_ignore_option(collection.schema, field_value, lower_val) == -1) res := false;
                };
                case (?(#Exclusive(lower_val))) {
                    if (Schema.cmp_candid_ignore_option(collection.schema, field_value, lower_val) < 1) res := false;
                };
                case (null) {};
            };

            switch (opt_upper_val) {
                case (?(#Inclusive(upper_val))) {
                    if (Schema.cmp_candid_ignore_option(collection.schema, field_value, upper_val) == 1) res := false;
                };
                case (?(#Exclusive(upper_val))) {
                    if (Schema.cmp_candid_ignore_option(collection.schema, field_value, upper_val) > -1) res := false;
                };
                case (null) {};
            };

            // Debug.print("candidMapFilterCondition(): retrieved field value for key '" # key # "': " # debug_show field_value # ", result: " # debug_show res);

            if (not res) return res;

        };

        true

    };

    func candid_map_multi_filter_condition(
        collection : T.StableCollection,
        id : T.DocumentId,
        candid_map : T.CandidMap,
        bounds : Buffer.Buffer<(lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)])>,
        is_and : Bool,
    ) : Bool {

        func filter_fn(
            (lower, upper) : (([(Text, ?T.CandidInclusivityQuery)], [(Text, ?T.CandidInclusivityQuery)]))
        ) : Bool {
            let res = candidMapFilterCondition(collection, id, candid_map, lower, upper);
            res;
        };

        if (is_and) {
            Itertools.all(bounds.vals(), filter_fn);
        } else {
            Itertools.any(bounds.vals(), filter_fn);
        };
    };

    public func get_composite_index(collection : T.StableCollection, index_name : Text) : T.CompositeIndex {
        let ?index = Map.get(collection.indexes, Map.thash, index_name) else Runtime.trap("Unreachable: IndexMap not found for index: " # index_name);
        let internal_index = switch (index) {
            case (#text_index(text_index)) text_index.internal_index;
            case (#composite_index(composite_index)) composite_index;
        };
    };

    func is_bounds_empty(
        bounds : Buffer.Buffer<T.LowerUpperBounds>
    ) : Bool {
        Itertools.all<T.LowerUpperBounds>(
            bounds.vals(),
            func((lower, upper) : T.LowerUpperBounds) : Bool {
                lower.size() == 0 and upper.size() == 0;
            },
        );
    };

    public func multiFilter(
        collection : T.StableCollection,
        documents : Iter<T.DocumentId>,
        bounds : Buffer.Buffer<(lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)])>,
        is_and : Bool,
    ) : Iter<T.DocumentId> {

        if (is_bounds_empty(bounds)) {
            // No filtering needed, return all documents
            return documents;
        };

        Iter.filter<T.DocumentId>(
            documents,
            func(id : T.DocumentId) : Bool {
                let candid_map = CollectionUtils.get_and_cache_candid_map(collection, id);
                candid_map_multi_filter_condition(collection, id, candid_map, bounds, is_and);
            },
        );
    };

    // Helper function to check if all filter bounds can be satisfied by indexed fields
    public func can_use_indexed_fields_for_filtering(
        indexed_fields : [(Text, Any)],
        bounds : Buffer.Buffer<(lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)])>,
    ) : Bool {

        let indexed_fields_map = Set.new<Text>();
        for ((field_name, _) in indexed_fields.vals()) {
            ignore Set.put(indexed_fields_map, Set.thash, field_name);
        };

        // Check if all fields in bounds are present in indexed fields
        for ((lower, upper) in bounds.vals()) {
            for ((field_name, _) in lower.vals()) {
                switch (Set.has(indexed_fields_map, Set.thash, field_name)) {
                    case (false) return false; // Field not in indexed fields
                    case (true) {};
                };
            };

            // todo: might be redundant to check upper again, but safer to do so
            for ((field_name, _) in upper.vals()) {
                switch (Set.has(indexed_fields_map, Set.thash, field_name)) {
                    case (false) return false; // Field not in indexed fields
                    case (true) {};
                };
            };
        };

        true;
    };

    // Helper function to filter using only indexed fields (without deserializing the full document)
    // Precondition: All fields required by bounds are present in indexed_fields_map
    public func filter_with_indexed_fields(
        collection : T.StableCollection,
        id : T.DocumentId,
        indexed_fields_map : Map.Map<Text, T.Candid>,
        bounds : Buffer.Buffer<(lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)])>,
        is_and : Bool,
    ) : Bool {

        func filter_fn(
            (lower, upper) : (([(Text, ?T.CandidInclusivityQuery)], [(Text, ?T.CandidInclusivityQuery)]))
        ) : Bool {
            for (((key, opt_lower_val), (upper_key, opt_upper_val)) in Itertools.zip(lower.vals(), upper.vals())) {
                assert key == upper_key;

                // Caller should have verified all fields are present - trap if not to catch bugs
                let ?field_value = Map.get(indexed_fields_map, Map.thash, key) else Runtime.trap("filter_with_indexed_fields: field '" # key # "' not found in indexed_fields_map. This indicates a bug in the precondition check.");

                var res = true;

                switch (opt_lower_val) {
                    case (?(#Inclusive(lower_val))) {
                        if (Schema.cmp_candid_ignore_option(collection.schema, field_value, lower_val) == -1) res := false;
                    };
                    case (?(#Exclusive(lower_val))) {
                        if (Schema.cmp_candid_ignore_option(collection.schema, field_value, lower_val) < 1) res := false;
                    };
                    case (null) {};
                };

                switch (opt_upper_val) {
                    case (?(#Inclusive(upper_val))) {
                        if (Schema.cmp_candid_ignore_option(collection.schema, field_value, upper_val) == 1) res := false;
                    };
                    case (?(#Exclusive(upper_val))) {
                        if (Schema.cmp_candid_ignore_option(collection.schema, field_value, upper_val) > -1) res := false;
                    };
                    case (null) {};
                };

                if (not res) return res;
            };

            true;
        };

        if (is_and) {
            Itertools.all(bounds.vals(), filter_fn);
        } else {
            Itertools.any(bounds.vals(), filter_fn);
        };
    };

    // Optimized multiFilter that uses indexed fields when available to avoid document deserialization
    public func multiFilterWithIndexedFields(
        collection : T.StableCollection,
        documents : Iter<(T.DocumentId, ?[(Text, T.Candid)])>,

        // todo: how do we check if we can solely use the indexed fields for this operation, if we have an iterator merged from multiple indexes with different indexed fiedls
        bounds : Buffer.Buffer<(lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)])>,
        is_and : Bool,
    ) : Iter<(T.DocumentId, ?[(Text, T.Candid)])> {

        if (is_bounds_empty(bounds)) {
            // No filtering needed, return all documents
            return documents;
        };

        let indexed_fields_map = Map.new<Text, T.Candid>();

        Iter.filter<(T.DocumentId, ?[(Text, T.Candid)])>(
            documents,
            func((id, opt_indexed_fields) : (T.DocumentId, ?[(Text, T.Candid)])) : Bool {
                Map.clear(indexed_fields_map);

                // Check if indexed fields are available and contain all required fields
                let use_indexed_fields = switch (opt_indexed_fields) {
                    case (?indexed_fields) {
                        can_use_indexed_fields_for_filtering(indexed_fields, bounds);
                    };
                    case (null) false;
                };

                if (use_indexed_fields) {
                    // Populate indexed fields map for filtering
                    let ?indexed_fields = opt_indexed_fields else Runtime.trap("multiFilterWithIndexedFields: indexed_fields should be present");
                    for ((field_name, candid_value) in indexed_fields.vals()) {
                        ignore Map.put(indexed_fields_map, Map.thash, field_name, candid_value);
                    };

                    // Use indexed fields for filtering
                    filter_with_indexed_fields(collection, id, indexed_fields_map, bounds, is_and);
                } else {
                    // Indexed fields not available or incomplete, fall back to loading full document
                    let candid_map = CollectionUtils.get_and_cache_candid_map(collection, id);
                    candid_map_multi_filter_condition(collection, id, candid_map, bounds, is_and);

                };
            },
        );
    };

    public func get_and_cache_candid_map(collection : T.StableCollection, id : T.DocumentId) : T.CandidMap {
        func lookup_candid_map(id : T.DocumentId) : T.CandidMap {
            let ?candid_document = CollectionUtils.lookupCandidDocument(collection, id) else Runtime.trap("Couldn't find document with id: " # debug_show id);
            CandidMap.new(collection.schema_map, id, candid_document);
        };

        // switch (TwoQueueCache.get(collection.candid_map_cache, Map.bhash, id, lookup_candid_map)) {
        //     case (?candid) candid;
        //     case (null) {
        //         let candid_map = lookup_candid_map(id);
        //         TwoQueueCache.put(collection.candid_map_cache, Map.bhash, id, candid_map);
        //         candid_map;
        //     };
        // };

        lookup_candid_map(id);

    };

    public func get_candid_map_no_cache(collection : T.StableCollection, id : T.DocumentId, opt_candid_blob : ?T.CandidBlob) : T.CandidMap {
        func lookup_candid_map(id : T.DocumentId) : T.CandidMap {
            let ?candid_document = CollectionUtils.lookupCandidDocument(collection, id) else Runtime.trap("Couldn't find document with id: " # debug_show id);
            CandidMap.new(collection.schema_map, id, candid_document);
        };

        switch (TwoQueueCache.peek(collection.candid_map_cache, Map.bhash, id)) {
            case (?candid) candid;
            case (null) switch (opt_candid_blob) {
                case (?candid_blob) {
                    let candid = CollectionUtils.decodePartialCandidBlob(collection, candid_blob);
                    CandidMap.new(collection.schema_map, id, candid);
                };
                case (null) lookup_candid_map(id);
            };
        };

    };

    public func put_candid_map_in_cache(collection : T.StableCollection, id : T.DocumentId, candid_map : T.CandidMap) : () {
        // TwoQueueCache.put(collection.candid_map_cache, Map.bhash, id, candid_map);
    };

    public func remove_candid_map_from_cache(collection : T.StableCollection, id : T.DocumentId) : ?T.CandidMap {
        // TwoQueueCache.remove(collection.candid_map_cache, Map.bhash, id);
        null;
    };

};
