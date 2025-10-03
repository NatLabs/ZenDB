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
import Float "mo:base@0.16.0/Float";
import Int "mo:base@0.16.0/Int";
import Int32 "mo:base@0.16.0/Int32";
import Blob "mo:base@0.16.0/Blob";
import Nat64 "mo:base@0.16.0/Nat64";
import Int16 "mo:base@0.16.0/Int16";
import Int64 "mo:base@0.16.0/Int64";
import Int8 "mo:base@0.16.0/Int8";
import Nat16 "mo:base@0.16.0/Nat16";
import Nat8 "mo:base@0.16.0/Nat8";
import InternetComputer "mo:base@0.16.0/ExperimentalInternetComputer";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.3.2";
import Decoder "mo:serde@3.3.2/Candid/Blob/Decoder";
import Candid "mo:serde@3.3.2/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import BitMap "mo:bit-map@0.1.2";
import ByteUtils "mo:byte-utils@0.1.1";

import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";
import Int8Cmp "mo:memory-collection@0.3.2/TypeUtils/Int8Cmp";
import Cmp "mo:augmented-btrees@0.7.1/Cmp";
import Vector "mo:vector@0.4.2";

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
module CollectionUtils {

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
                        BTree.newStableMemory();
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
        DocumentStore.getBtreeUtils(collection.documents);
    };

    public func getIndexColumns(collection : T.StableCollection, index_key_details : [(Text, SortDirection)], document_id : T.DocumentId, candid_map : T.CandidMap) : ?[Candid] {
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

        Logger.lazyDebug(
            collection.logger,
            func() : Text {
                "Retrieved index key values (" # debug_show (indexKeyValues) # ") for index key details (" # debug_show (index_key_details) # ") for id [" # debug_show document_id # "] in collection (" # debug_show collection.name # ")";
            },
        );

        ?indexKeyValues;

    };

    public func lookupDocument<Record>(collection : T.StableCollection, blobify : T.InternalCandify<Record>, id : T.DocumentId) : Record {
        let ?documentDetails = DocumentStore.get(collection.documents, DocumentStore.getBtreeUtils(collection.documents), id) else Debug.trap("lookupDocument: document not found for id: " # debug_show id);
        let document = blobify.from_blob(documentDetails);
        document;
    };

    public func lookupCandidBlob(collection : T.StableCollection, id : T.DocumentId) : Blob {
        let ?documentDetails : ?Blob = DocumentStore.get(collection.documents, DocumentStore.getBtreeUtils(collection.documents), id) else Debug.trap("lookupCandidBlob: document not found for id: " # debug_show id);
        documentDetails;
    };

    public func decodeCandidBlob(collection : T.StableCollection, candid_blob : Blob) : Candid.Candid {
        let candid_result = Candid.decode(candid_blob, collection.schema_keys, null);
        let #ok(candid_values) = candid_result else Debug.trap("decodeCandidBlob: decoding candid blob failed: " # debug_show candid_result);
        let candid = candid_values[0];
        candid;
    };

    public func lookupCandidDocument(collection : T.StableCollection, id : T.DocumentId) : ?Candid.Candid {
        let ?document_details = DocumentStore.get(collection.documents, DocumentStore.getBtreeUtils(collection.documents), id) else return null;
        let candid = decodeCandidBlob(collection, document_details);

        ?candid;
    };

    public func candidMapFilterCondition(collection : T.StableCollection, id : T.DocumentId, candid_document : Candid.Candid, lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)]) : Bool {

        let candid_map = CandidMap.new(collection.schema_map, id, candid_document);

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

    public func multiFilter(
        collection : T.StableCollection,
        documents : Iter<T.DocumentId>,
        bounds : Buffer.Buffer<(lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)])>,
        is_and : Bool,
    ) : Iter<T.DocumentId> {

        Iter.filter<T.DocumentId>(
            documents,
            func(id : T.DocumentId) : Bool {
                let ?candid = CollectionUtils.lookupCandidDocument(collection, id) else Debug.trap("multiFilter: candid_map_bytes not found");

                func filter_fn(
                    (lower, upper) : (([(Text, ?T.CandidInclusivityQuery)], [(Text, ?T.CandidInclusivityQuery)]))
                ) : Bool {

                    let res = candidMapFilterCondition(collection, id, candid, lower, upper);

                    res;
                };

                let res = if (is_and) {
                    Itertools.all(bounds.vals(), filter_fn);
                } else {
                    Itertools.any(bounds.vals(), filter_fn);
                };

                res;
            },
        );
    };

};
