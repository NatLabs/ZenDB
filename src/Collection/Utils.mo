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

    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type InternalCandify<A> = T.InternalCandify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;

    public let { thash; bhash } = Map;

    public func newBtree<K, V>(collection : StableCollection) : T.BTree<K, V> {
        switch (collection.memory_type) {
            case (#heap) { BTree.newHeap() };
            case (#stableMemory) {
                switch (Vector.removeLast(collection.freed_btrees)) {
                    case (?memory_btree) {
                        #stableMemory(memory_btree);
                    };
                    case (null) {
                        BTree.newStableMemory();
                    };
                };
            };
        };
    };

    public func getIndexKeyUtils() : TypeUtils.TypeUtils<[T.CandidQuery]> {
        Orchid.Orchid;
    };

    public func getIndexDataUtils(collection : StableCollection) : T.BTreeUtils<[T.CandidQuery], T.DocumentId> {
        switch (collection.memory_type) {
            case (#stableMemory(_)) {
                #stableMemory(MemoryBTree.createUtils<[T.CandidQuery], T.DocumentId>(Orchid.Orchid, TypeUtils.Nat));
            };
            case (#heap(_)) {
                #heap({
                    blobify = Orchid.Orchid.blobify;
                    cmp = Orchid.Orchid.btree_cmp;
                });
            };
        };

    };

    public func getMainBtreeUtils(collection : StableCollection) : T.BTreeUtils<Nat, T.Document> {
        DocumentStore.getBtreeUtils(collection.documents);
    };

    public func getIndexColumns(collection : T.StableCollection, index_key_details : [(Text, SortDirection)], id : Nat, candid_map : T.CandidMap) : ?[Candid] {
        let buffer = Buffer.Buffer<Candid>(8);

        var field_columns_excluding_document_id = 0;
        var field_columns_with_missing_value_at_path = 0;

        var option_field_type_count = 0;
        var null_option_field_value_count = 0;

        for ((index_key, dir) in index_key_details.vals()) {
            if (index_key == C.UNIQUE_INDEX_NULL_EXEMPT_ID) {
                let val = if (null_option_field_value_count == option_field_type_count) {
                    #Nat(id); // use the document id to ensure the key is unique in the index
                } else {
                    // if at least one optional field has a value, we don't need to exempt the key from the btree's uniqueness restriction
                    // so we can set the value to a dummy value
                    #Nat(0);
                };

                buffer.add(val)

            } else if (index_key == C.DOCUMENT_ID) {
                buffer.add(#Nat(id));
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
                "Retrieved index key values (" # debug_show (indexKeyValues) # ") for index key details (" # debug_show (index_key_details) # ") for id [" # debug_show id # "] in collection (" # debug_show collection.name # ")";
            },
        );

        ?indexKeyValues;

    };

    public func lookupDocument<Record>(collection : T.StableCollection, blobify : T.InternalCandify<Record>, id : Nat) : Record {
        let ?documentDetails = DocumentStore.get(collection.documents, DocumentStore.getBtreeUtils(collection.documents), id) else Debug.trap("lookupDocument: document not found for id: " # debug_show id);
        let document = blobify.from_blob(documentDetails);
        document;
    };

    public func lookupCandidBlob(collection : StableCollection, id : Nat) : Blob {
        let ?documentDetails : ?Blob = DocumentStore.get(collection.documents, DocumentStore.getBtreeUtils(collection.documents), id) else Debug.trap("lookupCandidBlob: document not found for id: " # debug_show id);
        documentDetails;
    };

    public func decodeCandidBlob(collection : StableCollection, candid_blob : Blob) : Candid.Candid {
        let candid_result = Candid.decode(candid_blob, collection.schema_keys, null);
        let #ok(candid_values) = candid_result else Debug.trap("decodeCandidBlob: decoding candid blob failed: " # debug_show candid_result);
        let candid = candid_values[0];
        candid;
    };

    public func lookupCandidDocument(collection : StableCollection, id : Nat) : ?Candid.Candid {
        let ?document_details = DocumentStore.get(collection.documents, DocumentStore.getBtreeUtils(collection.documents), id) else return null;
        let candid = decodeCandidBlob(collection, document_details);

        ?candid;
    };

    // public func lookup_candid_map_bytes(collection : StableCollection, id : Nat) : ?[Nat8] {
    //     let ?document_details = MemoryBTree.get(collection.documents, getMainBtreeUtils(collection), id) else return null;
    //     let bytes = document_details.1;

    //     ?bytes;
    // };

    public func candidMapFilterCondition(collection : StableCollection, id : Nat, candid_document : Candid.Candid, lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)]) : Bool {

        let candid_map = CandidMap.new(collection.schema_map, id, candid_document);

        for (((key, opt_lower_val), (upper_key, opt_upper_val)) in Itertools.zip(lower.vals(), upper.vals())) {
            assert key == upper_key;

            //    Debug.print("candid_map: " # debug_show candid_map.extractCandid());

            let field_value = switch (CandidMap.get(candid_map, collection.schema_map, key)) {
                case (?val) val;
                case (null) return false; // nested field is missing
            };

            var res = true;

            switch (opt_lower_val) {
                case (?(#Inclusive(lower_val))) {
                    if (Schema.cmpCandidIgnoreOption(collection.schema, field_value, lower_val) == -1) res := false;
                };
                case (?(#Exclusive(lower_val))) {
                    if (Schema.cmpCandidIgnoreOption(collection.schema, field_value, lower_val) < 1) res := false;
                };
                case (null) {};
            };

            switch (opt_upper_val) {
                case (?(#Inclusive(upper_val))) {
                    if (Schema.cmpCandidIgnoreOption(collection.schema, field_value, upper_val) == 1) res := false;
                };
                case (?(#Exclusive(upper_val))) {
                    if (Schema.cmpCandidIgnoreOption(collection.schema, field_value, upper_val) > -1) res := false;
                };
                case (null) {};
            };

            // Debug.print("candidMapFilterCondition(): retrieved field value for key '" # key # "': " # debug_show field_value # ", result: " # debug_show res);

            if (not res) return res;

        };

        true

    };

    public func documentIdsFromIndexIntervals(collection : StableCollection, index_name : Text, _intervals : [(Nat, Nat)], sorted_in_reverse : Bool) : Iter<Nat> {

        let intervals = if (sorted_in_reverse) {
            Array.reverse(_intervals);
        } else {
            _intervals;
        };

        // Debug.print("documentIdsFromIndexIntervals: intervals: " # debug_show intervals);

        if (index_name == C.DOCUMENT_ID) {
            let main_btree_utils = DocumentStore.getBtreeUtils(collection.documents);

            let document_ids = Itertools.flatten(
                Iter.map(
                    intervals.vals(),
                    func(interval : (Nat, Nat)) : Iter<(Nat)> {
                        let document_ids = DocumentStore.rangeKeys(collection.documents, main_btree_utils, interval.0, interval.1);

                        if (sorted_in_reverse) {
                            return document_ids.rev();
                        };

                        document_ids;
                    },
                )
            );

            return document_ids;
        };

        let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);

        let index_data_utils = CollectionUtils.getIndexDataUtils(collection);

        Itertools.flatten(
            Iter.map(
                intervals.vals(),
                func(interval : (Nat, Nat)) : Iter<(Nat)> {
                    let document_ids = BTree.rangeVals(index.data, index_data_utils, interval.0, interval.1);

                    if (sorted_in_reverse) {
                        return document_ids.rev();
                    };
                    document_ids;
                },
            )
        );
    };

    public func multiFilter(
        collection : StableCollection,
        documents : Iter<Nat>,
        bounds : Buffer.Buffer<(lower : [(Text, ?T.CandidInclusivityQuery)], upper : [(Text, ?T.CandidInclusivityQuery)])>,
        is_and : Bool,
    ) : Iter<Nat> {

        Iter.filter<Nat>(
            documents,
            func(id : Nat) : Bool {
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
