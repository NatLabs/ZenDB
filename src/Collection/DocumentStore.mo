import Debug "mo:base@0.16.0/Debug";
import Option "mo:base@0.16.0/Option";

import BpTree "mo:augmented-btrees@0.7.1/BpTree";
import Cmp "mo:augmented-btrees@0.7.1/Cmp";
import BpTreeTypes "mo:augmented-btrees@0.7.1/BpTree/Types";
import BpTreeMethods "mo:augmented-btrees@0.7.1/BpTree/Methods";
import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";
import RevIter "mo:itertools@0.2.2/RevIter";

import T "../Types";
import C "../Constants";
import BTree "../BTree";
import Utils "../Utils";

/// BTree api wrapper for storing documents and handling their various versions.
module DocumentStore {

    public type DocumentStore = T.BTree<T.DocumentId, T.Document>;

    public func new_stable_memory() : T.BTree<T.DocumentId, T.Document> {
        #stableMemory(MemoryBTree.new(?C.STABLE_MEMORY_BTREE_ORDER));
    };

    public func new_heap() : T.BTree<T.DocumentId, T.Document> {
        #heap(BpTree.new(?C.HEAP_BTREE_ORDER));
    };

    public func new(is_stable_memory : Bool) : T.BTree<T.DocumentId, T.Document> {
        if (is_stable_memory) {
            new_stable_memory();
        } else {
            new_heap();
        };
    };

    let stable_memory_document_blobify : T.Blobify<T.Document> = {
        to_blob = func(document : T.Document) : Blob {
            switch (document) {
                case (#v0(candid_blob)) {
                    Utils.concat_blob("\00", candid_blob);
                };
            };
        };
        from_blob = func(blob : Blob) : T.Document {
            let version_id = blob.get(0);

            switch (version_id) {
                case (0) { #v0(Utils.slice_blob(blob, 1, blob.size())) };
                case (_) Debug.trap("Decoding document failed: Unsupported version id " # debug_show version_id);
            };

        };
    };

    let stable_memory_document_typeutils : T.TypeUtils<T.Document> = {
        blobify = stable_memory_document_blobify;
        cmp = #BlobCmp(Cmp.Blob);
    };

    public let StableMemoryUtils : T.BTreeUtils<T.DocumentId, T.Document> = #stableMemory(
        {
            key = TypeUtils.Blob;
            value = stable_memory_document_typeutils;
        } : T.MemoryBTreeUtils<T.DocumentId, T.Document>
    );

    public let HeapUtils : T.BTreeUtils<T.DocumentId, T.Document> = #heap(
        {
            blobify = TypeUtils.Blobify.Blob;
            cmp = Cmp.Blob;
        } : T.BpTreeUtils<T.DocumentId>
    );

    public func getBtreeUtils(store : DocumentStore) : T.BTreeUtils<T.DocumentId, T.Document> {
        switch (store) {
            case (#stableMemory(_)) StableMemoryUtils;
            case (#heap(_)) HeapUtils;
        };
    };

    public func size(store : DocumentStore) : Nat {
        BTree.size(store);
    };

    public func clear<K, V>(store : DocumentStore) {
        BTree.clear(store);
    };

    func extract_candid_blob_from_document_v0(document : T.Document) : Blob {
        switch (document) {
            case (#v0(candid_blob)) {
                candid_blob;
            };
        };
    };

    public func get(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, key : T.DocumentId) : ?Blob {
        Option.map<T.Document, Blob>(
            BTree.get(store, cmp, key),
            extract_candid_blob_from_document_v0,
        );
    };

    public func put(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, key : T.DocumentId, candid_blob : Blob) : ?Blob {

        let document = switch (C.CURRENT_DOCUMENT_VERSION) {
            case (0) { #v0(candid_blob) };
            case (_) Debug.trap("Unsupported document version " # debug_show C.CURRENT_DOCUMENT_VERSION);
        };

        Option.map<T.Document, Blob>(
            BTree.put(store, cmp, key, document),
            extract_candid_blob_from_document_v0,
        );
    };

    public func remove(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, key : T.DocumentId) : ?Blob {
        Option.map<T.Document, Blob>(
            BTree.remove(store, cmp, key),
            extract_candid_blob_from_document_v0,
        );
    };

    public func scan(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, start_key : ?T.DocumentId, end_key : ?T.DocumentId) : T.RevIter<(T.DocumentId, Blob)> {
        RevIter.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.scan<T.DocumentId, T.Document>(store, cmp, start_key, end_key),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );

    };

    public func scanKeys(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, start_key : ?T.DocumentId, end_key : ?T.DocumentId) : T.RevIter<T.DocumentId> {
        BTree.scanKeys(store, cmp, start_key, end_key);
    };

    public func scanVals(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, start_key : ?T.DocumentId, end_key : ?T.DocumentId) : T.RevIter<Blob> {
        RevIter.map<T.Document, Blob>(
            BTree.scanVals(store, cmp, start_key, end_key),
            extract_candid_blob_from_document_v0,
        );
    };

    public func keys(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>) : T.RevIter<T.DocumentId> {
        BTree.keys(store, cmp);
    };

    public func vals(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>) : T.RevIter<Blob> {
        RevIter.map<T.Document, Blob>(
            BTree.vals(store, cmp),
            extract_candid_blob_from_document_v0,
        );
    };

    public func entries(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>) : T.RevIter<(T.DocumentId, Blob)> {
        RevIter.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.entries(store, cmp),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );

    };

    public func range(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : T.RevIter<(T.DocumentId, Blob)> {
        RevIter.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.range(store, cmp, start, end),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );
    };

    public func range_keys(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : T.RevIter<T.DocumentId> {
        BTree.range_keys(store, cmp, start, end);
    };

    public func range_vals(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : T.RevIter<Blob> {
        RevIter.map<T.Document, Blob>(
            BTree.range_vals(store, cmp, start, end),
            extract_candid_blob_from_document_v0,
        );
    };

    public func get_expected_index<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, key : K) : BpTreeTypes.ExpectedIndex {
        BTree.getExpectedIndex(btree, cmp, key);
    };

    public func get_scan_as_interval<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start_key : ?K, end_key : ?K) : T.Interval {
        BTree.getScanAsInterval(btree, cmp, start_key, end_key);
    };

    public func getMin(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>) : ?(T.DocumentId, Blob) {
        Option.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.getMin(store, cmp),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );
    };

    public func getMax(store : DocumentStore, cmp : T.BTreeUtils<T.DocumentId, T.Document>) : ?(T.DocumentId, Blob) {
        Option.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.getMax(store, cmp),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );
    };

};
