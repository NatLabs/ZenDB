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
/// This module acts as an extension of the StableCollection module.
module DocumentStore {

    public type DocumentStore = T.BTree<T.DocumentId, T.Document>;
    public type StableCollection = T.StableCollection;

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

    func getBtreeUtils(collection : StableCollection) : T.BTreeUtils<T.DocumentId, T.Document> {
        switch (collection.documents) {
            case (#stableMemory(_)) StableMemoryUtils;
            case (#heap(_)) HeapUtils;
        };
    };

    public func size(collection : StableCollection) : Nat {
        BTree.size(collection.documents);
    };

    public func clear<K, V>(collection : StableCollection) {
        BTree.clear(collection.documents);
    };

    func extract_candid_blob_from_document_v0(document : T.Document) : Blob {
        switch (document) {
            case (#v0(candid_blob)) {
                candid_blob;
            };
        };
    };

    public func get(collection : StableCollection, key : T.DocumentId) : ?Blob {
        let cmp = getBtreeUtils(collection);
        Option.map<T.Document, Blob>(
            BTree.get(collection.documents, cmp, key),
            extract_candid_blob_from_document_v0,
        );
    };

    public func put(collection : StableCollection, key : T.DocumentId, candid_blob : Blob) : ?Blob {
        let cmp = getBtreeUtils(collection);
        let document = switch (C.CURRENT_DOCUMENT_VERSION) {
            case (0) { #v0(candid_blob) };
            case (_) Debug.trap("Unsupported document version " # debug_show C.CURRENT_DOCUMENT_VERSION);
        };

        Option.map<T.Document, Blob>(
            BTree.put(collection.documents, cmp, key, document),
            extract_candid_blob_from_document_v0,
        );
    };

    public func remove(collection : StableCollection, key : T.DocumentId) : ?Blob {
        let cmp = getBtreeUtils(collection);
        Option.map<T.Document, Blob>(
            BTree.remove(collection.documents, cmp, key),
            extract_candid_blob_from_document_v0,
        );
    };

    public func scan(collection : StableCollection, start_key : ?T.DocumentId, end_key : ?T.DocumentId) : T.RevIter<(T.DocumentId, Blob)> {
        let cmp = getBtreeUtils(collection);
        RevIter.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.scan<T.DocumentId, T.Document>(collection.documents, cmp, start_key, end_key),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );

    };

    public func scanKeys(collection : StableCollection, start_key : ?T.DocumentId, end_key : ?T.DocumentId) : T.RevIter<T.DocumentId> {
        let cmp = getBtreeUtils(collection);
        BTree.scanKeys(collection.documents, cmp, start_key, end_key);
    };

    public func scanVals(collection : StableCollection, start_key : ?T.DocumentId, end_key : ?T.DocumentId) : T.RevIter<Blob> {
        let cmp = getBtreeUtils(collection);
        RevIter.map<T.Document, Blob>(
            BTree.scanVals(collection.documents, cmp, start_key, end_key),
            extract_candid_blob_from_document_v0,
        );
    };

    public func keys(collection : StableCollection) : T.RevIter<T.DocumentId> {
        let cmp = getBtreeUtils(collection);
        BTree.keys(collection.documents, cmp);
    };

    public func vals(collection : StableCollection) : T.RevIter<Blob> {
        let cmp = getBtreeUtils(collection);
        RevIter.map<T.Document, Blob>(
            BTree.vals(collection.documents, cmp),
            extract_candid_blob_from_document_v0,
        );
    };

    public func entries(collection : StableCollection) : T.RevIter<(T.DocumentId, Blob)> {
        let cmp = getBtreeUtils(collection);
        RevIter.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.entries(collection.documents, cmp),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );

    };

    public func range(collection : StableCollection, start : Nat, end : Nat) : T.RevIter<(T.DocumentId, Blob)> {
        let cmp = getBtreeUtils(collection);
        RevIter.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.range(collection.documents, cmp, start, end),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );
    };

    public func range_keys(collection : StableCollection, start : Nat, end : Nat) : T.RevIter<T.DocumentId> {
        let cmp = getBtreeUtils(collection);
        BTree.range_keys(collection.documents, cmp, start, end);
    };

    public func range_vals(collection : StableCollection, start : Nat, end : Nat) : T.RevIter<Blob> {
        let cmp = getBtreeUtils(collection);
        RevIter.map<T.Document, Blob>(
            BTree.range_vals(collection.documents, cmp, start, end),
            extract_candid_blob_from_document_v0,
        );
    };

    public func get_expected_index(collection : StableCollection, key : T.DocumentId) : BpTreeTypes.ExpectedIndex {
        let cmp = getBtreeUtils(collection);
        BTree.getExpectedIndex(collection.documents, cmp, key);
    };

    public func get_scan_as_interval(collection : StableCollection, start_key : ?T.DocumentId, end_key : ?T.DocumentId) : T.Interval {
        let cmp = getBtreeUtils(collection);
        BTree.getScanAsInterval(collection.documents, cmp, start_key, end_key);
    };

    public func getMin(collection : StableCollection) : ?(T.DocumentId, Blob) {
        let cmp = getBtreeUtils(collection);
        Option.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.getMin(collection.documents, cmp),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );
    };

    public func getMax(collection : StableCollection) : ?(T.DocumentId, Blob) {
        let cmp = getBtreeUtils(collection);
        Option.map<(T.DocumentId, T.Document), (T.DocumentId, Blob)>(
            BTree.getMax(collection.documents, cmp),
            func(pair : (T.DocumentId, T.Document)) : (T.DocumentId, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );
    };

};
