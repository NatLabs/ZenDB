import Debug "mo:base/Debug";
import Option "mo:base/Option";

import BpTree "mo:augmented-btrees/BpTree";
import Cmp "mo:augmented-btrees/Cmp";
import BpTreeTypes "mo:augmented-btrees/BpTree/Types";
import BpTreeMethods "mo:augmented-btrees/BpTree/Methods";
import MemoryBTree "mo:memory-collection@0.3.0/MemoryBTree/Stable";
import RevIter "mo:itertools/RevIter";

import T "../Types";
import C "../Constants";
import BTree "../BTree";
import Utils "../Utils";

/// BTree api wrapper for storing documents and handling their various versions.
module DocumentStore {

    public type DocumentStore = T.BTree<Nat, T.Document>;

    public func newStableMemory() : T.BTree<Nat, T.Document> {
        #stableMemory(MemoryBTree.new(?C.STABLE_MEMORY_BTREE_ORDER));
    };

    public func newHeap() : T.BTree<Nat, T.Document> {
        #heap(BpTree.new(?C.HEAP_BTREE_ORDER));
    };

    public func new(is_stable_memory : Bool) : T.BTree<Nat, T.Document> {
        if (is_stable_memory) {
            newStableMemory();
        } else {
            newHeap();
        };
    };

    let stable_memory_document_blobify : T.Blobify<T.Document> = {
        to_blob = func(document : T.Document) : Blob {
            switch (document) {
                case (#v0(candid_blob)) {
                    Utils.concatBlob("\00", candid_blob);
                };
            };
        };
        from_blob = func(blob : Blob) : T.Document {
            let version_id = blob.get(0);

            switch (version_id) {
                case (0) { #v0(Utils.sliceBlob(blob, 1, blob.size())) };
                case (_) Debug.trap("Decoding document failed: Unsupported version id " # debug_show version_id);
            };

        };
    };

    let stable_memory_document_typeutils : T.TypeUtils<T.Document> = {
        blobify = stable_memory_document_blobify;
        cmp = #BlobCmp(Cmp.Blob);
    };

    public let StableMemoryUtils : T.BTreeUtils<Nat, T.Document> = #stableMemory(
        {
            key = Utils.typeutils_nat_as_nat64;
            value = stable_memory_document_typeutils;
        } : T.MemoryBTreeUtils<Nat, T.Document>
    );

    public let HeapUtils : T.BTreeUtils<Nat, T.Document> = #heap(
        {
            blobify = Utils.typeutils_nat_as_nat64.blobify;
            cmp = Cmp.Blob;
        } : T.BpTreeUtils<Nat>
    );

    public func getBtreeUtils(store : DocumentStore) : T.BTreeUtils<Nat, T.Document> {
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

    public func get(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>, key : Nat) : ?Blob {
        Option.map<T.Document, Blob>(
            BTree.get(store, cmp, key),
            extract_candid_blob_from_document_v0,
        );
    };

    public func put(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>, key : Nat, candid_blob : Blob) : ?Blob {

        let document = switch (C.CURRENT_DOCUMENT_VERSION) {
            case (0) { #v0(candid_blob) };
            case (_) Debug.trap("Unsupported document version " # debug_show C.CURRENT_DOCUMENT_VERSION);
        };

        Option.map<T.Document, Blob>(
            BTree.put(store, cmp, key, document),
            extract_candid_blob_from_document_v0,
        );
    };

    public func remove(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>, key : Nat) : ?Blob {
        Option.map<T.Document, Blob>(
            BTree.remove(store, cmp, key),
            extract_candid_blob_from_document_v0,
        );
    };

    public func scan(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>, start_key : ?Nat, end_key : ?Nat) : T.RevIter<(Nat, Blob)> {
        RevIter.map<(Nat, T.Document), (Nat, Blob)>(
            BTree.scan<Nat, T.Document>(store, cmp, start_key, end_key),
            func(pair : (Nat, T.Document)) : (Nat, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );

    };

    public func keys(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>) : T.RevIter<Nat> {
        BTree.keys(store, cmp);
    };

    public func vals(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>) : T.RevIter<Blob> {
        RevIter.map<T.Document, Blob>(
            BTree.vals(store, cmp),
            extract_candid_blob_from_document_v0,
        );
    };

    public func entries(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>) : T.RevIter<(Nat, Blob)> {
        RevIter.map<(Nat, T.Document), (Nat, Blob)>(
            BTree.entries(store, cmp),
            func(pair : (Nat, T.Document)) : (Nat, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );

    };

    public func range(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>, start : Nat, end : Nat) : T.RevIter<(Nat, Blob)> {
        RevIter.map<(Nat, T.Document), (Nat, Blob)>(
            BTree.range(store, cmp, start, end),
            func(pair : (Nat, T.Document)) : (Nat, Blob) {
                (pair.0, extract_candid_blob_from_document_v0(pair.1));
            },
        );
    };

    public func rangeKeys(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>, start : Nat, end : Nat) : T.RevIter<Nat> {
        BTree.rangeKeys(store, cmp, start, end);
    };

    public func rangeVals(store : DocumentStore, cmp : T.BTreeUtils<Nat, T.Document>, start : Nat, end : Nat) : T.RevIter<Blob> {
        RevIter.map<T.Document, Blob>(
            BTree.rangeVals(store, cmp, start, end),
            extract_candid_blob_from_document_v0,
        );
    };

    public func getExpectedIndex<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, key : K) : BpTreeTypes.ExpectedIndex {
        BTree.getExpectedIndex(btree, cmp, key);
    };

    public func getScanAsInterval<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start_key : ?K, end_key : ?K) : (Nat, Nat) {
        BTree.getScanAsInterval(btree, cmp, start_key, end_key);
    };

};
