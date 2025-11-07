import Debug "mo:base@0.16.0/Debug";
import Option "mo:base@0.16.0/Option";

import BpTree "mo:augmented-btrees@0.7.1/BpTree";
import BpTreeTypes "mo:augmented-btrees@0.7.1/BpTree/Types";
import BpTreeMethods "mo:augmented-btrees@0.7.1/BpTree/Methods";
import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import RevIter "mo:itertools@0.2.2/RevIter";

import T "Types";
import C "Constants"

/// BTree api wrapper around the stable memory and heap btree implementations.
module BTree {

    public func newStableMemory<K, V>() : T.BTree<K, V> {
        #stableMemory(MemoryBTree.new(?C.STABLE_MEMORY_BTREE_ORDER));
    };

    public func newHeap<K, V>() : T.BTree<K, V> {
        #heap(BpTree.new<Blob, V>(?C.HEAP_BTREE_ORDER));
    };

    public func new<K, V>(is_stable_memory : Bool) : T.BTree<K, V> {
        if (is_stable_memory) {
            newStableMemory();
        } else {
            newHeap();
        };
    };

    public func size<K, V>(btree : T.BTree<K, V>) : Nat {
        switch (btree) {
            case (#stableMemory(memory_btree)) {
                return MemoryBTree.size(memory_btree);
            };
            case (#heap(heap_btree)) {
                return BpTree.size(heap_btree);
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func clear<K, V>(btree : T.BTree<K, V>) {
        switch (btree) {
            case (#stableMemory(memory_btree)) {
                MemoryBTree.clear(memory_btree);
            };
            case (#heap(heap_btree)) {
                BpTree.clear(heap_btree);
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func get<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, key : K) : ?V {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.get(memory_btree, memory_btree_utils, key);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.get(heap_btree, heap_btree_utils.cmp, heap_btree_utils.blobify.to_blob(key));
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func put<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, key : K, value : V) : ?V {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.insert(memory_btree, memory_btree_utils, key, value);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.insert(heap_btree, heap_btree_utils.cmp, heap_btree_utils.blobify.to_blob(key), value);
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func remove<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, key : K) : ?V {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.remove(memory_btree, memory_btree_utils, key);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.remove(heap_btree, heap_btree_utils.cmp, heap_btree_utils.blobify.to_blob(key));
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    func tuple_map_blob_to_key<K, V>(pair : (Blob, V), heap_btree_utils : T.BpTreeUtils<K>) : (K, V) {
        let key = heap_btree_utils.blobify.from_blob(pair.0);
        (key, pair.1);
    };

    public func scan<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start_key : ?K, end_key : ?K) : T.RevIter<(K, V)> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.scan(memory_btree, memory_btree_utils, start_key, end_key);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.scan<Blob, V>(
                    heap_btree,
                    heap_btree_utils.cmp,
                    Option.map(start_key, heap_btree_utils.blobify.to_blob),
                    Option.map(end_key, heap_btree_utils.blobify.to_blob),
                ) |> RevIter.map<(Blob, V), (K, V)>(
                    _,
                    func(pair : (Blob, V)) : (K, V) {
                        let key = heap_btree_utils.blobify.from_blob(pair.0);
                        (key, pair.1);
                    },
                );
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func scanKeys<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start_key : ?K, end_key : ?K) : T.RevIter<K> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.scanKeys(memory_btree, memory_btree_utils, start_key, end_key);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.scan<Blob, V>(
                    heap_btree,
                    heap_btree_utils.cmp,
                    Option.map(start_key, heap_btree_utils.blobify.to_blob),
                    Option.map(end_key, heap_btree_utils.blobify.to_blob),
                ) |> RevIter.map<(Blob, V), K>(
                    _,
                    func(pair : (Blob, V)) : K {
                        heap_btree_utils.blobify.from_blob(pair.0);
                    },
                );
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func scanVals<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start_key : ?K, end_key : ?K) : T.RevIter<V> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.scanVals(memory_btree, memory_btree_utils, start_key, end_key);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.scan<Blob, V>(
                    heap_btree,
                    heap_btree_utils.cmp,
                    Option.map(start_key, heap_btree_utils.blobify.to_blob),
                    Option.map(end_key, heap_btree_utils.blobify.to_blob),
                ) |> RevIter.map<(Blob, V), V>(
                    _,
                    func(pair : (Blob, V)) : V {
                        pair.1;
                    },
                );
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func keys<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>) : T.RevIter<K> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.keys(memory_btree, memory_btree_utils);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return RevIter.map<Blob, K>(
                    BpTree.keys(heap_btree),
                    func(blob_key : Blob) : K {
                        let key = heap_btree_utils.blobify.from_blob(blob_key);
                        key;
                    },
                );
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func vals<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>) : T.RevIter<V> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.vals(memory_btree, memory_btree_utils);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.vals(heap_btree);
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func entries<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>) : T.RevIter<(K, V)> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.entries(memory_btree, memory_btree_utils);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return RevIter.map<(Blob, V), (K, V)>(
                    BpTree.entries(heap_btree),
                    func(pair : (Blob, V)) : (K, V) {
                        let key = heap_btree_utils.blobify.from_blob(pair.0);
                        (key, pair.1);
                    },
                );

            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func range<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start : Nat, end : Nat) : T.RevIter<(K, V)> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.range(memory_btree, memory_btree_utils, start, end);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return RevIter.map<(Blob, V), (K, V)>(
                    BpTree.range<Blob, V>(heap_btree, start, end),
                    func(pair : (Blob, V)) : (K, V) {
                        let key = heap_btree_utils.blobify.from_blob(pair.0);
                        (key, pair.1);
                    },
                );

            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func range_keys<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start : Nat, end : Nat) : T.RevIter<K> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.rangeKeys(memory_btree, memory_btree_utils, start, end);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return RevIter.map<Blob, K>(
                    BpTree.rangeKeys(heap_btree, start, end),
                    func(blob_key : Blob) : K {
                        let key = heap_btree_utils.blobify.from_blob(blob_key);
                        key;
                    },
                );

            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func range_vals<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start : Nat, end : Nat) : T.RevIter<V> {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.rangeVals(memory_btree, memory_btree_utils, start, end);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.rangeVals(heap_btree, start, end);
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func getExpectedIndex<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, key : K) : BpTreeTypes.ExpectedIndex {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.getExpectedIndex(memory_btree, memory_btree_utils, key);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                return BpTree.getExpectedIndex<Blob, V>(heap_btree, heap_btree_utils.cmp, heap_btree_utils.blobify.to_blob(key));
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func getMin<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>) : ?(K, V) {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.getMin(memory_btree, memory_btree_utils);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                switch (BpTree.min(heap_btree)) {
                    case (?pair) {
                        let key = heap_btree_utils.blobify.from_blob(pair.0);
                        ?(key, pair.1);
                    };
                    case (null) null;
                };
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func getMax<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>) : ?(K, V) {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                return MemoryBTree.getMax(memory_btree, memory_btree_utils);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                switch (BpTree.max(heap_btree)) {
                    case (?pair) {
                        let key = heap_btree_utils.blobify.from_blob(pair.0);
                        ?(key, pair.1);
                    };
                    case (null) null;
                };
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    func memorybtree_scan_interval<K, V>(
        btree : T.MemoryBTree,
        btree_utils : MemoryBTree.BTreeUtils<K, V>,
        start_key : ?K,
        end_key : ?K,
    ) : T.Interval {

        let start_rank = switch (start_key) {
            case (?key) switch (MemoryBTree.getExpectedIndex(btree, btree_utils, key)) {
                case (#Found(rank)) rank;
                case (#NotFound(rank)) rank;
            };
            case (null) 0;
        };

        let end_rank = switch (end_key) {
            case (?key) {
                let res = MemoryBTree.getExpectedIndex(btree, btree_utils, key);
                // Debug.print("end_key expected index: " # debug_show (res));

                switch (res) {
                    case (#Found(rank)) rank + 1;
                    case (#NotFound(rank)) rank;
                };
            };
            case (null) MemoryBTree.size(btree);
        };

        if (start_rank > end_rank) {
            // the data is sorted in reverse order
            // (end_rank, start_rank);
            Debug.trap("Invalid scan interval: start_rank > end_rank");
        } else {
            (start_rank, end_rank);
        };

    };

    func bptree_scan_interval<K, V>(
        btree : BpTree.BpTree<Blob, V>,
        heap_btree_utils : T.BpTreeUtils<K>,
        start_key : ?K,
        end_key : ?K,
    ) : T.Interval {

        let start_rank = switch (start_key) {
            case (?key) {
                let res = BpTree.getExpectedIndex<Blob, V>(btree, heap_btree_utils.cmp, heap_btree_utils.blobify.to_blob(key));
                // Debug.print("start_key expected index: " # debug_show (res));

                switch (res) {
                    case (#Found(rank)) rank;
                    case (#NotFound(rank)) rank;
                };
            };
            case (null) 0;
        };

        let end_rank = switch (end_key) {
            case (?key) {
                let res = BpTree.getExpectedIndex<Blob, V>(btree, heap_btree_utils.cmp, heap_btree_utils.blobify.to_blob(key));
                // Debug.print("end_key expected index: " # debug_show (res));
                switch (res) {
                    case (#Found(rank)) rank + 1;
                    case (#NotFound(rank)) rank;
                };
            };
            case (null) BpTree.size(btree);
        };

        (start_rank, end_rank);
    };

    public func getScanAsInterval<K, V>(btree : T.BTree<K, V>, cmp : T.BTreeUtils<K, V>, start_key : ?K, end_key : ?K) : T.Interval {
        switch (btree, cmp) {
            case (#stableMemory(memory_btree), #stableMemory(memory_btree_utils)) {
                memorybtree_scan_interval(memory_btree, memory_btree_utils, start_key, end_key);
            };
            case (#heap(heap_btree), #heap(heap_btree_utils)) {
                bptree_scan_interval<K, V>(heap_btree, heap_btree_utils, start_key, end_key);
            };
            case (_) {
                Debug.trap("Invalid BTree type");
            };
        };
    };

    public func getMemoryStats<K, V>(btree : T.BTree<K, V>) : T.MemoryBTreeStats {
        switch (btree) {
            case (#stableMemory(btree)) { MemoryBTree.stats(btree) };
            case (#heap(_)) {
                // This data is not available for the heap-based B-Tree
                {
                    allocatedPages = 0;
                    bytesPerPage = 0;
                    allocatedBytes = 0;
                    usedBytes = 0;
                    freeBytes = 0;
                    dataBytes = 0;
                    metadataBytes = 0;
                    leafBytes = 0;
                    branchBytes = 0;
                    keyBytes = 0;
                    valueBytes = 0;
                    leafCount = 0;
                    branchCount = 0;
                    totalNodeCount = 0;
                };
            };
        };

    };

};
