import Nat "mo:base/Nat";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
import RevIter "mo:itertools/RevIter";
// import Branch "mo:augmented-btrees/BpTree/Branch";

import Blobify "../../Blobify";
import MemoryCmp "../../MemoryCmp";

module {
    public type Address = Nat;
    type Size = Nat;

    public type MemoryBlock = (Address, Size);

    type MemoryRegionV1 = MemoryRegion.MemoryRegionV1;
    type LruCache<K, V> = LruCache.LruCache<K, V>;
    type Blobify<A> = Blobify.Blobify<A>;
    type RevIter<A> = RevIter.RevIter<A>;

    public type MemoryCmp<A> = MemoryCmp.MemoryCmp<A>;

    public type Leaf = (
        nats : [var Nat], // [address, index, count]
        adjacent_nodes : [var ?Nat], // [parent, prev, next] (is_root if parent is null)
        key_blocks : [var ?(MemoryBlock)], // [... ((key address, key size), key blob)]
        val_blocks : [var ?(MemoryBlock)],
        kv_blobs : [var ?(Blob, Blob)],
        _branch_children_nodes : [var ?Nat], // [... child address]
        _branch_keys_blobs : [var ?Blob],
    );

    type Entry<A> = {
        #Blob : Blob;
        #Deserialized : (Blob, A);
    };

    public type Branch = (
        nats : [var Nat], // [address, index, count, subtree_size]
        parent : [var ?Nat], // parent
        key_blocks : [var ?(MemoryBlock)], // [... ((key address, key size), key blob)]
        _leaf_val_blocks : [var ?(MemoryBlock)],
        _leaf_kv_blobs : [var ?(Blob, Blob)],
        children_nodes : [var ?Nat], // [... child address]
        keys_blobs : [var ?Blob],
    );

    public type MemoryUtils<K, V> = (
        key : Blobify<K>,
        value : Blobify<V>,
        cmp : MemoryCmp<K>,
    );

    public type Node = {
        #leaf : Leaf;
        #branch : Branch;
    };

    public type NodeType = {
        #branch;
        #leaf;
    };

    public type MemoryIdBTree = {
        is_set : Bool; // is true, only keys are stored
        order : Nat;
        var count : Nat;
        var root : Nat;
        var branch_count : Nat; // number of branch nodes
        var leaf_count : Nat; // number of leaf nodes

        metadata : MemoryRegionV1;
        blocks : MemoryRegionV1;
        blobs : MemoryRegionV1;

        nodes_cache : LruCache<Address, Node>;
    };


};
