import Nat "mo:base/Nat";
import Blob "mo:base/Blob";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
import RevIter "mo:itertools/RevIter";

import Migrations "../Migrations";
import T "Types";

module MemoryBlock {

    type Address = Nat;
    type MemoryRegion = MemoryRegion.MemoryRegion;
    type LruCache<K, V> = LruCache.LruCache<K, V>;
    type RevIter<A> = RevIter.RevIter<A>;

    public type MemoryBTree = Migrations.MemoryBTree;
    public type Node = Migrations.Node;
    public type MemoryBlock = T.MemoryBlock;

    let {nhash} = LruCache;

    public func store_key(btree : MemoryBTree, key : Blob) : MemoryBlock {
        let mb_address = MemoryRegion.allocate(btree.blobs, key.size());
        MemoryRegion.storeBlob(btree.blobs, mb_address, key);

        // LruCache.put(btree.key_cache, nhash, mb_address, key);

        (mb_address, key.size());
    };

    public func store_val(btree : MemoryBTree, val : Blob) : MemoryBlock {
        let mb_address = MemoryRegion.allocate(btree.blobs, val.size());
        MemoryRegion.storeBlob(btree.blobs, mb_address, val);

        (mb_address, val.size());
    };

    public func replace_val(btree : MemoryBTree, prev_block : MemoryBlock, val : Blob) : MemoryBlock {
        let new_mb_address = MemoryRegion.resize(btree.blobs, prev_block.0, prev_block.1, val.size());
        MemoryRegion.storeBlob(btree.blobs, new_mb_address, val);

        (new_mb_address, val.size());
    };

    public func get_key(btree : MemoryBTree, mb : MemoryBlock) : Blob {
        switch(LruCache.get(btree.key_cache, nhash, mb.0)){
            case (?key_blob) return key_blob;
            case (_){};
        };
        
        let blob = MemoryRegion.loadBlob(btree.blobs, mb.0, mb.1);
        LruCache.put(btree.key_cache, nhash, mb.0, blob);
        blob;
    };

    public func peek_key(btree : MemoryBTree, mb : MemoryBlock) : Blob {
        switch(LruCache.get(btree.key_cache, nhash, mb.0)){
            case (?key_blob) return key_blob;
            case (_){};
        };
        
        let blob = MemoryRegion.loadBlob(btree.blobs, mb.0, mb.1);
        blob;
    };

    public func get_val(btree : MemoryBTree, mb : MemoryBlock) : Blob {
        let blob = MemoryRegion.loadBlob(btree.blobs, mb.0, mb.1);
        blob;
    };

    public func remove_val(btree : MemoryBTree, mb : MemoryBlock) {
        MemoryRegion.deallocate(btree.blobs, mb.0, mb.1);
    };

    public func remove_key(btree : MemoryBTree, mb : MemoryBlock) {
        MemoryRegion.deallocate(btree.blobs, mb.0, mb.1);
        ignore LruCache.remove(btree.key_cache, nhash, mb.0);
    };
};
