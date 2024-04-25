import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Iter "mo:base/Iter";
import Debug "mo:base/Debug";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
import RevIter "mo:itertools/RevIter";

import Migrations "../migrations";
import T "Types";

module MemoryBlock {

    type Address = Nat;
    type MemoryRegion = MemoryRegion.MemoryRegion;
    type LruCache<K, V> = LruCache.LruCache<K, V>;
    type Iter<A> = Iter.Iter<A>;
    type RevIter<A> = RevIter.RevIter<A>;

    public type MemoryMvBTree = Migrations.MemoryMvBTree;
    public type Node = Migrations.Node;
    public type MemoryBlock = T.MemoryBlock;
    type UniqueId = T.UniqueId;

    let { nhash } = LruCache;

    /// blocks region storing multi-value blocks as a linked list
    /// Linked List size - 13 + (20 * n) bytes
    ///
    /// Linked List Block Structure
    /// |--------------------------------|---------------------------------|-----------------------------------------------|
    /// |------------ Header ------------|------------ Key Data  ----------|-------------------- Node ---------------------|
    /// |--------------------------------|---------------------------------|-----------------------------------------------|
    /// |     3 bytes    |     1 byte    |     8 bytes    |     2 bytes    |     8 bytes     |    4 bytes    |    8 bytes  |
    /// |  magic number  |  lyt version  |   key address  |    key size    |  value address  |   value size  |   Next Node | --> Node (1...n) --> NULL
    /// |--------------------------------|---------------------------------|-----------------------------------------------|
    ///
    /// the location of subequent values are stored in the next value offset
    /// 20 bytes are allocated for the each value, 12 bytes for the value and 8 bytes for the next value offset

    let FIRST_NODE_SIZE = 34;
    let SUBSEQUENT_NODE_SIZE = 20;

    let MAGIC_NUMBER : Blob = "LLB";
    let KEY_MEM_BLOCK_ADDRESS_START = 4;
    let KEY_MEM_BLOCK_SIZE_START = 12;
    let HEAD_NODE_START = 14;
    let VAL_MEM_BLOCK_ADDRESS_START = 0;
    let VAL_MEM_BLOCK_SIZE_START = 8;
    let NEXT_NODE_START = 12;
    let ADDRESS_SIZE = 8;

    let NULL_ADDRESS = 18_446_744_073_709_551_615; // max Nat64 val

    func store_blob(btree : MemoryMvBTree, key : Blob) : Address {
        let mb_address = MemoryRegion.allocate(btree.blobs, key.size());
        MemoryRegion.storeBlob(btree.blobs, mb_address, key);
        mb_address;
    };

    public func id_exists(btree : MemoryMvBTree, block_address : Nat) : Bool {
        true;
    };

    /// Creates a new linked list block and returns its address
    public func store(btree : MemoryMvBTree, key : Blob, val : Blob) : UniqueId {
        let key_mb_address = store_blob(btree, key);
        let val_mb_address = store_blob(btree, val);

        // store block in blocks region
        let block_address = MemoryRegion.allocate(btree.blocks, FIRST_NODE_SIZE);

        MemoryRegion.storeBlob(btree.blocks, block_address, MAGIC_NUMBER);
        MemoryRegion.storeNat8(btree.blocks, block_address + 3, 0); // lyt version
        MemoryRegion.storeNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(key_mb_address)); // key mem block address
        MemoryRegion.storeNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START, Nat16.fromNat(key.size())); // key mem block size

        let node = block_address + HEAD_NODE_START;

        MemoryRegion.storeNat64(btree.blocks, node + VAL_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(val_mb_address)); // val mem block address
        MemoryRegion.storeNat32(btree.blocks, node + VAL_MEM_BLOCK_SIZE_START, Nat32.fromNat(val.size())); // val mem block size
        MemoryRegion.storeNat64(btree.blocks, node + NEXT_NODE_START, Nat64.fromNat(NULL_ADDRESS)); // next node address

        block_address;
    };

    public func append(btree : MemoryMvBTree, block_address : Nat, val : Blob) {
        assert MemoryRegion.loadBlob(btree.blocks, block_address, 3) == MAGIC_NUMBER;
        let val_mb_address = store_blob(btree, val);

        // b => c
        // copy b to b'
        // replace b with new value
        // point b to b'
        // b => b' => c
        let b = block_address + HEAD_NODE_START;
        let b_val_address = MemoryRegion.loadNat64(btree.blocks, b + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let b_val_size = MemoryRegion.loadNat16(btree.blocks, b + VAL_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);
        let b_next = MemoryRegion.loadNat64(btree.blocks, b + NEXT_NODE_START) |> Nat64.toNat(_);
        // let b_data = MemoryRegion.loadBlob(btree.blobs, b, SUBSEQUENT_NODE_SIZE);

        let b_prime = MemoryRegion.allocate(btree.blocks, SUBSEQUENT_NODE_SIZE);
        // MemoryRegion.storeBlob(btree.blocks, b_prime, b_data);
        MemoryRegion.storeNat64(btree.blocks, b_prime + VAL_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(b_val_address)); // val mem block address
        MemoryRegion.storeNat32(btree.blocks, b_prime + VAL_MEM_BLOCK_SIZE_START, Nat32.fromNat(b_val_size)); // val mem block size
        MemoryRegion.storeNat64(btree.blocks, b_prime + NEXT_NODE_START, Nat64.fromNat(b_next)); // next node address

        MemoryRegion.storeNat64(btree.blocks, b + VAL_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(val_mb_address)); // val mem block address
        MemoryRegion.storeNat32(btree.blocks, b + VAL_MEM_BLOCK_SIZE_START, Nat32.fromNat(val.size())); // val mem block size
        MemoryRegion.storeNat64(btree.blocks, b + NEXT_NODE_START, Nat64.fromNat(b_prime)); // next node address

    };

    public func replace_val(btree : MemoryMvBTree, block_address : Nat, new_val : Blob) {
        assert MemoryRegion.loadBlob(btree.blocks, block_address, 3) == MAGIC_NUMBER;

        let prev_val_address = MemoryRegion.loadNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let prev_val_size = MemoryRegion.loadNat16(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        let new_val_address = MemoryRegion.resize(btree.blobs, prev_val_address, prev_val_size, new_val.size());
        MemoryRegion.storeBlob(btree.blobs, new_val_address, new_val);

        // update block entry
        MemoryRegion.storeNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(new_val_address));
        MemoryRegion.storeNat32(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START, Nat32.fromNat(new_val.size()));

    };

    public func get_key_from_block(btree : MemoryMvBTree, mb : MemoryBlock) : Blob {
        let blob = MemoryRegion.loadBlob(btree.blobs, mb.0, mb.1);
        blob;
    };

    public func get_key_blob(btree : MemoryMvBTree, block_address : Nat) : Blob {
        assert MemoryRegion.loadBlob(btree.blocks, block_address, 3) == MAGIC_NUMBER;

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        let blob = MemoryRegion.loadBlob(btree.blobs, key_mb_address, key_mb_size);

        blob;
    };

    public func get_key_block(btree : MemoryMvBTree, block_address : Nat) : MemoryBlock {
        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        (key_mb_address, key_mb_size);
    };

    public func get_val_block(btree : MemoryMvBTree, block_address : Nat) : MemoryBlock {
        let node = block_address + HEAD_NODE_START;
        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, node + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let val_mb_size = MemoryRegion.loadNat32(btree.blocks, node + VAL_MEM_BLOCK_SIZE_START) |> Nat32.toNat(_);

        (val_mb_address, val_mb_size);
    };

    public func get_val_blob(btree : MemoryMvBTree, block_address : Nat) : Blob {
        assert MemoryRegion.loadBlob(btree.blocks, block_address, 3) == MAGIC_NUMBER;

        let node = block_address + HEAD_NODE_START;
        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, node + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let val_mb_size = MemoryRegion.loadNat32(btree.blocks, node + VAL_MEM_BLOCK_SIZE_START) |> Nat32.toNat(_);

        let blob = MemoryRegion.loadBlob(btree.blobs, val_mb_address, val_mb_size);

        blob;
    };

    public func get_val_blobs(btree : MemoryMvBTree, block_address : Nat) : Iter<Blob> {
        assert MemoryRegion.loadBlob(btree.blocks, block_address, 3) == MAGIC_NUMBER;

        object {
            var node = block_address + HEAD_NODE_START;
            
            public func next() : ?Blob {
                if (node == NULL_ADDRESS) return null;

                let val_mb_address = MemoryRegion.loadNat64(btree.blocks, node + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
                let val_mb_size = MemoryRegion.loadNat32(btree.blocks, node + VAL_MEM_BLOCK_SIZE_START) |> Nat32.toNat(_);

                let blob = MemoryRegion.loadBlob(btree.blobs, val_mb_address, val_mb_size);

                node := MemoryRegion.loadNat64(btree.blocks, node + NEXT_NODE_START) |> Nat64.toNat(_);

                ?(blob);
            };
        };
    };

    public func remove(btree : MemoryMvBTree, block_address : Nat) {
        assert MemoryRegion.loadBlob(btree.blocks, block_address, 3) == MAGIC_NUMBER;

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);
        MemoryRegion.deallocate(btree.blobs, key_mb_address, key_mb_size);
        MemoryRegion.deallocate(btree.blocks, block_address, HEAD_NODE_START);

        let node = block_address + HEAD_NODE_START;

        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, node + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let val_mb_size = MemoryRegion.loadNat16(btree.blocks, node + VAL_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);
        MemoryRegion.deallocate(btree.blobs, val_mb_address, val_mb_size);

        var current_node = node;

        while (current_node != NULL_ADDRESS) {
            let next_node = MemoryRegion.loadNat64(btree.blocks, current_node + NEXT_NODE_START) |> Nat64.toNat(_);
            MemoryRegion.deallocate(btree.blocks, current_node, SUBSEQUENT_NODE_SIZE);
            current_node := next_node;
        };

    };

    // public func remove_val(btree : MemoryMvBTree, block_address : Nat, matching_val: Blob) {

    //     let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
    //     let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);
    //     MemoryRegion.deallocate(btree.blobs, key_mb_address, key_mb_size);

    //     let val_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
    //     let val_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);
    //     MemoryRegion.deallocate(btree.blobs, val_mb_address, val_mb_size);

    //     MemoryRegion.deallocate(btree.blocks, block_address, FIRST_NODE_SIZE);
    // };

};
