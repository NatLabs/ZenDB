import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
import RevIter "mo:itertools/RevIter";

import Migrations "../migrations";
import T "Types";

module MemoryBlock {

    type Address = Nat;
    type MemoryRegion = MemoryRegion.MemoryRegion;
    type RevIter<A> = RevIter.RevIter<A>;

    public type MemoryBTreeIndex = Migrations.MemoryBTreeIndex;
    public type Node = Migrations.Node;
    public type MemoryBlock = T.MemoryBlock;

    let {nhash} = LruCache;

    // blocks region
    // header - 64 bytes
    // each entry - 23 bytes
    // -----------------------------------------------
    // |     10 bytes        (8 | 2)    |  8 bytes   |
    // | key mem block (address | size) | Nat value  |
    // -----------------------------------------------

    let BLOCK_HEADER_SIZE = 64;
    let BLOCK_ENTRY_SIZE = 18;

    let KEY_MEM_BLOCK_ADDRESS_START = 0;
    let KEY_MEM_BLOCK_SIZE_START = 8;
    let VAL_START = 10;

    func store_blob(btree : MemoryBTreeIndex, key : Blob) : Address {
        let mb_address = MemoryRegion.allocate(btree.blobs, key.size());
        MemoryRegion.storeBlob(btree.blobs, mb_address, key);
        mb_address
    };

    public func store(btree : MemoryBTreeIndex, key : Blob, val : Nat) : Nat {
        let key_mb_address = store_blob(btree, key);

        // store block in blocks region
        let block_address = MemoryRegion.allocate(btree.blocks, BLOCK_ENTRY_SIZE);
        MemoryRegion.storeNat8(btree.blocks, block_address, 0); // reference count
        MemoryRegion.storeNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(key_mb_address)); // key mem block address
        MemoryRegion.storeNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START, Nat16.fromNat(key.size())); // key mem block size

        MemoryRegion.storeNat64(btree.blocks, block_address + VAL_START, Nat64.fromNat(val));

        block_address
    };

    public func replace_val(btree : MemoryBTreeIndex, block_address : Nat, new_val : Nat) {
        MemoryRegion.storeNat64(btree.blocks, block_address + VAL_START, Nat64.fromNat(new_val));
    };

    public func get_key_blob(btree : MemoryBTreeIndex, block_address : Nat) : Blob {

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        let blob = MemoryRegion.loadBlob(btree.blobs, key_mb_address, key_mb_size);

        blob;
    };

    public func get_key_block(btree : MemoryBTreeIndex, block_address : Nat) : MemoryBlock {

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        (key_mb_address, key_mb_size);
    };

    public func get_val_block(btree : MemoryBTreeIndex, block_address : Nat) : MemoryBlock {
        let val = MemoryRegion.loadNat64(btree.blocks, block_address + VAL_START) |> Nat64.toNat(_);
        (0, 0);
    };

    public func get_val(btree : MemoryBTreeIndex, block_address : Nat) : Nat {
        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + VAL_START) |> Nat64.toNat(_);
    };

    public func remove(btree : MemoryBTreeIndex, block_address : Nat) {

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);
        MemoryRegion.deallocate(btree.blobs, key_mb_address, key_mb_size);

        MemoryRegion.deallocate(btree.blocks, block_address, BLOCK_ENTRY_SIZE);
    };


};
