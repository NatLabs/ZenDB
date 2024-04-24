import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";

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

    public type MemoryIdBTree = Migrations.MemoryIdBTree;
    public type Node = Migrations.Node;
    public type MemoryBlock = T.MemoryBlock;
    type UniqueId = T.UniqueId;

    let {nhash} = LruCache;

    // blocks region
    // header - 64 bytes
    // each entry - 23 bytes
    // ---------------------------------------------------------------------------------------
    // |      1 byte     |     10 bytes        (8 | 2)    |        12 bytes       (8 | 4)    |
    // | reference count | key mem block (address | size) | value mem block (address | size) |
    // ---------------------------------------------------------------------------------------

    let BLOCK_HEADER_SIZE = 64;
    let BLOCK_ENTRY_SIZE = 23;

    let REFERENCE_COUNT_START = 0;
    let KEY_MEM_BLOCK_ADDRESS_START = 1;
    let KEY_MEM_BLOCK_SIZE_START = 9;
    let VAL_MEM_BLOCK_ADDRESS_START = 11;
    let VAL_MEM_BLOCK_SIZE_START = 19;

    func store_blob(btree : MemoryIdBTree, key : Blob) : Address {
        let mb_address = MemoryRegion.allocate(btree.blobs, key.size());
        MemoryRegion.storeBlob(btree.blobs, mb_address, key);
        mb_address
    };

    func get_location_from_id(id : UniqueId) : Address {
        (id * BLOCK_ENTRY_SIZE) + BLOCK_HEADER_SIZE
    };

    func get_id_from_location(address : Address) : UniqueId {
        (address - BLOCK_HEADER_SIZE) / BLOCK_ENTRY_SIZE
    };

    public func id_exists(btree : MemoryIdBTree, id : UniqueId) : Bool {
        true
    };

    public func store(btree : MemoryIdBTree, key : Blob, val : Blob) : UniqueId {
        let key_mb_address = store_blob(btree, key);
        let val_mb_address = store_blob(btree, val);

        // store block in blocks region
        let block_address = MemoryRegion.allocate(btree.blocks, BLOCK_ENTRY_SIZE);
        MemoryRegion.storeNat8(btree.blocks, block_address, 0); // reference count
        MemoryRegion.storeNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(key_mb_address)); // key mem block address
        MemoryRegion.storeNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START, Nat16.fromNat(key.size())); // key mem block size

        MemoryRegion.storeNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(val_mb_address)); // val mem block address
        MemoryRegion.storeNat32(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START, Nat32.fromNat(val.size())); // val mem block size

        get_id_from_location(block_address)
    };

    public func next_id(btree : MemoryIdBTree) : UniqueId {
        let block_address = MemoryRegion.allocate(btree.blocks, BLOCK_ENTRY_SIZE);
        MemoryRegion.storeNat8(btree.blocks, block_address, 0); // reference count
       
        let _next_id = get_id_from_location(block_address);
        MemoryRegion.deallocate(btree.blocks, block_address, BLOCK_ENTRY_SIZE);
        _next_id
    };

    public func get_ref_count(btree : MemoryIdBTree, id : UniqueId) : Nat {
        let block_address = get_location_from_id(id);
        let ref_count = MemoryRegion.loadNat8(btree.blocks, block_address + REFERENCE_COUNT_START);
        Nat8.toNat(ref_count)
    };

    func update_ref_count(btree : MemoryIdBTree, id : UniqueId, ref_count : Nat) {
        let block_address = get_location_from_id(id);
        MemoryRegion.storeNat8(btree.blocks, block_address + REFERENCE_COUNT_START, Nat8.fromNat(ref_count));
    };

    public func increment_ref_count(btree : MemoryIdBTree, id : UniqueId) {
        let block_location = get_location_from_id(id);
        let ref_count = MemoryRegion.loadNat8(btree.blocks, block_location + REFERENCE_COUNT_START);
        MemoryRegion.storeNat8(btree.blocks, block_location + REFERENCE_COUNT_START, ref_count + 1);
    };

    public func decrement_ref_count(btree : MemoryIdBTree, id : UniqueId) : Nat {
        let block_location = get_location_from_id(id);
        let ref_count = MemoryRegion.loadNat8(btree.blocks, block_location + REFERENCE_COUNT_START);

        if (ref_count == 0) return 0;
        MemoryRegion.storeNat8(btree.blocks, block_location + REFERENCE_COUNT_START, ref_count - 1);

        Nat8.toNat(ref_count - 1);
    };

    public func replace_val(btree : MemoryIdBTree, id : UniqueId, new_val : Blob) {
        let block_address = get_location_from_id(id);

        let prev_val_address = MemoryRegion.loadNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let prev_val_size = MemoryRegion.loadNat16(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        let new_val_address = MemoryRegion.resize(btree.blobs, prev_val_address, prev_val_size, new_val.size());
        MemoryRegion.storeBlob(btree.blobs, new_val_address, new_val);

        // update block entry
        MemoryRegion.storeNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START, Nat64.fromNat(new_val_address));
        MemoryRegion.storeNat32(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START, Nat32.fromNat(new_val.size()));

    };

    public func get_key_from_block(btree : MemoryIdBTree, mb : MemoryBlock) : Blob {
        let blob = MemoryRegion.loadBlob(btree.blobs, mb.0, mb.1);
        blob;
    };

    public func get_key_blob(btree : MemoryIdBTree, id : UniqueId) : Blob {
        let block_address = get_location_from_id(id);

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        let blob = MemoryRegion.loadBlob(btree.blobs, key_mb_address, key_mb_size);

        blob;
    };

    public func get_key_block(btree : MemoryIdBTree, id : UniqueId) : MemoryBlock {
        let block_address = get_location_from_id(id);

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        (key_mb_address, key_mb_size);
    };

    public func get_val_block(btree : MemoryIdBTree, id : UniqueId) : MemoryBlock {
        let block_address = get_location_from_id(id);

        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let val_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        (val_mb_address, val_mb_size);
    };

    public func get_val_blob(btree : MemoryIdBTree, id : UniqueId) : Blob {
        let block_address = get_location_from_id(id);

        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let val_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);

        let blob = MemoryRegion.loadBlob(btree.blobs, val_mb_address, val_mb_size);

        blob;
    };

    public func remove(btree : MemoryIdBTree, id : UniqueId) {
        let block_address = get_location_from_id(id);

        assert MemoryRegion.loadNat8(btree.blocks, block_address + REFERENCE_COUNT_START) == 0;
        
        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + KEY_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);
        MemoryRegion.deallocate(btree.blobs, key_mb_address, key_mb_size);

        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + VAL_MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let val_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + VAL_MEM_BLOCK_SIZE_START) |> Nat16.toNat(_);
        MemoryRegion.deallocate(btree.blobs, val_mb_address, val_mb_size);

        MemoryRegion.deallocate(btree.blocks, block_address, BLOCK_ENTRY_SIZE);
    };


};
