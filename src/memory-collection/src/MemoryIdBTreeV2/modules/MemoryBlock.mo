import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Debug "mo:base/Debug";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
import RevIter "mo:itertools/RevIter";

import Migrations "../Migrations";
import T "Types";

module MemoryBlock {
    
    /// blocks region
    /// header - 64 bytes
    /// each entry - 23 bytes

    /// Memory Layout - (15 bytes)
    ///
    /// | Field           | Size (bytes) | Description |
    /// |-----------------|--------------|-------------|
    /// | reference count |  1           | reference count                     |
    /// | address         |  8           | address of key blob in blobs region |
    /// | key size        |  2           | size of key blob                    |
    /// | value size      |  4           | size of value blob                  |

    type Address = Nat;
    type MemoryRegion = MemoryRegion.MemoryRegion;
    type LruCache<K, V> = LruCache.LruCache<K, V>;
    type RevIter<A> = RevIter.RevIter<A>;

    public type MemoryBTree = Migrations.MemoryBTree;
    public type Node = Migrations.Node;
    public type MemoryBlock = T.MemoryBlock;
    type UniqueId = T.UniqueId;

    let {nhash} = LruCache;

    let BLOCK_HEADER_SIZE = 64;
    let BLOCK_ENTRY_SIZE = 15;

    let REFERENCE_COUNT_START = 0;
    let MEM_BLOCK_ADDRESS_START = 1;
    let KEY_SIZE_START = 9;
    let VAL_SIZE_START = 11;

    func store_blob(btree : MemoryBTree, key : Blob) : Address {
        let mb_address = MemoryRegion.allocate(btree.blobs, key.size());
        MemoryRegion.storeBlob(btree.blobs, mb_address, key);
        mb_address
    };

    func store_kv_pair(btree : MemoryBTree, key : Blob, val: Blob) : Address {
        let mb_address = MemoryRegion.allocate(btree.blobs, key.size() + val.size());
        MemoryRegion.storeBlob(btree.blobs, mb_address, key);
        MemoryRegion.storeBlob(btree.blobs, mb_address + key.size(), val);

        mb_address
    };

    func get_location_from_id(id : UniqueId) : Address {
        (id * BLOCK_ENTRY_SIZE) + BLOCK_HEADER_SIZE
    };

    func get_id_from_location(address : Address) : UniqueId {
        (address - BLOCK_HEADER_SIZE) / BLOCK_ENTRY_SIZE
    };

    public func id_exists(btree : MemoryBTree, id : UniqueId) : Bool {
        let block_address = get_location_from_id(id);
        true
    };

    public func store(btree : MemoryBTree, key : Blob, val : Blob) : UniqueId {
        let kv_address = store_kv_pair(btree, key, val);

        // store block in blocks region
        let block_address = MemoryRegion.allocate(btree.blocks, BLOCK_ENTRY_SIZE);
        MemoryRegion.storeNat8(btree.blocks, block_address, 0); // reference count
        MemoryRegion.storeNat64(btree.blocks, block_address + MEM_BLOCK_ADDRESS_START, Nat64.fromNat(kv_address)); // key mem block address
        MemoryRegion.storeNat16(btree.blocks, block_address + KEY_SIZE_START, Nat16.fromNat(key.size())); // key mem block size
        MemoryRegion.storeNat32(btree.blocks, block_address + VAL_SIZE_START, Nat32.fromNat(val.size())); // val mem block size

        get_id_from_location(block_address)
    };

    public func next_id(btree : MemoryBTree) : UniqueId {
        let block_address = MemoryRegion.allocate(btree.blocks, BLOCK_ENTRY_SIZE);
       
        let _next_id = get_id_from_location(block_address);
        MemoryRegion.deallocate(btree.blocks, block_address, BLOCK_ENTRY_SIZE);
        _next_id
    };

    public func get_ref_count(btree : MemoryBTree, id : UniqueId) : Nat {
        let block_address = get_location_from_id(id);
        let ref_count = MemoryRegion.loadNat8(btree.blocks, block_address + REFERENCE_COUNT_START);
        Nat8.toNat(ref_count)
    };

    public func increment_ref_count(btree : MemoryBTree, id : UniqueId) {
        let block_location = get_location_from_id(id);
        let ref_count = MemoryRegion.loadNat8(btree.blocks, block_location + REFERENCE_COUNT_START);
        MemoryRegion.storeNat8(btree.blocks, block_location + REFERENCE_COUNT_START, ref_count + 1);
    };

    public func decrement_ref_count(btree : MemoryBTree, id : UniqueId) : Nat {
        let block_location = get_location_from_id(id);
        let ref_count = MemoryRegion.loadNat8(btree.blocks, block_location + REFERENCE_COUNT_START);

        if (ref_count == 0) return 0;
        MemoryRegion.storeNat8(btree.blocks, block_location + REFERENCE_COUNT_START, ref_count - 1);

        Nat8.toNat(ref_count - 1);
    };

    public func replace_val(btree : MemoryBTree, id : UniqueId, new_val : Blob) {
        let block_address = get_location_from_id(id);

        let prev_kv_address = MemoryRegion.loadNat64(btree.blocks, block_address + MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let prev_key_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_SIZE_START) |> Nat16.toNat(_);
        let prev_val_size = MemoryRegion.loadNat16(btree.blocks, block_address + VAL_SIZE_START) |> Nat16.toNat(_);

        if (prev_val_size == new_val.size()) {
            MemoryRegion.storeBlob(btree.blobs, prev_kv_address + prev_key_size, new_val);
            return;
        };

        let key_blob = MemoryRegion.loadBlob(btree.blobs, prev_kv_address, prev_key_size);

        let prev_kv_size = prev_key_size + prev_val_size;
        let new_kv_size = key_blob.size() + new_val.size();

        let new_kv_address = MemoryRegion.resize(btree.blobs, prev_kv_address, prev_kv_size, new_kv_size);

        if (new_kv_address == prev_kv_address) { // new size < old size
            MemoryRegion.storeBlob(btree.blobs, new_kv_address + prev_key_size, new_val);
            MemoryRegion.storeNat32(btree.blocks, block_address + VAL_SIZE_START, Nat32.fromNat(new_val.size()));
            return;
        };

        MemoryRegion.storeBlob(btree.blobs, new_kv_address, key_blob);
        MemoryRegion.storeBlob(btree.blobs, new_kv_address + key_blob.size(), new_val);

        MemoryRegion.storeNat64(btree.blocks, block_address + MEM_BLOCK_ADDRESS_START, Nat64.fromNat(new_kv_address));
        MemoryRegion.storeNat32(btree.blocks, block_address + VAL_SIZE_START, Nat32.fromNat(new_val.size()));
    };

    public func get_key_blob(btree : MemoryBTree, id : UniqueId) : Blob {
        let block_address = get_location_from_id(id);

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_SIZE_START) |> Nat16.toNat(_);

        let blob = MemoryRegion.loadBlob(btree.blobs, key_mb_address, key_mb_size);

        blob;
    };

    public func get_key_block(btree : MemoryBTree, id : UniqueId) : MemoryBlock {
        let block_address = get_location_from_id(id);

        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_SIZE_START) |> Nat16.toNat(_);

        (key_mb_address, key_mb_size);
    };

    public func get_val_block(btree : MemoryBTree, id : UniqueId) : MemoryBlock {
        let block_address = get_location_from_id(id);

        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_SIZE_START) |> Nat16.toNat(_);
        let val_mb_size = MemoryRegion.loadNat32(btree.blocks, block_address + VAL_SIZE_START) |> Nat32.toNat(_);

        (val_mb_address + key_size, val_mb_size);
    };

    public func get_val_blob(btree : MemoryBTree, id : UniqueId) : Blob {
        let block_address = get_location_from_id(id);

        let val_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_SIZE_START) |> Nat16.toNat(_);
        let val_mb_size = MemoryRegion.loadNat32(btree.blocks, block_address + VAL_SIZE_START) |> Nat32.toNat(_);

        let blob = MemoryRegion.loadBlob(btree.blobs, val_mb_address + key_size, val_mb_size);

        blob;
    };

    public func remove(btree : MemoryBTree, id : UniqueId) {
        let block_address = get_location_from_id(id);

        assert MemoryRegion.loadNat8(btree.blocks, block_address + REFERENCE_COUNT_START) == 0;
        
        let key_mb_address = MemoryRegion.loadNat64(btree.blocks, block_address + MEM_BLOCK_ADDRESS_START) |> Nat64.toNat(_);
        let key_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + KEY_SIZE_START) |> Nat16.toNat(_);
        let val_mb_size = MemoryRegion.loadNat16(btree.blocks, block_address + VAL_SIZE_START) |> Nat16.toNat(_);
        MemoryRegion.deallocate(btree.blobs, key_mb_address, val_mb_size + key_mb_size);

        MemoryRegion.deallocate(btree.blocks, block_address, BLOCK_ENTRY_SIZE);
    };


};
