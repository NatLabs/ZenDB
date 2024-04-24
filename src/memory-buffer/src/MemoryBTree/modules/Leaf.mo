import Debug "mo:base/Debug";
import Array "mo:base/Array";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Int "mo:base/Int";
import Float "mo:base/Float";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
// import Branch "mo:augmented-btrees/BpTree/Branch";

import MemoryFns "MemoryFns";
import MemoryBlock "MemoryBlock";
import T "Types";
import Migrations "../Migrations";

module Leaf {
    public type Leaf = Migrations.Leaf;
    type Address = T.Address;
    type MemoryBTree = Migrations.MemoryBTree;
    type MemoryBlock = T.MemoryBlock;
    type BTreeUtils<K, V> = T.BTreeUtils<K, V>;
    type NodeType = Migrations.Node;

    let { nhash } = LruCache;
    public let HEADER_SIZE = 64;

    public let MAGIC_START = 0;
    public let MAGIC_SIZE = 3;

    public let NODE_TYPE_START = 3;
    public let NODE_TYPE_SIZE = 1;

    public let LAYOUT_VERSION_START = 4;
    public let LAYOUT_VERSION_SIZE = 1;

    public let INDEX_START = 5;
    public let INDEX_SIZE = 2;

    public let COUNT_START = 7;
    public let COUNT_SIZE = 2;

    public let PARENT_START = 9;
    public let ADDRESS_SIZE = 8;

    public let PREV_START = 17;

    public let NEXT_START = 25;

    public let KEYS_START = HEADER_SIZE;
    public let MAX_KEY_SIZE = 2;

    public let MAX_VALUE_SIZE = 4;

    public let KEY_MEMORY_BLOCK_SIZE = 10;
    public let VALUE_MEMORY_BLOCK_SIZE = 12;

    // access constants
    public let AC = {
        ADDRESS = 0;
        INDEX = 1;
        COUNT = 2;

        PARENT = 0;
        PREV = 1;
        NEXT = 2;
    };

    public let Flags = {
        IS_LEAF_MASK : Nat8 = 0x80;
        IS_ROOT_MASK : Nat8 = 0x40; // DOUBLES AS HAS_PARENT_MASK
        HAS_PREV_MASK : Nat8 = 0x20;
        HAS_NEXT_MASK : Nat8 = 0x10;
    };

    public let KV_MEMORY_BLOCK_SIZE = 14; // ADDRESS_SIZE + MAX_KEY_SIZE + MAX_VALUE_SIZE
    // public let KV_MEMORY_BLOCK_SIZE_FOR_SET = 10; // ADDRESS_SIZE + MAX_KEY_SIZE

    public let NULL_ADDRESS : Nat64 = 0x00;

    public let MAGIC : Blob = "BTN";

    public let LAYOUT_VERSION : Nat8 = 0;

    public let NODE_TYPE : Nat8 = 1; // leaf

    public func get_memory_size(btree : MemoryBTree) : Nat {
        let bytes_per_node =
        // MAGIC_SIZE // magic
        // + LAYOUT_VERSION_SIZE // layout version
        // + NODE_TYPE_SIZE // node type
        // + ADDRESS_SIZE // parent address
        // + INDEX_SIZE // Node's position in parent node
        // + ADDRESS_SIZE // prev leaf address
        // + ADDRESS_SIZE // next leaf address
        // + COUNT_SIZE // number of elements in the node
        64
        // keys
        + (
            (
                ADDRESS_SIZE // address of memory block
                + MAX_KEY_SIZE // key size
            ) * btree.order
        )
        // values
        + (
            (
                ADDRESS_SIZE // address of memory block
                + MAX_VALUE_SIZE // value size
            ) * btree.order
        );

        bytes_per_node;
    };

    public func get_keys_offset(leaf_address : Nat, i : Nat) : Nat {
        leaf_address + KEYS_START + (i * (Leaf.MAX_KEY_SIZE + Leaf.ADDRESS_SIZE));
    };

    public func get_vals_offset(btree : MemoryBTree, leaf_address : Nat, i : Nat) : Nat {
        get_keys_offset(leaf_address, btree.order) + (i * VALUE_MEMORY_BLOCK_SIZE);
    };

    public func new(btree : MemoryBTree) : Nat {
        let bytes_per_node = Leaf.get_memory_size(btree);

        let leaf_address = MemoryRegion.allocate(btree.metadata, bytes_per_node);

        MemoryRegion.storeBlob(btree.metadata, leaf_address, Leaf.MAGIC);
        MemoryRegion.storeNat8(btree.metadata, leaf_address + Leaf.LAYOUT_VERSION_START, Leaf.LAYOUT_VERSION); // layout version
        MemoryRegion.storeNat8(btree.metadata, leaf_address + Leaf.NODE_TYPE_START, Leaf.NODE_TYPE); // node type

        MemoryRegion.storeNat16(btree.metadata, leaf_address + Leaf.INDEX_START, 0); // node's position in parent node
        MemoryRegion.storeNat16(btree.metadata, leaf_address + Leaf.COUNT_START, 0); // number of elements in the node

        // adjacent nodes
        MemoryRegion.storeNat64(btree.metadata, leaf_address + Leaf.PARENT_START, NULL_ADDRESS);
        MemoryRegion.storeNat64(btree.metadata, leaf_address + Leaf.PREV_START, NULL_ADDRESS);
        MemoryRegion.storeNat64(btree.metadata, leaf_address + Leaf.NEXT_START, NULL_ADDRESS);

        var i = 0;

        // keys
        while (i < btree.order) {
            let key_offset = get_keys_offset(leaf_address, i);
            MemoryRegion.storeNat64(btree.metadata, key_offset, NULL_ADDRESS);
            MemoryRegion.storeNat16(btree.metadata, key_offset + Leaf.ADDRESS_SIZE, 0);

            i += 1;
        };

        // vals
        i := 0;
        while (i < btree.order) {
            let val_offset = get_vals_offset(btree, leaf_address, i);
            MemoryRegion.storeNat64(btree.metadata, val_offset, NULL_ADDRESS);
            MemoryRegion.storeNat32(btree.metadata, val_offset + Leaf.ADDRESS_SIZE, 0);

            i += 1;
        };

        // loads from stable memory and adds to cache

        leaf_address;
    };

    func read_keys_into(btree : MemoryBTree, leaf_address : Nat, keys : [var ?(MemoryBlock, Blob)]) {
        var i = 0;

        label while_loop while (i < btree.order) {
            let key_offset = get_keys_offset(leaf_address, i);
            let mb_address = MemoryRegion.loadNat64(btree.metadata, key_offset);

            if (mb_address == NULL_ADDRESS) break while_loop;

            let key_size = MemoryRegion.loadNat16(btree.metadata, key_offset + ADDRESS_SIZE) |> Nat16.toNat(_);
            let key_block = (Nat64.toNat(mb_address), key_size);
            let key_blob = MemoryBlock.get_key(btree, key_block);

            keys[i] := ?(key_block, key_blob);

            i += 1;
        };
    };

    public func validate(btree : MemoryBTree, address : Nat) : Bool {
        // Debug.print("received magic " # debug_show (MemoryRegion.loadBlob(btree.metadata, address, MAGIC_SIZE), MAGIC));
        MemoryRegion.loadBlob(btree.metadata, address, MAGIC_SIZE) == MAGIC
        // assert MemoryRegion.loadNat8(btree.metadata, address + LAYOUT_VERSION_START) == LAYOUT_VERSION;
        and MemoryRegion.loadNat8(btree.metadata, address + NODE_TYPE_START) == NODE_TYPE;
    };

    public func from_memory(btree : MemoryBTree, address : Nat) : Leaf {

        let leaf : Leaf = (
            [var 0, 0, 0, 0],
            [var null, null, null],
            Array.init(btree.order, null),
            Array.init(btree.order, null),
            Array.init(btree.order, null),
            Array.init<?Nat>(btree.order, null),
            Array.init(btree.order, null),
        );

        from_memory_into(btree, address, leaf, true);

        leaf;
    };

    public func from_memory_into(btree : MemoryBTree, address : Nat, leaf : Leaf, load_keys : Bool) {
        assert MemoryRegion.loadBlob(btree.metadata, address, MAGIC_SIZE) == MAGIC;
        // assert MemoryRegion.loadNat8(btree.metadata, address + LAYOUT_VERSION_START) == LAYOUT_VERSION;
        assert MemoryRegion.loadNat8(btree.metadata, address + NODE_TYPE_START) == NODE_TYPE;

        leaf.0 [AC.ADDRESS] := address;
        leaf.0 [AC.INDEX] := MemoryRegion.loadNat16(btree.metadata, address + INDEX_START) |> Nat16.toNat(_);
        leaf.0 [AC.COUNT] := MemoryRegion.loadNat16(btree.metadata, address + COUNT_START) |> Nat16.toNat(_);

        leaf.1 [AC.PARENT] := do {
            let p = MemoryRegion.loadNat64(btree.metadata, address + PARENT_START);
            if (p == NULL_ADDRESS) null else ?Nat64.toNat(p);
        };

        leaf.1 [AC.PREV] := do {
            let n = MemoryRegion.loadNat64(btree.metadata, address + PREV_START);
            if (n == NULL_ADDRESS) null else ?Nat64.toNat(n);
        };

        leaf.1 [AC.NEXT] := do {
            let n = MemoryRegion.loadNat64(btree.metadata, address + NEXT_START);
            if (n == NULL_ADDRESS) null else ?Nat64.toNat(n);
        };

        var i = 0;

        label while_loop while (i < leaf.0 [AC.COUNT]) {
            if (not load_keys) {
                leaf.2 [i] := null;
                leaf.3 [i] := null;
                leaf.4 [i] := null;
                i += 1;
                continue while_loop;
            };
     
            let key_offset = get_keys_offset(address, i);
            let mb_address = MemoryRegion.loadNat64(btree.metadata, key_offset);

            if (mb_address == NULL_ADDRESS) {
                leaf.2 [i] := null;
                leaf.3 [i] := null;
                leaf.4 [i] := null;
                i += 1;
                continue while_loop;
            };

            let key_size = MemoryRegion.loadNat16(btree.metadata, key_offset + ADDRESS_SIZE) |> Nat16.toNat(_);
            let key_block = (Nat64.toNat(mb_address), key_size);
            let key_blob = MemoryBlock.get_key(btree, key_block);

            leaf.2 [i] := ?(key_block);

            let val_offset = get_vals_offset(btree, address, i);
            let mb_val_address = MemoryRegion.loadNat64(btree.metadata, val_offset);
            let val_size = MemoryRegion.loadNat32(btree.metadata, val_offset + ADDRESS_SIZE) |> Nat32.toNat(_);

            let val_block = (Nat64.toNat(mb_val_address), val_size);
            let val_blob = MemoryBlock.get_val(btree, val_block);

            leaf.3 [i] := ?(val_block);
            leaf.4 [i] := ?(key_blob, val_blob);

            i += 1;
        };

        // while (i < leaf.0[AC.COUNT]){
        //     leaf.2 [i] := null;
        //     leaf.3 [i] := null;
        //     leaf.4 [i] := null;
        //     i += 1;
        // };

        // i := 0;
        // while (i < leaf.0[AC.COUNT]) {
        //     leaf.5 [i] := null;
        //     leaf.6 [i] := null;
        //     i += 1;
        // };

    };

    func calc_heuristic(btree : MemoryBTree) : Float {
        let cache_capacity = Float.fromInt(LruCache.capacity(btree.nodes_cache));
        let cache_size = Float.fromInt(LruCache.size(btree.nodes_cache));
        let branch_count = Float.fromInt(btree.branch_count);
        let leaf_count = Float.fromInt(btree.leaf_count);
        let nodes_count = (branch_count + leaf_count);

        let space_left = cache_capacity - cache_size;
        let nodes_not_in_cache = nodes_count - cache_size;

        var heuristic : Float = 0;

        if (space_left == 0) return 0;
        if (cache_capacity < branch_count) return 0;
        if (nodes_not_in_cache < space_left) {
            heuristic := 2;
        } else {
            heuristic := 10 - (((nodes_not_in_cache - space_left) / space_left) * 5.0) + 2.0;
        };

        return 10 - heuristic;
    };

    public func add_to_cache(btree : MemoryBTree, address : Nat) {
        if (LruCache.capacity(btree.nodes_cache) == 0) return;

        // update node to first position in cache
        switch (LruCache.get(btree.nodes_cache, nhash, address)) {
            case (? #leaf(_)) return;
            case (? #branch(_)) Debug.trap("Leaf.add_to_cache: returned branch instead of leaf");
            case (_) {};
        };

        // loading to the heap is expensive,
        // so we want to limit the number of nodes we load into the cache
        // this is a nice heuristic that does that
        // performs well when cache is full
        let heuristic = calc_heuristic(btree);
        if (Float.fromInt(address % 10) >= heuristic) return;

        let leaf : Leaf = if (LruCache.size(btree.nodes_cache) == LruCache.capacity(btree.nodes_cache)) {
            let ?prev_address = LruCache.lastKey(btree.nodes_cache) else Debug.trap("Leaf.add_to_cache: last is null");
            let ? #leaf(node) or ? #branch(node) = LruCache.peek(btree.nodes_cache, nhash, prev_address) else Debug.trap("Leaf.add_to_cache: leaf is null");
            from_memory_into(btree, address, node, true);
            node;
        } else {
            // loads from stable memory and adds to cache
            Leaf.from_memory(btree, address);
        };

        LruCache.put(btree.nodes_cache, nhash, address, #leaf(leaf));
    };

    public func display(btree : MemoryBTree, btree_utils : BTreeUtils<Nat, Nat>, leaf_address : Nat) {
        let leaf = Leaf.from_memory(btree, leaf_address);

        Debug.print(
            "Leaf -> " # debug_show (
                leaf.0,
                leaf.1,
                Array.map(
                    Array.freeze<?MemoryBlock>(leaf.2),
                    func(key_block : ?MemoryBlock) : ?Nat {
                        switch (key_block) {
                            case (?key_block) ?btree_utils.key.from_blob(MemoryBlock.get_key(btree, key_block));
                            case (_) null;
                        };
                    },
                ),
                Array.map(
                    Array.freeze(leaf.2),
                    func(val_block : ?MemoryBlock) : ?Nat {
                        switch (val_block) {
                            case (?val_block) ?btree_utils.val.from_blob(MemoryBlock.get_val(btree, val_block));
                            case (_) null;
                        };
                    },
                ),
                // leaf.5,
                // leaf.6
            )
        );
    };

    public func get_count(btree : MemoryBTree, address : Nat) : Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.0 [AC.COUNT];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        MemoryRegion.loadNat16(btree.metadata, address + COUNT_START) |> Nat16.toNat(_);
    };

    public func get_key_block(btree : MemoryBTree, address : Nat, i : Nat) : ?MemoryBlock {

        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) switch (leaf.2 [i]){
                case (null){
                    let key_offset = get_keys_offset(address, i);
                    let mb_address = MemoryRegion.loadNat64(btree.metadata, key_offset);

                    let key_size = MemoryRegion.loadNat16(btree.metadata, key_offset + ADDRESS_SIZE) |> Nat16.toNat(_);
                    leaf.2 [i] := if (mb_address == NULL_ADDRESS) null else ?(Nat64.toNat(mb_address), key_size);
                    return leaf.2 [i];
                };
                case (?key_block) return ?key_block;
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let key_offset = get_keys_offset(address, i);
        let mb_address = MemoryRegion.loadNat64(btree.metadata, key_offset);

        let key_size = MemoryRegion.loadNat16(btree.metadata, key_offset + ADDRESS_SIZE) |> Nat16.toNat(_);
        let key_block = if (mb_address == NULL_ADDRESS) null else ?(Nat64.toNat(mb_address), key_size);

        key_block;
    };

    public func get_key_blob(btree : MemoryBTree, address : Nat, i : Nat) : ?(Blob) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) switch (leaf.4 [i]) {
                case (?(key_blob, _)) return ?key_blob;
                case (null) {
                    let ?key_block = get_key_block(btree, address, i) else return null;
                    let key_blob = MemoryBlock.get_key(btree, key_block);

                    let ?val_block = get_val_block(btree, address, i) else return null;
                    let val_blob = MemoryBlock.get_val(btree, val_block);
                    leaf.4 [i] := ?(key_blob, val_blob);
                    return ?val_blob;
                };
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let ?key_block = Leaf.get_key_block(btree, address, i) else Debug.trap("Leaf.get_key_blob: key_block is null");
        let key_blob = MemoryBlock.get_key(btree, key_block);

        ?key_blob;
    };

    public func set_key_to_null(btree : MemoryBTree, address : Nat, i : Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) {
                leaf.2 [i] := null;
                leaf.4 [i] := null;
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let key_offset = get_keys_offset(address, i);
        MemoryRegion.storeNat64(btree.metadata, key_offset, NULL_ADDRESS);
        MemoryRegion.storeNat16(btree.metadata, key_offset + ADDRESS_SIZE, 0);
    };

    public func get_keys(btree : MemoryBTree, address : Nat) : [var ?(MemoryBlock, Blob)] {
        // switch(LruCache.peek(btree.nodes_cache, nhash, address)){
        //     case (? #leaf(leaf)) return leaf.2;
        //     case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
        //     case (_) {};
        // };

        let keys = Array.init<?(MemoryBlock, Blob)>(btree.order, null);
        read_keys_into(btree, address, keys);
        keys;
    };

    public func get_val_block(btree : MemoryBTree, address : Nat, index : Nat) : ?(MemoryBlock) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) switch (leaf.3 [index]){
                case (null){
                    let val_offset = get_vals_offset(btree, address, index);
                    let mb_val_address = MemoryRegion.loadNat64(btree.metadata, val_offset);
                    let val_size = MemoryRegion.loadNat32(btree.metadata, val_offset + ADDRESS_SIZE) |> Nat32.toNat(_);
                    leaf.3 [index] := if (mb_val_address == NULL_ADDRESS) null else ?(Nat64.toNat(mb_val_address), val_size);
                    return leaf.3 [index];
                };
                case (?val_block) return ?val_block;
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let val_offset = get_vals_offset(btree, address, index);
        let mb_val_address = MemoryRegion.loadNat64(btree.metadata, val_offset);
        if (mb_val_address == NULL_ADDRESS) return null;

        let val_size = MemoryRegion.loadNat32(btree.metadata, val_offset + ADDRESS_SIZE) |> Nat32.toNat(_);
        let val_block = (Nat64.toNat(mb_val_address), val_size);

        ?(val_block);

    };

    public func get_val_blob(btree : MemoryBTree, address : Nat, index : Nat) : ?(Blob) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) switch (leaf.4 [index]) {
                case (null) {
                    let ?key_block = get_key_block(btree, address, index) else return null;
                    let key_blob = MemoryBlock.get_key(btree, key_block);

                    let ?val_block = get_val_block(btree, address, index) else return null;
                    let val_blob = MemoryBlock.get_val(btree, val_block);
                    leaf.4 [index] := ?(key_blob, val_blob);
                    return ?val_blob;
                };
                case (?(_, val_blob)) return ?val_blob;

            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let ?val_block = get_val_block(btree, address, index) else Debug.trap("Leaf.get_val_blob: val_block is null");
        let val_blob = MemoryBlock.get_val(btree, val_block);

        ?(val_blob);

    };

    public func set_kv_to_null(btree : MemoryBTree, address : Nat, i : Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) {
                leaf.2 [i] := null;
                leaf.3 [i] := null;
                leaf.4 [i] := null;
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let key_offset = get_keys_offset(address, i);
        MemoryRegion.storeNat64(btree.metadata, key_offset, NULL_ADDRESS);
        MemoryRegion.storeNat16(btree.metadata, key_offset + ADDRESS_SIZE, 0);

        let val_offset = get_vals_offset(btree, address, i);
        MemoryRegion.storeNat64(btree.metadata, val_offset, NULL_ADDRESS);
        MemoryRegion.storeNat32(btree.metadata, val_offset + ADDRESS_SIZE, 0);
    };

    public func get_kv_blobs(btree : MemoryBTree, address : Nat, index : Nat) : ?(Blob, Blob) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) switch (leaf.4[index]){
                case (null){
                    let ?key_block = get_key_block(btree, address, index) else return null;
                    let key_blob = MemoryBlock.get_key(btree, key_block);

                    let ?val_block = get_val_block(btree, address, index) else return null;
                    let val_blob = MemoryBlock.get_val(btree, val_block);
                    leaf.4 [index] := ?(key_blob, val_blob);
                    return ?(key_blob, val_blob);
                };
                case (?kv_blob) return ?kv_blob;
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let ?key_block = get_key_block(btree, address, index) else return null;
        let key_blob = MemoryBlock.peek_key(btree, key_block);

        let ?val_block = get_val_block(btree, address, index) else Debug.trap("Leaf.get_val_blob: val_block is null");
        let val_blob = MemoryBlock.get_val(btree, val_block);

        ?(key_blob, val_blob);

    };

    public func get_parent(btree : MemoryBTree, address : Nat) : ?Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.1 [AC.PARENT];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let parent = MemoryRegion.loadNat64(btree.metadata, address + PARENT_START);
        if (parent == NULL_ADDRESS) return null;
        ?Nat64.toNat(parent);
    };

    public func get_index(btree : MemoryBTree, address : Nat) : Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.0 [AC.INDEX];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        MemoryRegion.loadNat16(btree.metadata, address + INDEX_START) |> Nat16.toNat(_);
    };

    public func get_next(btree : MemoryBTree, address : Nat) : ?Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.1 [AC.NEXT];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let next = MemoryRegion.loadNat64(btree.metadata, address + NEXT_START);
        if (next == NULL_ADDRESS) return null;
        ?Nat64.toNat(next);
    };

    public func get_prev(btree : MemoryBTree, address : Nat) : ?Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.1 [AC.PREV];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let prev = MemoryRegion.loadNat64(btree.metadata, address + PREV_START);
        if (prev == NULL_ADDRESS) return null;
        ?Nat64.toNat(prev);
    };

    public func binary_search<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>, address : Nat, cmp : (K, K) -> Int8, search_key : K, arr_len : Nat) : Int {
        if (arr_len == 0) return -1; // should insert at index Int.abs(i + 1)
        var l = 0;

        // arr_len will always be between 4 and 512
        var r = arr_len - 1 : Nat;

        while (l < r) {
            let mid = (l + r) / 2;

            let ?key_blob = Leaf.get_key_blob(btree, address, mid) else Debug.trap("1. binary_search_blob_seq: accessed a null value");
            let key = btree_utils.key.from_blob(key_blob);

            let result = cmp(search_key, key);

            if (result == -1) {
                r := mid;

            } else if (result == 1) {
                l := mid + 1;
            } else {
                return mid;
            };
        };

        let insertion = l;

        // Check if the insertion point is valid
        // return the insertion point but negative and subtracting 1 indicating that the key was not found
        // such that the insertion index for the key is Int.abs(insertion) - 1
        // [0,  1,  2]
        //  |   |   |
        // -1, -2, -3
        switch (Leaf.get_key_blob(btree, address, insertion)) {
            case (?(key_blob)) {
                let key = btree_utils.key.from_blob(key_blob);
                let result = cmp(search_key, key);

                if (result == 0) insertion else if (result == -1) -(insertion + 1) else -(insertion + 2);
            };
            case (_) {
                Debug.print("insertion = " # debug_show insertion);
                Debug.print("arr_len = " # debug_show arr_len);
                Debug.print(
                    "arr = " # debug_show Array.freeze(get_keys(btree, address))
                );
                Debug.trap("2. binary_search_blob_seq: accessed a null value");
            };
        };
    };

    public func binary_search_blob_seq(btree : MemoryBTree, address : Nat, cmp : (Blob, Blob) -> Int8, search_key : Blob, arr_len : Nat) : Int {
        if (arr_len == 0) return -1; // should insert at index Int.abs(i + 1)
        var l = 0;

        // arr_len will always be between 4 and 512
        var r = arr_len - 1 : Nat;

        while (l < r) {
            let mid = (l + r) / 2;

            let ?key_blob = Leaf.get_key_blob(btree, address, mid) else Debug.trap("1. binary_search_blob_seq: accessed a null value");
            let result = cmp(search_key, key_blob);

            if (result == -1) {
                r := mid;
            } else if (result == 1) {
                l := mid + 1;
            } else {
                return mid;
            };
        };

        let insertion = l;

        // Check if the insertion point is valid
        // return the insertion point but negative and subtracting 1 indicating that the key was not found
        // such that the insertion index for the key is Int.abs(insertion) - 1
        // [0,  1,  2]
        //  |   |   |
        // -1, -2, -3
        switch (Leaf.get_key_blob(btree, address, insertion)) {
            case (?(key_blob)) {
                let result = cmp(search_key, key_blob);

                if (result == 0) insertion else if (result == -1) -(insertion + 1) else -(insertion + 2);
            };
            case (_) {
                Debug.print("insertion = " # debug_show insertion);
                Debug.print("arr_len = " # debug_show arr_len);
                Debug.print(
                    "arr = " # debug_show Array.freeze(get_keys(btree, address))
                );
                Debug.trap("2. binary_search_blob_seq: accessed a null value");
            };
        };
    };

    public func update_count(btree : MemoryBTree, address : Nat, new_count : Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) leaf.0 [AC.COUNT] := new_count;
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        MemoryRegion.storeNat16(btree.metadata, address + COUNT_START, Nat16.fromNat(new_count));
    };

    public func update_index(btree : MemoryBTree, address : Nat, new_index : Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) leaf.0 [AC.INDEX] := new_index;
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        MemoryRegion.storeNat16(btree.metadata, address + INDEX_START, Nat16.fromNat(new_index));
    };

    public func update_parent(btree : MemoryBTree, address : Nat, opt_parent : ?Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) leaf.1 [AC.PARENT] := opt_parent;
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let parent = switch (opt_parent) {
            case (null) NULL_ADDRESS;
            case (?_parent) Nat64.fromNat(_parent);
        };

        MemoryRegion.storeNat64(btree.metadata, address + PARENT_START, parent);
    };

    public func update_next(btree : MemoryBTree, address : Nat, opt_next : ?Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) leaf.1 [AC.NEXT] := opt_next;
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let next = switch (opt_next) {
            case (null) NULL_ADDRESS;
            case (?_next) Nat64.fromNat(_next);
        };

        MemoryRegion.storeNat64(btree.metadata, address + NEXT_START, next);
    };

    public func update_prev(btree : MemoryBTree, address : Nat, opt_prev : ?Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) leaf.1 [AC.PREV] := opt_prev;
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let prev = switch (opt_prev) {
            case (null) NULL_ADDRESS;
            case (?_prev) Nat64.fromNat(_prev);
        };

        MemoryRegion.storeNat64(btree.metadata, address + PREV_START, prev);
    };

    public func clear(btree : MemoryBTree, leaf_address : Nat) {
        Leaf.update_index(btree, leaf_address, 0);
        Leaf.update_count(btree, leaf_address, 0);
        Leaf.update_parent(btree, leaf_address, null);
        Leaf.update_prev(btree, leaf_address, null);
        Leaf.update_next(btree, leaf_address, null);
    };

    public func insert(btree : MemoryBTree, leaf_address : Nat, index : Nat, key_block : MemoryBlock, key_blob : Blob, val_block : MemoryBlock, val_blob : Blob) {
        let count = Leaf.get_count(btree, leaf_address);

        // Debug.print("inserting into leaf");
        // Debug.print("index = " # debug_show index);
        // Debug.print("count = " # debug_show count);
        assert index <= count and count < btree.order;

        switch (LruCache.peek(btree.nodes_cache, nhash, leaf_address)) {
            case (? #leaf(leaf)) {
                var i = count;
                while (i > index) {
                    leaf.2 [i] := leaf.2 [i - 1];
                    leaf.3 [i] := leaf.3 [i - 1];

                    leaf.4 [i] := leaf.4 [i - 1];

                    i -= 1;
                };

                leaf.2 [index] := ?key_block;
                leaf.3 [index] := ?val_block;

                leaf.4 [index] := ?(key_blob, val_blob);

            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let key_start = get_keys_offset(leaf_address, index);
        let key_end = get_keys_offset(leaf_address, count);

        assert (key_end - key_start : Nat) / KEY_MEMORY_BLOCK_SIZE == (count - index : Nat);

        MemoryFns.shift(btree.metadata, key_start, key_end, KEY_MEMORY_BLOCK_SIZE);
        // Leaf.shift(btree, leaf_address, index, count, 1);
        // Leaf.put(btree, leaf_address, index, key_block, key_blob, val_block, val_blob);
        MemoryRegion.storeNat64(btree.metadata, key_start, Nat64.fromNat(key_block.0));
        MemoryRegion.storeNat16(btree.metadata, key_start + ADDRESS_SIZE, Nat16.fromNat(key_block.1));

        let val_start = get_vals_offset(btree, leaf_address, index);
        let val_end = get_vals_offset(btree, leaf_address, count);

        assert (val_end - val_start : Nat) / VALUE_MEMORY_BLOCK_SIZE == (count - index : Nat);

        MemoryFns.shift(btree.metadata, val_start, val_end, VALUE_MEMORY_BLOCK_SIZE);
        MemoryRegion.storeNat64(btree.metadata, val_start, Nat64.fromNat(val_block.0));
        MemoryRegion.storeNat32(btree.metadata, val_start + ADDRESS_SIZE, Nat32.fromNat(val_block.1));

        Leaf.update_count(btree, leaf_address, count + 1);
    };

    public func put(btree : MemoryBTree, leaf_address : Nat, index : Nat, key_block : MemoryBlock, key_blob : Blob, val_block : MemoryBlock, val_blob : Blob) {
        switch (LruCache.peek(btree.nodes_cache, nhash, leaf_address)) {
            case (? #leaf(leaf)) {
                leaf.2 [index] := ?key_block;
                leaf.4 [index] := ?(key_blob, val_blob);
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let key_offset = get_keys_offset(leaf_address, index);
        MemoryRegion.storeNat64(btree.metadata, key_offset, Nat64.fromNat(key_block.0));
        MemoryRegion.storeNat16(btree.metadata, key_offset + ADDRESS_SIZE, Nat16.fromNat(key_block.1));

        put_val(btree, leaf_address, index, val_block, val_blob);
    };

    public func put_val(btree : MemoryBTree, leaf_address : Nat, index : Nat, val_block : MemoryBlock, val_blob : Blob) {
        switch (LruCache.peek(btree.nodes_cache, nhash, leaf_address)) {
            case (? #leaf(leaf)) {
                leaf.3 [index] := ?val_block;
                let ?prev = leaf.4 [index] else Debug.trap("Leaf.put_val: key_blob is null");
                leaf.4 [index] := ?(prev.0, val_blob);
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let val_offset = get_vals_offset(btree, leaf_address, index);
        MemoryRegion.storeNat64(btree.metadata, val_offset, Nat64.fromNat(val_block.0));
        MemoryRegion.storeNat32(btree.metadata, val_offset + ADDRESS_SIZE, Nat32.fromNat(val_block.1));
    };

    public func split(btree : MemoryBTree, leaf_address : Nat, elem_index : Nat, key_block : MemoryBlock, key_blob : Blob, val_block : MemoryBlock, val_blob : Blob) : Nat {
        let arr_len = btree.order;
        let median = (arr_len / 2) + 1;

        let is_elem_added_to_right = elem_index >= median;

        var i = 0;
        let right_cnt = arr_len + 1 - median : Nat;

        let right_leaf_address = Leaf.new(btree);

        var already_inserted = false;
        var offset = if (is_elem_added_to_right) 0 else 1;

        var elems_removed_from_left = 0;

        if (not is_elem_added_to_right) {
            let key_start = get_keys_offset(leaf_address, i + median - offset);
            let key_end = get_keys_offset(leaf_address, arr_len);

            let new_key_start = get_keys_offset(right_leaf_address, 0);
            // let new_key_end = get_keys_offset(right_leaf_address, right_cnt);

            var blob_slice = MemoryRegion.loadBlob(btree.metadata, key_start, key_end - key_start);
            MemoryRegion.storeBlob(btree.metadata, new_key_start, blob_slice);

            let val_start = get_vals_offset(btree, leaf_address, i + median - offset);
            let val_end = get_vals_offset(btree, leaf_address, arr_len);

            let new_val_start = get_vals_offset(btree, right_leaf_address, 0);
            // let new_val_end = get_vals_offset(btree, right_leaf_address, right_cnt);

            blob_slice := MemoryRegion.loadBlob(btree.metadata, val_start, val_end - val_start);
            MemoryRegion.storeBlob(btree.metadata, new_val_start, blob_slice);
            elems_removed_from_left += right_cnt;
        } else {
            // | left | elem | right |
            // left
            var size = elem_index - (i + median - offset) : Nat;
            var key_start = get_keys_offset(leaf_address, i + median - offset);
            var key_end = get_keys_offset(leaf_address, elem_index);

            var new_key_start = get_keys_offset(right_leaf_address, 0);
            // let new_key_end = get_keys_offset(right_leaf_address, right_cnt);

            var blob_slice = MemoryRegion.loadBlob(btree.metadata, key_start, key_end - key_start);
            MemoryRegion.storeBlob(btree.metadata, new_key_start, blob_slice);

            var val_start = get_vals_offset(btree, leaf_address, i + median - offset);
            var val_end = get_vals_offset(btree, leaf_address, elem_index);

            var new_val_start = get_vals_offset(btree, right_leaf_address, 0);
            // let new_val_end = get_vals_offset(btree, right_leaf_address, right_cnt);

            blob_slice := MemoryRegion.loadBlob(btree.metadata, val_start, val_end - val_start);
            MemoryRegion.storeBlob(btree.metadata, new_val_start, blob_slice);

            // elem
            // key_start := get_keys_offset(leaf_address, elem_index);
            // key_end := get_keys_offset(leaf_address, elem_index + 1);

            new_key_start := get_keys_offset(right_leaf_address, size);

            // blob_slice := MemoryRegion.loadBlob(btree.metadata, key_start, key_end - key_start);
            // MemoryRegion.storeBlob(btree.metadata, new_key_start, blob_slice);
            MemoryRegion.storeNat64(btree.metadata, new_key_start, Nat64.fromNat(key_block.0));
            MemoryRegion.storeNat16(btree.metadata, new_key_start + ADDRESS_SIZE, Nat16.fromNat(key_block.1));

            // val_start := get_vals_offset(btree, leaf_address, elem_index);
            // val_end := get_vals_offset(btree, leaf_address, elem_index + 1);

            new_val_start := get_vals_offset(btree, right_leaf_address, size);

            // blob_slice := MemoryRegion.loadBlob(btree.metadata, val_start, val_end - val_start);
            // MemoryRegion.storeBlob(btree.metadata, new_val_start, blob_slice);
            MemoryRegion.storeNat64(btree.metadata, new_val_start, Nat64.fromNat(val_block.0));
            MemoryRegion.storeNat32(btree.metadata, new_val_start + ADDRESS_SIZE, Nat32.fromNat(val_block.1));
            size += 1;

            // right
            key_start := get_keys_offset(leaf_address, elem_index);
            key_end := get_keys_offset(leaf_address, arr_len);

            new_key_start := get_keys_offset(right_leaf_address, size);

            blob_slice := MemoryRegion.loadBlob(btree.metadata, key_start, key_end - key_start);
            MemoryRegion.storeBlob(btree.metadata, new_key_start, blob_slice);

            val_start := get_vals_offset(btree, leaf_address, elem_index);
            val_end := get_vals_offset(btree, leaf_address, arr_len);

            new_val_start := get_vals_offset(btree, right_leaf_address, size);

            blob_slice := MemoryRegion.loadBlob(btree.metadata, val_start, val_end - val_start);
            MemoryRegion.storeBlob(btree.metadata, new_val_start, blob_slice);

            size += (arr_len - elem_index : Nat);
            elems_removed_from_left += size;
        };
        // while (i < right_cnt) {
        //     let j = i + median - offset : Nat;

        //     if (j >= median and j == elem_index and not already_inserted) {
        //         offset += 1;
        //         already_inserted := true;
        //         Leaf.put(btree, right_leaf_address, i, key_block, key_blob, val_block, val_blob);

        //     } else {
        //         // decrement left leaf count
        //         let ?key_block = get_key_block(btree, leaf_address, j) else Debug.trap("Leaf.split: key is null");
        //         let ?key_blob = get_key_blob(btree, leaf_address, j) else Debug.trap("Leaf.split: key_blob is null");
        //         let ?val_block = get_val_block(btree, leaf_address, j) else Debug.trap("Leaf.split: val_block is null");
        //         let ?val_blob = get_val_blob(btree, leaf_address, j) else Debug.trap("Leaf.split: val_blob is null");
        //         Leaf.set_kv_to_null(btree, leaf_address, j);

        //         Leaf.put(btree, right_leaf_address, i, key_block, key_blob, val_block, val_blob);
        //         elems_removed_from_left += 1;
        //     };

        //     i += 1;
        // };

        Leaf.update_count(btree, leaf_address, arr_len - elems_removed_from_left);

        if (not is_elem_added_to_right) {
            Leaf.insert(btree, leaf_address, elem_index, key_block, key_blob, val_block, val_blob);
        };

        Leaf.update_count(btree, leaf_address, median);
        Leaf.update_count(btree, right_leaf_address, right_cnt);

        let left_index = Leaf.get_index(btree, leaf_address);
        Leaf.update_index(btree, right_leaf_address, left_index + 1);

        let left_parent = Leaf.get_parent(btree, leaf_address);
        Leaf.update_parent(btree, right_leaf_address, left_parent);

        // update leaf pointers
        Leaf.update_prev(btree, right_leaf_address, ?leaf_address);

        let lefts_next_node = Leaf.get_next(btree, leaf_address);
        Leaf.update_next(btree, right_leaf_address, lefts_next_node);
        Leaf.update_next(btree, leaf_address, ?right_leaf_address);

        switch (Leaf.get_next(btree, right_leaf_address)) {
            case (?next_address) {
                Leaf.update_prev(btree, next_address, ?right_leaf_address);
            };
            case (_) {};
        };

        right_leaf_address;
    };

    public func shift(btree : MemoryBTree, leaf_address : Nat, start : Nat, end : Nat, offset : Int) {
        if (offset == 0) return;
        switch (LruCache.peek(btree.nodes_cache, nhash, leaf_address)) {
            case (? #leaf(leaf)) if (offset >= 0) {
                var i = end;
                while (i > start) {
                    let prev = i - 1 : Nat;
                    let curr = Int.abs(offset + i - 1);

                    leaf.2 [curr] := leaf.2 [prev];
                    leaf.3 [curr] := leaf.3 [prev];
                    leaf.4 [curr] := leaf.4 [prev];

                    i -= 1;
                };

            } else {
                var i = start;
                while (i < end) {
                    let curr = Int.abs(offset + i);

                    leaf.2 [curr] := leaf.2 [i];
                    leaf.3 [curr] := leaf.3 [i];
                    leaf.4 [curr] := leaf.4 [i];

                    leaf.2 [i] := null;
                    leaf.3 [i] := null;
                    leaf.4 [i] := null;

                    i += 1;
                };
            };

            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let key_start = get_keys_offset(leaf_address, start);
        let key_end = get_keys_offset(leaf_address, end);

        MemoryFns.shift(btree.metadata, key_start, key_end, offset * KEY_MEMORY_BLOCK_SIZE);

        let val_start = get_vals_offset(btree, leaf_address, start);
        let val_end = get_vals_offset(btree, leaf_address, end);

        MemoryFns.shift(btree.metadata, val_start, val_end, offset * VALUE_MEMORY_BLOCK_SIZE);
    };

    public func remove(btree : MemoryBTree, leaf_address : Nat, index : Nat) {
        let count = Leaf.get_count(btree, leaf_address);

        Leaf.shift(btree, leaf_address, index + 1, count, -1); // updates the cache
        Leaf.update_count(btree, leaf_address, count - 1); // updates the cache as well
    };

    // public func get_larger_neighbour(btree: MemoryBTree, )

    public func redistribute(btree : MemoryBTree, leaf : Nat, neighbour : Nat) : Bool {
        let leaf_count = Leaf.get_count(btree, leaf);
        let neighbour_count = Leaf.get_count(btree, neighbour);

        let sum_count = leaf_count + neighbour_count;
        let min_count_for_both_nodes = btree.order;

        if (sum_count < min_count_for_both_nodes) return false; // not enough entries to distribute

        // Debug.print("redistribute: leaf_count = " # debug_show leaf_count);
        // Debug.print("redistribute: neighbour_count = " # debug_show neighbour_count);

        let data_to_move = (sum_count / 2) - leaf_count : Nat;

        // Debug.print("data_to_move = " # debug_show data_to_move);

        let leaf_index = Leaf.get_index(btree, leaf);
        let neighbour_index = Leaf.get_index(btree, neighbour);

        // distribute data between adjacent nodes
        if (neighbour_index < leaf_index) {
            // neighbour is before leaf
            // Debug.print("neighbour is before leaf");

            var i = 0;
            Leaf.shift(btree, leaf, 0, leaf_count, data_to_move);
            // while (i < data_to_move) {

            //     let ?key_block = Leaf.get_key_block(btree, neighbour, neighbour_count - i - 1) else Debug.trap("Leaf.redistribute: key_block is null");
            //     let ?key_blob = Leaf.get_key_blob(btree, neighbour, neighbour_count - i - 1) else Debug.trap("Leaf.redistribute: key_blob is null");
            //     let ?val_block = Leaf.get_val_block(btree, neighbour, neighbour_count - i - 1) else Debug.trap("Leaf.redistribute: val_block is null");
            //     let ?val_blob = Leaf.get_val_blob(btree, neighbour, neighbour_count - i - 1) else Debug.trap("Leaf.redistribute: val_blob is null");

            //     Leaf.remove(btree, neighbour, neighbour_count - i - 1 : Nat);

            //     // no need to call update_fields as we are the adjacent node is before the leaf node
            //     // which means that all its keys are less than the leaf node's keys
            //     // leaf_node.3[data_to_move - i - 1] := opt_kv;
            //     let new_index = data_to_move - i - 1 : Nat;
            //     Leaf.put(btree, leaf, new_index, key_block, key_blob, val_block, val_blob);

            //     i += 1;
            // };

            let key_start = get_keys_offset(neighbour, neighbour_count - data_to_move);
            let key_end = get_keys_offset(neighbour, neighbour_count);

            let new_key_start = get_keys_offset(leaf, 0);

            var blob_slice = MemoryRegion.loadBlob(btree.metadata, key_start, key_end - key_start);
            MemoryRegion.storeBlob(btree.metadata, new_key_start, blob_slice);

            let val_start = get_vals_offset(btree, neighbour, neighbour_count - data_to_move);
            let val_end = get_vals_offset(btree, neighbour, neighbour_count);

            let new_val_start = get_vals_offset(btree, leaf, 0);

            blob_slice := MemoryRegion.loadBlob(btree.metadata, val_start, val_end - val_start);
            MemoryRegion.storeBlob(btree.metadata, new_val_start, blob_slice);
        } else {
            // adj_node is after leaf_node
            // Debug.print("neighbour is after leaf_node");

            // var i = 0;
            // while (i < data_to_move) {
            //     let ?key_block = Leaf.get_key_block(btree, neighbour, i) else Debug.trap("Leaf.redistribute: key_block is null");
            //     let ?key_blob = Leaf.get_key_blob(btree, neighbour, i) else Debug.trap("Leaf.redistribute: key_blob is null");
            //     let ?val_block = Leaf.get_val_block(btree, neighbour, i) else Debug.trap("Leaf.redistribute: val_block is null");
            //     let ?val_blob = Leaf.get_val_blob(btree, neighbour, i) else Debug.trap("Leaf.redistribute: val_blob is null");

            //     Leaf.insert(btree, leaf, leaf_count + i, key_block, key_blob, val_block, val_blob);

            //     i += 1;
            // };

            let key_start = get_keys_offset(neighbour, 0);
            let key_end = get_keys_offset(neighbour, data_to_move);

            let new_key_start = get_keys_offset(leaf, leaf_count);

            var blob_slice = MemoryRegion.loadBlob(btree.metadata, key_start, key_end - key_start);
            MemoryRegion.storeBlob(btree.metadata, new_key_start, blob_slice);

            let val_start = get_vals_offset(btree, neighbour, 0);
            let val_end = get_vals_offset(btree, neighbour, data_to_move);

            let new_val_start = get_vals_offset(btree, leaf, leaf_count);

            blob_slice := MemoryRegion.loadBlob(btree.metadata, val_start, val_end - val_start);
            MemoryRegion.storeBlob(btree.metadata, new_val_start, blob_slice);

            Leaf.shift(btree, neighbour, data_to_move, neighbour_count, -data_to_move);

        };

        Leaf.update_count(btree, leaf, leaf_count + data_to_move);
        Leaf.update_count(btree, neighbour, neighbour_count - data_to_move);

        // Debug.print("end redistribution");
        true;
    };

    // only deallocates the memory allocated in the metadata region
    // the values stored in the blob region are not deallocated
    // as they could have been moved to a different leaf node
    public func deallocate(btree : MemoryBTree, leaf : Nat) {
        let memory_size = Leaf.get_memory_size(btree);
        MemoryRegion.deallocate(btree.metadata, leaf, memory_size);
    };

    public func merge(btree : MemoryBTree, leaf : Nat, neighbour : Nat) {
        let leaf_index = Leaf.get_index(btree, leaf);
        let neighbour_index = Leaf.get_index(btree, neighbour);

        var left = leaf;
        var right = neighbour;

        if (leaf_index > neighbour_index) {
            left := neighbour;
            right := leaf;
        };

        let left_count = Leaf.get_count(btree, left);
        let right_count = Leaf.get_count(btree, right);

        var i = 0;

        // while (i < right_count) {
        //     let ?key_block = Leaf.get_key_block(btree, right, i) else Debug.trap("Leaf.merge: key_block is null");
        //     let ?key_blob = Leaf.get_key_blob(btree, right, i) else Debug.trap("Leaf.merge: key_blob is null");
        //     let ?val_block = Leaf.get_val_block(btree, right, i) else Debug.trap("Leaf.merge: val_block is null");
        //     let ?val_blob = Leaf.get_val_blob(btree, right, i) else Debug.trap("Leaf.merge: val_blob is null");

        //     Leaf.insert(btree, left, left_count + i, key_block, key_blob, val_block, val_blob);

        //     i += 1;
        // };

        let key_start = get_keys_offset(right, 0);
        let key_end = get_keys_offset(right, right_count);

        let new_key_start = get_keys_offset(left, left_count);

        var blob_slice = MemoryRegion.loadBlob(btree.metadata, key_start, key_end - key_start);
        MemoryRegion.storeBlob(btree.metadata, new_key_start, blob_slice);

        let val_start = get_vals_offset(btree, right, 0);
        let val_end = get_vals_offset(btree, right, right_count);

        let new_val_start = get_vals_offset(btree, left, left_count);

        blob_slice := MemoryRegion.loadBlob(btree.metadata, val_start, val_end - val_start);
        MemoryRegion.storeBlob(btree.metadata, new_val_start, blob_slice);

        Leaf.update_count(btree, left, left_count + right_count);

        // update leaf pointers
        // a <=> b <=> c
        // delete b
        // a <=> c

        let a = left;
        let _b = right;
        let opt_c = Leaf.get_next(btree, right);

        Leaf.update_next(btree, a, opt_c);
        switch (opt_c) {
            case (?c) Leaf.update_prev(btree, c, ?a);
            case (_) {};
        };

    };

};
