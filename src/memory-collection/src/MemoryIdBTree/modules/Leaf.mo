

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
import Utils "../../Utils";
module Leaf {
    public type Leaf = Migrations.Leaf;
    type Address = T.Address;
    type MemoryIdBTree = Migrations.MemoryIdBTree;
    type MemoryBlock = T.MemoryBlock;
    type BTreeUtils<K, V> = T.BTreeUtils<K, V>;
    type NodeType = Migrations.Node;
    type UniqueId = T.UniqueId;

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

    public let KV_IDS_START = HEADER_SIZE;

    // access constants
    public let AC = {
        ADDRESS = 0;
        INDEX = 1;
        COUNT = 2;

        PARENT = 0;
        PREV = 1;
        NEXT = 2;
    };

    public let NULL_ADDRESS : Nat64 = Nat64.maximumValue;

    public let MAGIC : Blob = "BTN";

    public let LAYOUT_VERSION : Nat8 = 0;

    public let NODE_TYPE : Nat8 = 1; // leaf

    /// Leaf Memory Layout
    ///
    /// |     Field      |     Size    | Offset |   Type   |                              Description                              |
    /// |----------------|-------------|--------|----------|-----------------------------------------------------------------------|
    /// | MAGIC          | 3           | 0      | Blob     | Magic number                                                          |
    /// | NODE TYPE      | 1           | 3      | Nat8     | Node type                                                             |
    /// | LAYOUT VERSION | 1           | 4      | Nat8     | Layout version                                                        |
    /// | INDEX          | 2           | 5      | Nat16    | Node's position in parent node                                        |
    /// | COUNT          | 2           | 7      | Nat16    | Number of elements in the node                                        |
    /// | PARENT         | 8           | 9      | Nat64    | Parent address                                                        |
    /// | PREV           | 8           | 17     | Nat64    | Previous leaf address                                                 |
    /// | NEXT           | 8           | 25     | Nat64    | Next leaf address                                                     |
    /// | EXTRA          | 31          | 33     | -        | Extra space from header (size 64) for future use                      |
    /// | KV unique Ids  | 8 * order   | 64     | Nat64    | Unique ids for each key-value pair stored in the blocks memory region |

    public func get_memory_size(btree : MemoryIdBTree) : Nat {
        let bytes_per_node = HEADER_SIZE
        + ( ADDRESS_SIZE  * btree.order ); // key-value pairs

        bytes_per_node;
    };

    public func get_kv_id_offset(leaf_address : Nat, i : Nat) : Nat {
        leaf_address + KV_IDS_START + (i * Leaf.ADDRESS_SIZE);
    };

    public func new(btree : MemoryIdBTree) : Nat {
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
            let key_offset = get_kv_id_offset(leaf_address, i);
            MemoryRegion.storeNat64(btree.metadata, key_offset, NULL_ADDRESS);
            i += 1;
        };

        // loads from stable memory and adds to cache

        leaf_address;
    };

    public func validate(btree : MemoryIdBTree, address : Nat) : Bool {
        // Debug.print("received magic " # debug_show (MemoryRegion.loadBlob(btree.metadata, address, MAGIC_SIZE), MAGIC));
        MemoryRegion.loadBlob(btree.metadata, address, MAGIC_SIZE) == MAGIC
        // assert MemoryRegion.loadNat8(btree.metadata, address + LAYOUT_VERSION_START) == LAYOUT_VERSION;
        and MemoryRegion.loadNat8(btree.metadata, address + NODE_TYPE_START) == NODE_TYPE;
    };

    public func from_memory(btree : MemoryIdBTree, address : Nat) : Leaf {

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

    public func from_memory_into(btree : MemoryIdBTree, address : Nat, leaf : Leaf, load_keys : Bool) {
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
            let key_id : Nat = get_kv_id(btree, address, i) |> Utils.unwrap(_, "Leaf.from_memory_into: key_id is null");
            // Debug.print("cmp: " # debug_show (key_id, NULL_ADDRESS));
            // Debug.print("is null = " # debug_show (Nat64.fromNat(key_id) == NULL_ADDRESS));
            // Debug.print("is null = " # debug_show (Nat64.equal(Nat64.fromNat(key_id), NULL_ADDRESS)));

            if (key_id ==  Nat64.toNat(NULL_ADDRESS)) {
                leaf.2 [i] := null;
                leaf.3 [i] := null;
                leaf.4 [i] := null;
                i += 1;
                continue while_loop;
            };

            // Debug.print("key_id = " # debug_show key_id);


            let key_block = MemoryBlock.get_key_block(btree, key_id);
            let key_blob = MemoryBlock.get_key_blob(btree, key_id);
            // Debug.print("key_blob = " # debug_show key_blob);

            leaf.2 [i] := ?(key_block);

            let val_block = MemoryBlock.get_val_block(btree, key_id);
            let val_blob = MemoryBlock.get_val_blob(btree, key_id);
            // Debug.print("val_blob = " # debug_show val_blob);
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

    func calc_heuristic(btree : MemoryIdBTree) : Float {
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

    public func add_to_cache(btree : MemoryIdBTree, address : Nat) {
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

    public func display(btree : MemoryIdBTree, btree_utils : BTreeUtils<Nat, Nat>, leaf_address : Nat) {
        // let leaf = Leaf.from_memory(btree, leaf_address);

        // Debug.print(
        //     "Leaf -> " # debug_show (
        //         leaf.0,
        //         leaf.1,
        //         Array.map(
        //             Array.freeze<?MemoryBlock>(leaf.2),
        //             func(key_block : ?MemoryBlock) : ?Nat {
        //                 switch (key_block) {
        //                     case (?key_block) ?btree_utils.key.from_blob(MemoryBlock.get_key_blob(btree, key_block));
        //                     case (_) null;
        //                 };
        //             },
        //         ),
        //         Array.map(
        //             Array.freeze(leaf.2),
        //             func(val_block : ?MemoryBlock) : ?Nat {
        //                 switch (val_block) {
        //                     case (?val_block) {
        //                         let val_blob = MemoryBlock.get_val_blob(btree, val_block);
        //                         ?btree_utils.val.from_blob(val_blob);
        //                     };
        //                     case (_) null;
        //                 };
        //             },
        //         ),
        //         // leaf.5,
        //         // leaf.6
        //     )
        // );
    };

    public func get_count(btree : MemoryIdBTree, address : Nat) : Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.0 [AC.COUNT];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        MemoryRegion.loadNat16(btree.metadata, address + COUNT_START) |> Nat16.toNat(_);
    };

    public func get_kv_id(btree : MemoryIdBTree, address : Nat, i : Nat) : ?UniqueId {
        let kv_id_offset = get_kv_id_offset(address, i);
        let opt_id = MemoryRegion.loadNat64(btree.metadata, kv_id_offset);

        if (opt_id == NULL_ADDRESS) null else ?(Nat64.toNat(opt_id));
    };

    public func get_key_block(btree : MemoryIdBTree, address : Nat, i : Nat) : ?MemoryBlock {
        let ?id = get_kv_id(btree, address, i);
        ?MemoryBlock.get_key_block(btree, id);
    };

    public func get_val_block(btree : MemoryIdBTree, address : Nat, i : Nat) : ?MemoryBlock {
        let ?id = get_kv_id(btree, address, i);
        ?MemoryBlock.get_val_block(btree, id);
    };

    public func get_key_blob(btree : MemoryIdBTree, address : Nat, i : Nat) : ?(Blob) {
        let ?id = get_kv_id(btree, address, i);
        ?MemoryBlock.get_key_blob(btree, id);
    };

    public func set_key_to_null(btree : MemoryIdBTree, address : Nat, i : Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) {
                leaf.2 [i] := null;
                leaf.4 [i] := null;
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let id_offset = get_kv_id_offset(address, i);
        MemoryRegion.storeNat64(btree.metadata, id_offset, NULL_ADDRESS);
    };

    public func get_val_blob(btree : MemoryIdBTree, address : Nat, index : Nat) : ?(Blob) {

        let ?id = get_kv_id(btree, address, index);
        ?MemoryBlock.get_val_blob(btree, id);
    };

    public func set_kv_to_null(btree : MemoryIdBTree, address : Nat, i : Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) {
                leaf.2 [i] := null;
                leaf.3 [i] := null;
                leaf.4 [i] := null;
            };
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let key_offset = get_kv_id_offset(address, i);
        MemoryRegion.storeNat64(btree.metadata, key_offset, NULL_ADDRESS);
    };

    public func get_kv_blobs(btree : MemoryIdBTree, address : Nat, index : Nat) : ?(Blob, Blob) {
        let ?id = get_kv_id(btree, address, index) else return null;
        // Debug.print("get_kv_blobs: id = " # debug_show id);
        let key_blob = MemoryBlock.get_key_blob(btree, id);
        let val_blob = MemoryBlock.get_val_blob(btree, id);

        ?(key_blob, val_blob);

    };

    public func get_parent(btree : MemoryIdBTree, address : Nat) : ?Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.1 [AC.PARENT];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let parent = MemoryRegion.loadNat64(btree.metadata, address + PARENT_START);
        if (parent == NULL_ADDRESS) return null;
        ?Nat64.toNat(parent);
    };

    public func get_index(btree : MemoryIdBTree, address : Nat) : Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.0 [AC.INDEX];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        MemoryRegion.loadNat16(btree.metadata, address + INDEX_START) |> Nat16.toNat(_);
    };

    public func get_next(btree : MemoryIdBTree, address : Nat) : ?Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.1 [AC.NEXT];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let next = MemoryRegion.loadNat64(btree.metadata, address + NEXT_START);
        if (next == NULL_ADDRESS) return null;
        ?Nat64.toNat(next);
    };

    public func get_prev(btree : MemoryIdBTree, address : Nat) : ?Nat {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) return leaf.1 [AC.PREV];
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        let prev = MemoryRegion.loadNat64(btree.metadata, address + PREV_START);
        if (prev == NULL_ADDRESS) return null;
        ?Nat64.toNat(prev);
    };

    public func binary_search<K, V>(btree : MemoryIdBTree, btree_utils : BTreeUtils<K, V>, address : Nat, cmp : (K, K) -> Int8, search_key : K, arr_len : Nat) : Int {
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
                // Debug.print(
                //     "arr = " # debug_show Array.freeze(get_keys(btree, address))
                // );
                Debug.trap("2. binary_search_blob_seq: accessed a null value");
            };
        };
    };

    public func binary_search_blob_seq(btree : MemoryIdBTree, address : Nat, cmp : (Blob, Blob) -> Int8, search_key : Blob, arr_len : Nat) : Int {
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
                // Debug.print(
                //     "arr = " # debug_show Array.freeze(get_keys(btree, address))
                // );
                Debug.trap("2. binary_search_blob_seq: accessed a null value");
            };
        };
    };

    public func update_count(btree : MemoryIdBTree, address : Nat, new_count : Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) leaf.0 [AC.COUNT] := new_count;
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        MemoryRegion.storeNat16(btree.metadata, address + COUNT_START, Nat16.fromNat(new_count));
    };

    public func update_index(btree : MemoryIdBTree, address : Nat, new_index : Nat) {
        switch (LruCache.peek(btree.nodes_cache, nhash, address)) {
            case (? #leaf(leaf)) leaf.0 [AC.INDEX] := new_index;
            case (? #branch(_)) Debug.trap("Search cache for leaf: returned branch instead of leaf");
            case (_) {};
        };

        MemoryRegion.storeNat16(btree.metadata, address + INDEX_START, Nat16.fromNat(new_index));
    };

    public func update_parent(btree : MemoryIdBTree, address : Nat, opt_parent : ?Nat) {
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

    public func update_next(btree : MemoryIdBTree, address : Nat, opt_next : ?Nat) {
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

    public func update_prev(btree : MemoryIdBTree, address : Nat, opt_prev : ?Nat) {
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

    public func clear(btree : MemoryIdBTree, leaf_address : Nat) {
        Leaf.update_index(btree, leaf_address, 0);
        Leaf.update_count(btree, leaf_address, 0);
        Leaf.update_parent(btree, leaf_address, null);
        Leaf.update_prev(btree, leaf_address, null);
        Leaf.update_next(btree, leaf_address, null);
    };

    public func insert(btree : MemoryIdBTree, leaf_address : Nat, index : Nat, new_id: UniqueId) {
        let count = Leaf.get_count(btree, leaf_address);

        assert index <= count and count < btree.order;

        let start = get_kv_id_offset(leaf_address, index);
        let end = get_kv_id_offset(leaf_address, count);

        assert (end - start : Nat) / ADDRESS_SIZE == (count - index : Nat);

        MemoryFns.shift(btree.metadata, start, end, ADDRESS_SIZE);
        MemoryRegion.storeNat64(btree.metadata, start, Nat64.fromNat(new_id));
        
        Leaf.update_count(btree, leaf_address, count + 1);
    };

    public func insert_with_count(btree : MemoryIdBTree, leaf_address : Nat, index : Nat, new_id: UniqueId, count: Nat) {
        assert index <= count and count < btree.order;

        let start = get_kv_id_offset(leaf_address, index);
        let end = get_kv_id_offset(leaf_address, count);

        assert (end - start : Nat) / ADDRESS_SIZE == (count - index : Nat);

        MemoryFns.shift(btree.metadata, start, end, ADDRESS_SIZE);
        MemoryRegion.storeNat64(btree.metadata, start, Nat64.fromNat(new_id));
        
        Leaf.update_count(btree, leaf_address, count + 1);
    };

    public func put(btree : MemoryIdBTree, leaf_address : Nat, index : Nat, new_id: UniqueId) {

        let id_offset = get_kv_id_offset(leaf_address, index);
        MemoryRegion.storeNat64(btree.metadata, id_offset, Nat64.fromNat(new_id));
    };

    public func split(btree : MemoryIdBTree, leaf_address : Nat, elem_index : Nat, new_id: UniqueId) : Nat {
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
            let start = get_kv_id_offset(leaf_address, i + median - offset);
            let end = get_kv_id_offset(leaf_address, arr_len);

            let new_start = get_kv_id_offset(right_leaf_address, 0);
            var blob_slice = MemoryRegion.loadBlob(btree.metadata, start, end - start);
            MemoryRegion.storeBlob(btree.metadata, new_start, blob_slice);

            elems_removed_from_left += right_cnt;
        } else {
            // | left | elem | right |
            // left
            var size = elem_index - (i + median - offset) : Nat;
            var start = get_kv_id_offset(leaf_address, i + median - offset);
            var end = get_kv_id_offset(leaf_address, elem_index);

            var new_start = get_kv_id_offset(right_leaf_address, 0);
            var blob_slice = MemoryRegion.loadBlob(btree.metadata, start, end - start);
            MemoryRegion.storeBlob(btree.metadata, new_start, blob_slice);

            // elem
            new_start := get_kv_id_offset(right_leaf_address, size);
            MemoryRegion.storeNat64(btree.metadata, new_start, Nat64.fromNat(new_id));
            size += 1;

            // right
            start := get_kv_id_offset(leaf_address, elem_index);
            end := get_kv_id_offset(leaf_address, arr_len);

            new_start := get_kv_id_offset(right_leaf_address, size);
            blob_slice := MemoryRegion.loadBlob(btree.metadata, start, end - start);
            MemoryRegion.storeBlob(btree.metadata, new_start, blob_slice);

            size += (arr_len - elem_index : Nat);
            elems_removed_from_left += size;
        };

        Leaf.update_count(btree, leaf_address, arr_len - elems_removed_from_left);

        if (not is_elem_added_to_right) {
            Leaf.insert(btree, leaf_address, elem_index, new_id);
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

    public func shift(btree : MemoryIdBTree, leaf_address : Nat, start : Nat, end : Nat, offset : Int) {
        if (offset == 0) return;

        let _start = get_kv_id_offset(leaf_address, start);
        let _end = get_kv_id_offset(leaf_address, end);

        MemoryFns.shift(btree.metadata, _start, _end, offset * ADDRESS_SIZE);

    };

    public func remove(btree : MemoryIdBTree, leaf_address : Nat, index : Nat) {
        let count = Leaf.get_count(btree, leaf_address);

        Leaf.shift(btree, leaf_address, index + 1, count, -1); // updates the cache
        Leaf.update_count(btree, leaf_address, count - 1); // updates the cache as well
    };

    public func redistribute(btree : MemoryIdBTree, leaf : Nat, neighbour : Nat) : Bool {
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

            Leaf.shift(btree, leaf, 0, leaf_count, data_to_move);

            let start = get_kv_id_offset(neighbour, neighbour_count - data_to_move);
            let end = get_kv_id_offset(neighbour, neighbour_count);

            let new_start = get_kv_id_offset(leaf, 0);

            var blob_slice = MemoryRegion.loadBlob(btree.metadata, start, end - start);
            MemoryRegion.storeBlob(btree.metadata, new_start, blob_slice);
        } else {
            // adj_node is after leaf_node
            // Debug.print("neighbour is after leaf_node");

            let start = get_kv_id_offset(neighbour, 0);
            let end = get_kv_id_offset(neighbour, data_to_move);

            let new_start = get_kv_id_offset(leaf, leaf_count);

            var blob_slice = MemoryRegion.loadBlob(btree.metadata, start, end - start);
            MemoryRegion.storeBlob(btree.metadata, new_start, blob_slice);

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
    public func deallocate(btree : MemoryIdBTree, leaf : Nat) {
        let memory_size = Leaf.get_memory_size(btree);
        MemoryRegion.deallocate(btree.metadata, leaf, memory_size);
    };

    public func merge(btree : MemoryIdBTree, leaf : Nat, neighbour : Nat) {
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

        let start = get_kv_id_offset(right, 0);
        let end = get_kv_id_offset(right, right_count);

        let new_start = get_kv_id_offset(left, left_count);
        let blob_slice = MemoryRegion.loadBlob(btree.metadata, start, end - start);
        MemoryRegion.storeBlob(btree.metadata, new_start, blob_slice);

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
