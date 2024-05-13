/// A BTreeIndex that stores entries with duplicate keys for non-unique indexes

import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Nat8 "mo:base/Nat8";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Blob "mo:base/Blob";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
import RevIter "mo:itertools/RevIter";
import Itertools "mo:itertools/Iter";
// import Branch "mo:augmented-btrees/BpTree/Branch";

import Blobify "../Blobify";
import MemoryCmp "../MemoryCmp";
import Methods "modules/Methods";
import MemoryBlock "modules/MemoryBlock";
import Branch "modules/Branch";
import Utils "../Utils";
import Migrations "Migrations";
import Leaf "modules/Leaf";
import T "modules/Types";

module {
    type Address = Nat;
    type MemoryRegion = MemoryRegion.MemoryRegion;
    type LruCache<K, V> = LruCache.LruCache<K, V>;
    type Blobify<A> = Blobify.Blobify<A>;
    type RevIter<A> = RevIter.RevIter<A>;
    type Iter<A> = Iter.Iter<A>;
    type UniqueId = T.UniqueId;

    public type MemoryCmp<A> = MemoryCmp.MemoryCmp<A>;

    public type MemoryMvBTree = Migrations.MemoryMvBTree;
    public type Node = Migrations.Node;
    public type Leaf = Migrations.Leaf;
    public type Branch = Migrations.Branch;
    public type VersionedMemoryMvBTree = Migrations.VersionedMemoryMvBTree;

    public type MemoryBlock = T.MemoryBlock;
    public type BTreeUtils<K, V> = T.BTreeUtils<K, V>;

    let CACHE_LIMIT = 50_000;
    let DEFAULT_ORDER = 32;

    public func _new_with_options(order : ?Nat, opt_cache_size : ?Nat, is_set : Bool) : MemoryMvBTree {
        let cache_size = Option.get(opt_cache_size, CACHE_LIMIT);
        let btree_map : MemoryMvBTree = {
            is_set;
            order = Option.get(order, DEFAULT_ORDER);
            var count = 0;
            var root = 0;
            var branch_count = 0;
            var leaf_count = 0;

            metadata = MemoryRegion.new();
            blocks = MemoryRegion.new();
            blobs = MemoryRegion.new();

            nodes_cache = LruCache.new(0);
            key_cache = LruCache.new<Nat, Blob>(cache_size);
        };

        init_region_header(btree_map);

        let leaf_address = Leaf.new(btree_map);
        update_leaf_count(btree_map, 1);
        update_root(btree_map, leaf_address);

        return btree_map;
    };

    public func new_set(order : ?Nat, cache_size : ?Nat) : MemoryMvBTree {
        _new_with_options(order, cache_size, true);
    };

    public func new(order : ?Nat) : MemoryMvBTree {
        _new_with_options(order, ?0, false);
    };

    public let BLOBS_REGION_HEADER_SIZE = 64;
    public let METADATA_REGION_HEADER_SIZE = 96;

    public let POINTER_SIZE = 12;
    public let LAYOUT_VERSION = 0;

    let MC = {
        MAGIC_NUMBER_ADDRESS = 00;
        LAYOUT_VERSION_ADDRESS = 3;
        REGION_ID_ADDRESS = 4;
        BLOBS_REGION_ID_ADDRESS = 8;
        ORDER_ADDRESS = 12;
        ROOT_ADDRESS = 20;
        COUNT_ADDRESS = 28;
        BRANCH_COUNT = 36;
        LEAF_COUNT = 44;
        BLOB_START = BLOBS_REGION_HEADER_SIZE;
        NODES_START = METADATA_REGION_HEADER_SIZE;
    };

    public let Layout = [MC];

    func init_region_header(btree : MemoryMvBTree) {
        assert MemoryRegion.size(btree.blobs) == 0;
        assert MemoryRegion.size(btree.metadata) == 0;

        // Each Region has a 64 byte header
        ignore MemoryRegion.allocate(btree.blobs, BLOBS_REGION_HEADER_SIZE); // Reserved Space for the Region Header
        MemoryRegion.storeBlob(btree.blobs, MC.MAGIC_NUMBER_ADDRESS, "BLB"); // MAGIC NUMBER (BLB -> Blob Region) 3 bytes
        MemoryRegion.storeNat8(btree.blobs, MC.LAYOUT_VERSION_ADDRESS, Nat8.fromNat(LAYOUT_VERSION)); // |1 byte | Layout Version (1)
        MemoryRegion.storeNat32(btree.blobs, MC.REGION_ID_ADDRESS, Nat32.fromNat(MemoryRegion.id(btree.metadata))); // store the pointers region id in the blob region
        assert MemoryRegion.size(btree.blobs) == BLOBS_REGION_HEADER_SIZE;

        ignore MemoryRegion.allocate(btree.blocks, BLOBS_REGION_HEADER_SIZE); // Reserved Space for the Region Header
        MemoryRegion.storeBlob(btree.blocks, MC.MAGIC_NUMBER_ADDRESS, "BLK"); // |3 bytes| MAGIC NUMBER (BLK -> Blocks Region)
        MemoryRegion.storeNat8(btree.blocks, MC.LAYOUT_VERSION_ADDRESS, Nat8.fromNat(LAYOUT_VERSION)); // |1 byte | Layout Version (1)
        MemoryRegion.storeNat32(btree.blocks, MC.REGION_ID_ADDRESS, Nat32.fromNat(MemoryRegion.id(btree.metadata))); // store the metadata region id in the pointers region
        assert MemoryRegion.size(btree.blocks) == BLOBS_REGION_HEADER_SIZE;

        // | 64 byte header | -> 3 bytes + 1 byte + 8 bytes + 52 bytes
        ignore MemoryRegion.allocate(btree.metadata, METADATA_REGION_HEADER_SIZE); // Reserved Space for the Region Header
        MemoryRegion.storeBlob(btree.metadata, MC.MAGIC_NUMBER_ADDRESS, "BTR"); // |3 bytes| MAGIC NUMBER (BTM -> BTree Map Region)
        MemoryRegion.storeNat8(btree.metadata, MC.LAYOUT_VERSION_ADDRESS, Nat8.fromNat(LAYOUT_VERSION)); // |1 byte | Layout Version (1)
        MemoryRegion.storeNat32(btree.metadata, MC.REGION_ID_ADDRESS, Nat32.fromNat(MemoryRegion.id(btree.blocks))); // store the blocks region id in the pointers region
        MemoryRegion.storeNat32(btree.metadata, MC.BLOBS_REGION_ID_ADDRESS, Nat32.fromNat(MemoryRegion.id(btree.blobs))); // store the blobs region id in the pointers region
        MemoryRegion.storeNat16(btree.metadata, MC.ORDER_ADDRESS, Nat16.fromNat(btree.order)); // |2 bytes| Order -> Number of elements allowed in each node
        MemoryRegion.storeNat64(btree.metadata, MC.ROOT_ADDRESS, 0); // |8 bytes| Root -> Address of the root node
        MemoryRegion.storeNat64(btree.metadata, MC.COUNT_ADDRESS, 0); // |8 bytes| Count -> Number of elements in the buffer
        MemoryRegion.storeNat64(btree.metadata, MC.BRANCH_COUNT, 0); // |8 bytes| Branch Count -> Number of Branch Nodes
        MemoryRegion.storeNat64(btree.metadata, MC.LEAF_COUNT, 0); // |8 bytes| Leaf Count -> Number of Leaf Nodes
        assert MemoryRegion.size(btree.metadata) == METADATA_REGION_HEADER_SIZE;
    };

    public func size(btree : MemoryMvBTree) : Nat {
        btree.count;
    };

    public func fromVersioned(btree : VersionedMemoryMvBTree) : MemoryMvBTree {
        Migrations.getCurrentVersion(btree);
    };

    public func toVersioned(btree : MemoryMvBTree) : VersionedMemoryMvBTree {
        #v0(btree);
    };

    public func bytes(btree : MemoryMvBTree) : Nat {
        MemoryRegion.allocated(btree.blobs);
    };

    public func metadataBytes(btree : MemoryMvBTree) : Nat {
        MemoryRegion.allocated(btree.metadata);
    };

    func update_root(btree : MemoryMvBTree, new_root : Address) {
        btree.root := new_root;
        MemoryRegion.storeNat64(btree.metadata, MC.ROOT_ADDRESS, Nat64.fromNat(new_root));
    };

    func update_count(btree : MemoryMvBTree, new_count : Nat) {
        btree.count := new_count;
        MemoryRegion.storeNat64(btree.metadata, MC.COUNT_ADDRESS, Nat64.fromNat(new_count));
    };

    func update_branch_count(btree : MemoryMvBTree, new_count : Nat) {
        btree.branch_count := new_count;
        MemoryRegion.storeNat64(btree.metadata, MC.BRANCH_COUNT, Nat64.fromNat(new_count));
    };

    func update_leaf_count(btree : MemoryMvBTree, new_count : Nat) {
        btree.leaf_count := new_count;
        MemoryRegion.storeNat64(btree.metadata, MC.LEAF_COUNT, Nat64.fromNat(new_count));
    };

    func inc_subtree_size(btree : MemoryMvBTree, branch_address : Nat, _child_index : Nat) {
        let subtree_size = Branch.get_subtree_size(btree, branch_address);
        Branch.update_subtree_size(btree, branch_address, subtree_size + 1);
    };

    public func insert<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K, value : V) : ?V {
        let key_blob = btree_utils.key.to_blob(key);

        let leaf_address = Methods.get_leaf_address_and_update_path(btree, btree_utils, key, ?key_blob, inc_subtree_size);
        let count = Leaf.get_count(btree, leaf_address);

        let int_index = switch (btree_utils.cmp) {
            case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, leaf_address, cmp, key, count);
            case (#blob_cmp(cmp)) {
                Leaf.binary_search_blob_seq(btree, leaf_address, cmp, key_blob, count);
            };
        };

        let elem_index = if (int_index >= 0) Int.abs(int_index) else Int.abs(int_index + 1);

        let val_blob = btree_utils.val.to_blob(value);

        if (int_index >= 0) {
            // existing key
            let ?key_val_id = Leaf.get_kv_id(btree, leaf_address, elem_index) else Debug.trap("insert: accessed a null value");
            // let prev_val_blob = MemoryBlock.get_val_blob(btree, key_val_id);

            MemoryBlock.append(btree, key_val_id, val_blob);
            // Leaf.update_count(btree, leaf_address, count + 1);
            // update_count(btree, btree.count + 1);

            Methods.update_leaf_to_root(btree, leaf_address, decrement_subtree_size);

            // return ?btree_utils.val.from_blob(prev_val_blob);

            return null;
        };

        let key_val_id = MemoryBlock.store(btree, key_blob, val_blob);

        if (count < btree.order) {
            Leaf.insert_with_count(btree, leaf_address, elem_index, key_val_id, count);
            update_count(btree, btree.count + 1);

            // Methods.update_leaf_to_root(btree, leaf_address, inc_subtree_size);
            return null;
        };

        // split leaf
        var left_node_address = leaf_address;
        var right_node_address = Leaf.split(btree, left_node_address, elem_index, key_val_id);
        update_leaf_count(btree, btree.leaf_count + 1);
        // Debug.print("left leaf after split: " # debug_show Leaf.from_memory(btree, left_node_address));
        // Debug.print("right leaf after split: " # debug_show Leaf.from_memory(btree, right_node_address));

        var opt_parent = Leaf.get_parent(btree, right_node_address);
        var right_index = Leaf.get_index(btree, right_node_address);

        let ?first_key_id = Leaf.get_kv_id(btree, right_node_address, 0) else Debug.trap("insert: first_key_id accessed a null value");
        var median_key_id = first_key_id;

        // assert Leaf.get_count(btree, left_node_address) == (btree.order / 2) + 1;
        // assert Leaf.get_count(btree, right_node_address) == (btree.order / 2);

        while (Option.isSome(opt_parent)) {

            let ?parent_address = opt_parent else Debug.trap("insert: Failed to get parent address");

            let parent_count = Branch.get_count(btree, parent_address);
            // assert MemoryRegion.loadBlob(btree.metadata, parent_address, Branch.MC.MAGIC_SIZE) == Branch.MC.MAGIC;

            // insert right node in parent if there is enough space
            if (parent_count < btree.order) {
                // Debug.print("found branch with enough space");
                // Debug.print("parent before insert: " # debug_show Branch.from_memory(btree, parent_address));

                Branch.insert(btree, parent_address, right_index, median_key_id, right_node_address);
                update_count(btree, btree.count + 1);

                // Debug.print("parent after insert: " # debug_show Branch.from_memory(btree, parent_address));

                // Methods.update_branch_to_root(btree, parent_address, inc_subtree_size);
                return null;
            };

            // otherwise split parent
            left_node_address := parent_address;
            right_node_address := Branch.split(btree, left_node_address, right_index, median_key_id, right_node_address);
            update_branch_count(btree, btree.branch_count + 1);

            let ?first_key_id = Branch.get_key_id(btree, right_node_address, btree.order - 2) else Debug.trap("4. insert: accessed a null value in first key of branch");
            Branch.set_key_id_to_null(btree, right_node_address, btree.order - 2);
            median_key_id := first_key_id;

            right_index := Branch.get_index(btree, right_node_address);
            opt_parent := Branch.get_parent(btree, right_node_address);

        };

        // Debug.print("new root");
        // new root
        let new_root = Branch.new(btree);
        update_branch_count(btree, btree.branch_count + 1);

        Branch.put_key_id(btree, new_root, 0, median_key_id);

        Branch.add_child(btree, new_root, left_node_address);
        Branch.add_child(btree, new_root, right_node_address);

        assert Branch.get_count(btree, new_root) == 2;

        update_root(btree, new_root);
        update_count(btree, btree.count + 1);

        null;
    };

    public func get<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?V {
        let key_blob = btree_utils.key.to_blob(key);

        let leaf_address = Methods.get_leaf_address(btree, btree_utils, key, ?key_blob);
        let count = Leaf.get_count(btree, leaf_address);

        let int_index = switch (btree_utils.cmp) {
            case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, leaf_address, cmp, key, count);
            case (#blob_cmp(cmp)) {
                Leaf.binary_search_blob_seq(btree, leaf_address, cmp, key_blob, count);
            };
        };

        if (int_index < 0) return null;

        let elem_index = Int.abs(int_index);

        let ?val_blob = Leaf.get_val_blob(btree, leaf_address, elem_index) else Debug.trap("get: accessed a null value");
        let value = btree_utils.val.from_blob(val_blob);
        ?value;
    };

    public func getAll<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : Iter<V> {
        let key_blob = btree_utils.key.to_blob(key);

        let leaf_address = Methods.get_leaf_address(btree, btree_utils, key, ?key_blob);
        let count = Leaf.get_count(btree, leaf_address);

        let int_index = switch (btree_utils.cmp) {
            case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, leaf_address, cmp, key, count);
            case (#blob_cmp(cmp)) {
                Leaf.binary_search_blob_seq(btree, leaf_address, cmp, key_blob, count);
            };
        };

        if (int_index < 0) return Itertools.empty();

        Debug.print("had to search for the key in the leaf node");
        let elem_index = Int.abs(int_index);

        let ?id = Leaf.get_kv_id(btree, leaf_address, elem_index) else Debug.trap("getAll: accessed a null value");
        Iter.map(
            MemoryBlock.get_val_blobs(btree, id),
            func(blob : Blob) : V {
                btree_utils.val.from_blob(blob);
            },
        );

    };

    public func getMin<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let leaf_address = Methods.get_min_leaf_address(btree);

        let ?key_blob = Leaf.get_key_blob(btree, leaf_address, 0) else Debug.trap("getMin: accessed a null value");
        let ?val_blob = Leaf.get_val_blob(btree, leaf_address, 0) else Debug.trap("getMin: accessed a null value");

        let key = btree_utils.key.from_blob(key_blob);
        let value = btree_utils.val.from_blob(val_blob);
        ?(key, value);
    };

    public func getMax<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let leaf_address = Methods.get_max_leaf_address(btree);
        let count = Leaf.get_count(btree, leaf_address);

        let ?key_blob = Leaf.get_key_blob(btree, leaf_address, count - 1 : Nat) else Debug.trap("getMax: accessed a null value");
        let ?val_blob = Leaf.get_val_blob(btree, leaf_address, count - 1 : Nat) else Debug.trap("getMax: accessed a null value");

        let key = btree_utils.key.from_blob(key_blob);
        let value = btree_utils.val.from_blob(val_blob);
        ?(key, value);
    };

    func clear_region_after_header(region : MemoryRegion, header_size : Nat) {
        var prev_size = header_size;

        for ((mem_block) in MemoryRegion.getFreeMemory(region).vals()) {
            assert prev_size <= mem_block.0;
            MemoryRegion.deallocate(region, prev_size, mem_block.0 - prev_size);
            prev_size := mem_block.0 + mem_block.1;
        };

        if (prev_size < MemoryRegion.size(region)) {
            MemoryRegion.deallocate(region, prev_size, MemoryRegion.size(region) - prev_size);
        };

        assert MemoryRegion.allocated(region) == header_size;

    };

    public func clear(btree : MemoryMvBTree) {
        // clear cache
        LruCache.clear(btree.nodes_cache);

        // the first leaf node should be at the address where the header ends
        let leaf_address = MC.NODES_START;
        assert Leaf.validate(btree, leaf_address);

        Leaf.clear(btree, leaf_address);
        assert Leaf.validate(btree, leaf_address);

        update_root(btree, leaf_address);
        update_count(btree, 0);
        update_branch_count(btree, 0);
        update_leaf_count(btree, 1);

        let leaf_memory_size = Leaf.get_memory_size(btree);
        let everything_after_leaf = leaf_address + leaf_memory_size;
        clear_region_after_header(btree.metadata, everything_after_leaf);

        clear_region_after_header(btree.blocks, BLOBS_REGION_HEADER_SIZE);

        // Debug.print("blobs size: " # debug_show MemoryRegion.size(btree.blobs));
        clear_region_after_header(btree.blobs, BLOBS_REGION_HEADER_SIZE);

    };

    func decrement_subtree_size(btree : MemoryMvBTree, branch_address : Nat, _child_index : Nat) {
        let subtree_size = Branch.get_subtree_size(btree, branch_address);
        Branch.update_subtree_size(btree, branch_address, subtree_size - 1);
    };

    public func remove<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?V {
        let key_blob = btree_utils.key.to_blob(key);

        let leaf_address = Methods.get_leaf_address_and_update_path(btree, btree_utils, key, ?key_blob, decrement_subtree_size);
        let count = Leaf.get_count(btree, leaf_address);

        let int_index = switch (btree_utils.cmp) {
            case (#cmp(cmp)) Leaf.binary_search(btree, btree_utils, leaf_address, cmp, key, count);
            case (#blob_cmp(cmp)) {
                Leaf.binary_search_blob_seq(btree, leaf_address, cmp, key_blob, count);
            };
        };

        if (int_index < 0) {
            // key not found, so revert the path to its original state by incrementing the subtree size
            Methods.update_leaf_to_root(btree, leaf_address, inc_subtree_size);
            return null;
        };

        let elem_index = Int.abs(int_index);

        let ?prev_kv_id = Leaf.get_kv_id(btree, leaf_address, elem_index) else Debug.trap("remove: prev_kv_id is null");

        let prev_val_blob = MemoryBlock.get_val_blob(btree, prev_kv_id);
        let prev_val = btree_utils.val.from_blob(prev_val_blob);

        MemoryBlock.remove(btree, prev_kv_id); // deallocate key and value blocks
        Leaf.remove(btree, leaf_address, elem_index); // remove the deleted key-value pair from the leaf
        update_count(btree, btree.count - 1);

        let ?parent_address = Leaf.get_parent(btree, leaf_address) else return ?prev_val; // if parent is null then leaf_node is the root
        var parent = parent_address;
        // Debug.print("Leaf's parent: " # debug_show Branch.from_memory(btree, parent));

        let leaf_index = Leaf.get_index(btree, leaf_address);

        if (elem_index == 0) {
            // if the first element is removed then update the parent key
            let ?next_key_id = Leaf.get_kv_id(btree, leaf_address, 0) else Debug.trap("remove: next_key_block is null");
            Branch.update_median_key_id(btree, parent, leaf_index, next_key_id);
        };

        let min_count = btree.order / 2;
        let leaf_count = Leaf.get_count(btree, leaf_address);

        if (leaf_count >= min_count) return ?prev_val;

        // redistribute entries from larger neighbour to the current leaf below min_count
        let ?neighbour = Branch.get_larger_neighbour(btree, parent, leaf_index) else Debug.trap("remove: neighbour is null");

        let neighbour_index = Leaf.get_index(btree, neighbour);

        let left = if (leaf_index < neighbour_index) { leaf_address } else {
            neighbour;
        };
        let right = if (leaf_index < neighbour_index) { neighbour } else {
            leaf_address;
        };

        // let left_index = if (leaf_index < neighbour_index) { leaf_index } else { neighbour_index };
        let right_index = if (leaf_index < neighbour_index) { neighbour_index } else {
            leaf_index;
        };

        // Debug.print("leaf.redistribute");

        if (Leaf.redistribute(btree, leaf_address, neighbour)) {

            let ?key_id = Leaf.get_kv_id(btree, right, 0) else Debug.trap("remove: key_block is null");
            Branch.put_key_id(btree, parent, right_index - 1, key_id);

            return ?prev_val;
        };

        // Debug.print("merging leaf");

        // remove merged leaf from parent
        // Debug.print("remove merged index: " # debug_show right_index);
        // Debug.print("parent: " # debug_show Branch.from_memory(btree, parent));

        // merge leaf with neighbour
        Leaf.merge(btree, left, right);
        Branch.remove(btree, parent, right_index);
        Branch.rm_from_cache(btree, right);

        // deallocate right leaf that was merged into left
        Leaf.deallocate(btree, right);
        update_leaf_count(btree, btree.leaf_count - 1);

        // Debug.print("parent: " # debug_show Branch.from_memory(btree, parent));
        // Debug.print("leaf_nodes: " # debug_show Iter.toArray(Methods.leaf_addresses(btree)));

        func set_only_child_to_root(parent : Address) : ?V {
            if (Branch.get_count(btree, parent) == 1) {
                let ?child = Branch.get_child(btree, parent, 0) else Debug.trap("set_only_child_to_root: child is null");

                switch (Branch.get_node_type(btree, child)) {
                    case (#leaf) Leaf.update_parent(btree, child, null);
                    case (#branch) Branch.update_parent(btree, child, null);
                };

                update_root(btree, child);
                return ?prev_val;

            } else {
                return ?prev_val;
            };
        };

        var branch = parent;
        let ?branch_parent = Branch.get_parent(btree, branch) else return set_only_child_to_root(parent);

        parent := branch_parent;

        while (Branch.get_count(btree, branch) < min_count) {
            // Debug.print("redistribute branch");
            // Debug.print("parent before redistribute: " # debug_show Branch.from_memory(btree, parent));
            // Debug.print("branch before redistribute: " # debug_show Branch.from_memory(btree, branch));

            if (Branch.redistribute(btree, branch)) {
                // Debug.print("parent after redistribute: " # debug_show Branch.from_memory(btree, parent));
                // Debug.print("branch after redistribute: " # debug_show Branch.from_memory(btree, branch));
                return ?prev_val;
            };

            // Debug.print("branch after redistribute: " # debug_show Branch.from_memory(btree, branch));

            // Debug.print("merging branch");
            // Debug.print("parent before merge: " # debug_show Branch.from_memory(btree, parent));
            let branch_index = Branch.get_index(btree, branch);
            let ?neighbour = Branch.get_larger_neighbour(btree, parent, branch_index) else Debug.trap("Branch.merge: neighbour should not be null");

            let merged_branch = Branch.merge(btree, branch, neighbour);
            let merged_branch_index = Branch.get_index(btree, merged_branch);
            Branch.remove(btree, parent, merged_branch_index);
            Branch.rm_from_cache(btree, merged_branch);
            Branch.deallocate(btree, merged_branch);
            update_branch_count(btree, btree.branch_count - 1);

            // Debug.print("parent after merge: " # debug_show Branch.from_memory(btree, parent));
            // Debug.print("leaf_nodes: " # debug_show Iter.toArray(Methods.leaf_addresses(btree)));

            branch := parent;
            let ?branch_parent = Branch.get_parent(btree, branch) else return set_only_child_to_root(parent);

            parent := branch_parent;

            if (Branch.get_count(btree, parent) == 1) {
                return set_only_child_to_root(parent);
            };
        };

        return ?prev_val;
    };

    public func removeMin<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let ?min = getMin<K, V>(btree, btree_utils) else return null;
        ignore remove<K, V>(btree, btree_utils, min.0);
        ?min;
    };

    public func removeMax<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let ?max = getMax<K, V>(btree, btree_utils) else return null;
        ignore remove<K, V>(btree, btree_utils, max.0);
        ?max;
    };

    public func fromArray<K, V>(btree_utils : BTreeUtils<K, V>, arr : [(K, V)], order : ?Nat) : MemoryMvBTree {
        fromEntries(btree_utils, arr.vals(), order);
    };

    public func fromEntries<K, V>(btree_utils : BTreeUtils<K, V>, entries : Iter<(K, V)>, order : ?Nat) : MemoryMvBTree {
        let btree = new(order);

        for ((k, v) in entries) {
            ignore insert(btree, btree_utils, k, v);
        };

        btree;
    };

    public func getCeiling<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?(K, V) {
        let key_blob = btree_utils.key.to_blob(key);
        let leaf_address = Methods.get_leaf_address<K, V>(btree, btree_utils, key, ?key_blob);

        let i = switch (btree_utils.cmp) {
            case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, leaf_address, cmp, key, Leaf.get_count(btree, leaf_address));
            case (#blob_cmp(cmp)) {
                Leaf.binary_search_blob_seq(btree, leaf_address, cmp, key_blob, Leaf.get_count(btree, leaf_address));
            };
        };

        if (i >= 0) {
            let ?(k, v) = Leaf.get_kv_blobs(btree, leaf_address, Int.abs(i)) else return null;
            return ?Methods.deserialize_kv_blobs<K, V>(btree_utils, k, v);
        };

        let expected_index = Int.abs(i) - 1 : Nat;

        if (expected_index == Leaf.get_count(btree, leaf_address)) {
            let ?next_address = Leaf.get_next(btree, leaf_address) else return null;
            let ?(k, v) = Leaf.get_kv_blobs(btree, next_address, 0) else return null;
            return ?Methods.deserialize_kv_blobs<K, V>(btree_utils, k, v);
        };

        let ?(k, v) = Leaf.get_kv_blobs(btree, leaf_address, expected_index) else return null;
        return ?Methods.deserialize_kv_blobs<K, V>(btree_utils, k, v);
    };

    public func getFloor<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?(K, V) {
        let key_blob = btree_utils.key.to_blob(key);
        let leaf_address = Methods.get_leaf_address<K, V>(btree, btree_utils, key, ?key_blob);

        let i = switch (btree_utils.cmp) {
            case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, leaf_address, cmp, key, Leaf.get_count(btree, leaf_address));
            case (#blob_cmp(cmp)) {
                Leaf.binary_search_blob_seq(btree, leaf_address, cmp, key_blob, Leaf.get_count(btree, leaf_address));
            };
        };

        if (i >= 0) {
            let ?kv_blobs = Leaf.get_kv_blobs(btree, leaf_address, Int.abs(i)) else return null;
            return ?Methods.deserialize_kv_blobs<K, V>(btree_utils, kv_blobs.0, kv_blobs.1);
        };

        let expected_index = Int.abs(i) - 1 : Nat;

        if (expected_index == 0) {
            let ?prev_address = Leaf.get_prev(btree, leaf_address) else return null;
            let prev_count = Leaf.get_count(btree, prev_address);
            let ?(k, v) = Leaf.get_kv_blobs(btree, prev_address, prev_count - 1) else return null;
            return ?Methods.deserialize_kv_blobs<K, V>(btree_utils, k, v);
        };

        let ?kv_blobs = Leaf.get_kv_blobs(btree, leaf_address, expected_index - 1) else return null;
        return ?Methods.deserialize_kv_blobs<K, V>(btree_utils, kv_blobs.0, kv_blobs.1);
    };

    // Returns the key-value pair at the given index.
    // Throws an error if the index is greater than the size of the tree.
    public func getFromIndex<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, index : Nat) : (K, V) {
        if (index >= btree.count) return Debug.trap("getFromIndex: index is out of bounds");

        let (leaf_address, i) = Methods.get_leaf_node_by_index(btree, index);

        let ?entry = Leaf.get_kv_blobs(btree, leaf_address, i) else Debug.trap("getFromIndex: accessed a null value");

        Methods.deserialize_kv_blobs(btree_utils, entry.0, entry.1);
    };

    // Returns the index of the given key in the tree.
    // Throws an error if the key does not exist in the tree.
    public func getIndex<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : Nat {
        let key_blob = btree_utils.key.to_blob(key);
        let (leaf_address, index_pos) = Methods.get_leaf_node_and_index(btree, btree_utils, key_blob);

        // Leaf.display(btree, btree_utils, leaf_address);
        // Debug.print("leaf_address: " # debug_show Leaf.from_memory(btree,  leaf_address));
        // Debug.print("index_pos: " # debug_show index_pos);
        // Debug.print("key_blob: " # debug_show key_blob);
        let count = Leaf.get_count(btree, leaf_address);
        let int_index = switch (btree_utils.cmp) {
            case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, leaf_address, cmp, key, count);
            case (#blob_cmp(cmp)) {
                Leaf.binary_search_blob_seq(btree, leaf_address, cmp, key_blob, count);
            };
        };

        // Debug.print("int_index: " # debug_show int_index);

        if (int_index < 0) {
            Debug.trap("getIndex(): key does not exist in the tree. Try using getCeiling() or getFloor() to get the closest key");
        };

        index_pos + Int.abs(int_index);
    };

    public func range<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, start : Nat, end : Nat) : RevIter<(K, V)> {
        let (start_node, start_node_index) = Methods.get_leaf_node_by_index(btree, start);
        let (end_node, end_node_index) = Methods.get_leaf_node_by_index(btree, end);

        let start_index = start_node_index : Nat;
        let end_index = end_node_index + 1 : Nat; // + 1 because the end index is exclusive

        RevIter.map<(Blob, Blob), (K, V)>(
            Methods.new_blobs_iterator(btree, start_node, start_index, end_node, end_index),
            func((key_blob, val_blob) : (Blob, Blob)) : (K, V) {
                Methods.deserialize_kv_blobs<K, V>(btree_utils, key_blob, val_blob);
            },
        );
    };

    public func scan<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, start : ?K, end : ?K) : RevIter<(K, V)> {
        let start_address = switch (start) {
            case (?key) {
                let key_blob = btree_utils.key.to_blob(key);
                Methods.get_leaf_address(btree, btree_utils, key, ?key_blob);
            };
            case (null) Methods.get_min_leaf_address(btree);
        };

        let start_index = switch (start) {
            case (?key) switch (btree_utils.cmp) {
                case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, start_address, cmp, key, Leaf.get_count(btree, start_address));
                case (#blob_cmp(cmp)) {
                    let key_blob = btree_utils.key.to_blob(key);
                    Leaf.binary_search_blob_seq(btree, start_address, cmp, key_blob, Leaf.get_count(btree, start_address));
                };
            };
            case (null) 0;
        };

        // if start_index is negative then the element was not found
        // moreover if start_index is negative then abs(i) - 1 is the index of the first element greater than start
        var i = if (start_index >= 0) Int.abs(start_index) else Int.abs(start_index) - 1 : Nat;

        let end_address = switch (end) {
            case (?key) {
                let key_blob = btree_utils.key.to_blob(key);
                Methods.get_leaf_address(btree, btree_utils, key, ?key_blob);
            };
            case (null) Methods.get_max_leaf_address(btree);
        };

        let end_index = switch (end) {
            case (?key) switch (btree_utils.cmp) {
                case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, end_address, cmp, key, Leaf.get_count(btree, end_address));
                case (#blob_cmp(cmp)) {
                    let key_blob = btree_utils.key.to_blob(key);
                    Leaf.binary_search_blob_seq(btree, end_address, cmp, key_blob, Leaf.get_count(btree, end_address));
                };
            };
            case (null) Leaf.get_count(btree, end_address);
        };

        var j = if (end_index >= 0) Int.abs(end_index) + 1 else Int.abs(end_index) - 1 : Nat;

        RevIter.map<(Blob, Blob), (K, V)>(
            Methods.new_blobs_iterator(btree, start_address, i, end_address, j),
            func((key_blob, val_blob) : (Blob, Blob)) : (K, V) {
                Methods.deserialize_kv_blobs<K, V>(btree_utils, key_blob, val_blob);
            },
        );
    };

    public func scan_v2<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>, start : ?K, end : ?K) : Iter<(K, V)> {
        let start_address = switch (start) {
            case (?key) {
                let key_blob = btree_utils.key.to_blob(key);
                Methods.get_leaf_address(btree, btree_utils, key, ?key_blob);
            };
            case (null) Methods.get_min_leaf_address(btree);
        };

        let start_index = switch (start) {
            case (?key) switch (btree_utils.cmp) {
                case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, start_address, cmp, key, Leaf.get_count(btree, start_address));
                case (#blob_cmp(cmp)) {
                    let key_blob = btree_utils.key.to_blob(key);
                    Leaf.binary_search_blob_seq(btree, start_address, cmp, key_blob, Leaf.get_count(btree, start_address));
                };
            };
            case (null) 0;
        };

        // if start_index is negative then the element was not found
        // moreover if start_index is negative then abs(i) - 1 is the index of the first element greater than start
        var i = if (start_index >= 0) Int.abs(start_index) else Int.abs(start_index) - 1 : Nat;

        let end_address = switch (end) {
            case (?key) {
                let key_blob = btree_utils.key.to_blob(key);
                Methods.get_leaf_address(btree, btree_utils, key, ?key_blob);
            };
            case (null) Methods.get_max_leaf_address(btree);
        };

        let end_index = switch (end) {
            case (?key) switch (btree_utils.cmp) {
                case (#cmp(cmp)) Leaf.binary_search<K, V>(btree, btree_utils, end_address, cmp, key, Leaf.get_count(btree, end_address));
                case (#blob_cmp(cmp)) {
                    let key_blob = btree_utils.key.to_blob(key);
                    Leaf.binary_search_blob_seq(btree, end_address, cmp, key_blob, Leaf.get_count(btree, end_address));
                };
            };
            case (null) Leaf.get_count(btree, end_address);
        };

        var j = if (end_index >= 0) Int.abs(end_index) + 1 else Int.abs(end_index) - 1 : Nat;

        func id_to_blob(id : Nat) : Iter<(Blob, Blob)> {
            let key = MemoryBlock.get_key_blob(btree, id);
            let vals = MemoryBlock.get_val_blobs(btree, id);

            Iter.map<Blob, (Blob, Blob)>(
                vals,
                func(val : Blob) : (Blob, Blob) {
                    (key, val);
                },
            );
        };

        let ids = Methods.new_internal_id_iterator(btree, start_address, i, end_address, j);
        Iter.map<Nat, Iter<(Blob, Blob)>>(ids, id_to_blob ) 
            |> Itertools.flatten<(Blob, Blob)>(_)
            |> Iter.map<(Blob, Blob), (K, V)>(
                _,
                func((key_blob, val_blob) : (Blob, Blob)) : (K, V) {
                    Methods.deserialize_kv_blobs<K, V>(btree_utils, key_blob, val_blob);
                },
            );

    };

    public func entries<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : RevIter<(K, V)> {
        Methods.entries(btree, btree_utils);
    };

    public func toEntries<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : [(K, V)] {
        Utils.sized_iter_to_array<(K, V)>(entries(btree, btree_utils), btree.count);
    };

    public func toArray<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : [(K, V)] {
        Utils.sized_iter_to_array<(K, V)>(entries(btree, btree_utils), btree.count);
    };

    public func keys<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : RevIter<K> {
        Methods.keys(btree, btree_utils);
    };

    public func toKeys<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : [K] {
        Utils.sized_iter_to_array<K>(keys(btree, btree_utils), btree.count);
    };

    public func vals<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : RevIter<V> {
        Methods.vals(btree, btree_utils);
    };

    public func toVals<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : [V] {
        Utils.sized_iter_to_array<V>(vals(btree, btree_utils), btree.count);
    };

    public func leafNodes<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : RevIter<[?(K, V)]> {
        Methods.leaf_nodes(btree, btree_utils);
    };

    public func toLeafNodes<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : [[?(K, V)]] {
        Utils.sized_iter_to_array<[?(K, V)]>(leafNodes<K, V>(btree, btree_utils), btree.leaf_count);
    };

    public func toNodeKeys<K, V>(btree : MemoryMvBTree, btree_utils : BTreeUtils<K, V>) : [[(Nat, [?K])]] {
        Methods.node_keys(btree, btree_utils);
    };

};
