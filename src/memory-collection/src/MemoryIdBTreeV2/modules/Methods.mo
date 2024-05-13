import Debug "mo:base/Debug";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Buffer "mo:base/Buffer";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
import RevIter "mo:itertools/RevIter";
import BufferDeque "mo:buffer-deque/BufferDeque";
// import Branch "mo:augmented-btrees/BpTree/Branch";

import T "Types";
import Leaf "Leaf";
import Branch "Branch";
import Migrations "../Migrations";
import BTreeUtils "../BTreeUtils";

module {
    public type Leaf = Migrations.Leaf;
    type MemoryBTree = Migrations.MemoryBTree;
    type MemoryBlock = T.MemoryBlock;

    type Address = Nat;
    type MemoryRegion = MemoryRegion.MemoryRegion;
    type LruCache<K, V> = LruCache.LruCache<K, V>;
    type RevIter<A> = RevIter.RevIter<A>;
    type Node = Migrations.Node;
    public type BTreeUtils<K, V> = T.BTreeUtils<K, V>;

    public type Branch = Migrations.Branch;

    public func get_leaf_address<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>, key : K, _opt_key_blob : ?Blob) : Nat {
        var curr_address = btree.root;
        var opt_key_blob : ?Blob = _opt_key_blob;

        loop {
            switch (Branch.get_node_type(btree, curr_address)) {
                case (#leaf) {
                    assert Leaf.validate(btree, curr_address);
                    Leaf.add_to_cache(btree, curr_address);
                    return curr_address;
                };
                case (#branch) {
                    // load breanch from stable memory
                    // and add it to the cache
                    Branch.add_to_cache(btree, curr_address);
                    assert MemoryRegion.loadBlob(btree.metadata, curr_address, Branch.MC.MAGIC_SIZE) == Branch.MC.MAGIC;

                    let count = Branch.get_count(btree, curr_address);

                    let int_index = switch (btree_utils.cmp) {
                        case (#GenCmp(cmp)) Branch.binary_search<K, V>(btree, btree_utils, curr_address, cmp, key, count - 1);
                        case (#BlobCmp(cmp)) {

                            let key_blob = switch (opt_key_blob) {
                                case (null) {
                                    let key_blob = btree_utils.key.to_blob(key);
                                    opt_key_blob := ?key_blob;
                                    key_blob;
                                };
                                case (?key_blob) key_blob;
                            };

                            Branch.binary_search_blob_seq(btree, curr_address, cmp, key_blob, count - 1);
                        };
                    };

                    let child_index = if (int_index >= 0) Int.abs(int_index) + 1 else Int.abs(int_index + 1);
                    let ?child_address = Branch.get_child(btree, curr_address, child_index) else Debug.trap("get_leaf_node: accessed a null value");
                    curr_address := child_address;
                };
            };
        };
    };

    public func get_leaf_address_and_update_path<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>, key : K, _opt_key_blob : ?Blob, update : (MemoryBTree, Nat, Nat) -> ()) : Nat {
        var curr_address = btree.root;
        var opt_key_blob : ?Blob = _opt_key_blob;

        loop {
            switch (Branch.get_node_type(btree, curr_address)) {
                case (#leaf) {
                    assert Leaf.validate(btree, curr_address);
                    Leaf.add_to_cache(btree, curr_address);
                    return curr_address;
                };
                case (#branch) {
                    // load breanch from stable memory
                    // and add it to the cache
                    Branch.add_to_cache(btree, curr_address);
                    assert MemoryRegion.loadBlob(btree.metadata, curr_address, Branch.MC.MAGIC_SIZE) == Branch.MC.MAGIC;

                    let count = Branch.get_count(btree, curr_address);

                    let int_index = switch (btree_utils.cmp) {
                        case (#GenCmp(cmp)) Branch.binary_search<K, V>(btree, btree_utils, curr_address, cmp, key, count - 1);
                        case (#BlobCmp(cmp)) {

                            let key_blob = switch (opt_key_blob) {
                                case (null) {
                                    let key_blob = btree_utils.key.to_blob(key);
                                    opt_key_blob := ?key_blob;
                                    key_blob;
                                };
                                case (?key_blob) key_blob;
                            };

                            Branch.binary_search_blob_seq(btree, curr_address, cmp, key_blob, count - 1);
                        };
                    };

                    let child_index = if (int_index >= 0) Int.abs(int_index) + 1 else Int.abs(int_index + 1);
                    let ?child_address = Branch.get_child(btree, curr_address, child_index) else Debug.trap("get_leaf_node: accessed a null value");
                    update(btree, curr_address, child_index);
                    curr_address := child_address;
                };
            };
        };
    };

    public func get_min_leaf_address(btree : MemoryBTree) : Nat {
        var curr = btree.root;

        loop {
            switch (Branch.get_node_type(btree, curr)) {
                case (#branch) {
                    let ?first_child = Branch.get_child(btree, curr, 0) else Debug.trap("get_min_leaf: accessed a null value");
                    curr := first_child;
                };
                case (#leaf) return curr;
            };
        };
    };

    public func get_max_leaf_address(btree : MemoryBTree) : Nat {
        var curr = btree.root;

        loop {
            switch (Branch.get_node_type(btree, curr)) {
                case (#branch) {
                    let count = Branch.get_count(btree, curr);
                    let ?first_child = Branch.get_child(btree, curr, count - 1) else Debug.trap("get_min_leaf: accessed a null value");
                    curr := first_child;
                };
                case (#leaf) return curr;
            };
        };
    };

    public func update_leaf_to_root(btree : MemoryBTree, leaf_address : Nat, update : (MemoryBTree, Nat, Nat) -> ()) {
        var parent = Leaf.get_parent(btree, leaf_address);
        var child_index = Leaf.get_index(btree, leaf_address);

        loop {
            switch (parent) {
                case (?branch_address) {
                    update(btree, branch_address, child_index);
                    child_index := Branch.get_index(btree, branch_address);
                    parent := Branch.get_parent(btree, branch_address);
                };

                case (_) return;
            };
        };
    };

    public func update_branch_to_root(btree : MemoryBTree, branch_address : Nat, update : (MemoryBTree, Nat, Nat) -> ()) {
        var parent = Branch.get_parent(btree, branch_address);
        var child_index = Branch.get_index(btree, branch_address);

        loop {
            switch (parent) {
                case (?branch_address) {
                    update(btree, branch_address, child_index);
                    child_index := Branch.get_index(btree, branch_address);
                    parent := Branch.get_parent(btree, branch_address);
                };

                case (_) return;
            };
        };
    };

    // Returns the leaf node and rank of the first element in the leaf node
    public func get_leaf_node_and_index<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>, key : Blob) : (Address, Nat) {
        let node_type = Branch.get_node_type(btree, btree.root);

        let branch = switch (node_type) {
            case (#branch(_)) btree.root;
            case (#leaf(_)) return (btree.root, 0);
        };

        var rank = Branch.get_subtree_size(btree, branch);

        func get_node(parent : Address, key : Blob) : Address {
            let parent_count = Branch.get_count(btree, parent);
            var i = parent_count - 1 : Nat;

            label get_node_loop while (i >= 1) {
                let ?child = Branch.get_child(btree, parent, i) else Debug.trap("get_leaf_node_and_index 0: accessed a null value");
                let ?search_key = Branch.get_key_blob(btree, parent, i - 1) else Debug.trap("get_leaf_node_and_index 1: accessed a null value");

                switch (Branch.get_node_type(btree, child)) {
                    case (#branch(_)) {
                        
                        // Debug.print("branch child: " # debug_show child);
                        switch(btree_utils.cmp) {
                            case (#GenCmp(cmp)) {
                                let ds_key = btree_utils.key.from_blob(key);
                                let ds_search_key = btree_utils.key.from_blob(search_key);
                                // Debug.print("(key, search_key, res) -> " # debug_show (key, search_key, cmp(ds_key, ds_search_key)));

                                if (cmp(ds_key, ds_search_key) >= 0) {
                                    return get_node(child, key);
                                };
                            };
                            case (#BlobCmp(cmp)) {
                                // Debug.print("(key, search_key, res) -> " # debug_show (key, search_key, cmp(key, search_key)));
                                if (cmp(key, search_key) >= 0) {
                                    return get_node(child, key);
                                };
                            };
                        };

                        rank -= Branch.get_subtree_size(btree, child);
                    };
                    case (#leaf(_)) {
                        // Debug.print("leaf child: " # debug_show child);
                        // subtract before comparison because we want the rank of the first element in the leaf node
                        rank -= Leaf.get_count(btree, child);

                        switch(btree_utils.cmp) {
                            case (#GenCmp(cmp)) {
                                let ds_key = btree_utils.key.from_blob(key);
                                let ds_search_key = btree_utils.key.from_blob(search_key);
                                // Debug.print("(key, search_key, res) -> " # debug_show (key, search_key, cmp(ds_key, ds_search_key)));
                                if (cmp(ds_key, ds_search_key) >= 0) {
                                    return child;
                                };
                            };
                            case (#BlobCmp(cmp)) {
                                // Debug.print("(key, search_key, res) -> " # debug_show (key, search_key, cmp(key, search_key)));
                                if (cmp(key, search_key) >= 0) {
                                    return child;
                                };
                            };
                        };

                    };
                };

                i -= 1;
            };

            let ?first_child = Branch.get_child(btree, parent, 0) else Debug.trap("get_leaf_node_and_index 2: accessed a null value");
            // Debug.print("first_child: " # debug_show first_child);
            
            switch (Branch.get_node_type(btree, first_child)) {
                case (#branch) {
                    return get_node(first_child, key);
                };
                case ( #leaf) {
                    rank -= Leaf.get_count(btree, first_child);
                    return first_child;
                };
            };
        };

        (get_node(branch, key), rank);
    };

    public func get_leaf_node_by_index<K, V>(btree : MemoryBTree, rank : Nat) : (Address, Nat) {
        let root = switch (Branch.get_node_type(btree, btree.root)) {
            case (#branch) btree.root;
            case (#leaf(_)) return (btree.root, rank);
        };

        var search_index = rank;

        func get_node(parent : Address) : Address {
            var i = Branch.get_count(btree, parent) - 1 : Nat;
            var child_index = Branch.get_subtree_size(btree, parent);

            label get_node_loop loop {
                let ?child = Branch.get_child(btree, parent, i) else Debug.trap("get_leaf_node_by_index 0: accessed a null value");

                switch (Branch.get_node_type(btree, child)) {
                    case (#branch) {
                        let subtree = Branch.get_subtree_size(btree, child);

                        child_index -= subtree;
                        if (child_index <= search_index) {
                            search_index -= child_index;
                            return get_node(child);
                        };

                    };
                    case (#leaf) {
                        let subtree = Leaf.get_count(btree, child);
                        child_index -= subtree;

                        if (child_index <= search_index) {
                            search_index -= child_index;
                            return child;
                        };

                    };
                };

                i -= 1;
            };

            Debug.trap("get_leaf_node_by_index 3: reached unreachable code");
        };

        (get_node(root), search_index);
    };


    public func new_blobs_iterator(
        btree : MemoryBTree,
        start_leaf : Nat,
        start_index : Nat,
        end_leaf : Nat,
        end_index : Nat // exclusive
    ) : RevIter<(Blob, Blob)> {

        var start = start_leaf;
        var i = start_index;
        var start_count = Leaf.get_count(btree, start_leaf);

        var end = end_leaf;
        var j = end_index;

        var terminate = false;

        func next() : ?(Blob, Blob) {
            if (terminate) return null;

            if (start == end and i >= j) {
                return null;
            };

            if (i >= start_count) {
                switch (Leaf.get_next(btree, start)) {
                    case (null) {
                        terminate := true;
                    };
                    case (?next_address) {
                        start := next_address;
                        start_count := Leaf.get_count(btree, next_address);
                    };
                };

                i := 0;
                return next();
            };

            let opt_kv = Leaf.get_kv_blobs(btree, start, i);

            i += 1;
            return opt_kv;
        };

        func nextFromEnd() : ?(Blob, Blob) {
            if (terminate) return null;

            if (start == end and i >= j) return null;

            if (j == 0) {
                switch (Leaf.get_prev(btree, end)) {
                    case (null) terminate := true;
                    case (?prev_address) {
                        end := prev_address;
                        j := Leaf.get_count(btree, prev_address);
                    };
                };

                return nextFromEnd();
            };

            let opt_kv = Leaf.get_kv_blobs(btree, end, j - 1);

            j -= 1;

            return opt_kv;
        };

        RevIter.new(next, nextFromEnd);
    };

    public func key_val_blobs(btree : MemoryBTree) : RevIter<(Blob, Blob)> {
        let min_leaf = get_min_leaf_address(btree);
        let max_leaf = get_max_leaf_address(btree);
        let max_leaf_count = Leaf.get_count(btree, max_leaf);

        new_blobs_iterator(btree, min_leaf, 0, max_leaf, max_leaf_count);
    };

    public func deserialize_kv_blobs<K, V>(btree_utils : BTreeUtils<K, V>, key_blob : Blob, val_blob : Blob) : (K, V) {
        let key = btree_utils.key.from_blob(key_blob);
        let value = btree_utils.val.from_blob(val_blob);
        (key, value);
    };

    public func entries<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>) : RevIter<(K, V)> {
        RevIter.map<(Blob, Blob), (K, V)>(
            key_val_blobs(btree),
            func((key_blob, val_blob) : (Blob, Blob)) : (K, V) {
                deserialize_kv_blobs(btree_utils, key_blob, val_blob)
            },
        );
    };

    public func keys<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>) : RevIter<(K)> {
        RevIter.map<(Blob, Blob), (K)>(
            key_val_blobs(btree),
            func((key_blob, _) : (Blob, Blob)) : (K) {
                let key = btree_utils.key.from_blob(key_blob);
                key;
            },
        );
    };

    public func vals<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>) : RevIter<(V)> {
        RevIter.map<(Blob, Blob), V>(
            key_val_blobs(btree),
            func((_, val_blob) : (Blob, Blob)) : V {
                let value = btree_utils.val.from_blob(val_blob);
                value;
            },
        );
    };

    public func new_leaf_address_iterator(
        btree : MemoryBTree,
        start_leaf : Nat,
        end_leaf : Nat,
    ) : RevIter<Nat> {

        var start = start_leaf;
        var end = end_leaf;

        var terminate = false;

        func next() : ?Nat {
            if (terminate) return null;

            if (start == end) terminate := true;

            let curr = start;

            switch (Leaf.get_next(btree, start)) {
                case (null) terminate := true;
                case (?next_address) start := next_address;
            };

            return ?curr;
        };

        func nextFromEnd() : ?Nat {
            if (terminate) return null;

            if (start == end) terminate := true;

            let curr = end;

            switch (Leaf.get_prev(btree, end)) {
                case (null) terminate := true;
                case (?prev_address) end := prev_address;
            };

            return ?curr;
        };

        RevIter.new(next, nextFromEnd);
    };

    public func leaf_addresses(btree : MemoryBTree) : RevIter<Nat> {
        let min_leaf = get_min_leaf_address(btree);
        let max_leaf = get_max_leaf_address(btree);

        new_leaf_address_iterator(btree, min_leaf, max_leaf);
    };

    public func leaf_nodes<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>) : RevIter<[?(K, V)]> {
        let min_leaf = get_min_leaf_address(btree);
        let max_leaf = get_max_leaf_address(btree);

        RevIter.map<Nat, [?(K, V)]>(
            new_leaf_address_iterator(btree, min_leaf, max_leaf),
            func(leaf_address : Nat) : [?(K, V)] {
                
                let count = Leaf.get_count(btree, leaf_address);
                Array.tabulate<?(K, V)>(
                    btree.order,
                    func(i : Nat) : ?(K, V) {
                        if (i >= count) return null;

                        // Debug.print("(address, i) -> " # debug_show (leaf_address, i));
                        let ?(key, val) = Leaf.get_kv_blobs(btree, leaf_address, i) else Debug.trap("leaf_nodes: accessed a null value");
                        ?(btree_utils.key.from_blob(key), btree_utils.val.from_blob(val));
                    },
                );
            },
        );
    };

    public func node_keys<K, V>(btree : MemoryBTree, btree_utils : BTreeUtils<K, V>) : [[(Nat, [?K])]] {
        var nodes = BufferDeque.fromArray<Address>([btree.root]);
        var buffer = Buffer.Buffer<[(Nat, [?K])]>(btree.branch_count);

        while (nodes.size() > 0) {
            let row = Buffer.Buffer<(Nat, [?K])>(nodes.size());

            for (_ in Iter.range(1, nodes.size())) {
                let ?node = nodes.popFront() else Debug.trap("node_keys: accessed a null value");

                switch (Branch.get_node_type(btree, node)) {
                    case (#leaf) {};
                    case (#branch) {
                        let keys = Array.tabulate<?K>(
                            btree.order - 1,
                            func(i : Nat) : ?K {
                                switch (Branch.get_key_blob(btree, node, i)) {
                                    case (?key_blob) {
                                        let key = btree_utils.key.from_blob(key_blob);
                                        return ?key;
                                    };
                                    case (_) return null;
                                };
                            },
                        );

                        row.add((node, keys));

                        for (i in Iter.range(0, Branch.get_count(btree, node) - 1)) {
                            let ?child = Branch.get_child(btree, node, i) else Debug.trap("node_keys: accessed a null value");
                            nodes.addBack(child);
                        };

                    };

                };
            };

            buffer.add(Buffer.toArray(row));

        };

        Buffer.toArray(buffer);
    };

    public func validate_memory(btree : MemoryBTree, btree_utils : BTreeUtils<Nat, Nat >) : Bool {
        // LruCache.clear(btree.nodes_cache);

        func _validate(address : Nat) : (index : Nat, subtree_size : Nat) {
            assert MemoryRegion.loadBlob(btree.metadata, address, Leaf.MAGIC_SIZE) == Leaf.MAGIC;

            switch (Branch.get_node_type(btree, address)) {
                case (#leaf) {
                    let leaf = Leaf.from_memory(btree, address);

                    let index = Leaf.get_index(btree, address);
                    let count = Leaf.get_count(btree, address);

                    assert index == leaf.0[Leaf.AC.INDEX];
                    assert count == leaf.0[Leaf.AC.COUNT];
                    assert address == leaf.0[Leaf.AC.ADDRESS];

                    var i = 0;

                    var prev_key : ?Blob = null;
                    while (i < count) {
                        let ?key_block = Leaf.get_key_block(btree, address, i) else Debug.trap("validate: accessed a null value");
                        let ?val_block = Leaf.get_val_block(btree, address, i) else Debug.trap("validate: accessed a null value");
                        let ?key = Leaf.get_key_blob(btree, address, i) else Debug.trap("validate: accessed a null value");
                        let ?val = Leaf.get_val_blob(btree, address, i) else Debug.trap("validate: accessed a null value");

                        assert leaf.2[i] == ?key_block;
                        assert leaf.3[i] == ?val_block;
                        assert leaf.4[i] == ?(key, val);

                        switch (prev_key) {
                            case (null) {};
                            case (?prev) {
                                switch (btree_utils.cmp) {
                                    case (#GenCmp(cmp)) {
                                        let _prev = btree_utils.key.from_blob(prev);
                                        let _key = btree_utils.key.from_blob(key);
                                        // Debug.print("l (prev, key): " # debug_show (_prev, _key));

                                        if (cmp(_prev, _key) != -1) {
                                            Debug.print("key mismatch at index: " # debug_show i);
                                            Debug.print("prev: " # debug_show prev);
                                            Debug.print("key: " # debug_show key);
                                            Leaf.display(btree, btree_utils, address);

                                            assert false;
                                        };
                                    };
                                    case (#BlobCmp(cmp)) {
                                        if (cmp(prev, key) != -1) {
                                            Debug.print("key mismatch at index: " # debug_show i);
                                            Debug.print("prev: " # debug_show prev);
                                            Debug.print("key: " # debug_show key);
                                            Leaf.display(btree, btree_utils, address);

                                            assert false;
                                        };
                                    };
                                };
                            };
                        };

                        prev_key := ?key;

                        i += 1;
                    };

                    assert i == count;
                    (index, count);
                };
                case (#branch) {
                    let branch = Branch.from_memory(btree, address);

                    let index = Branch.get_index(btree, address);
                    let count = Branch.get_count(btree, address);
                    let subtree_size = Branch.get_subtree_size(btree, address);
                    var children_subtree = 0;

                    assert index == branch.0[Branch.AC.INDEX];
                    assert count == branch.0[Branch.AC.COUNT];
                    assert address == branch.0[Branch.AC.ADDRESS];
                    assert subtree_size == branch.0[Branch.AC.SUBTREE_SIZE];
                    
                    var i = 0;

                    var prev_key : ?Blob = null;

                    while (i < count) {
                        if (i + 1 < count) {
                            let ?key = Branch.get_key_blob(btree, address, i) else Debug.trap("validate: accessed a null value");

                            assert ?key == branch.6[i];

                            switch (prev_key) {
                                case (null) {};
                                case (?prev) {
                                    switch (btree_utils.cmp) {
                                        case (#GenCmp(cmp)) {
                                            let _prev = btree_utils.key.from_blob(prev);
                                            let _key = btree_utils.key.from_blob(key);
                                            // Debug.print("b (prev, key): " # debug_show (_prev, _key));
                                            if (cmp(_prev, _key) != -1) {
                                                Debug.print("key mismatch at index: " # debug_show i);
                                                Debug.print("prev: " # debug_show prev);
                                                Debug.print("key: " # debug_show key);
                                                Branch.display(btree, btree_utils, address);
                                                assert false;
                                            };
                                        };
                                        case (#BlobCmp(cmp)) {
                                            if (cmp(prev, key) != -1) {
                                                Debug.print("key mismatch at index: " # debug_show i);
                                                Debug.print("prev: " # debug_show prev);
                                                Debug.print("key: " # debug_show key);
                                                Branch.display(btree,  btree_utils, address);

                                                assert false;
                                            };
                                        };
                                    };
                                };
                            };

                            prev_key := ?key;
                        };

                        let ?child = Branch.get_child(btree, address, i) else Debug.trap("validate: accessed a null value");
                        // Debug.print("address: " # debug_show address # " -> child: " # debug_show child);
                        let (child_index, child_subtree_size) = _validate(child);

                        assert child_index == i;
                        children_subtree += child_subtree_size;

                        i += 1;
                    };

                    assert i == count;
                    if (children_subtree != subtree_size) {
                        Debug.print("children_subtree: " # debug_show children_subtree);
                        Debug.print("branch subtree_size: " # debug_show subtree_size);
                        assert false;
                    };

                    (index, subtree_size);
                };
            };
        };

        let response = _validate(btree.root);
        // Debug.print("Validate response: " # debug_show response);
        // Debug.print("expected response: " # debug_show (0, Branch.get_node_subtree_size(btree, btree.root)));
        response == (0, Branch.get_node_subtree_size(btree, btree.root));
    };

};
