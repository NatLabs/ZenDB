import RevIter "mo:itertools/RevIter";

import Migrations "Migrations";
import MemoryIdBTree "Base";
import T "modules/Types";

module VersionedMemoryIdBTree {
    public type MemoryIdBTree = Migrations.MemoryIdBTree;
    public type VersionedMemoryIdBTree = Migrations.VersionedMemoryIdBTree;
    public type MemoryBlock = T.MemoryBlock;
    public type BTreeUtils<K, V> = T.BTreeUtils<K, V>;
    type RevIter<A> = RevIter.RevIter<A>;

    public func new(order : ?Nat) : VersionedMemoryIdBTree {
        let btree = MemoryIdBTree.new(order);
        MemoryIdBTree.toVersioned(btree);
    };

    public func fromArray<K, V>(
        btree_utils : BTreeUtils<K, V>,
        arr : [(K, V)],
        order : ?Nat,
    ) : VersionedMemoryIdBTree {
        let btree = MemoryIdBTree.fromArray(btree_utils, arr, order);
        MemoryIdBTree.toVersioned(btree);
    };

    public func toArray<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>) : [(K, V)] {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.toArray(state, btree_utils);
    };

    public func insert<K, V>(
        btree : VersionedMemoryIdBTree,
        btree_utils : BTreeUtils<K, V>,
        key : K,
        val : V,
    ) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.insert<K, V>(state, btree_utils, key, val);
    };

    public func remove<K, V>(
        btree : VersionedMemoryIdBTree,
        btree_utils : BTreeUtils<K, V>,
        key : K,
    ) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.remove(state, btree_utils, key);
    };

    public func removeMax<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.removeMax(state, btree_utils);
    };

    public func removeMin<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.removeMin(state, btree_utils);
    };

    public func get<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.get(state, btree_utils, key);
    };

    public func getMax<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.getMax(state, btree_utils);
    };

    public func getMin<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.getMin(state, btree_utils);
    };

    public func getCeiling<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.getCeiling(state, btree_utils, key);
    };

    public func getFloor<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.getFloor(state, btree_utils, key);
    };

    public func getFromIndex<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>, index : Nat) : (K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.getFromIndex<K, V>(state, btree_utils, index);
    };

    public func getIndex<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>, key : K) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.getIndex(state, btree_utils, key);
    };

    public func clear(btree : VersionedMemoryIdBTree) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.clear(state);
    };

    public func entries<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.entries(state, btree_utils);
    };

    public func keys<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>) : RevIter<K> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.keys(state, btree_utils);
    };

    public func vals<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>) : RevIter<V> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.vals(state, btree_utils);
    };

    public func scan<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>, start : ?K, end : ?K) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.scan(state, btree_utils, start, end);
    };

    public func range<K, V>(btree : VersionedMemoryIdBTree, btree_utils : BTreeUtils<K, V>, start : Nat, end : Nat) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.range(state, btree_utils, start, end);
    };

    public func size(btree : VersionedMemoryIdBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.size(state);
    };

    public func bytes(btree : VersionedMemoryIdBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.bytes(state);
    };

    public func metadataBytes(btree : VersionedMemoryIdBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryIdBTree.metadataBytes(state);
    };

};
