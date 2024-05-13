import RevIter "mo:itertools/RevIter";

import Migrations "Migrations";
import MemoryBTree "Base";
import T "modules/Types";

module VersionedMemoryBTree {
    public type MemoryBTree = Migrations.MemoryBTree;
    public type VersionedMemoryBTree = Migrations.VersionedMemoryBTree;
    public type MemoryBlock = T.MemoryBlock;
    public type BTreeUtils<K, V> = T.BTreeUtils<K, V>;
    type RevIter<A> = RevIter.RevIter<A>;

    public func new(order : ?Nat) : VersionedMemoryBTree {
        let btree = MemoryBTree.new(order);
        MemoryBTree.toVersioned(btree);
    };

    public func fromArray<K, V>(
        btree_utils : BTreeUtils<K, V>,
        arr : [(K, V)],
        order : ?Nat,
    ) : VersionedMemoryBTree {
        let btree = MemoryBTree.fromArray(btree_utils, arr, order);
        MemoryBTree.toVersioned(btree);
    };

    public func toArray<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>) : [(K, V)] {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.toArray(state, btree_utils);
    };

    public func insert<K, V>(
        btree : VersionedMemoryBTree,
        btree_utils : BTreeUtils<K, V>,
        key : K,
        val : V,
    ) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.insert<K, V>(state, btree_utils, key, val);
    };

    public func remove<K, V>(
        btree : VersionedMemoryBTree,
        btree_utils : BTreeUtils<K, V>,
        key : K,
    ) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.remove(state, btree_utils, key);
    };

    public func removeMax<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.removeMax(state, btree_utils);
    };

    public func removeMin<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.removeMin(state, btree_utils);
    };

    public func get<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.get(state, btree_utils, key);
    };

    public func getMax<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.getMax(state, btree_utils);
    };

    public func getMin<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.getMin(state, btree_utils);
    };

    public func getCeiling<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.getCeiling(state, btree_utils, key);
    };

    public func getFloor<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.getFloor(state, btree_utils, key);
    };

    public func getFromIndex<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>, index : Nat) : (K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.getFromIndex<K, V>(state, btree_utils, index);
    };

    public func getIndex<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>, key : K) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.getIndex(state, btree_utils, key);
    };

    public func clear(btree : VersionedMemoryBTree) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.clear(state);
    };

    public func entries<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.entries(state, btree_utils);
    };

    public func keys<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>) : RevIter<K> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.keys(state, btree_utils);
    };

    public func vals<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>) : RevIter<V> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.vals(state, btree_utils);
    };

    public func scan<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>, start : ?K, end : ?K) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.scan(state, btree_utils, start, end);
    };

    public func range<K, V>(btree : VersionedMemoryBTree, btree_utils : BTreeUtils<K, V>, start : Nat, end : Nat) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.range(state, btree_utils, start, end);
    };

    public func size(btree : VersionedMemoryBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.size(state);
    };

    public func bytes(btree : VersionedMemoryBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.bytes(state);
    };

    public func metadataBytes(btree : VersionedMemoryBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTree.metadataBytes(state);
    };

};
