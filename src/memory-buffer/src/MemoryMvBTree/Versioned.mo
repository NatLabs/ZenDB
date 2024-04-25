import RevIter "mo:itertools/RevIter";

import Migrations "Migrations";
import MemoryMvBTree "Base";
import T "modules/Types";

module VersionedMemoryMvBTree {
    public type MemoryMvBTree = Migrations.MemoryMvBTree;
    public type VersionedMemoryMvBTree = Migrations.VersionedMemoryMvBTree;
    public type MemoryBlock = T.MemoryBlock;
    public type BTreeUtils<K, V> = T.BTreeUtils<K, V>;
    type RevIter<A> = RevIter.RevIter<A>;

    public func new(order : ?Nat) : VersionedMemoryMvBTree {
        let btree = MemoryMvBTree.new(order);
        MemoryMvBTree.toVersioned(btree);
    };

    public func fromArray<K, V>(
        btree_utils : BTreeUtils<K, V>,
        arr : [(K, V)],
        order : ?Nat,
    ) : VersionedMemoryMvBTree {
        let btree = MemoryMvBTree.fromArray(btree_utils, arr, order);
        MemoryMvBTree.toVersioned(btree);
    };

    public func toArray<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>) : [(K, V)] {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.toArray(state, btree_utils);
    };

    public func insert<K, V>(
        btree : VersionedMemoryMvBTree,
        btree_utils : BTreeUtils<K, V>,
        key : K,
        val : V,
    ) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.insert<K, V>(state, btree_utils, key, val);
    };

    public func remove<K, V>(
        btree : VersionedMemoryMvBTree,
        btree_utils : BTreeUtils<K, V>,
        key : K,
    ) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.remove(state, btree_utils, key);
    };

    public func removeMax<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.removeMax(state, btree_utils);
    };

    public func removeMin<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.removeMin(state, btree_utils);
    };

    public func get<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?V {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.get(state, btree_utils, key);
    };

    public func getMax<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.getMax(state, btree_utils);
    };

    public func getMin<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.getMin(state, btree_utils);
    };

    public func getCeiling<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.getCeiling(state, btree_utils, key);
    };

    public func getFloor<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : ?(K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.getFloor(state, btree_utils, key);
    };

    public func getFromIndex<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>, index : Nat) : (K, V) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.getFromIndex<K, V>(state, btree_utils, index);
    };

    public func getIndex<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>, key : K) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.getIndex(state, btree_utils, key);
    };

    public func clear(btree : VersionedMemoryMvBTree) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.clear(state);
    };

    public func entries<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.entries(state, btree_utils);
    };

    public func keys<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>) : RevIter<K> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.keys(state, btree_utils);
    };

    public func vals<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>) : RevIter<V> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.vals(state, btree_utils);
    };

    public func scan<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>, start : ?K, end : ?K) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.scan(state, btree_utils, start, end);
    };

    public func range<K, V>(btree : VersionedMemoryMvBTree, btree_utils : BTreeUtils<K, V>, start : Nat, end : Nat) : RevIter<(K, V)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.range(state, btree_utils, start, end);
    };

    public func size(btree : VersionedMemoryMvBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.size(state);
    };

    public func bytes(btree : VersionedMemoryMvBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.bytes(state);
    };

    public func metadataBytes(btree : VersionedMemoryMvBTree) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryMvBTree.metadataBytes(state);
    };

};
