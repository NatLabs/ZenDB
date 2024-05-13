import RevIter "mo:itertools/RevIter";

import Migrations "Migrations";
import MemoryBTreeIndex "Base";
import T "modules/Types";

module VersionedMemoryBTreeIndex {
    public type MemoryBTreeIndex = Migrations.MemoryBTreeIndex;
    public type VersionedMemoryBTreeIndex = Migrations.VersionedMemoryBTreeIndex;
    public type MemoryBlock = T.MemoryBlock;
    public type IndexUtils<K> = T.IndexUtils<K>;
    type RevIter<A> = RevIter.RevIter<A>;

    public func new(order : ?Nat) : VersionedMemoryBTreeIndex {
        let btree = MemoryBTreeIndex.new(order);
        MemoryBTreeIndex.toVersioned(btree);
    };

    public func fromArray<K>(
        btree_utils : IndexUtils<K>,
        arr : [(K, Nat)],
        order : ?Nat,
    ) : VersionedMemoryBTreeIndex {
        let btree = MemoryBTreeIndex.fromArray(btree_utils, arr, order);
        MemoryBTreeIndex.toVersioned(btree);
    };

    public func toArray<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>) : [(K, Nat)] {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.toArray(state, btree_utils);
    };

    public func insert<K>(
        btree : VersionedMemoryBTreeIndex,
        btree_utils : IndexUtils<K>,
        key : K,
        val : Nat,
    ) : ?Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.insert<K>(state, btree_utils, key, val);
    };

    public func remove<K>(
        btree : VersionedMemoryBTreeIndex,
        btree_utils : IndexUtils<K>,
        key : K,
    ) : ?Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.remove(state, btree_utils, key);
    };

    public func removeMax<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>) : ?(K, Nat) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.removeMax(state, btree_utils);
    };

    public func removeMin<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>) : ?(K, Nat) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.removeMin(state, btree_utils);
    };

    public func get<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>, key : K) : ?Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.get(state, btree_utils, key);
    };

    public func getMax<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>) : ?(K, Nat) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.getMax(state, btree_utils);
    };

    public func getMin<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>) : ?(K, Nat) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.getMin(state, btree_utils);
    };

    public func getCeiling<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>, key : K) : ?(K, Nat) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.getCeiling(state, btree_utils, key);
    };

    public func getFloor<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>, key : K) : ?(K, Nat) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.getFloor(state, btree_utils, key);
    };

    public func getFromIndex<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>, index : Nat) : (K, Nat) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.getFromIndex<K>(state, btree_utils, index);
    };

    public func getIndex<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>, key : K) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.getIndex(state, btree_utils, key);
    };

    public func clear(btree : VersionedMemoryBTreeIndex) {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.clear(state);
    };

    public func entries<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>) : RevIter<(K, Nat)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.entries(state, btree_utils);
    };

    public func keys<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>) : RevIter<K> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.keys(state, btree_utils);
    };

    public func vals<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>) : RevIter<Nat> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.vals(state, btree_utils);
    };

    public func scan<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>, start : ?K, end : ?K) : RevIter<(K, Nat)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.scan(state, btree_utils, start, end);
    };

    public func range<K>(btree : VersionedMemoryBTreeIndex, btree_utils : IndexUtils<K>, start : Nat, end : Nat) : RevIter<(K, Nat)> {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.range(state, btree_utils, start, end);
    };

    public func size(btree : VersionedMemoryBTreeIndex) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.size(state);
    };

    public func bytes(btree : VersionedMemoryBTreeIndex) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.bytes(state);
    };

    public func metadataBytes(btree : VersionedMemoryBTreeIndex) : Nat {
        let state = Migrations.getCurrentVersion(btree);
        MemoryBTreeIndex.metadataBytes(state);
    };

};
