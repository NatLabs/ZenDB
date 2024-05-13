import RevIter "mo:itertools/RevIter";

import Migrations "Migrations";
import MemoryBTreeIndex "Base";
import VersionedMemoryBTreeIndex "Versioned";
import T "modules/Types";

module {
    public type MemoryBTreeIndex = Migrations.MemoryBTreeIndex;
    public type VersionedMemoryBTreeIndex = Migrations.VersionedMemoryBTreeIndex;
    public type MemoryBlock = T.MemoryBlock;
    public type IndexUtils<K> = T.IndexUtils<K>;
    type RevIter<A> = RevIter.RevIter<A>;

    /// Create a new stable store
    public func newStableStore(order: ?Nat) : VersionedMemoryBTreeIndex = VersionedMemoryBTreeIndex.new(order);

    /// Upgrade an older version of the BTree to the latest version 
    public func upgrade<K, V>(versions: VersionedMemoryBTreeIndex) : VersionedMemoryBTreeIndex {
        Migrations.upgrade(versions);
    };

    /// MemoryBTreeIndex class
    public class MemoryBTreeIndexClass<K, V>(versions: VersionedMemoryBTreeIndex, btree_utils: IndexUtils<K>){
        let state = Migrations.getCurrentVersion(versions);

        /// Get the value associated with a key
        public func get(key: K) : ?V = MemoryBTreeIndex.get<K, V>(state, btree_utils, key);
        
        /// Get the entry with the maximum key
        public func getMax() : ?(K, V) = MemoryBTreeIndex.getMax<K, V>(state, btree_utils);
        
        /// Get the entry with the minimum key
        public func getMin() : ?(K, V) = MemoryBTreeIndex.getMin<K, V>(state, btree_utils);
        
        /// Get the entry that either matches the key or is the next largest key
        public func getCeiling(key: K) : ?(K, V) = MemoryBTreeIndex.getCeiling<K, V>(state, btree_utils, key);
        
        /// Get the entry that either matches the key or is the next smallest key
        public func getFloor(key: K) : ?(K, V) = MemoryBTreeIndex.getFloor<K, V>(state, btree_utils, key);
        
        /// Get the entry at the given index in the sorted order
        public func getFromIndex(i: Nat) : (K, V) = MemoryBTreeIndex.getFromIndex<K, V>(state, btree_utils, i);
        
        /// Get the index (sorted position) of the given key in the btree
        public func getIndex(key: K) : Nat = MemoryBTreeIndex.getIndex<K, V>(state, btree_utils, key);


        /// Insert a new key-value pair into the BTree
        public func insert(key: K, val: V) : ?V = MemoryBTreeIndex.insert<K, V>(state, btree_utils, key, val);
        
        /// Remove the key-value pair associated with the given key
        public func remove(key: K) : ?V = MemoryBTreeIndex.remove<K, V>(state, btree_utils, key);
        
        /// Remove the entry with the maximum key
        public func removeMax() : ?(K, V) = MemoryBTreeIndex.removeMax<K, V>(state, btree_utils);
        
        /// Remove the entry with the minimum key
        public func removeMin() : ?(K, V) = MemoryBTreeIndex.removeMin<K, V>(state, btree_utils);


        /// Clear the BTree - Remove all entries from the BTree
        public func clear() = MemoryBTreeIndex.clear(state);
        
        /// Returns a reversible iterator over the entries in the BTree
        public func entries() : RevIter<(K, V)> = MemoryBTreeIndex.entries(state, btree_utils);
        
        /// Returns a reversible iterator over the keys in the BTree
        public func keys() : RevIter<(K, Nat)> = MemoryBTreeIndex.keys(state, btree_utils);
        
        /// Returns a reversible iterator over the values in the BTree
        public func vals()  : RevIter<(V)> = MemoryBTreeIndex.vals(state, btree_utils);
        
        /// Returns a reversible iterator over the entries in the given range
        public func range(i: Nat, j: Nat) : RevIter<(K, V)> = MemoryBTreeIndex.range(state, btree_utils, i, j);
        
        /// Returns a reversible iterator over the entries in the given range
        public func scan(start: ?K, end: ?K) : RevIter<(K, V)> = MemoryBTreeIndex.scan(state, btree_utils, start, end);


        /// Returns the number of entries in the BTree
        public func size() : Nat = MemoryBTreeIndex.size(state);


        /// Returns the number of bytes used to store the keys and values data
        public func bytes() : Nat = MemoryBTreeIndex.bytes(state);
        
        /// Retuens the number of bytes used to store information about the nodes and structure of the BTree
        public func metadataBytes() : Nat = MemoryBTreeIndex.metadataBytes(state);

    };

    /// 
    public func fromArray<K, V>(versions: VersionedMemoryBTreeIndex, btree_utils: IndexUtils<K>, arr: [(K, V)]) : MemoryBTreeIndexClass<K, V> {
        let state = Migrations.getCurrentVersion(versions);
        
        for ((k, v) in arr.vals()){
            ignore MemoryBTreeIndex.insert<K, V>(state, btree_utils, k, v);
        };

        MemoryBTreeIndexClass(versions, btree_utils);
    };
}