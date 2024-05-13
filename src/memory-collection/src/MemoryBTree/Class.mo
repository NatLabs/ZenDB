import RevIter "mo:itertools/RevIter";

import Migrations "Migrations";
import MemoryBTree "Base";
import VersionedMemoryBTree "Versioned";
import T "modules/Types";

module {
    public type MemoryBTree = Migrations.MemoryBTree;
    public type VersionedMemoryBTree = Migrations.VersionedMemoryBTree;
    public type MemoryBlock = T.MemoryBlock;
    public type BTreeUtils<K, V> = T.BTreeUtils<K, V>;
    type RevIter<A> = RevIter.RevIter<A>;

    /// Create a new stable store
    public func newStableStore(order: ?Nat) : VersionedMemoryBTree = VersionedMemoryBTree.new(order);

    /// Upgrade an older version of the BTree to the latest version 
    public func upgrade<K, V>(versions: VersionedMemoryBTree) : VersionedMemoryBTree {
        Migrations.upgrade(versions);
    };

    /// MemoryBTree class
    public class MemoryBTreeClass<K, V>(versions: VersionedMemoryBTree, btree_utils: BTreeUtils<K, V>){
        let state = Migrations.getCurrentVersion(versions);

        /// Get the value associated with a key
        public func get(key: K) : ?V = MemoryBTree.get<K, V>(state, btree_utils, key);
        
        /// Get the entry with the maximum key
        public func getMax() : ?(K, V) = MemoryBTree.getMax<K, V>(state, btree_utils);
        
        /// Get the entry with the minimum key
        public func getMin() : ?(K, V) = MemoryBTree.getMin<K, V>(state, btree_utils);
        
        /// Get the entry that either matches the key or is the next largest key
        public func getCeiling(key: K) : ?(K, V) = MemoryBTree.getCeiling<K, V>(state, btree_utils, key);
        
        /// Get the entry that either matches the key or is the next smallest key
        public func getFloor(key: K) : ?(K, V) = MemoryBTree.getFloor<K, V>(state, btree_utils, key);
        
        /// Get the entry at the given index in the sorted order
        public func getFromIndex(i: Nat) : (K, V) = MemoryBTree.getFromIndex<K, V>(state, btree_utils, i);
        
        /// Get the index (sorted position) of the given key in the btree
        public func getIndex(key: K) : Nat = MemoryBTree.getIndex<K, V>(state, btree_utils, key);


        /// Insert a new key-value pair into the BTree
        public func insert(key: K, val: V) : ?V = MemoryBTree.insert<K, V>(state, btree_utils, key, val);
        
        /// Remove the key-value pair associated with the given key
        public func remove(key: K) : ?V = MemoryBTree.remove<K, V>(state, btree_utils, key);
        
        /// Remove the entry with the maximum key
        public func removeMax() : ?(K, V) = MemoryBTree.removeMax<K, V>(state, btree_utils);
        
        /// Remove the entry with the minimum key
        public func removeMin() : ?(K, V) = MemoryBTree.removeMin<K, V>(state, btree_utils);


        /// Clear the BTree - Remove all entries from the BTree
        public func clear() = MemoryBTree.clear(state);
        
        /// Returns a reversible iterator over the entries in the BTree
        public func entries() : RevIter<(K, V)> = MemoryBTree.entries(state, btree_utils);
        
        /// Returns a reversible iterator over the keys in the BTree
        public func keys() : RevIter<(K)> = MemoryBTree.keys(state, btree_utils);
        
        /// Returns a reversible iterator over the values in the BTree
        public func vals()  : RevIter<(V)> = MemoryBTree.vals(state, btree_utils);
        
        /// Returns a reversible iterator over the entries in the given range
        public func range(i: Nat, j: Nat) : RevIter<(K, V)> = MemoryBTree.range(state, btree_utils, i, j);
        
        /// Returns a reversible iterator over the entries in the given range
        public func scan(start: ?K, end: ?K) : RevIter<(K, V)> = MemoryBTree.scan(state, btree_utils, start, end);


        /// Returns the number of entries in the BTree
        public func size() : Nat = MemoryBTree.size(state);


        /// Returns the number of bytes used to store the keys and values data
        public func bytes() : Nat = MemoryBTree.bytes(state);
        
        /// Retuens the number of bytes used to store information about the nodes and structure of the BTree
        public func metadataBytes() : Nat = MemoryBTree.metadataBytes(state);

    };

    /// 
    public func fromArray<K, V>(versions: VersionedMemoryBTree, btree_utils: BTreeUtils<K, V>, arr: [(K, V)]) : MemoryBTreeClass<K, V> {
        let state = Migrations.getCurrentVersion(versions);
        
        for ((k, v) in arr.vals()){
            ignore MemoryBTree.insert<K, V>(state, btree_utils, k, v);
        };

        MemoryBTreeClass(versions, btree_utils);
    };
}