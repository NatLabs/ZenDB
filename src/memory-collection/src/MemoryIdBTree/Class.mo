import RevIter "mo:itertools/RevIter";

import Migrations "Migrations";
import MemoryIdBTree "Base";
import VersionedMemoryIdBTree "Versioned";
import T "modules/Types";

module {
    public type MemoryIdBTree = Migrations.MemoryIdBTree;
    public type VersionedMemoryIdBTree = Migrations.VersionedMemoryIdBTree;
    public type MemoryBlock = T.MemoryBlock;
    public type BTreeUtils<K, V> = T.BTreeUtils<K, V>;
    type RevIter<A> = RevIter.RevIter<A>;

    /// Create a new stable store
    public func newStableStore(order: ?Nat) : VersionedMemoryIdBTree = VersionedMemoryIdBTree.new(order);

    /// Upgrade an older version of the BTree to the latest version 
    public func upgrade<K, V>(versions: VersionedMemoryIdBTree) : VersionedMemoryIdBTree {
        Migrations.upgrade(versions);
    };

    /// MemoryIdBTree class
    public class MemoryIdBTreeClass<K, V>(versions: VersionedMemoryIdBTree, btree_utils: BTreeUtils<K, V>){
        let state = Migrations.getCurrentVersion(versions);

        /// Get the value associated with a key
        public func get(key: K) : ?V = MemoryIdBTree.get<K, V>(state, btree_utils, key);
        
        /// Get the entry with the maximum key
        public func getMax() : ?(K, V) = MemoryIdBTree.getMax<K, V>(state, btree_utils);
        
        /// Get the entry with the minimum key
        public func getMin() : ?(K, V) = MemoryIdBTree.getMin<K, V>(state, btree_utils);
        
        /// Get the entry that either matches the key or is the next largest key
        public func getCeiling(key: K) : ?(K, V) = MemoryIdBTree.getCeiling<K, V>(state, btree_utils, key);
        
        /// Get the entry that either matches the key or is the next smallest key
        public func getFloor(key: K) : ?(K, V) = MemoryIdBTree.getFloor<K, V>(state, btree_utils, key);
        
        /// Get the entry at the given index in the sorted order
        public func getFromIndex(i: Nat) : (K, V) = MemoryIdBTree.getFromIndex<K, V>(state, btree_utils, i);
        
        /// Get the index (sorted position) of the given key in the btree
        public func getIndex(key: K) : Nat = MemoryIdBTree.getIndex<K, V>(state, btree_utils, key);


        /// Insert a new key-value pair into the BTree
        public func insert(key: K, val: V) : ?V = MemoryIdBTree.insert<K, V>(state, btree_utils, key, val);
        
        /// Remove the key-value pair associated with the given key
        public func remove(key: K) : ?V = MemoryIdBTree.remove<K, V>(state, btree_utils, key);
        
        /// Remove the entry with the maximum key
        public func removeMax() : ?(K, V) = MemoryIdBTree.removeMax<K, V>(state, btree_utils);
        
        /// Remove the entry with the minimum key
        public func removeMin() : ?(K, V) = MemoryIdBTree.removeMin<K, V>(state, btree_utils);


        /// Clear the BTree - Remove all entries from the BTree
        public func clear() = MemoryIdBTree.clear(state);
        
        /// Returns a reversible iterator over the entries in the BTree
        public func entries() : RevIter<(K, V)> = MemoryIdBTree.entries(state, btree_utils);
        
        /// Returns a reversible iterator over the keys in the BTree
        public func keys() : RevIter<(K)> = MemoryIdBTree.keys(state, btree_utils);
        
        /// Returns a reversible iterator over the values in the BTree
        public func vals()  : RevIter<(V)> = MemoryIdBTree.vals(state, btree_utils);
        
        /// Returns a reversible iterator over the entries in the given range
        public func range(i: Nat, j: Nat) : RevIter<(K, V)> = MemoryIdBTree.range(state, btree_utils, i, j);
        
        /// Returns a reversible iterator over the entries in the given range
        public func scan(start: ?K, end: ?K) : RevIter<(K, V)> = MemoryIdBTree.scan(state, btree_utils, start, end);


        /// Returns the number of entries in the BTree
        public func size() : Nat = MemoryIdBTree.size(state);


        /// Returns the number of bytes used to store the keys and values data
        public func bytes() : Nat = MemoryIdBTree.bytes(state);
        
        /// Retuens the number of bytes used to store information about the nodes and structure of the BTree
        public func metadataBytes() : Nat = MemoryIdBTree.metadataBytes(state);


        /// Functions for Unique Id References to values in the BTree

        /// Get the id associated with a key
        public func getId(key: K) : ?Nat = MemoryIdBTree.getId(state, btree_utils, key);

        /// Get the next available id that will be assigned to a new value
        public func nextId() : Nat = MemoryIdBTree.nextId(state);

        /// Get the entry associated with the given id
        public func lookup(id: Nat) : ?(K, V) = MemoryIdBTree.lookup(state, btree_utils, id);

        /// Get the key associated with the given id
        public func lookupKey(id: Nat) : ?K = MemoryIdBTree.lookupKey(state, btree_utils, id);

        /// Get the value associated with the given id
        public func lookupVal(id: Nat) : ?V = MemoryIdBTree.lookupVal(state, btree_utils, id);

        /// Reference a value by its id and increment the reference count
        /// Values will not be removed from the BTree until the reference count is zero
        public func reference(id: Nat)  = MemoryIdBTree.reference(state, btree_utils, id);

        /// Get the reference count associated with the given id
        public func getRefCount(id: Nat) : ?Nat = MemoryIdBTree.getRefCount(state, btree_utils, id);

    };

    /// 
    public func fromArray<K, V>(versions: VersionedMemoryIdBTree, btree_utils: BTreeUtils<K, V>, arr: [(K, V)]) : MemoryIdBTreeClass<K, V> {
        let state = Migrations.getCurrentVersion(versions);
        
        for ((k, v) in arr.vals()){
            ignore MemoryIdBTree.insert<K, V>(state, btree_utils, k, v);
        };

        MemoryIdBTreeClass(versions, btree_utils);
    };
}