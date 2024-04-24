import Nat "mo:base/Nat";

import MemoryRegion "mo:memory-region/MemoryRegion";
import LruCache "mo:lru-cache";
import RevIter "mo:itertools/RevIter";
// import Branch "mo:augmented-btrees/BpTree/Branch";

import Blobify "../../Blobify";
import MemoryCmp "../../MemoryCmp";

module {
    public type Address = Nat;
    type Size = Nat;

    public type MemoryBlock = (Address, Size);

    type MemoryRegionV1 = MemoryRegion.MemoryRegionV1;
    type LruCache<K, V> = LruCache.LruCache<K, V>;
    type Blobify<A> = Blobify.Blobify<A>;
    type RevIter<A> = RevIter.RevIter<A>;

    public type MemoryCmp<A> = MemoryCmp.MemoryCmp<A>;

    public type BTreeUtils<K, V> =  {
        key: Blobify<K>;
        val: Blobify<V>;
        cmp: MemoryCmp<K>;
    };

    public type NodeType = {
        #branch;
        #leaf;
    };

};
