/// Default Utils for the MemoryBTree module

import Blobify "../Blobify";
import MemoryCmp "../MemoryCmp";
import Int8Cmp "../Int8Cmp";
import Int "mo:base/Int";

module {

    type Blobify<A> = Blobify.Blobify<A>;
    type MemoryCmp<A> = MemoryCmp.MemoryCmp<A>;

    /// Blobify and cmp utils for the key and value types of a BTree
    public type BTreeUtils<K, V> = {
        key: Blobify<K>;
        val: Blobify<V>;
        cmp: MemoryCmp<K>;
    };

    /// A blobify and cmp util for a single type
    public type SingleUtil<K> = {
        blobify: Blobify<K>;
        cmp: MemoryCmp<K>;
    };

    /// Create BTreeUtils for a given key and value type
    ///
    /// ```motoko
    ///     import MemoryBTree "mo:memory-collection/MemoryBTree";
    ///
    ///     let btree_utils = BTree.createUtils(
    ///         BTreeUtils.BigEndian.Nat, 
    ///         BTreeUtils.Text
    ///     );
    ///
    ///     let sstore = MemoryBTree.newStableStore(null);
    ///     let mbtree = MemoryBTree.MemoryBTree(sstore, btree_utils);
    ///     mbtree.insert(0, "hello");
    /// 
    /// ```
    public func createUtils<K, V>(key: SingleUtil<K>, val: SingleUtil<V>) : BTreeUtils<K, V> {
        return { 
            key = key.blobify;
            val = val.blobify;
            cmp = key.cmp 
        };
    };

    public module BigEndian = {
        public let Nat : SingleUtil<Nat> = {
            blobify = Blobify.BigEndian.Nat;
            cmp = MemoryCmp.BigEndian.Nat;
        };

        public let Nat8 : SingleUtil<Nat8> = {
            blobify = Blobify.BigEndian.Nat8;
            cmp = MemoryCmp.BigEndian.Nat8;
        };

        public let Nat16 : SingleUtil<Nat16> = {
            blobify = Blobify.BigEndian.Nat16;
            cmp = MemoryCmp.BigEndian.Nat16;
        };

        public let Nat32 : SingleUtil<Nat32> = {
            blobify = Blobify.BigEndian.Nat32;
            cmp = MemoryCmp.BigEndian.Nat32;
        };

        public let Nat64 : SingleUtil<Nat64> = {
            blobify = Blobify.BigEndian.Nat64;
            cmp = MemoryCmp.BigEndian.Nat64;
        };

    };

    public let Nat  : SingleUtil<Nat> = {
        blobify = Blobify.Nat;
        cmp = MemoryCmp.Nat;
    };

    public let Nat8  : SingleUtil<Nat8> = {
        blobify = Blobify.Nat8;
        cmp = MemoryCmp.Nat8;
    };

    public let Nat16  : SingleUtil<Nat16> = {
        blobify = Blobify.Nat16;
        cmp = MemoryCmp.Nat16;
    };

    public let Nat32  : SingleUtil<Nat32> = {
        blobify = Blobify.Nat32;
        cmp = MemoryCmp.Nat32;
    };

    public let Nat64  : SingleUtil<Nat64> = {
        blobify = Blobify.Nat64;
        cmp = MemoryCmp.Nat64;
    };

    public let Blob  : SingleUtil<Blob> = {
        blobify = Blobify.Blob;
        cmp = MemoryCmp.Blob;
    };

    public let Bool  : SingleUtil<Bool> = {
        blobify = Blobify.Bool;
        cmp = MemoryCmp.Bool;
    };

    public let Text  : SingleUtil<Text> = {
        blobify = Blobify.Text;
        cmp = MemoryCmp.Text;
    };

    public let Char  : SingleUtil<Char> = {
        blobify = Blobify.Char;
        cmp = MemoryCmp.Char;
    };

    public let Principal  : SingleUtil<Principal> = {
        blobify = Blobify.Principal;
        cmp = MemoryCmp.Principal;
    };


    /// BTree Utils for motoko types using candid serialization
    public module Candid {
        public let Nat : SingleUtil<Nat> = {
            blobify = Blobify.Candid.Nat;
            cmp = #GenCmp(Int8Cmp.Nat);
        };

        public let Nat8 : SingleUtil<Nat8> = {
            blobify = Blobify.Candid.Nat8;
            cmp = #BlobCmp(Int8Cmp.Blob);
        };

        // Using #GenCmp because its serialized as little endian 
        // and must be deserialized before it can be compared
        public let Nat16 : SingleUtil<Nat16> = {
            blobify = Blobify.Candid.Nat16;
            cmp = #GenCmp(Int8Cmp.Nat16);
        };

        public let Nat32 : SingleUtil<Nat32> = {
            blobify = Blobify.Candid.Nat32;
            cmp = #GenCmp(Int8Cmp.Nat32);
        };

        public let Nat64 : SingleUtil<Nat64> = {
            blobify = Blobify.Candid.Nat64;
            cmp = #GenCmp(Int8Cmp.Nat64);
        };

        public let Int : SingleUtil<Int> = {
            blobify = Blobify.Candid.Int;
            cmp = #GenCmp(Int8Cmp.Int);
        };

        public let Int8 : SingleUtil<Int8> = {
            blobify = Blobify.Candid.Int8;
            cmp = #GenCmp(Int8Cmp.Int8);
        };

        public let Int16 : SingleUtil<Int16> = {
            blobify = Blobify.Candid.Int16;
            cmp = #GenCmp(Int8Cmp.Int16);
        };

        public let Int32 : SingleUtil<Int32> = {
            blobify = Blobify.Candid.Int32;
            cmp = #GenCmp(Int8Cmp.Int32);
        };

        public let Int64 : SingleUtil<Int64> = {
            blobify = Blobify.Candid.Int64;
            cmp = #GenCmp(Int8Cmp.Int64);
        };

        public let Float : SingleUtil<Float> = {
            blobify = Blobify.Candid.Float;
            cmp = #GenCmp(Int8Cmp.Float);
        };

        public let Bool : SingleUtil<Bool> = {
            blobify = Blobify.Candid.Bool;
            cmp = #BlobCmp(Int8Cmp.Blob);
        };

        public let Text : SingleUtil<Text> = {
            blobify = Blobify.Candid.Text;
            cmp = #BlobCmp(Int8Cmp.Blob);
        };

        public let Principal : SingleUtil<Principal> = {
            blobify = Blobify.Candid.Principal;
            cmp = #BlobCmp(Int8Cmp.Blob);
        };

        public let Char : SingleUtil<Char> = {
            blobify = Blobify.Candid.Char;
            cmp = #BlobCmp(Int8Cmp.Blob);
        };
        
    };
}