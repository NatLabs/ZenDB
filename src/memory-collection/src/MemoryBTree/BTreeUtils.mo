import Blobify "../Blobify";
import MemoryCmp "../MemoryCmp";

module {

    type Blobify<A> = Blobify.Blobify<A>;
    type MemoryCmp<A> = MemoryCmp.MemoryCmp<A>;

    public type BTreeUtils<K, V> = {
        key: Blobify<K>;
        val: Blobify<V>;
        cmp: MemoryCmp<K>;
    };

    public type SingleUtil<K> = {
        blobify: Blobify<K>;
        cmp: MemoryCmp<K>;
    };

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
    
}