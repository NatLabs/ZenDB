import Blobify "../Blobify";
import MemoryCmp "../MemoryCmp";

module {

    type Blobify<A> = Blobify.Blobify<A>;
    type MemoryCmp<A> = MemoryCmp.MemoryCmp<A>;

    public type IndexUtils<K> = {
        blobify: Blobify<K>;
        cmp: MemoryCmp<K>;
    };

    public module BigEndian = {
        public let Nat : IndexUtils<Nat> = {
            blobify = Blobify.BigEndian.Nat;
            cmp = MemoryCmp.BigEndian.Nat;
        };

        public let Nat8 : IndexUtils<Nat8> = {
            blobify = Blobify.BigEndian.Nat8;
            cmp = MemoryCmp.BigEndian.Nat8;
        };

        public let Nat16 : IndexUtils<Nat16> = {
            blobify = Blobify.BigEndian.Nat16;
            cmp = MemoryCmp.BigEndian.Nat16;
        };

        public let Nat32 : IndexUtils<Nat32> = {
            blobify = Blobify.BigEndian.Nat32;
            cmp = MemoryCmp.BigEndian.Nat32;
        };

        public let Nat64 : IndexUtils<Nat64> = {
            blobify = Blobify.BigEndian.Nat64;
            cmp = MemoryCmp.BigEndian.Nat64;
        };

    };

    public let Nat  : IndexUtils<Nat> = {
        blobify = Blobify.Nat;
        cmp = MemoryCmp.Nat;
    };

    public let Nat8  : IndexUtils<Nat8> = {
        blobify = Blobify.Nat8;
        cmp = MemoryCmp.Nat8;
    };

    public let Nat16  : IndexUtils<Nat16> = {
        blobify = Blobify.Nat16;
        cmp = MemoryCmp.Nat16;
    };

    public let Nat32  : IndexUtils<Nat32> = {
        blobify = Blobify.Nat32;
        cmp = MemoryCmp.Nat32;
    };

    public let Nat64  : IndexUtils<Nat64> = {
        blobify = Blobify.Nat64;
        cmp = MemoryCmp.Nat64;
    };

    public let Blob  : IndexUtils<Blob> = {
        blobify = Blobify.Blob;
        cmp = MemoryCmp.Blob;
    };

    public let Bool  : IndexUtils<Bool> = {
        blobify = Blobify.Bool;
        cmp = MemoryCmp.Bool;
    };

    public let Text  : IndexUtils<Text> = {
        blobify = Blobify.Text;
        cmp = MemoryCmp.Text;
    };

    public let Char  : IndexUtils<Char> = {
        blobify = Blobify.Char;
        cmp = MemoryCmp.Char;
    };

    public let Principal  : IndexUtils<Principal> = {
        blobify = Blobify.Principal;
        cmp = MemoryCmp.Principal;
    };
    
}