import Prim "mo:prim";

import Blob "mo:base/Blob";

import Int8Cmp "Int8Cmp";
module {
    public type MemoryCmp<A> = {
        #cmp : (A, A) -> Int8;
        #blob_cmp : (Blob, Blob) -> Int8;
    };

    public let Default = #blob_cmp(Int8Cmp.Blob);

    public module BigEndian {
        public let Nat = #blob_cmp(
            func (a: Blob, b: Blob) : Int8 {
                if (a.size() > b.size()) return 1;
                if (a.size() < b.size()) return -1;

                Prim.blobCompare(a, b);
            }
        );

        public let Nat8 = #blob_cmp(Prim.blobCompare);
        public let Nat16 = #blob_cmp(Prim.blobCompare);
        public let Nat32 = #blob_cmp(Prim.blobCompare);
        public let Nat64 = #blob_cmp(Prim.blobCompare);
    };

    public let Nat = #cmp(Int8Cmp.Nat);

    public let Nat8 = #cmp(Int8Cmp.Nat8);
    public let Nat16 = #cmp(Int8Cmp.Nat16);
    public let Nat32 = #cmp(Int8Cmp.Nat32);
    public let Nat64 = #cmp(Int8Cmp.Nat64);

    public let Blob = #blob_cmp(Prim.blobCompare);

    public let Bool = #blob_cmp(Prim.blobCompare);

    public let Char = #blob_cmp(Prim.blobCompare);

    public let Text = #blob_cmp(Prim.blobCompare);

    public let Principal = #blob_cmp(Prim.blobCompare);

}