/// Blobify is a module that provides a generic interface for converting
/// values to and from blobs. It is intended to be used for serializing
/// and deserializing values that will be stored in persistent stable memory.
///
/// The Blobify module provides a default implementation for the following
/// types:
/// - Nat
/// - Nat8
/// - Nat16
/// - Nat32
/// - Nat64
/// - Blob
/// - Bool
/// - Text
/// - Principal

import TextModule "mo:base/Text";
import CharModule "mo:base/Char";
import BlobModule "mo:base/Blob";
import ArrayModule "mo:base/Array";
import NatModule "mo:base/Nat";
import Nat8Module "mo:base/Nat8";
import Nat16Module "mo:base/Nat16";
import Nat32Module "mo:base/Nat32";
import Nat64Module "mo:base/Nat64";
import PrincipalModule "mo:base/Principal";

import Debug "mo:base/Debug";
import Char "mo:fuzz/Char";

module Blobify {

    let Base = {
        Array = ArrayModule;
        Blob = BlobModule;
        Nat = NatModule;
        Nat8 = Nat8Module;
        Nat16 = Nat16Module;
        Nat32 = Nat32Module;
        Nat64 = Nat64Module;
        Text = TextModule;
        Principal = PrincipalModule;
    };

    public type Blobify<A> = {
        to_blob : (A) -> Blob;
        from_blob : (Blob) -> A;
    };

    // Default blobify helpers return blobs in big-endian format.
    public module BigEndian {
        public let Nat : Blobify<Nat> = {
            to_blob = func(n : Nat) : Blob {
                if (n == 0) { return "\00" };
                var num = n;
                var nbytes = 0;

                while (num > 0) {
                    num /= 255;
                    nbytes += 1;
                };

                num := n;

                let arr = ArrayModule.reverse(ArrayModule.tabulate(
                    nbytes,
                    func(_ : Nat) : Nat8 {
                        let tmp = num % 255;
                        num /= 255;
                        Nat8Module.fromNat(tmp);
                    },
                ));

                Base.Blob.fromArray(arr);
            };
            from_blob = func(blob : Blob) : Nat {
                var n = 0;
                let bytes = Base.Blob.toArray(blob);

                var j = 0;

                while (j < bytes.size()) {
                    let byte = bytes.get(j);
                    n *= 255;
                    n += Base.Nat8.toNat(byte);

                    j += 1;
                };

                n;
            };
        };

        public let Nat8 : Blobify<Nat8> = {
            to_blob = func(n : Nat8) : Blob { Base.Blob.fromArray([n]) };
            from_blob = func(blob : Blob) : Nat8 { Base.Blob.toArray(blob)[0] };
        };

        public let Nat16 : Blobify<Nat16> = {
            to_blob = func(n : Nat16) : Blob {
                Base.Blob.fromArray([
                    Base.Nat8.fromNat16(n >> 8),
                    Base.Nat8.fromNat16(n & 0xff),
                ]);
            };
            from_blob = func(blob : Blob) : Nat16 {
                let bytes = Base.Blob.toArray(blob);

                let _n16 = Base.Nat16.fromNat8(bytes[0] << 8) | Base.Nat16.fromNat8(bytes[1]);
            };
        };

        public let Nat32 : Blobify<Nat32> = {
            to_blob = func(n : Nat32) : Blob {
                Base.Blob.fromArray([
                    Base.Nat8.fromNat(Base.Nat32.toNat(n >> 24)),
                    Base.Nat8.fromNat(Base.Nat32.toNat((n >> 16) & 0xff)),
                    Base.Nat8.fromNat(Base.Nat32.toNat((n >> 8) & 0xff)),
                    Base.Nat8.fromNat(Base.Nat32.toNat(n & 0xff)),
                ]);
            };
            from_blob = func(blob : Blob) : Nat32 {
                let bytes = Base.Blob.toArray(blob);

                let _n32 = Base.Nat32.fromNat(Base.Nat8.toNat(bytes[0] << 24)) | Base.Nat32.fromNat(Base.Nat8.toNat(bytes[1] << 16)) | Base.Nat32.fromNat(Base.Nat8.toNat(bytes[2] << 8)) | Base.Nat32.fromNat(Base.Nat8.toNat(bytes[3]));
            };
        };
        
        public let Nat64 : Blobify<Nat64> = {
            to_blob = func(n : Nat64) : Blob {
                Base.Blob.fromArray([
                    Base.Nat8.fromNat(Base.Nat64.toNat(n >> 56)),
                    Base.Nat8.fromNat(Base.Nat64.toNat((n >> 48) & 0xff)),
                    Base.Nat8.fromNat(Base.Nat64.toNat((n >> 40) & 0xff)),
                    Base.Nat8.fromNat(Base.Nat64.toNat((n >> 32) & 0xff)),
                    Base.Nat8.fromNat(Base.Nat64.toNat((n >> 24) & 0xff)),
                    Base.Nat8.fromNat(Base.Nat64.toNat((n >> 16) & 0xff)),
                    Base.Nat8.fromNat(Base.Nat64.toNat((n >> 8) & 0xff)),
                    Base.Nat8.fromNat(Base.Nat64.toNat(n & 0xff)),
                ]);
            };
            from_blob = func(blob : Blob) : Nat64 {
                let bytes = Base.Blob.toArray(blob);

                let _n64 = Base.Nat64.fromNat(Base.Nat8.toNat(bytes[0] << 56)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[1] << 48)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[2] << 40)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[3] << 32)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[4] << 24)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[5] << 16)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[6] << 8)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[7]));
            };
        };  
    };

    public let Nat8 : Blobify<Nat8> = {
        to_blob = func(n : Nat8) : Blob { Base.Blob.fromArray([n]) };
        from_blob = func(blob : Blob) : Nat8 { Base.Blob.toArray(blob)[0] };
    };

    public let Nat16 : Blobify<Nat16> = {
        to_blob = func(n : Nat16) : Blob {
            Base.Blob.fromArray([
                Base.Nat8.fromNat16(n & 0xff),
                Base.Nat8.fromNat16(n >> 8),
            ]);
        };
        from_blob = func(blob : Blob) : Nat16 {
            let bytes = Base.Blob.toArray(blob);

            let _n16 = Base.Nat16.fromNat8(bytes[1] << 8) | Base.Nat16.fromNat8(bytes[0]);
        };
    };

    public let Nat32 : Blobify<Nat32> = {
        to_blob = func(n : Nat32) : Blob {
            Base.Blob.fromArray([
                Base.Nat8.fromNat(Base.Nat32.toNat(n & 0xff)),
                Base.Nat8.fromNat(Base.Nat32.toNat((n >> 8) & 0xff)),
                Base.Nat8.fromNat(Base.Nat32.toNat((n >> 16) & 0xff)),
                Base.Nat8.fromNat(Base.Nat32.toNat(n >> 24)),
            ]);
        };
        from_blob = func(blob : Blob) : Nat32 {
            let bytes = Base.Blob.toArray(blob);

            let _n32 = Base.Nat32.fromNat(Base.Nat8.toNat(bytes[3] << 24)) | Base.Nat32.fromNat(Base.Nat8.toNat(bytes[2] << 16)) | Base.Nat32.fromNat(Base.Nat8.toNat(bytes[1] << 8)) | Base.Nat32.fromNat(Base.Nat8.toNat(bytes[0]));
        };
    };

    public let Nat64 : Blobify<Nat64> = {
        to_blob = func(n : Nat64) : Blob {
            Base.Blob.fromArray([
                Base.Nat8.fromNat(Base.Nat64.toNat(n & 0xff)),
                Base.Nat8.fromNat(Base.Nat64.toNat((n >> 8) & 0xff)),
                Base.Nat8.fromNat(Base.Nat64.toNat((n >> 16) & 0xff)),
                Base.Nat8.fromNat(Base.Nat64.toNat((n >> 24) & 0xff)),
                Base.Nat8.fromNat(Base.Nat64.toNat((n >> 32) & 0xff)),
                Base.Nat8.fromNat(Base.Nat64.toNat((n >> 40) & 0xff)),
                Base.Nat8.fromNat(Base.Nat64.toNat((n >> 48) & 0xff)),
                Base.Nat8.fromNat(Base.Nat64.toNat(n >> 56)),
            ]);
        };
        from_blob = func(blob : Blob) : Nat64 {
            let bytes = Base.Blob.toArray(blob);

            let _n64 = Base.Nat64.fromNat(Base.Nat8.toNat(bytes[7] << 56)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[6] << 48)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[5] << 40)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[4] << 32)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[3] << 24)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[2] << 16)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[1] << 8)) | Base.Nat64.fromNat(Base.Nat8.toNat(bytes[0]));
        };
    };

    // Default blobify helpers return blobs in little-endian format.
    public let Nat : Blobify<Nat> = {
        to_blob = func(n : Nat) : Blob {
            if (n == 0) { return "\00" };
            var num = n;
            var nbytes = 0;

            while (num > 0) {
                num /= 255;
                nbytes += 1;
            };

            num := n;

            let arr = ArrayModule.tabulate(
                nbytes,
                func(_ : Nat) : Nat8 {
                    let tmp = num % 255;
                    num /= 255;
                    Nat8Module.fromNat(tmp);
                },
            );

            Base.Blob.fromArray(arr);
        };
        from_blob = func(blob : Blob) : Nat {
            var n = 0;
            let bytes = Base.Blob.toArray(blob);

            var j = bytes.size();

            while (j > 0) {
                let byte = bytes.get(j - 1);
                n *= 255;
                n += Base.Nat8.toNat(byte);

                j -= 1;
            };

            n;
        };
    };

    public let Blob : Blobify<Blob> = {
        to_blob = func(b : Blob) : Blob = b;
        from_blob = func(blob : Blob) : Blob = blob;
    };

    public let Bool : Blobify<Bool> = {
        to_blob = func(b : Bool) : Blob = Base.Blob.fromArray([if (b) 1 else 0]);
        from_blob = func(blob : Blob) : Bool {
            blob == "\01";
        };
    };

    public let Char : Blobify<Char> = {
        to_blob = func(c : Char) : Blob = Base.Text.encodeUtf8(CharModule.toText(c));
        from_blob = func(blob : Blob) : Char {
            let ?t = TextModule.decodeUtf8(blob) else Debug.trap("from_blob() on Blobify.Char failed to decodeUtf8");
            let ?c = t.chars().next() else Debug.trap("from_blob() on Blobify.Char failed to get first char");
            c
        };
    };

    public let Text : Blobify<Text> = {
        to_blob = func(t : Text) : Blob = TextModule.encodeUtf8(t);
        from_blob = func(blob : Blob) : Text {
            let ?text = TextModule.decodeUtf8(blob) else Debug.trap("from_blob() on Blobify.Text failed to decodeUtf8");
            text;
        };
    };

    public let Principal : Blobify<Principal> = {
        to_blob = func(p : Principal) : Blob { Base.Principal.toBlob(p) };
        from_blob = func(blob : Blob) : Principal {
            Base.Principal.fromBlob(blob);
        };
    };
};
