import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Int32 "mo:base/Int32";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Int16 "mo:base/Int16";
import Int64 "mo:base/Int64";
import Int8 "mo:base/Int8";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import Itertools "mo:itertools/Iter";

import T "../Types";
import ByteUtils "../ByteUtils";

module {
    type CandidQuery = T.CandidQuery;

    public let OrchidTypeCode = {
        // primitive types
        // Null : Nat8 = 0x7f;
        Bool : Nat8 = 0x7e;
        Nat : Nat8 = 0x7d;
        Int : Nat8 = 0x7c;
        Nat8 : Nat8 = 0x7b;
        Nat16 : Nat8 = 0x7a;
        Nat32 : Nat8 = 0x79;
        Nat64 : Nat8 = 0x78;
        Int8 : Nat8 = 0x77;
        Int16 : Nat8 = 0x76;
        Int32 : Nat8 = 0x75;
        Int64 : Nat8 = 0x74;
        // Float32 : Nat8 = 0x73;
        Float : Nat8 = 0x72;
        Text : Nat8 = 0x71;
        // Reserved : Nat8 = 0x70;
        Empty : Nat8 = 0x6f;
        Principal : Nat8 = 0x68;

        // compound types

        Option : Nat8 = 0x6e;
        Array : Nat8 = 0x6d;
        Record : Nat8 = 0x6c;
        Variant : Nat8 = 0x6b;
        // Func : Nat8 = 0x6a;
        // Service : Nat8 = 0x69;

        // custom types
        Blob : Nat8 = 0x5f;
        Null : Nat8 = 0x60;
        Minimum : Nat8 = 0;
        Maximum : Nat8 = 255;

    };

    public let Orchid : TypeUtils.TypeUtils<[CandidQuery]> = {
        blobify = {
            to_blob = func(candid_values : [CandidQuery]) : Blob {
                let buffer = Buffer.Buffer<Nat8>(100);
                buffer.add(candid_values.size() |> Nat8.fromNat(_));

                var i = 0;
                while (i < candid_values.size()) {
                    encode(buffer, candid_values[i]);
                    i += 1;
                };

                Blob.fromArray(
                    Buffer.toArray(buffer)
                );

            };
            from_blob = func(blob : Blob) : [CandidQuery] {
                // we don't need to decode the index keys because we are only interested in the index values
                // but it might be a good idea for debugging
                return [];

                let bytes = Blob.toArray(blob);

                let size = bytes[0] |> Nat8.toNat(_);

                var i = 1;

                let buffer = Buffer.Buffer<CandidQuery>(8);
                //                case (#Nat(n)) {
                //     buffer.add(OrchidTypeCode.Nat);
                //     var num = n;
                //     var size : Nat8 = 0;

                //     while (num > 0) {
                //         num /= 255;
                //         size += 1;
                //     };

                //     // buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                //     // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                //     // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));
                //     // nat is limited to (2 ^ (255 * 8)) - 1
                //     buffer.add(size & 0xff);

                //     num := n;

                //     let bytes = Array.tabulate(
                //         Nat8.toNat(size),
                //         func(i : Nat) : Nat8 {
                //             let tmp = num % 255;
                //             num /= 255;
                //             Nat8.fromNat(tmp);
                //         },
                //     );

                //     for (i in Itertools.range(0, bytes.size())) {
                //         buffer.add(bytes[bytes.size() - 1 - i]);
                //     };

                // };

                func read() : Nat8 {
                    let byte = bytes[i];
                    i += 1;
                    byte;
                };

                label decoding while (i < bytes.size()) {

                    let type_code = read();

                    if (type_code == OrchidTypeCode.Nat) {
                        let size = read() |> Nat8.toNat(_);

                        var num = 0;

                        for (i in Itertools.range(0, size)) {
                            let byte = read();
                            num *= 255;
                            num += Nat8.toNat(byte);
                        };

                        buffer.add(#Nat(num));

                    } else if (type_code == OrchidTypeCode.Text) {
                        var text = "";

                        label extracting_text loop {

                            let byte = read();
                            if (byte == 0) break extracting_text;

                            let char = byte |> Nat8.toNat(_) |> Nat32.fromNat(_) |> Char.fromNat32(_);
                            text #= Char.toText(char);
                        };

                        buffer.add(#Text(text));

                    } else break decoding;

                };

                Buffer.toArray(buffer);
            };
        };
        cmp = TypeUtils.MemoryCmp.Default;

    };

    func encode(buffer : Buffer.Buffer<Nat8>, candid : CandidQuery) {

        switch (candid) {
            case (#Minimum) buffer.add(OrchidTypeCode.Minimum);
            case (#Array(_) or #Record(_) or #Map(_) or #Variant(_) or #Tuple(_)) Debug.trap("Orchid does not support compound types: " # debug_show (candid));
            case (#Option(option_type)) {
                buffer.add(OrchidTypeCode.Option);
                encode(buffer, option_type);
            };

            case (#Principal(p)) {
                buffer.add(OrchidTypeCode.Principal);

                let blob = Principal.toBlob(p);
                let bytes = Blob.toArray(blob);

                var i = 0;
                while (i < bytes.size()) {
                    buffer.add(bytes[i]);
                    i += 1;
                };

                buffer.add(0); // null terminator, helps with lexicographic comparison, if the principal ends before the other one, it will be considered smaller because the null terminator is smaller than any other byte

            };
            case (#Text(t)) {
                let utf8 = Text.encodeUtf8(t);
                let bytes = Blob.toArray(utf8);
                // let size = Nat8.fromNat(utf8.size()); -> the size will throw of the comparison, because text should be compared in lexicographic order and not by size
                buffer.add(OrchidTypeCode.Text);

                var i = 0;
                while (i < bytes.size()) {
                    buffer.add(bytes[i]);
                    i += 1;
                };

                buffer.add(0); // null terminator, helps with lexicographic comparison, if the text ends before the other one, it will be considered smaller because the null terminator is smaller than any other byte

            };
            case (#Blob(b)) {

                let bytes = Blob.toArray(b);

                buffer.add(OrchidTypeCode.Blob);

                var i = 0;
                while (i < bytes.size()) {
                    buffer.add(bytes[i]);
                    i += 1;
                };

                buffer.add(0); // null terminator, helps with lexicographic comparison, if the blob ends before the other one, it will be considered smaller because the null terminator is smaller than any other byte

            };
            case (#Float(f)) Debug.trap("Orchid does not support Float type");
            case (#Int(int)) {
                buffer.add(OrchidTypeCode.Int);

                let sign : Nat8 = if (int < 0) 0 else 1;

                var num = Int.abs(int);
                var size : Nat8 = 0;

                while (num > 0) {
                    num /= 255;
                    size += 1;
                };

                buffer.add(sign);

                // buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));

                buffer.add(size & 0xff);

                num := Int.abs(int);

                let bytes = Array.tabulate(
                    Nat8.toNat(size),
                    func(i : Nat) : Nat8 {
                        let tmp = num % 255;
                        num /= 255;
                        Nat8.fromNat(tmp);
                    },
                );

                for (i in Itertools.range(0, bytes.size())) {
                    buffer.add(bytes[bytes.size() - 1 - i]);
                };

            };
            case (#Int64(i)) {
                buffer.add(OrchidTypeCode.Int64);

                let n = Int64.toNat64(i);

                let msbyte = Nat8.fromNat(Nat64.toNat(n >> 56));
                let msbyte_with_flipped_msbit = msbyte ^ 0x80;

                buffer.add(msbyte_with_flipped_msbit);

                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat(n & 0xff)));

            };
            case (#Int32(i)) {
                buffer.add(OrchidTypeCode.Int32);

                let n = Int32.toNat32(i);

                let msbyte = Nat8.fromNat(Nat32.toNat(n >> 24));
                let msbyte_with_flipped_msbit = msbyte ^ 0x80;

                buffer.add(msbyte_with_flipped_msbit);
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));

            };
            case (#Int16(i)) {
                buffer.add(OrchidTypeCode.Int16);

                let n = Int16.toNat16(i);

                let most_significant_byte = Nat8.fromNat(Nat16.toNat(n >> 8));
                let msbyte_with_flipped_msbit = most_significant_byte ^ 0x80;

                buffer.add(msbyte_with_flipped_msbit);
                buffer.add(Nat8.fromNat(Nat16.toNat(n & 0xff)));
            };
            case (#Int8(i)) {
                buffer.add(OrchidTypeCode.Int8);

                let byte = Int8.toNat8(i);
                let int8_with_flipped_msb = byte ^ 0x80;

                buffer.add(int8_with_flipped_msb);
            };

            case (#Nat64(n)) {
                buffer.add(OrchidTypeCode.Nat64);

                buffer.add(Nat8.fromNat(Nat64.toNat(n >> 56)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat(n & 0xff)));

            };
            case (#Nat32(n)) {
                buffer.add(OrchidTypeCode.Nat32);

                buffer.add(Nat8.fromNat(Nat32.toNat(n >> 24)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));
            };
            case (#Nat16(n)) {
                buffer.add(OrchidTypeCode.Nat16);

                buffer.add(Nat8.fromNat(Nat16.toNat(n >> 8)));
                buffer.add(Nat8.fromNat(Nat16.toNat(n & 0xff)));
            };
            case (#Nat8(n)) {
                buffer.add(OrchidTypeCode.Nat8);
                buffer.add(n);
            };
            case (#Nat(n)) {
                buffer.add(OrchidTypeCode.Nat);
                var num = n;
                var size : Nat8 = 0;

                while (num > 0) {
                    num /= 255;
                    size += 1;
                };

                // buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));
                // nat is limited to (2 ^ (255 * 8)) - 1
                buffer.add(size & 0xff);

                num := n;

                let bytes = Array.tabulate(
                    Nat8.toNat(size),
                    func(i : Nat) : Nat8 {
                        let tmp = num % 255;
                        num /= 255;
                        Nat8.fromNat(tmp);
                    },
                );

                for (i in Itertools.range(0, bytes.size())) {
                    buffer.add(bytes[bytes.size() - 1 - i]);
                };

            };
            case (#Bool(b)) {
                buffer.add(OrchidTypeCode.Bool);
                buffer.add(if (b) 1 else 0);
            };
            case (#Empty) buffer.add(OrchidTypeCode.Empty);
            case (#Null) buffer.add(OrchidTypeCode.Null);
            case (#Maximum) buffer.add(OrchidTypeCode.Maximum);
        };
    };

};
