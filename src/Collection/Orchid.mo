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
import Option "mo:base/Option";
import Iter "mo:base/Iter";

import TypeUtils "mo:memory-collection/TypeUtils";

import Itertools "mo:itertools/Iter";

import T "../Types";
import ByteUtils "../ByteUtils";

module {
    type CandidQuery = T.CandidQuery;

    /// Orchid
    /// This is a module for encoding Composite Index Keys, represented as an array of candid values, into its binary representation.

    public let OrchidTypeCode = {
        Minimum : Nat8 = 1; // avoid clashing with 0x00, which is reserved for the null terminator
        Null : Nat8 = 2;
        Empty : Nat8 = 3;
        Bool : Nat8 = 4;
        Nat8 : Nat8 = 5;
        Nat16 : Nat8 = 6;
        Nat32 : Nat8 = 7;
        Nat64 : Nat8 = 8;
        Int8 : Nat8 = 9;
        Int16 : Nat8 = 10;
        Int32 : Nat8 = 11;
        Int64 : Nat8 = 12;
        NatAsNat64 : Nat8 = 13; // nat64
        IntAsInt64 : Nat8 = 14; // int64
        Float : Nat8 = 15;

        Principal : Nat8 = 100; // null terminated utf8 bytes
        Option : Nat8 = 150; // null terminated utf8 bytes

        Blob : Nat8 = 18; // null terminated utf8 bytes
        // Blob8 : Nat8 = 246; // size is 1 byte
        // Blob16 : Nat8 = 247; // size is 2 bytes
        // Blob32 : Nat8 = 248; // size is 4 bytes
        // Blob64 : Nat8 = 249; // size is 8 bytes

        Text : Nat8 = 250; // null terminated utf8 bytes
        // Text8 : Nat8 = 251; // size is 1 byte
        // Text16 : Nat8 = 252; // size is 2 bytes
        // Text32 : Nat8 = 253; // size is 4 bytes
        // Text64 : Nat8 = 254; // size is 8 bytes

        Maximum : Nat8 = 255;

    };

    func utf8_plus_1(text : Text) : [Nat8] {
        let utf8 = Text.encodeUtf8(text);
        let bytes = Blob.toArray(utf8);
        Array.map(bytes, func(b : Nat8) : Nat8 { b + 1 });
    };

    func arbitrary_bytes_to_utf8_plus_1(bytes : [Nat8]) : [Nat8] {
        var t = "";

        for (i in Itertools.range(0, bytes.size())) {
            let byte = bytes[i];
            let char = byte |> Nat8.toNat(_) |> Nat32.fromNat(_) |> Char.fromNat32(_);
            t #= Char.toText(char);
        };

        utf8_plus_1(t);
    };

    func utf8_plus_1_to_arbitrary_bytes(bytes : [Nat8]) : [Nat8] {
        // First, subtract 1 from each byte to reverse the +1 operation
        let original_utf8 = Array.map(bytes, func(b : Nat8) : Nat8 { b - 1 });

        // Then convert back to text
        let text = Text.decodeUtf8(Blob.fromArray(original_utf8));

        // Convert text characters back to original bytes
        switch (text) {
            case (null) { [] };
            case (?t) {
                Iter.toArray(
                    Iter.map(
                        Text.toIter(t),
                        func(c : Char) : Nat8 {
                            let byte = Char.toNat32(c);
                            Nat32.toNat(byte) |> Nat8.fromNat(_);
                        },
                    )
                );

            };
        };
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
                let bytes = arbitrary_bytes_to_utf8_plus_1(Blob.toArray(blob));

                ByteUtils.Buffer.addBytes(buffer, bytes.vals());

                buffer.add(0); // null terminator, helps with lexicographic comparison, if the principal ends before the other one, it will be considered smaller because the null terminator is smaller than any other byte

            };
            case (#Text(t)) {
                let bytes = Text.encodeUtf8(t);
                // let size = Nat8.fromNat(utf8.size()); -> the size will throw of the comparison, because text should be compared in lexicographic order and not by size
                buffer.add(OrchidTypeCode.Text);

                var i = 0;
                while (i < bytes.size()) {
                    buffer.add(bytes.get(i));
                    i += 1;
                };

                buffer.add(0); // null terminator, helps with lexicographic comparison, if the text ends before the other one, it will be considered smaller because the null terminator is smaller than any other byte

            };
            case (#Blob(b)) {

                let bytes = arbitrary_bytes_to_utf8_plus_1(Blob.toArray(b));

                buffer.add(OrchidTypeCode.Blob);

                var i = 0;
                while (i < bytes.size()) {
                    buffer.add(bytes[i]);
                    i += 1;
                };

                buffer.add(0); // null terminator, helps with lexicographic comparison, if the blob ends before the other one, it will be considered smaller because the null terminator is smaller than any other byte

            };
            case (#Float(f)) {
                buffer.add(OrchidTypeCode.Float);
                let msbyte_index = buffer.size();
                ByteUtils.Buffer.BigEndian.addFloat64(buffer, f);

                if (f < 0.0) {
                    // For negative numbers, flip all bits to reverse their order
                    for (i in Iter.range(msbyte_index, msbyte_index + 7)) {
                        buffer.put(i, ^(buffer.get(i)));
                    };
                } else {
                    // For positive numbers, just flip the sign bit
                    buffer.put(msbyte_index, buffer.get(msbyte_index) ^ 0x80);
                };
            };

            case (#Int8(i)) {
                buffer.add(OrchidTypeCode.Int8);

                let byte = Int8.toNat8(i);
                let int8_with_flipped_msb = byte ^ 0x80;

                buffer.add(int8_with_flipped_msb);
            };
            case (#Int16(i)) {
                buffer.add(OrchidTypeCode.Int16);

                let n = Int16.toNat16(i);

                let most_significant_byte = Nat8.fromNat(Nat16.toNat(n >> 8));
                let msbyte_with_flipped_msbit = most_significant_byte ^ 0x80;

                buffer.add(msbyte_with_flipped_msbit);
                buffer.add(Nat8.fromNat(Nat16.toNat(n & 0xff)));
            };

            case (#Int32(i)) {
                buffer.add(OrchidTypeCode.Int32);

                let n = Int32.toNat32(i);

                // Need custom logic for the most significant byte to flip the sign bit
                let msbyte = Nat8.fromNat(Nat32.toNat(n >> 24));
                let msbyte_with_flipped_msbit = msbyte ^ 0x80;

                buffer.add(msbyte_with_flipped_msbit);
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));

            };
            case (#Int64(i)) {
                buffer.add(OrchidTypeCode.Int64);

                let n = Int64.toNat64(i);

                // Need custom logic for the most significant byte to flip the sign bit
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
            case (#Int(int)) {
                buffer.add(OrchidTypeCode.IntAsInt64);

                let int64 = Int64.fromInt(int);
                let n = Int64.toNat64(int64);

                // Need custom logic for the most significant byte to flip the sign bit
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

            case (#Nat64(n)) {
                buffer.add(OrchidTypeCode.Nat64);
                ByteUtils.Buffer.BE.addNat64(buffer, n);
            };

            case (#Nat32(n)) {
                buffer.add(OrchidTypeCode.Nat32);
                ByteUtils.Buffer.BE.addNat32(buffer, n);
            };

            case (#Nat16(n)) {
                buffer.add(OrchidTypeCode.Nat16);
                ByteUtils.Buffer.BE.addNat16(buffer, n);
            };

            case (#Nat8(n)) {
                buffer.add(OrchidTypeCode.Nat8);
                buffer.add(n);
            };

            case (#Nat(n)) {
                let n64 = Nat64.fromNat(n);
                buffer.add(OrchidTypeCode.NatAsNat64);
                ByteUtils.Buffer.BE.addNat64(buffer, n64);
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

    let to_blob = func(candid_values : [CandidQuery]) : Blob {
        let buffer = Buffer.Buffer<Nat8>(100);
        //! Do not store the size as it affects the ordering
        // buffer.add(candid_values.size() |> Nat8.fromNat(_));

        var i = 0;
        while (i < candid_values.size()) {
            encode(buffer, candid_values[i]);
            i += 1;
        };

        Blob.fromArray(
            Buffer.toArray(buffer)
        );

    };

    let from_blob = func(blob : Blob) : [CandidQuery] {

        let bytes = Itertools.peekable(blob.vals());

        // let size = bytes[0] |> Nat8.toNat(_);

        let buffer = Buffer.Buffer<CandidQuery>(8);

        func read() : Nat8 {
            let ?byte = bytes.next() else Debug.trap("Orchid: not enough bytes to read");
            byte;
        };

        label decoding while (Option.isSome(bytes.peek())) {

            let type_code = read();

            if (type_code == OrchidTypeCode.NatAsNat64) {

                let n64 = ByteUtils.BE.toNat64(bytes);

                buffer.add(#Nat(Nat64.toNat(n64)));

            } else if (type_code == OrchidTypeCode.Text) {
                var text = "";

                label extracting_text loop {

                    let byte = read();
                    if (byte == 0) break extracting_text;

                    let char = byte |> Nat8.toNat(_) |> Nat32.fromNat(_) |> Char.fromNat32(_);
                    text #= Char.toText(char);
                };

                buffer.add(#Text(text));

            } else if (type_code == OrchidTypeCode.Nat8) {
                let n = ByteUtils.BE.toNat8(bytes);
                buffer.add(#Nat8(n));
            } else if (type_code == OrchidTypeCode.Nat16) {
                let n = ByteUtils.BE.toNat16(bytes);
                buffer.add(#Nat16(n));
            } else if (type_code == OrchidTypeCode.Nat32) {
                let n = ByteUtils.BE.toNat32(bytes);
                buffer.add(#Nat32(n));
            } else if (type_code == OrchidTypeCode.Nat64) {
                let n = ByteUtils.BE.toNat64(bytes);
                buffer.add(#Nat64(n));
            } else if (type_code == OrchidTypeCode.IntAsInt64) {
                let msbyte = read() ^ 0x80;
                let int64 = ByteUtils.BE.toInt64(Itertools.prepend(msbyte, bytes));
                buffer.add(#Int(Int64.toInt(int64)));
            } else if (type_code == OrchidTypeCode.Int8) {
                let byte = read();
                let int8_with_flipped_msb = byte ^ 0x80;
                let i = Int8.fromNat8(int8_with_flipped_msb);
                buffer.add(#Int8(i));
            } else if (type_code == OrchidTypeCode.Int16) {
                let msbyte = read() ^ 0x80;
                let int16 = ByteUtils.BE.toInt16(Itertools.prepend(msbyte, bytes));
                buffer.add(#Int16(int16));
            } else if (type_code == OrchidTypeCode.Int32) {
                let msbyte = read() ^ 0x80;
                let int32 = ByteUtils.BE.toInt32(Itertools.prepend(msbyte, bytes));
                buffer.add(#Int32(int32));

            } else if (type_code == OrchidTypeCode.Int64) {
                let msbyte = read() ^ 0x80;
                let int64 = ByteUtils.BE.toInt64(Itertools.prepend(msbyte, bytes));
                buffer.add(#Int64(int64));

            } else if (type_code == OrchidTypeCode.Float) {
                let msbyte = read();
                let is_negative = msbyte & 0x80 == 0x00; //0x00 is negative because the sign bit is flipped

                let float = if (is_negative) {
                    // For negative numbers, flip all bits to reverse their order
                    let float_bytes = Iter.map(
                        Itertools.prepend(
                            msbyte,
                            Itertools.take(bytes, 7),
                        ),
                        Nat8.bitnot,
                    );

                    ByteUtils.BE.toFloat64(float_bytes);
                } else {
                    // For positive numbers, just flip the sign bit
                    let float_bytes = Itertools.prepend(msbyte ^ 0x80, bytes);
                    ByteUtils.BE.toFloat64(float_bytes);
                };

                buffer.add(#Float(float));
            } else if (type_code == OrchidTypeCode.Bool) {
                let byte = read();
                buffer.add(#Bool(if (byte == 1) true else false));
            } else if (type_code == OrchidTypeCode.Empty) {
                buffer.add(#Empty);
            } else if (type_code == OrchidTypeCode.Null) {
                buffer.add(#Null);
            } else if (type_code == OrchidTypeCode.Principal) {
                var principal_bytes = Buffer.Buffer<Nat8>(100);

                label extracting_principal loop {

                    let byte = read();
                    if (byte == 0) break extracting_principal;

                    principal_bytes.add(byte);
                };

                let blob = Blob.fromArray(utf8_plus_1_to_arbitrary_bytes(Buffer.toArray(principal_bytes)));
                let p = Principal.fromBlob(blob);
                buffer.add(#Principal(p));
            } else if (type_code == OrchidTypeCode.Blob) {
                var blob_bytes = Buffer.Buffer<Nat8>(100);

                label extracting_blob loop {

                    let byte = read();
                    if (byte == 0) break extracting_blob;

                    blob_bytes.add(byte);
                };

                let blob = Blob.fromArray(utf8_plus_1_to_arbitrary_bytes(Buffer.toArray(blob_bytes)));
                buffer.add(#Blob(blob));
            }

            else break decoding;

        };

        Buffer.toArray(buffer);
    };

    let btree_cmp = func(a : Blob, b : Blob) : Int8 {
        if (a < b) -1 else if (a > b) 1 else 0;
    };

    public let Orchid : TypeUtils.TypeUtils<[CandidQuery]> and {
        btree_cmp : (Blob, Blob) -> Int8;
    } = {
        blobify = { to_blob; from_blob };
        btree_cmp;
        cmp = #BlobCmp(btree_cmp);
    };

};
