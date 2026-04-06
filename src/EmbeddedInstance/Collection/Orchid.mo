import Prim "mo:prim";

import Runtime "mo:core@2.4/Runtime";
import Debug "mo:core@2.4/Debug";
import Buffer "mo:base@0.16/Buffer";
import Principal "mo:core@2.4/Principal";
import Array "mo:core@2.4/Array";
import Text "mo:core@2.4/Text";
import Char "mo:core@2.4/Char";
import Nat32 "mo:core@2.4/Nat32";
import Nat "mo:core@2.4/Nat";
import Int "mo:core@2.4/Int";
import Int32 "mo:core@2.4/Int32";
import Blob "mo:core@2.4/Blob";
import Nat64 "mo:core@2.4/Nat64";
import Int16 "mo:core@2.4/Int16";
import Int64 "mo:core@2.4/Int64";
import Int8 "mo:core@2.4/Int8";
import Nat16 "mo:core@2.4/Nat16";
import Nat8 "mo:core@2.4/Nat8";
import Option "mo:core@2.4/Option";
import Iter "mo:core@2.4/Iter";

import TypeUtils "mo:memory-collection@0.4/TypeUtils";
import ByteUtils "mo:byte-utils@0.2";
import Itertools "mo:itertools@0.2/Iter";
import Cmp "mo:augmented-btrees@0.9/Cmp";
import Map "mo:map@9.0/Map";

import T "../Types";

module {
    type CandidQuery = T.CandidQuery;

    /// Orchid
    /// Encodes composite index keys (arrays of candid values) into a binary representation
    /// that is lexicographically ordered. Raw byte comparison (e.g. Prim.blobCompare) on
    /// two encoded blobs produces the same ordering as semantic comparison of the original
    /// values. This makes the encoding compatible with tail-compressed B+Tree separators.
    ///
    /// Ordering properties by type:
    ///   Fixed-width types (Bool, Nat/Nat8/16/32/64, Int/Int8/16/32/64, Float, Null, Empty):
    ///     Encoded as [type_code][fixed bytes]. ByteUtils.Sorted ensures signed integers
    ///     and floats are XOR-adjusted so raw byte order matches semantic order.
    ///     Note: Nat and Int are encoded as 64-bit values (type codes 13, 14).
    ///   Variable-length types (Text, Blob, Principal):
    ///     Encoded as [type_code][escaped bytes][0x00 0x00].
    ///     Content bytes are escaped: 0x00 → 0x00 0xFF. Terminator is 0x00 0x00.
    ///     This is lex-correct because the terminator is smaller than any continuation
    ///     byte, so a shorter value sorts before any longer value that shares its prefix.
    ///   Option: [TypeCode.Option][inner encoded value]. Null (TypeCode 2) < Option (TypeCode 21).
    ///   Minimum/Maximum sentinels: single byte 0x01 / 0xFF — below/above all real type codes.

    public let TypeCode = {
        Minimum : Nat8 = 1; // below all real type codes
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
        NatAsNat64 : Nat8 = 13; // #Nat encoded as 64-bit
        IntAsInt64 : Nat8 = 14; // #Int encoded as 64-bit
        Nat : Nat8 = 15; // reserved for future variable-length encoding
        Int : Nat8 = 16; // reserved for future variable-length encoding
        Float : Nat8 = 17;
        Principal : Nat8 = 18;
        Text : Nat8 = 19;
        Blob : Nat8 = 20;
        Option : Nat8 = 21;
        Maximum : Nat8 = 255; // above all real type codes
    };

    public func type_code_to_text(type_code : Nat8) : Text {
        let type_code_name_map = [
            (TypeCode.Minimum, "Minimum"),
            (TypeCode.Null, "Null"),
            (TypeCode.Empty, "Empty"),
            (TypeCode.Bool, "Bool"),
            (TypeCode.Nat8, "Nat8"),
            (TypeCode.Nat16, "Nat16"),
            (TypeCode.Nat32, "Nat32"),
            (TypeCode.Nat64, "Nat64"),
            (TypeCode.Int8, "Int8"),
            (TypeCode.Int16, "Int16"),
            (TypeCode.Int32, "Int32"),
            (TypeCode.Int64, "Int64"),
            (TypeCode.NatAsNat64, "NatAsNat64"),
            (TypeCode.IntAsInt64, "IntAsInt64"),
            (TypeCode.Nat, "Nat"),
            (TypeCode.Int, "Int"),
            (TypeCode.Float, "Float"),
            (TypeCode.Principal, "Principal"),
            (TypeCode.Option, "Option"),
            (TypeCode.Text, "Text"),
            (TypeCode.Blob, "Blob"),
            (TypeCode.Maximum, "Maximum"),
        ];

        Option.get(
            Option.map(
                Array.find(
                    type_code_name_map,
                    func(pair : (Nat8, Text)) : Bool { pair.0 == type_code },
                ),
                func(pair : (Nat8, Text)) : Text { pair.1 },
            ),
            debug_show (type_code),
        );
    };

    /// Writes all bytes from `src` into `buffer`, escaping every 0x00 byte as 0x00 0xFF,
    /// then appends the 0x00 0x00 terminator.
    ///
    /// Lexicographic correctness:
    ///   - 0x00 0x00  <  0x00 0xFF  <  0x01..0xFF, so the terminator of a shorter value
    ///     sorts before any byte of a longer value that shares the same prefix.
    ///   - Embedded 0x00 bytes (e.g. U+0000 in UTF-8) are escaped to 0x00 0xFF, which
    ///     sorts above the terminator but below any non-zero continuation byte — correct.
    func encode_escaped(buffer : Buffer.Buffer<Nat8>, src : Iter.Iter<Nat8>) {
        for (byte in src) {
            if (byte == 0x00) {
                buffer.add(0x00); buffer.add(0xFF);
            } else {
                buffer.add(byte);
            };
        };

        buffer.add(0x00); buffer.add(0x00); // terminator
    };

    /// Reads escape-encoded bytes from `src` up to and consuming the 0x00 0x00 terminator.
    /// 0x00 0xFF sequences are unescaped back to 0x00.
    /// Returns the decoded byte array (without the terminator).
    func decode_escaped(src : T.Iter<Nat8>) : [Nat8] {
        let buf = Buffer.Buffer<Nat8>(16);
        label l loop {
            let b1 = switch (src.next()) {
                case (null) Runtime.trap("Orchid: unexpected end of stream - missing terminator");
                case (?b) b;
            };
            if (b1 == 0x00) {
                let b2 = switch (src.next()) {
                    case (null) Runtime.trap("Orchid: unexpected end of stream inside escaped field");
                    case (?b) b;
                };
                if (b2 == 0x00) {
                    break l; // terminator — stop
                } else if (b2 == 0xFF) {
                    buf.add(0x00); // unescape embedded null
                } else {
                    Runtime.trap("Orchid: invalid escape sequence 0x00 " # debug_show b2);
                };
            } else {
                buf.add(b1);
            };
        };
        Buffer.toArray(buf)
    };

    func encode(buffer : Buffer.Buffer<Nat8>, candid : CandidQuery) {

        switch (candid) {
            case (#Minimum) buffer.add(TypeCode.Minimum);
            case (#Array(_) or #Record(_) or #Map(_) or #Variant(_) or #Tuple(_)) {
                Runtime.trap("Orchid does not support compound types: " # debug_show (candid));
            };
            case (#Option(option_type)) {
                buffer.add(TypeCode.Option);
                encode(buffer, option_type);
            };

            case (#Principal(p)) {
                buffer.add(TypeCode.Principal);
                // No size prefix — use escape+terminator for lex correctness
                encode_escaped(buffer, Principal.toBlob(p).vals());
            };
            case (#Blob(b)) {
                buffer.add(TypeCode.Blob);
                // No size prefix — use escape+terminator for lex correctness
                encode_escaped(buffer, b.vals());
            };
            case (#Text(t)) {
                buffer.add(TypeCode.Text);
                // No size prefix — use escape+terminator for lex correctness
                encode_escaped(buffer, Text.encodeUtf8(t).vals());
            };
            case (#Float(f)) {
                buffer.add(TypeCode.Float);
                ByteUtils.Buffer.Sorted.addFloat(buffer, f);
            };

            case (#Int8(i)) {
                buffer.add(TypeCode.Int8);
                ByteUtils.Buffer.Sorted.addInt8(buffer, i);
            };
            case (#Int16(i)) {
                buffer.add(TypeCode.Int16);
                ByteUtils.Buffer.Sorted.addInt16(buffer, i);
            };

            case (#Int32(i)) {
                buffer.add(TypeCode.Int32);
                ByteUtils.Buffer.Sorted.addInt32(buffer, i);
            };
            case (#Int64(i)) {
                buffer.add(TypeCode.Int64);
                ByteUtils.Buffer.Sorted.addInt64(buffer, i);
            };
            case (#Int(int)) {
                buffer.add(TypeCode.IntAsInt64);
                let int64 = Int64.fromInt(int);
                ByteUtils.Buffer.Sorted.addInt64(buffer, int64);
            };

            case (#Nat64(n)) {
                buffer.add(TypeCode.Nat64);
                ByteUtils.Buffer.Sorted.addNat64(buffer, n);
            };

            case (#Nat32(n)) {
                buffer.add(TypeCode.Nat32);
                ByteUtils.Buffer.Sorted.addNat32(buffer, n);
            };

            case (#Nat16(n)) {
                buffer.add(TypeCode.Nat16);
                ByteUtils.Buffer.Sorted.addNat16(buffer, n);
            };

            case (#Nat8(n)) {
                buffer.add(TypeCode.Nat8);
                buffer.add(n);
            };

            case (#Nat(n)) {
                buffer.add(TypeCode.NatAsNat64);
                ByteUtils.Buffer.Sorted.addNat64(buffer, Nat64.fromNat(n));
            };
            case (#Int(int)) {
                buffer.add(TypeCode.IntAsInt64);
                ByteUtils.Buffer.Sorted.addInt64(buffer, Int64.fromInt(int));
            };

            case (#Bool(b)) {
                buffer.add(TypeCode.Bool);
                buffer.add(if (b) 1 else 0);
            };

            case (#Empty) buffer.add(TypeCode.Empty);
            case (#Null) buffer.add(TypeCode.Null);
            case (#Maximum) buffer.add(TypeCode.Maximum);
        };
    };

    func decode(bytes : T.Iter<Nat8>) : T.Candid {
        func read() : Nat8 {
            let ?byte = bytes.next() else Runtime.trap("Orchid: not enough bytes to read");
            byte;
        };

        let type_code = read();

        if (type_code == TypeCode.NatAsNat64) {
            #Nat(Nat64.toNat(ByteUtils.Sorted.toNat64(bytes)));
        } else if (type_code == TypeCode.Nat) {
            Runtime.trap("Orchid: #Nat type code found in encoded data - this type is not supported");
        } else if (type_code == TypeCode.Int) {
            Runtime.trap("Orchid: #Int type code found in encoded data - this type is not supported");
        } else if (type_code == TypeCode.Nat8) {
            let n = ByteUtils.Sorted.toNat8(bytes);
            (#Nat8(n));
        } else if (type_code == TypeCode.Nat16) {
            let n = ByteUtils.Sorted.toNat16(bytes);
            (#Nat16(n));
        } else if (type_code == TypeCode.Nat32) {
            let n = ByteUtils.Sorted.toNat32(bytes);
            (#Nat32(n));
        } else if (type_code == TypeCode.Nat64) {
            let n = ByteUtils.Sorted.toNat64(bytes);
            (#Nat64(n));
        } else if (type_code == TypeCode.IntAsInt64) {
            #Int(Int64.toInt(ByteUtils.Sorted.toInt64(bytes)));
        } else if (type_code == TypeCode.Int8) {
            let i = ByteUtils.Sorted.toInt8(bytes);
            (#Int8(i));
        } else if (type_code == TypeCode.Int16) {
            let int16 = ByteUtils.Sorted.toInt16(bytes);
            (#Int16(int16));
        } else if (type_code == TypeCode.Int32) {
            let int32 = ByteUtils.Sorted.toInt32(bytes);
            (#Int32(int32));
        } else if (type_code == TypeCode.Int64) {
            let int64 = ByteUtils.Sorted.toInt64(bytes);
            (#Int64(int64));
        } else if (type_code == TypeCode.Float) {
            let float = ByteUtils.Sorted.toFloat(bytes);
            (#Float(float));
        } else if (type_code == TypeCode.Bool) {
            let byte = read();
            (#Bool(if (byte == 1) true else false));
        } else if (type_code == TypeCode.Empty) {
            (#Empty);
        } else if (type_code == TypeCode.Null) {
            (#Null);
        } else if (type_code == TypeCode.Option) {
            let option_value = decode(bytes);
            (#Option(option_value));
            // } else if (type_code == TypeCode.Minimum) {
            //     (#Minimum);
            // } else if (type_code == TypeCode.Maximum) {
            //     (#Maximum);
        } else if (type_code == TypeCode.Null) {
            #Null;
        } else if (type_code == TypeCode.Empty) {
            #Empty;
        } else if (type_code == TypeCode.Principal) {
            // No size prefix — read until escape terminator
            #Principal(Principal.fromBlob(Blob.fromArray(decode_escaped(bytes))));
        } else if (type_code == TypeCode.Blob) {
            // No size prefix — read until escape terminator
            #Blob(Blob.fromArray(decode_escaped(bytes)));
        } else if (type_code == TypeCode.Text) {
            // No size prefix — read until escape terminator
            let utf8 = Blob.fromArray(decode_escaped(bytes));
            let text = switch (Text.decodeUtf8(utf8)) {
                case (null) Runtime.trap("Orchid: invalid utf8 in encoded Text field");
                case (?t) t;
            };
            #Text(text);

        } else Runtime.trap("Orchid: unknown type code: " # debug_show (type_code));

    };

    let to_blob = func(candid_values : [CandidQuery]) : Blob {
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

    let from_blob = func(blob : Blob) : [CandidQuery] {

        let bytes = Itertools.peekable(blob.vals());

        func read() : Nat8 {
            let ?byte = bytes.next() else Runtime.trap("Orchid: not enough bytes to read");
            byte;
        };

        let candid_values_size = Nat8.toNat(read());

        let buffer = Buffer.Buffer<CandidQuery>(candid_values_size);

        for (_ in Nat.rangeInclusive(1, candid_values_size)) {
            let res = decode(bytes);
            buffer.add(res);
        };

        Buffer.toArray(buffer);
    };

    public let Orchid : TypeUtils.TypeUtils<[CandidQuery]> and {
        btree_cmp : (Blob, Blob) -> Int8;
    } = {
        blobify = { to_blob; from_blob };
        btree_cmp = Prim.blobCompare;
        cmp = #BlobCmp(Prim.blobCompare);
    };

};
