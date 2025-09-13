import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Principal "mo:base@0.16.0/Principal";
import Array "mo:base@0.16.0/Array";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Nat32 "mo:base@0.16.0/Nat32";
import Nat "mo:base@0.16.0/Nat";
import Int "mo:base@0.16.0/Int";
import Int32 "mo:base@0.16.0/Int32";
import Blob "mo:base@0.16.0/Blob";
import Nat64 "mo:base@0.16.0/Nat64";
import Int16 "mo:base@0.16.0/Int16";
import Int64 "mo:base@0.16.0/Int64";
import Int8 "mo:base@0.16.0/Int8";
import Nat16 "mo:base@0.16.0/Nat16";
import Nat8 "mo:base@0.16.0/Nat8";
import Option "mo:base@0.16.0/Option";
import Iter "mo:base@0.16.0/Iter";

import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";
import ByteUtils "mo:byte-utils@0.1.1";
import Itertools "mo:itertools@0.2.2/Iter";
import Cmp "mo:augmented-btrees@0.7.1/Cmp";
import Map "mo:map@9.0.1/Map";

import T "../Types";

module {
    type CandidQuery = T.CandidQuery;

    /// Orchid
    /// This is a module for encoding Composite Index Keys, represented as an array of candid values, into its binary representation.

    public let TypeCode = {
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

        Principal : Nat8 = 16; // null terminated utf8 bytes
        Text : Nat8 = 17; // texts with a size of 0 to 65535 bytes, size prefix of 2 bytes
        Blob : Nat8 = 18; // blobs with a size 0 to 65535 bytes, size prefix of 2 bytes

        Option : Nat8 = 19;

        Maximum : Nat8 = 255;

    };

    public func typeCodeToText(type_code : Nat8) : Text {
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

    func encode(buffer : Buffer.Buffer<Nat8>, candid : CandidQuery) {

        switch (candid) {
            case (#Minimum) buffer.add(TypeCode.Minimum);
            case (#Array(_) or #Record(_) or #Map(_) or #Variant(_) or #Tuple(_)) Debug.trap("Orchid does not support compound types: " # debug_show (candid));
            case (#Option(option_type)) {
                buffer.add(TypeCode.Option);
                encode(buffer, option_type);
            };

            case (#Principal(p)) {
                buffer.add(TypeCode.Principal);

                let blob = Principal.toBlob(p);
                buffer.add(Nat8.fromNat(blob.size()));

                ByteUtils.Buffer.addBytes(buffer, blob.vals());

            };
            case (#Blob(b)) {

                buffer.add(TypeCode.Blob);

                let size = Nat16.fromNat(b.size());

                ByteUtils.Buffer.BigEndian.addNat16(buffer, size);
                ByteUtils.Buffer.addBytes(buffer, b.vals());

            };
            case (#Text(t)) {
                buffer.add(TypeCode.Text);

                let bytes = Text.encodeUtf8(t);
                let size = Nat16.fromNat(bytes.size());

                ByteUtils.Buffer.BigEndian.addNat16(buffer, size);
                ByteUtils.Buffer.addBytes(buffer, bytes.vals());

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
                let n64 = Nat64.fromNat(n);
                buffer.add(TypeCode.NatAsNat64);
                ByteUtils.Buffer.Sorted.addNat64(buffer, n64);
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
            let ?byte = bytes.next() else Debug.trap("Orchid: not enough bytes to read");
            byte;
        };

        let type_code = read();

        if (type_code == TypeCode.NatAsNat64) {
            let n64 = ByteUtils.Sorted.toNat64(bytes);
            (#Nat(Nat64.toNat(n64)));
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
            let int64 = ByteUtils.Sorted.toInt64(bytes);
            (#Int(Int64.toInt(int64)));
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
            let principal_size = Nat8.toNat(read());

            let p = Principal.fromBlob(
                Blob.fromArray(
                    Array.tabulate<Nat8>(
                        principal_size,
                        func(i : Nat) : Nat8 = read(),
                    )
                )
            );

            (#Principal(p));
        } else if (type_code == TypeCode.Blob) {
            let blob_size = Nat16.toNat(ByteUtils.BigEndian.toNat16(bytes));

            let blob = Blob.fromArray(
                Array.tabulate<Nat8>(
                    blob_size,
                    func(i : Nat) : Nat8 = read(),
                )
            );

            (#Blob(blob));
        } else if (type_code == TypeCode.Text) {
            let text_size = Nat16.toNat(ByteUtils.BigEndian.toNat16(bytes));

            let utf8 = Blob.fromArray(
                Array.tabulate<Nat8>(
                    text_size,
                    func(i : Nat) : Nat8 = read(),
                )
            );

            let text = switch (Text.decodeUtf8(utf8)) {
                case (null) Debug.trap("Orchid: invalid utf8 text in CandidQuery");
                case (?t) t;
            };

            (#Text(text));

        } else Debug.trap("Orchid: unknown type code: " # debug_show (type_code));

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
            let ?byte = bytes.next() else Debug.trap("Orchid: not enough bytes to read");
            byte;
        };

        let candid_values_size = Nat8.toNat(read());

        let buffer = Buffer.Buffer<CandidQuery>(candid_values_size);

        for (_ in Iter.range(1, candid_values_size)) {
            let res = decode(bytes);
            buffer.add(res);
        };

        Buffer.toArray(buffer);
    };

    func btree_cmp(a : Blob, b : Blob) : Int8 {

        let iter_a = a.vals();
        let iter_b = b.vals();

        func next(iter : Iter.Iter<Nat8>) : Nat8 {
            switch (iter.next()) {
                case (null) Debug.trap("Orchid: not enough bytes to compare");
                case (?byte) byte;
            };
        };

        func cmp_n_bytes(n : Nat) : Int8 {
            var i = 0;

            while (i < n) {
                let byte_a = next_byte_a();
                let byte_b = next_byte_b();

                if (byte_a < byte_b) return -1;
                if (byte_a > byte_b) return 1;

                i += 1;

            };

            0;

        };

        func consume_iter(iter : T.Iter<Nat8>) {
            while (iter.next() != null) {};
        };

        func cmp_iters<A>(a : T.Iter<Nat8>, b : T.Iter<Nat8>) : Int8 {
            loop switch (a.next(), b.next()) {
                case (null, null) return 0;
                case (null, _) {
                    consume_iter(b);
                    return -1;
                };
                case (_, null) {
                    consume_iter(a);
                    return 1;
                };
                case (?a_val, ?b_val) {
                    let res = Cmp.Nat8(a_val, b_val);
                    if (res != 0) {
                        consume_iter(a);
                        consume_iter(b);

                        return res;
                    };
                };
            };

        };

        func next_byte_a() : Nat8 = next(iter_a);
        func next_byte_b() : Nat8 = next(iter_b);

        let composite_key_count_in_a = next_byte_a();
        let composite_key_count_in_b = next_byte_b();

        let composite_key_cnt = Nat8.min(composite_key_count_in_a, composite_key_count_in_b);

        var curr_composite_key : Nat8 = 0;

        func compare_at_type_code() : Int8 {
            let type_code_a = next_byte_a();
            let type_code_b = next_byte_b();

            if (type_code_a != type_code_b) {
                if (type_code_a == TypeCode.Minimum or type_code_b == TypeCode.Minimum) {
                    // Minimum is a special case, it is always less than any other type code
                    if (type_code_a == TypeCode.Minimum) return -1;
                    if (type_code_b == TypeCode.Minimum) return 1;
                } else if (type_code_a == TypeCode.Maximum or type_code_b == TypeCode.Maximum) {
                    // Maximum is a special case, it is always greater than any other type code
                    if (type_code_a == TypeCode.Maximum) return 1;
                    if (type_code_b == TypeCode.Maximum) return -1;
                } else if ((type_code_a == TypeCode.Null and type_code_b == TypeCode.Option) or (type_code_a == TypeCode.Option and type_code_b == TypeCode.Null)) {
                    // Null is less than Option
                    if (type_code_a == TypeCode.Null) return -1;
                    if (type_code_b == TypeCode.Null) return 1;
                } else if (type_code_a == TypeCode.Null or type_code_b == TypeCode.Null) {
                    // Null is less than any other type code
                    if (type_code_a == TypeCode.Null) return -1;
                    if (type_code_b == TypeCode.Null) return 1;
                } else Debug.trap("Orchid: type codes do not match: " # debug_show (typeCodeToText(type_code_a)) # " != " # debug_show typeCodeToText(type_code_b) # "\n" # debug_show (a, b));
            };

            let type_code = type_code_a;

            if (type_code == TypeCode.Null or type_code == TypeCode.Empty) {
                0;
            } else if (type_code == TypeCode.Bool) {
                cmp_n_bytes(1);
            } else if (type_code == TypeCode.Nat8) {
                cmp_n_bytes(1);
            } else if (type_code == TypeCode.Nat16) {
                cmp_n_bytes(2);
            } else if (type_code == TypeCode.Nat32) {
                cmp_n_bytes(4);
            } else if (type_code == TypeCode.Nat64) {
                cmp_n_bytes(8);
            } else if (type_code == TypeCode.NatAsNat64) {
                cmp_n_bytes(8);
            } else if (type_code == TypeCode.Int8) {
                cmp_n_bytes(1);
            } else if (type_code == TypeCode.Int16) {
                cmp_n_bytes(2);
            } else if (type_code == TypeCode.Int32) {
                cmp_n_bytes(4);
            } else if (type_code == TypeCode.Int64) {
                cmp_n_bytes(8);
            } else if (type_code == TypeCode.IntAsInt64) {
                cmp_n_bytes(8);
            } else if (type_code == TypeCode.Float) {
                cmp_n_bytes(8);
            } else if (type_code == TypeCode.Option) {
                compare_at_type_code();
            } else if (type_code == TypeCode.Principal) {
                let principal_a_size = Nat8.toNat(next_byte_a());
                let principal_b_size = Nat8.toNat(next_byte_b());

                cmp_iters(
                    Itertools.take<Nat8>(iter_a, principal_a_size),
                    Itertools.take<Nat8>(iter_b, principal_b_size),
                )

            } else if (type_code == TypeCode.Blob) {
                let blob_size_a = Nat16.toNat(ByteUtils.BigEndian.toNat16(iter_a));
                let blob_size_b = Nat16.toNat(ByteUtils.BigEndian.toNat16(iter_b));

                cmp_iters(
                    Itertools.take<Nat8>(iter_a, blob_size_a),
                    Itertools.take<Nat8>(iter_b, blob_size_b),
                );

            } else if (type_code == TypeCode.Text) {

                let text_size_a = Nat16.toNat(ByteUtils.BigEndian.toNat16(iter_a));
                let text_size_b = Nat16.toNat(ByteUtils.BigEndian.toNat16(iter_b));

                let text_bytes_iter_a = Itertools.take<Nat8>(iter_a, text_size_a);
                let text_bytes_iter_b = Itertools.take<Nat8>(iter_b, text_size_b);

                cmp_iters(text_bytes_iter_a, text_bytes_iter_b);

            } else Debug.trap("Orchid: unknown type code: " # debug_show (type_code));

        };

        while (curr_composite_key < composite_key_cnt) {
            let res : Int8 = compare_at_type_code();

            if (res != 0) return res;

            curr_composite_key += 1;
        };

        return 0;

    };

    public let Orchid : TypeUtils.TypeUtils<[CandidQuery]> and {
        btree_cmp : (Blob, Blob) -> Int8;
    } = {
        blobify = { to_blob; from_blob };
        btree_cmp;
        cmp = #BlobCmp(btree_cmp);
    };

};
