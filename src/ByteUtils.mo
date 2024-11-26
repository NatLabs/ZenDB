import Buffer "mo:base/Buffer";
import Iter "mo:base/Iter";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Nat8 "mo:base/Nat8";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Int8 "mo:base/Int8";
import Int16 "mo:base/Int16";
import Int32 "mo:base/Int32";
import Int64 "mo:base/Int64";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Debug "mo:base/Debug";

import Itertools "mo:itertools/Iter";
import FloatX "mo:xtended-numbers/FloatX";

module {
    /// An iterator of bytes.
    type Bytes = Iter.Iter<Nat8>;

    public func to_nat8(bytes : Bytes) : Nat8 {
        switch (bytes.next()) {
            case (?byte) { byte };
            case (_) { Debug.trap("ByteUtils: out of bounds") };
        };
    };

    public func to_nat16(bytes : Bytes) : Nat16 {
        let high = to_nat8(bytes);
        let low = to_nat8(bytes);
        Nat16.fromNat8(high) << 8 | Nat16.fromNat8(low);
    };

    public func to_nat32(bytes : Bytes) : Nat32 {
        let b1 = to_nat8(bytes);
        let b2 = to_nat8(bytes);
        let b3 = to_nat8(bytes);
        let b4 = to_nat8(bytes);

        Nat32.fromNat(Nat8.toNat(b1)) << 24 | Nat32.fromNat(Nat8.toNat(b2)) << 16 | Nat32.fromNat(Nat8.toNat(b3)) << 8 | Nat32.fromNat(Nat8.toNat(b4));
    };

    public func to_nat64(bytes : Bytes) : Nat64 {
        let b1 = to_nat8(bytes);
        let b2 = to_nat8(bytes);
        let b3 = to_nat8(bytes);
        let b4 = to_nat8(bytes);
        let b5 = to_nat8(bytes);
        let b6 = to_nat8(bytes);
        let b7 = to_nat8(bytes);
        let b8 = to_nat8(bytes);

        Nat64.fromNat(Nat8.toNat(b1)) << 56 | Nat64.fromNat(Nat8.toNat(b2)) << 48 | Nat64.fromNat(Nat8.toNat(b3)) << 40 | Nat64.fromNat(Nat8.toNat(b4)) << 32 | Nat64.fromNat(Nat8.toNat(b5)) << 24 | Nat64.fromNat(Nat8.toNat(b6)) << 16 | Nat64.fromNat(Nat8.toNat(b7)) << 8 | Nat64.fromNat(Nat8.toNat(b8));

    };

    public func to_int8(bytes : Bytes) : Int8 {
        Int8.fromNat8(to_nat8(bytes));
    };

    public func to_int16(bytes : Bytes) : Int16 {
        let nat16 = to_nat16(bytes);
        Int16.fromNat16(nat16);
    };

    public func to_int32(bytes : Bytes) : Int32 {
        let nat32 = to_nat32(bytes);
        Int32.fromNat32(nat32);
    };

    public func to_int64(bytes : Bytes) : Int64 {
        let nat64 = to_nat64(bytes);
        Int64.fromNat64(nat64);
    };

    public func to_nat(bytes : Bytes) : Nat {
        var n = 0;
        let bytes_arr : [Nat8] = Iter.toArray(bytes);

        var j = bytes_arr.size();

        while (j > 0) {
            let byte = bytes_arr.get(j - 1);
            n *= 255;
            n += Nat8.toNat(byte);

            j -= 1;
        };

        n;
    };

    // need to update to sleb128
    public func to_int(bytes : Bytes) : Int {

        let bytes_arr : [Nat8] = Iter.toArray(bytes);

        var num = 0;
        var is_negative = false;

        var j = bytes_arr.size();

        while (j > 0) {
            let byte = bytes_arr.get(j - 1);
            if (j == 1) {
                is_negative := Nat8.toNat(byte) == 1;
            } else {
                num *= 255;
                num += Nat8.toNat(byte);
            };

            j -= 1;
        };

        if (is_negative) {
            -(num);
        } else {
            (num);
        };

    };

    public func to_float64(bytes : Bytes) : Float {
        let ?fx = FloatX.decode(bytes, #f64, #lsb) else Debug.trap("ByteUtils: failed to decode float64");
        FloatX.toFloat(fx);
    };

    public func from_float64(f : Float) : [Nat8] {
        let fx = FloatX.fromFloat(f, #f64);
        let buffer = Buffer.Buffer<Nat8>(8);

        FloatX.encode(buffer, fx, #lsb);
        Buffer.toArray(buffer);
    };

    public func from_int8(i : Int8) : [Nat8] {
        [Int8.toNat8(i)];
    };

    public func from_int16(i : Int16) : [Nat8] {
        let nat16 = Int16.toNat16(i);

        [
            Nat16.toNat8(nat16 >> 8),
            Nat16.toNat8(nat16),
        ];

    };

    public func from_int32(i : Int32) : [Nat8] {
        let nat32 = Int32.toNat32(i);

        [
            Nat8.fromNat(Nat32.toNat((nat32 >> 24) & 0xff)),
            Nat8.fromNat(Nat32.toNat((nat32 >> 16) & 0xff)),
            Nat8.fromNat(Nat32.toNat((nat32 >> 8) & 0xff)),
            Nat8.fromNat(Nat32.toNat(nat32 & 0xff)),
        ];

    };

    public func from_int64(i : Int64) : [Nat8] {
        let nat64 = Int64.toNat64(i);

        [
            Nat8.fromNat(Nat64.toNat((nat64 >> 56) & 0xff)),
            Nat8.fromNat(Nat64.toNat((nat64 >> 48) & 0xff)),
            Nat8.fromNat(Nat64.toNat((nat64 >> 40) & 0xff)),
            Nat8.fromNat(Nat64.toNat((nat64 >> 32) & 0xff)),
            Nat8.fromNat(Nat64.toNat((nat64 >> 24) & 0xff)),
            Nat8.fromNat(Nat64.toNat((nat64 >> 16) & 0xff)),
            Nat8.fromNat(Nat64.toNat((nat64 >> 8) & 0xff)),
            Nat8.fromNat(Nat64.toNat(nat64 & 0xff)),
        ];

    };

    public func from_nat8(n : Nat8) : [Nat8] {
        [n];
    };

    public func from_nat16(n : Nat16) : [Nat8] {
        [
            Nat16.toNat8(n >> 8),
            Nat16.toNat8(n),
        ];
    };

    public func from_nat32(n : Nat32) : [Nat8] {
        [
            Nat8.fromNat(Nat32.toNat((n >> 24) & 0xff)),
            Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)),
            Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)),
            Nat8.fromNat(Nat32.toNat(n & 0xff)),
        ];
    };

    public func from_nat64(n : Nat64) : [Nat8] {
        [
            Nat8.fromNat(Nat64.toNat((n >> 56) & 0xff)),
            Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)),
            Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)),
            Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)),
            Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)),
            Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)),
            Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)),
            Nat8.fromNat(Nat64.toNat(n & 0xff)),
        ];
    };

    // replace with sleb128
    public func from_nat(n : Nat) : [Nat8] {
        var num = n;
        var nbytes = 0;

        while (num > 0) {
            num /= 255;
            nbytes += 1;
        };

        num := n;

        let arr = Array.tabulate(
            nbytes,
            func(_ : Nat) : Nat8 {
                let tmp = num % 255;
                num /= 255;
                Nat8.fromNat(tmp);
            },
        );

        arr;
    };

    public func from_int(n : Int) : [Nat8] {
        let is_negative = n < 0;

        var num : Nat = Int.abs(n);
        var nbytes = 0;

        while (num > 0) {
            num /= 255;
            nbytes += 1;
        };

        num := Int.abs(n);

        let arr = Array.tabulate(
            nbytes + 1,
            func(i : Nat) : Nat8 {
                if (i == nbytes) return Nat8.fromNat(if (is_negative) 1 else 0);

                let tmp = num % 255;
                num /= 255;
                Nat8.fromNat(tmp);
            },
        );

        arr;
    };

};
