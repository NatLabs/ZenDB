import B "mo:base/Buffer";
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

import FloatX "mo:xtended-numbers/FloatX";
import Itertools "mo:itertools/Iter";

module ByteUtils {
    /// An iterator of bytes.
    type Bytes = Iter.Iter<Nat8>;

    func to_nat8(bytes : Bytes) : Nat8 {
        switch (bytes.next()) {
            case (?byte) { byte };
            case (_) { Debug.trap("ByteUtils: out of bounds") };
        };
    };

    func buffer_add_all<A>(buffer : B.Buffer<A>, iter : Iter.Iter<A>) {
        for (elem in iter) { buffer.add(elem) };
    };

    public type BufferLike<A> = {
        add : (A) -> ();
        get : (Nat) -> A;
    };

    public module LittleEndian {

        public func toNat8(bytes : Bytes) : Nat8 {
            to_nat8(bytes);
        };

        public func toNat16(bytes : Bytes) : Nat16 {
            let low = to_nat8(bytes);
            let high = to_nat8(bytes);
            Nat16.fromNat8(low) | Nat16.fromNat8(high) << 8;
        };

        public func toNat32(bytes : Bytes) : Nat32 {
            let b1 = to_nat8(bytes);
            let b2 = to_nat8(bytes);
            let b3 = to_nat8(bytes);
            let b4 = to_nat8(bytes);

            Nat32.fromNat(Nat8.toNat(b1)) | Nat32.fromNat(Nat8.toNat(b2)) << 8 | Nat32.fromNat(Nat8.toNat(b3)) << 16 | Nat32.fromNat(Nat8.toNat(b4)) << 24;

        };

        public func toNat64(bytes : Bytes) : Nat64 {
            let b1 = to_nat8(bytes);
            let b2 = to_nat8(bytes);
            let b3 = to_nat8(bytes);
            let b4 = to_nat8(bytes);
            let b5 = to_nat8(bytes);
            let b6 = to_nat8(bytes);
            let b7 = to_nat8(bytes);
            let b8 = to_nat8(bytes);

            Nat64.fromNat(Nat8.toNat(b1)) | Nat64.fromNat(Nat8.toNat(b2)) << 8 | Nat64.fromNat(Nat8.toNat(b3)) << 16 | Nat64.fromNat(Nat8.toNat(b4)) << 24 | Nat64.fromNat(Nat8.toNat(b5)) << 32 | Nat64.fromNat(Nat8.toNat(b6)) << 40 | Nat64.fromNat(Nat8.toNat(b7)) << 48 | Nat64.fromNat(Nat8.toNat(b8)) << 56;

        };

        public func toInt8(bytes : Bytes) : Int8 {
            Int8.fromNat8(to_nat8(bytes));
        };

        public func toInt16(bytes : Bytes) : Int16 {
            let nat16 = toNat16(bytes);
            Int16.fromNat16(nat16);
        };

        public func toInt32(bytes : Bytes) : Int32 {
            let nat32 = toNat32(bytes);
            Int32.fromNat32(nat32);
        };

        public func toInt64(bytes : Bytes) : Int64 {
            let nat64 = toNat64(bytes);
            Int64.fromNat64(nat64);
        };

        public func toFloat64(bytes : Bytes) : Float {
            let ?fx = FloatX.decode(bytes, #f64, #lsb) else Debug.trap("ByteUtils: failed to decode float64");
            FloatX.toFloat(fx);
        };

        public func fromNat8(n : Nat8) : [Nat8] {
            [n];
        };

        public func fromNat16(n : Nat16) : [Nat8] {
            [
                Nat16.toNat8(n & 0xff),
                Nat16.toNat8((n >> 8) & 0xff),
            ];
        };

        public func fromNat32(n : Nat32) : [Nat8] {
            [
                Nat8.fromNat(Nat32.toNat(n & 0xff)),
                Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)),
                Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)),
                Nat8.fromNat(Nat32.toNat((n >> 24) & 0xff)),
            ];
        };

        public func fromNat64(n : Nat64) : [Nat8] {
            [
                Nat8.fromNat(Nat64.toNat(n & 0xff)),
                Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)),
                Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)),
                Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)),
                Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)),
                Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)),
                Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)),
                Nat8.fromNat(Nat64.toNat((n >> 56) & 0xff)),
            ];
        };

        public func fromInt8(i : Int8) : [Nat8] {
            [Int8.toNat8(i)];
        };

        public func fromInt16(i : Int16) : [Nat8] {
            let nat16 = Int16.toNat16(i);
            fromNat16(nat16);
        };

        public func fromInt32(i : Int32) : [Nat8] {
            let nat32 = Int32.toNat32(i);
            fromNat32(nat32);
        };

        public func fromInt64(i : Int64) : [Nat8] {
            let nat64 = Int64.toNat64(i);
            fromNat64(nat64);
        };

        public func fromFloat64(f : Float) : [Nat8] {
            let fx = FloatX.fromFloat(f, #f64);
            let buffer = B.Buffer<Nat8>(8);

            FloatX.encode(buffer, fx, #lsb);
            B.toArray(buffer);
        };

    };

    public module BigEndian {
        public func toNat8(bytes : Bytes) : Nat8 {
            to_nat8(bytes);
        };

        public func toNat16(bytes : Bytes) : Nat16 {
            let high = to_nat8(bytes);
            let low = to_nat8(bytes);
            Nat16.fromNat8(high) << 8 | Nat16.fromNat8(low);
        };

        public func toNat32(bytes : Bytes) : Nat32 {
            let b1 = to_nat8(bytes);
            let b2 = to_nat8(bytes);
            let b3 = to_nat8(bytes);
            let b4 = to_nat8(bytes);

            Nat32.fromNat(Nat8.toNat(b1)) << 24 | Nat32.fromNat(Nat8.toNat(b2)) << 16 | Nat32.fromNat(Nat8.toNat(b3)) << 8 | Nat32.fromNat(Nat8.toNat(b4));
        };

        public func toNat64(bytes : Bytes) : Nat64 {
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

        public func toInt8(bytes : Bytes) : Int8 {
            Int8.fromNat8(to_nat8(bytes));
        };

        public func toInt16(bytes : Bytes) : Int16 {
            let nat16 = toNat16(bytes);
            Int16.fromNat16(nat16);
        };

        public func toInt32(bytes : Bytes) : Int32 {
            let nat32 = toNat32(bytes);
            Int32.fromNat32(nat32);
        };

        public func toInt64(bytes : Bytes) : Int64 {
            let nat64 = toNat64(bytes);
            Int64.fromNat64(nat64);
        };

        public func toFloat64(bytes : Bytes) : Float {
            let ?fx = FloatX.decode(bytes, #f64, #msb) else Debug.trap("ByteUtils: failed to decode float64");
            FloatX.toFloat(fx);
        };

        public func fromNat8(n : Nat8) : [Nat8] {
            [n];
        };

        public func fromNat16(n : Nat16) : [Nat8] {
            [
                Nat16.toNat8((n >> 8) & 0xff),
                Nat16.toNat8(n & 0xff),
            ];
        };

        public func fromNat32(n : Nat32) : [Nat8] {
            [
                Nat8.fromNat(Nat32.toNat((n >> 24) & 0xff)),
                Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)),
                Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)),
                Nat8.fromNat(Nat32.toNat(n & 0xff)),
            ];
        };

        public func fromNat64(n : Nat64) : [Nat8] {
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

        public func fromInt8(i : Int8) : [Nat8] {
            [Int8.toNat8(i)];
        };

        public func fromInt16(i : Int16) : [Nat8] {
            let nat16 = Int16.toNat16(i);
            fromNat16(nat16);
        };

        public func fromInt32(i : Int32) : [Nat8] {
            let nat32 = Int32.toNat32(i);
            fromNat32(nat32);
        };

        public func fromInt64(i : Int64) : [Nat8] {
            let nat64 = Int64.toNat64(i);
            fromNat64(nat64);
        };

        public func fromFloat64(f : Float) : [Nat8] {
            let fx = FloatX.fromFloat(f, #f64);
            let buffer = B.Buffer<Nat8>(8);

            FloatX.encode(buffer, fx, #msb);
            B.toArray(buffer);
        };
    };

    public let LE = LittleEndian;
    public let BE = BigEndian;

    public func toLEB128_64(n64 : Nat64) : [Nat8] {
        let buffer = B.Buffer<Nat8>(10);
        Buffer.writeLEB128_64(buffer, n64);
        B.toArray(buffer);
    };

    public func fromLEB128_64(bytes : Bytes) : Nat64 {
        let buffer = B.Buffer<Nat8>(10);
        for (byte in bytes) { buffer.add(byte) };
        Buffer.readLEB128_64(buffer);
    };

    public func toSLEB128_64(n : Int64) : [Nat8] {
        let buffer = B.Buffer<Nat8>(10);
        Buffer.writeSLEB128_64(buffer, n);
        B.toArray(buffer);
    };

    public func fromSLEB128_64(bytes : Bytes) : Int64 {
        let buffer = B.Buffer<Nat8>(10);
        for (byte in bytes) { buffer.add(byte) };
        Buffer.readSLEB128_64(buffer);
    };

    public module Buffer {

        public func addBytes(buffer : B.Buffer<Nat8>, iter : Iter.Iter<Nat8>) {
            for (elem in iter) { buffer.add(elem) };
        };

        public module LittleEndian {
            // Rename existing write methods to add methods (add to end of buffer)
            public func addNat8(buffer : B.Buffer<Nat8>, n : Nat8) {
                buffer.add(n);
            };

            public func addNat16(buffer : B.Buffer<Nat8>, n : Nat16) {
                buffer.add(Nat16.toNat8(n & 0xff));
                buffer.add(Nat16.toNat8(n >> 8) & 0xff);
            };

            public func addNat32(buffer : B.Buffer<Nat8>, n : Nat32) {
                buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 24) & 0xff)));
            };

            public func addNat64(buffer : B.Buffer<Nat8>, n : Nat64) {
                buffer.add(Nat8.fromNat(Nat64.toNat(n & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 56) & 0xff)));
            };

            public func addInt8(buffer : B.Buffer<Nat8>, i : Int8) {
                buffer.add(Int8.toNat8(i));
            };

            public func addInt16(buffer : B.Buffer<Nat8>, i : Int16) {
                let nat16 = Int16.toNat16(i);
                addNat16(buffer, nat16);
            };

            public func addInt32(buffer : B.Buffer<Nat8>, i : Int32) {
                let nat32 = Int32.toNat32(i);
                addNat32(buffer, nat32);
            };

            public func addInt64(buffer : B.Buffer<Nat8>, i : Int64) {
                let nat64 = Int64.toNat64(i);
                addNat64(buffer, nat64);
            };

            public func addFloat64(buffer : B.Buffer<Nat8>, f : Float) {
                let fx = FloatX.fromFloat(f, #f64);
                FloatX.encode(buffer, fx, #lsb);
            };

            // Add new write methods (write at specific offset)
            public func writeNat8(buffer : B.Buffer<Nat8>, offset : Nat, n : Nat8) {
                buffer.put(offset, n);
            };

            public func writeNat16(buffer : B.Buffer<Nat8>, offset : Nat, n : Nat16) {
                buffer.put(offset, Nat16.toNat8(n & 0xff));
                buffer.put(offset + 1, Nat16.toNat8((n >> 8) & 0xff));
            };

            public func writeNat32(buffer : B.Buffer<Nat8>, offset : Nat, n : Nat32) {
                buffer.put(offset, Nat8.fromNat(Nat32.toNat(n & 0xff)));
                buffer.put(offset + 1, Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                buffer.put(offset + 2, Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                buffer.put(offset + 3, Nat8.fromNat(Nat32.toNat((n >> 24) & 0xff)));
            };

            public func writeNat64(buffer : B.Buffer<Nat8>, offset : Nat, n : Nat64) {
                buffer.put(offset, Nat8.fromNat(Nat64.toNat(n & 0xff)));
                buffer.put(offset + 1, Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                buffer.put(offset + 2, Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                buffer.put(offset + 3, Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                buffer.put(offset + 4, Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                buffer.put(offset + 5, Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                buffer.put(offset + 6, Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                buffer.put(offset + 7, Nat8.fromNat(Nat64.toNat((n >> 56) & 0xff)));
            };

            public func writeInt8(buffer : B.Buffer<Nat8>, offset : Nat, i : Int8) {
                buffer.put(offset, Int8.toNat8(i));
            };

            public func writeInt16(buffer : B.Buffer<Nat8>, offset : Nat, i : Int16) {
                let nat16 = Int16.toNat16(i);
                writeNat16(buffer, offset, nat16);
            };

            public func writeInt32(buffer : B.Buffer<Nat8>, offset : Nat, i : Int32) {
                let nat32 = Int32.toNat32(i);
                writeNat32(buffer, offset, nat32);
            };

            public func writeInt64(buffer : B.Buffer<Nat8>, offset : Nat, i : Int64) {
                let nat64 = Int64.toNat64(i);
                writeNat64(buffer, offset, nat64);
            };

            public func writeFloat64(buffer : B.Buffer<Nat8>, offset : Nat, f : Float) {
                let fx = FloatX.fromFloat(f, #f64);
                let tempBuffer = B.Buffer<Nat8>(8);
                FloatX.encode(tempBuffer, fx, #lsb);

                // Copy from temp buffer to target buffer at offset
                for (i in Iter.range(0, 7)) {
                    buffer.put(offset + i, tempBuffer.get(i));
                };
            };

            public func readNat8(buffer : B.Buffer<Nat8>, offset : Nat) : Nat8 {
                buffer.get(offset);
            };

            public func readNat16(buffer : B.Buffer<Nat8>, offset : Nat) : Nat16 {
                let low = buffer.get(offset);
                let high = buffer.get(offset + 1);
                Nat16.fromNat8(low) | Nat16.fromNat8(high) << 8;
            };

            public func readNat32(buffer : B.Buffer<Nat8>, offset : Nat) : Nat32 {
                let b1 = buffer.get(offset);
                let b2 = buffer.get(offset + 1);
                let b3 = buffer.get(offset + 2);
                let b4 = buffer.get(offset + 3);

                Nat32.fromNat(Nat8.toNat(b1)) | Nat32.fromNat(Nat8.toNat(b2)) << 8 | Nat32.fromNat(Nat8.toNat(b3)) << 16 | Nat32.fromNat(Nat8.toNat(b4)) << 24;
            };

            public func readNat64(buffer : B.Buffer<Nat8>, offset : Nat) : Nat64 {
                let b1 = buffer.get(offset);
                let b2 = buffer.get(offset + 1);
                let b3 = buffer.get(offset + 2);
                let b4 = buffer.get(offset + 3);
                let b5 = buffer.get(offset + 4);
                let b6 = buffer.get(offset + 5);
                let b7 = buffer.get(offset + 6);
                let b8 = buffer.get(offset + 7);

                Nat64.fromNat(Nat8.toNat(b1)) | Nat64.fromNat(Nat8.toNat(b2)) << 8 | Nat64.fromNat(Nat8.toNat(b3)) << 16 | Nat64.fromNat(Nat8.toNat(b4)) << 24 | Nat64.fromNat(Nat8.toNat(b5)) << 32 | Nat64.fromNat(Nat8.toNat(b6)) << 40 | Nat64.fromNat(Nat8.toNat(b7)) << 48 | Nat64.fromNat(Nat8.toNat(b8)) << 56;
            };

            public func readInt8(buffer : B.Buffer<Nat8>, offset : Nat) : Int8 {
                Int8.fromNat8(buffer.get(offset));
            };

            public func readInt16(buffer : B.Buffer<Nat8>, offset : Nat) : Int16 {
                let nat16 = readNat16(buffer, offset);
                Int16.fromNat16(nat16);
            };

            public func readInt32(buffer : B.Buffer<Nat8>, offset : Nat) : Int32 {
                let nat32 = readNat32(buffer, offset);
                Int32.fromNat32(nat32);
            };

            public func readInt64(buffer : B.Buffer<Nat8>, offset : Nat) : Int64 {
                let nat64 = readNat64(buffer, offset);
                Int64.fromNat64(nat64);
            };

        };

        public module BigEndian {
            // Rename existing write methods to add methods (add to end of buffer)
            public func addNat8(buffer : B.Buffer<Nat8>, n : Nat8) {
                buffer.add(n);
            };

            public func addNat16(buffer : B.Buffer<Nat8>, n : Nat16) {
                buffer.add(Nat16.toNat8((n >> 8) & 0xff));
                buffer.add(Nat16.toNat8(n & 0xff));
            };

            public func addNat32(buffer : B.Buffer<Nat8>, n : Nat32) {
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 24) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));
            };

            public func addNat64(buffer : B.Buffer<Nat8>, n : Nat64) {
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 56) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                buffer.add(Nat8.fromNat(Nat64.toNat(n & 0xff)));
            };

            public func addInt8(buffer : B.Buffer<Nat8>, i : Int8) {
                buffer.add(Int8.toNat8(i));
            };

            public func addInt16(buffer : B.Buffer<Nat8>, i : Int16) {
                let nat16 = Int16.toNat16(i);
                addNat16(buffer, nat16);
            };

            public func addInt32(buffer : B.Buffer<Nat8>, i : Int32) {
                let nat32 = Int32.toNat32(i);
                addNat32(buffer, nat32);
            };

            public func addInt64(buffer : B.Buffer<Nat8>, i : Int64) {
                let nat64 = Int64.toNat64(i);
                addNat64(buffer, nat64);
            };

            public func addFloat64(buffer : B.Buffer<Nat8>, f : Float) {
                let fx = FloatX.fromFloat(f, #f64);
                FloatX.encode(buffer, fx, #msb);
            };

            // Add new write methods (write at specific offset)
            public func writeNat8(buffer : B.Buffer<Nat8>, offset : Nat, n : Nat8) {
                buffer.put(offset, n);
            };

            public func writeNat16(buffer : B.Buffer<Nat8>, offset : Nat, n : Nat16) {
                buffer.put(offset, Nat16.toNat8((n >> 8) & 0xff));
                buffer.put(offset + 1, Nat16.toNat8(n & 0xff));
            };

            public func writeNat32(buffer : B.Buffer<Nat8>, offset : Nat, n : Nat32) {
                buffer.put(offset, Nat8.fromNat(Nat32.toNat((n >> 24) & 0xff)));
                buffer.put(offset + 1, Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                buffer.put(offset + 2, Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                buffer.put(offset + 3, Nat8.fromNat(Nat32.toNat(n & 0xff)));
            };

            public func writeNat64(buffer : B.Buffer<Nat8>, offset : Nat, n : Nat64) {
                buffer.put(offset, Nat8.fromNat(Nat64.toNat((n >> 56) & 0xff)));
                buffer.put(offset + 1, Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                buffer.put(offset + 2, Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                buffer.put(offset + 3, Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                buffer.put(offset + 4, Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                buffer.put(offset + 5, Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                buffer.put(offset + 6, Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                buffer.put(offset + 7, Nat8.fromNat(Nat64.toNat(n & 0xff)));
            };

            public func writeInt8(buffer : B.Buffer<Nat8>, offset : Nat, i : Int8) {
                buffer.put(offset, Int8.toNat8(i));
            };

            public func writeInt16(buffer : B.Buffer<Nat8>, offset : Nat, i : Int16) {
                let nat16 = Int16.toNat16(i);
                writeNat16(buffer, offset, nat16);
            };

            public func writeInt32(buffer : B.Buffer<Nat8>, offset : Nat, i : Int32) {
                let nat32 = Int32.toNat32(i);
                writeNat32(buffer, offset, nat32);
            };

            public func writeInt64(buffer : B.Buffer<Nat8>, offset : Nat, i : Int64) {
                let nat64 = Int64.toNat64(i);
                writeNat64(buffer, offset, nat64);
            };

            public func writeFloat64(buffer : B.Buffer<Nat8>, offset : Nat, f : Float) {
                let fx = FloatX.fromFloat(f, #f64);
                let tempBuffer = B.Buffer<Nat8>(8);
                FloatX.encode(tempBuffer, fx, #msb);

                // Copy from temp buffer to target buffer at offset
                for (i in Iter.range(0, 7)) {
                    buffer.put(offset + i, tempBuffer.get(i));
                };
            };

            public func readNat8(buffer : B.Buffer<Nat8>, offset : Nat) : Nat8 {
                buffer.get(offset);
            };

            public func readNat16(buffer : B.Buffer<Nat8>, offset : Nat) : Nat16 {
                let high = buffer.get(offset);
                let low = buffer.get(offset + 1);
                Nat16.fromNat8(high) << 8 | Nat16.fromNat8(low);
            };

            public func readNat32(buffer : B.Buffer<Nat8>, offset : Nat) : Nat32 {
                let b1 = buffer.get(offset);
                let b2 = buffer.get(offset + 1);
                let b3 = buffer.get(offset + 2);
                let b4 = buffer.get(offset + 3);

                Nat32.fromNat(Nat8.toNat(b1)) << 24 | Nat32.fromNat(Nat8.toNat(b2)) << 16 | Nat32.fromNat(Nat8.toNat(b3)) << 8 | Nat32.fromNat(Nat8.toNat(b4));
            };

            public func readNat64(buffer : B.Buffer<Nat8>, offset : Nat) : Nat64 {
                let b1 = buffer.get(offset);
                let b2 = buffer.get(offset + 1);
                let b3 = buffer.get(offset + 2);
                let b4 = buffer.get(offset + 3);
                let b5 = buffer.get(offset + 4);
                let b6 = buffer.get(offset + 5);
                let b7 = buffer.get(offset + 6);
                let b8 = buffer.get(offset + 7);

                Nat64.fromNat(Nat8.toNat(b1)) << 56 | Nat64.fromNat(Nat8.toNat(b2)) << 48 | Nat64.fromNat(Nat8.toNat(b3)) << 40 | Nat64.fromNat(Nat8.toNat(b4)) << 32 | Nat64.fromNat(Nat8.toNat(b5)) << 24 | Nat64.fromNat(Nat8.toNat(b6)) << 16 | Nat64.fromNat(Nat8.toNat(b7)) << 8 | Nat64.fromNat(Nat8.toNat(b8));
            };

            public func readInt8(buffer : B.Buffer<Nat8>, offset : Nat) : Int8 {
                Int8.fromNat8(buffer.get(offset));
            };

            public func readInt16(buffer : B.Buffer<Nat8>, offset : Nat) : Int16 {
                let nat16 = readNat16(buffer, offset);
                Int16.fromNat16(nat16);
            };

            public func readInt32(buffer : B.Buffer<Nat8>, offset : Nat) : Int32 {
                let nat32 = readNat32(buffer, offset);
                Int32.fromNat32(nat32);
            };

            public func readInt64(buffer : B.Buffer<Nat8>, offset : Nat) : Int64 {
                let nat64 = readNat64(buffer, offset);
                Int64.fromNat64(nat64);
            };
        };

        public let LE = LittleEndian;
        public let BE = BigEndian;

        // Encodings that have a consistent endianness

        // https://en.wikipedia.org/wiki/LEB128
        // limited to 64-bit unsigned integers
        // more performant than the general unsigned_leb128
        public func writeLEB128_64(buffer : BufferLike<Nat8>, n : Nat64) {
            var n64 : Nat64 = n;

            loop {
                var byte = n64 & 0x7F |> Nat64.toNat(_) |> Nat8.fromNat(_);
                n64 >>= 7;

                if (n64 > 0) byte := (byte | 0x80);
                buffer.add(byte);

            } while (n64 > 0);
        };

        // https://en.wikipedia.org/wiki/LEB128
        // limited to 64-bit signed integers
        // more performant than the general signed_leb128
        public func writeSLEB128_64(buffer : BufferLike<Nat8>, _n : Int64) {
            let n = Int64.toInt(_n);
            let is_negative = n < 0;

            // Convert to correct absolute value representation first
            var value : Nat64 = if (is_negative) {
                // For negative numbers in two's complement: bitwise NOT of abs(n)-1
                Nat64.fromNat(Int.abs(n) - 1);
            } else {
                Nat64.fromNat(Int.abs(n));
            };

            var more = true;

            while (more) {
                // Get lowest 7 bits
                var byte : Nat8 = Nat8.fromNat(Nat64.toNat(value & 0x7F));

                // Shift for next iteration
                value >>= 7;

                // Determine if we need more bytes
                if (
                    (value == 0 and (byte & 0x40) == 0) or
                    (is_negative and value == Nat64.fromNat(Int.abs(Int64.toInt(Int64.maximumValue))) and (byte & 0x40) != 0)
                ) {
                    more := false;
                } else {
                    byte |= 0x80; // Set continuation bit
                };

                // For negative numbers, invert bits (apply two's complement)
                if (is_negative) {
                    byte := byte ^ 0x7F;
                };

                buffer.add(byte);
            };
        };

        // https://en.wikipedia.org/wiki/LEB128
        public func readLEB128_64(buffer : BufferLike<Nat8>) : Nat64 {
            var n64 : Nat64 = 0;
            var shift : Nat64 = 0;
            var i = 0;

            label decoding_leb loop {
                let byte = buffer.get(i);
                i += 1;

                n64 |= (Nat64.fromNat(Nat8.toNat(byte & 0x7f)) << shift);

                if (byte & 0x80 == 0) break decoding_leb;
                shift += 7;

            };

            n64;
        };

        public func readSLEB128_64(buffer : BufferLike<Nat8>) : Int64 {
            var result : Nat64 = 0;
            var shift : Nat64 = 0;
            var byte : Nat8 = 0;
            var i = 0;

            label analyzing loop {
                byte := buffer.get(i);
                i += 1;

                // Add this byte's 7 bits to the result
                result |= Nat64.fromNat(Nat8.toNat(byte & 0x7F)) << shift;
                shift += 7;

                // If continuation bit is not set, we're done reading bytes
                if ((byte & 0x80) == 0) {
                    break analyzing;
                };
            };

            // Sign extend if this is a negative number
            if (byte & 0x40 != 0 and shift < 64) {
                // Fill the rest with 1s (sign extension)
                result |= ^((Nat64.fromNat(1) << shift) - 1);
            };

            Int64.fromNat64(result);
        };

    };

};
