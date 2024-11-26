import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Option "mo:base/Option";
import Iter "mo:base/Iter";
import Nat32 "mo:base/Nat32";
import Nat16 "mo:base/Nat16";

import Itertools "mo:itertools/Iter";

import BitBuffer "mo:bitbuffer/BitBuffer";

module {
    // default read natX functions is little endian

    public class ByteReader(bytes : [Nat8]) {
        var position = 0;

        public func read() : Nat8 {
            let byte = bytes[position];
            position += 1;
            byte;
        };

        public func read_bytes(n : Nat) : Iter.Iter<Nat8> {
            let iter = Itertools.fromArraySlice(bytes, position, position + n);
            position += n;
            iter;
        };

        public func read_nat_16() : Nat16 {
            Nat16.fromNat8(read()) << 8 | Nat16.fromNat8(read());
        };

        public func read_nat_32() : Nat32 {
            Nat32.fromNat(Nat8.toNat(read())) << 24 | Nat32.fromNat(Nat8.toNat(read())) << 16 | Nat32.fromNat(Nat8.toNat(read())) << 8 | Nat32.fromNat(Nat8.toNat(read()));
        };

        // func is_valid(n : Nat) : Bool {
        //     n <= (bitbuffer.bitSize() - offset - tailBits : Nat);
        // };

        // public func peekBit() : Bool {
        //     if (not is_valid(1)) {
        //         Debug.trap("BitReader.peekBit(): out of bounds or empty");
        //     };

        //     bitbuffer.getBit(offset);
        // };

        // public func readBit() : Bool {
        //     let bit = peekBit();
        //     offset += 1;
        //     bit;
        // };

        // public func peekBits(n : Nat) : Nat {
        //     if (not is_valid(n)) {
        //         Debug.trap("BitReader.peekBits: out of bounds at offset");
        //     };

        //     bitbuffer.getBits(offset, n);
        // };

        // public func skipBits(n : Nat) {
        //     if (not is_valid(n)) {
        //         Debug.trap("BitReader.skipBits: out of bounds");
        //     };

        //     offset += n;
        // };

        // public func readBits(n : Nat) : Nat {
        //     let bits = peekBits(n);
        //     offset += n;
        //     bits;
        // };

        // public func peekByte() : Nat8 {
        //     if (not is_valid(8)) {
        //         Debug.trap("BitReader.peekByte: out of bounds");
        //     };

        //     let nbits = bitSize();

        //     if (nbits < 8) {
        //         Nat8.fromNat(bitbuffer.getBits(offset, nbits));
        //     } else {
        //         BitBuffer.getByte(bitbuffer, offset);
        //     };

        // };

        // public func readByte() : Nat8 {
        //     let byte = peekByte();

        //     offset += 8;
        //     byte;
        // };

        // public func peekBytes(nbytes : Nat) : [Nat8] {
        //     let pos = getPosition();

        //     let res = Array.tabulate(
        //         nbytes,
        //         func(i : Nat) : Nat8 {
        //             readByte();
        //         },
        //     );

        //     setPosition(pos);
        //     res;
        // };

        // public func readBytes(nbytes : Nat) : [Nat8] {
        //     let min_bytes = Nat.min(nbytes, byteSize());

        //     Array.tabulate(
        //         min_bytes,
        //         func(i : Nat) : Nat8 {
        //             readByte();
        //         },
        //     );
        // };

        // public func readNat16()

        // public func getPosition() : Nat { offset };
        // public func setPosition(pos : Nat) { offset := pos };
        // public func resetPosition() { offset := 0 };

        // public func clearRead() {
        //     bitbuffer.dropBits(offset);
        //     offset := 0;
        // };

        // public func clear() {
        //     offset := 0;
        //     tailBits := 0;
        //     bitbuffer.clear();
        // };

        // public func bitSize() : Nat {
        //     if (tailBits + offset < (bitbuffer.bitSize() : Nat)) {
        //         (bitbuffer.bitSize() - offset - tailBits : Nat);
        //     } else {
        //         0;
        //     };
        // };

        // public func byteSizeExact() : Nat {
        //     bitSize() / 8;
        // };

        // public func byteSize() : Nat {
        //     (bitSize() + 8 - 1) / 8;
        // };

        // public func byteAlign() {
        //     let size = bitSize();

        //     if (size % 8 != 0) {
        //         offset += (size % 8);
        //     };
        // };

        // public func addBytes(bytes : [Nat8]) {
        //     BitBuffer.addBytes(bitbuffer, bytes);
        // };

        // public func hideTailBits(n : Nat) {
        //     tailBits := n;
        // };

        // public func hiddenTailBits() : Nat {
        //     tailBits;
        // };

        // public func showTailBits() {
        //     tailBits := 0;
        // };
    };

    // public func fromBytes(bytes : [Nat8]) : BitReader {
    //     let reader = BitReader(?(bytes.size() * 8));
    //     reader.addBytes(bytes);
    //     reader;
    // };
};
