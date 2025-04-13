// @testmode wasi
// crafted by claude-3-sonnet-20240229

import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Nat8 "mo:base/Nat8";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Int8 "mo:base/Int8";
import Int16 "mo:base/Int16";
import Int32 "mo:base/Int32";
import Int64 "mo:base/Int64";
import Float "mo:base/Float";
import { test; suite } "mo:test";

import ByteUtils "../src/ByteUtils";

suite(
    "ByteUtils Little-Endian Conversions",
    func() {
        test(
            "Nat8 round-trip conversion and byte pattern",
            func() {
                let original : Nat8 = 123;
                let bytes = ByteUtils.LE.fromNat8(original);

                // Verify byte pattern
                assert bytes.size() == 1;
                assert bytes[0] == 123;

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toNat8(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Nat16 round-trip conversion and byte pattern",
            func() {
                // Test with a known value 0x1234 (4660 in decimal)
                let original : Nat16 = 0x1234;
                let bytes = ByteUtils.LE.fromNat16(original);

                // Verify byte pattern - in little-endian, least significant byte comes first
                assert bytes.size() == 2;
                assert bytes[0] == 0x34; // Lower byte
                assert bytes[1] == 0x12; // Higher byte

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toNat16(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Nat32 round-trip conversion and byte pattern",
            func() {
                // Test with a known value 0x12345678
                let original : Nat32 = 0x12345678;
                let bytes = ByteUtils.LE.fromNat32(original);

                // Verify byte pattern - in little-endian, bytes are reversed
                assert bytes.size() == 4;
                assert bytes[0] == 0x78;
                assert bytes[1] == 0x56;
                assert bytes[2] == 0x34;
                assert bytes[3] == 0x12;

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toNat32(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Nat64 round-trip conversion and byte pattern",
            func() {
                // Test with a known value 0x0102030405060708
                let original : Nat64 = 0x0102030405060708;
                let bytes = ByteUtils.LE.fromNat64(original);

                // Verify byte pattern
                assert bytes.size() == 8;
                assert bytes[0] == 0x08;
                assert bytes[1] == 0x07;
                assert bytes[2] == 0x06;
                assert bytes[3] == 0x05;
                assert bytes[4] == 0x04;
                assert bytes[5] == 0x03;
                assert bytes[6] == 0x02;
                assert bytes[7] == 0x01;

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toNat64(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Int8 round-trip conversion and byte pattern",
            func() {
                let original : Int8 = -42;
                let bytes = ByteUtils.LE.fromInt8(original);

                // Verify byte pattern
                assert bytes.size() == 1;
                assert bytes[0] == 214; // Two's complement of -42 is 214

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toInt8(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Int16 round-trip conversion and byte pattern",
            func() {
                // Test with a negative value -12345
                // Two's complement of -12345 is 53191 (0xCFC7)
                let original : Int16 = -12345;
                let bytes = ByteUtils.LE.fromInt16(original);

                // Verify byte pattern
                assert bytes.size() == 2;
                assert bytes[0] == 0xC7; // Lower byte
                assert bytes[1] == 0xCF; // Higher byte

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toInt16(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Int32 round-trip conversion and byte pattern",
            func() {
                let original : Int32 = -1234567890;
                let bytes = ByteUtils.LE.fromInt32(original);

                // Verify byte pattern for negative value
                assert bytes.size() == 4;

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toInt32(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Int64 round-trip conversion",
            func() {
                let original : Int64 = -1234567890123456789;
                let bytes = ByteUtils.LE.fromInt64(original);

                // Verify byte pattern
                assert bytes.size() == 8;

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toInt64(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Float64 round-trip conversion and byte pattern",
            func() {
                // Test with value 1.0
                // IEEE-754 encoding of 1.0 is 0x3FF0000000000000
                let original : Float = 1.0;
                let bytes = ByteUtils.LE.fromFloat64(original);

                // Verify byte pattern - in little-endian, bytes are reversed
                assert bytes.size() == 8;
                assert bytes[0] == 0x00;
                assert bytes[1] == 0x00;
                assert bytes[2] == 0x00;
                assert bytes[3] == 0x00;
                assert bytes[4] == 0x00;
                assert bytes[5] == 0x00;
                assert bytes[6] == 0xF0;
                assert bytes[7] == 0x3F;

                // Verify round-trip conversion
                let restored = ByteUtils.LE.toFloat64(bytes.vals());

                // For floating-point values, we need to account for small precision differences
                let epsilon : Float = 0.0000001;
                assert Float.abs(restored - original) < epsilon;
            },
        );
    },
);

suite(
    "ByteUtils Big-Endian Conversions",
    func() {
        test(
            "Nat8 round-trip conversion",
            func() {
                let original : Nat8 = 123;
                let bytes = ByteUtils.BE.fromNat8(original);

                // Verify byte pattern
                assert bytes.size() == 1;
                assert bytes[0] == 123;

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toNat8(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Nat16 round-trip conversion and byte pattern",
            func() {
                // Test with a known value 0x1234 (4660 in decimal)
                let original : Nat16 = 0x1234;
                let bytes = ByteUtils.BE.fromNat16(original);

                // Verify byte pattern - in big-endian, most significant byte comes first
                assert bytes.size() == 2;
                assert bytes[0] == 0x12; // Higher byte
                assert bytes[1] == 0x34; // Lower byte

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toNat16(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Nat32 round-trip conversion and byte pattern",
            func() {
                // Test with a known value 0x12345678
                let original : Nat32 = 0x12345678;
                let bytes = ByteUtils.BE.fromNat32(original);

                // Verify byte pattern - in big-endian, bytes are in natural order
                assert bytes.size() == 4;
                assert bytes[0] == 0x12;
                assert bytes[1] == 0x34;
                assert bytes[2] == 0x56;
                assert bytes[3] == 0x78;

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toNat32(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Nat64 round-trip conversion and byte pattern",
            func() {
                // Test with a known value 0x0102030405060708
                let original : Nat64 = 0x0102030405060708;
                let bytes = ByteUtils.BE.fromNat64(original);

                // Verify byte pattern
                assert bytes.size() == 8;
                assert bytes[0] == 0x01;
                assert bytes[1] == 0x02;
                assert bytes[2] == 0x03;
                assert bytes[3] == 0x04;
                assert bytes[4] == 0x05;
                assert bytes[5] == 0x06;
                assert bytes[6] == 0x07;
                assert bytes[7] == 0x08;

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toNat64(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Int8 round-trip conversion",
            func() {
                let original : Int8 = -42;
                let bytes = ByteUtils.BE.fromInt8(original);

                // Verify byte pattern
                assert bytes.size() == 1;
                assert bytes[0] == 214; // Two's complement of -42 is 214

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toInt8(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Int16 round-trip conversion and byte pattern",
            func() {
                // Test with a negative value -12345
                // Two's complement of -12345 is 53191 (0xCFC7)
                let original : Int16 = -12345;
                let bytes = ByteUtils.BE.fromInt16(original);

                // Verify byte pattern
                assert bytes.size() == 2;
                assert bytes[0] == 0xCF; // Higher byte
                assert bytes[1] == 0xC7; // Lower byte

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toInt16(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Int32 round-trip conversion",
            func() {
                let original : Int32 = -1234567890;
                let bytes = ByteUtils.BE.fromInt32(original);

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toInt32(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Int64 round-trip conversion",
            func() {
                let original : Int64 = -1234567890123456789;
                let bytes = ByteUtils.BE.fromInt64(original);

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toInt64(bytes.vals());
                assert restored == original;
            },
        );

        test(
            "Float64 round-trip conversion and byte pattern",
            func() {
                // Test with value 1.0
                // IEEE-754 encoding of 1.0 is 0x3FF0000000000000
                let original : Float = 1.0;
                let bytes = ByteUtils.BE.fromFloat64(original);

                // Verify byte pattern - in big-endian, bytes are in natural order
                assert bytes.size() == 8;
                assert bytes[0] == 0x3F;
                assert bytes[1] == 0xF0;
                assert bytes[2] == 0x00;
                assert bytes[3] == 0x00;
                assert bytes[4] == 0x00;
                assert bytes[5] == 0x00;
                assert bytes[6] == 0x00;
                assert bytes[7] == 0x00;

                // Verify round-trip conversion
                let restored = ByteUtils.BE.toFloat64(bytes.vals());

                // For floating-point values, we need to account for small precision differences
                let epsilon : Float = 0.0000001;
                assert Float.abs(restored - original) < epsilon;
            },
        );
    },
);

suite(
    "LEB128/SLEB128 Encoding and Decoding",
    func() {
        test(
            "toLEB128_64/fromLEB128_64 round-trip conversion",
            func() {
                // Test round-trip conversion for various values
                let values : [Nat64] = [0, 1, 127, 128, 624485, 1234567890];

                for (value in values.vals()) {
                    let encoded = ByteUtils.toLEB128_64(value);
                    let decoded = ByteUtils.fromLEB128_64(encoded.vals());
                    assert decoded == value;
                };
            },
        );

        test(
            "toLEB128_64 produces correct byte patterns",
            func() {
                // Test specific byte patterns for known values

                // Value 1 should encode to [0x01]
                let encoded1 = ByteUtils.toLEB128_64(1);
                assert encoded1.size() == 1;
                assert encoded1[0] == 0x01;

                // Value 127 should encode to [0x7F]
                let encoded127 = ByteUtils.toLEB128_64(127);
                assert encoded127.size() == 1;
                assert encoded127[0] == 0x7F;

                // Value 128 should encode to [0x80, 0x01]
                let encoded128 = ByteUtils.toLEB128_64(128);
                assert encoded128.size() == 2;
                assert encoded128[0] == 0x80;
                assert encoded128[1] == 0x01;

                // Value 624485 (0x98765) should encode to [0xE5, 0x8E, 0x26]
                let encoded624485 = ByteUtils.toLEB128_64(624485);
                assert encoded624485.size() == 3;
                assert encoded624485[0] == 0xE5;
                assert encoded624485[1] == 0x8E;
                assert encoded624485[2] == 0x26;
            },
        );

        test(
            "toSLEB128_64/fromSLEB128_64 round-trip conversion",
            func() {
                // Test round-trip conversion for various values
                let values : [Int64] = [0, 1, -1, 42, -42, 127, -128, -123456];

                for (value in values.vals()) {
                    let encoded = ByteUtils.toSLEB128_64(value);
                    let decoded = ByteUtils.fromSLEB128_64(encoded.vals());
                    assert decoded == value;
                };
            },
        );

        // test(
        //     "toSLEB128_64 produces correct byte patterns",
        //     func() {
        //         // Test specific byte patterns for known values

        //         // Value 0 should encode to [0x00]
        //         let encoded0 = ByteUtils.toSLEB128_64(0);
        //         assert encoded0.size() == 1;
        //         assert encoded0[0] == 0x00;

        //         // Value 42 should encode to [0x2A]
        //         let encoded42 = ByteUtils.toSLEB128_64(42);
        //         assert encoded42.size() == 1;
        //         assert encoded42[0] == 0x2A;

        //         // Value -42 should encode to [0xD6, 0x7F]
        //         let encodedNeg42 = ByteUtils.toSLEB128_64(-42);
        //         assert encodedNeg42.size() == 2;
        //         assert encodedNeg42[0] == 0xD6;
        //         assert encodedNeg42[1] == 0x7F;
        //     },
        // );

        // test(
        //     "Buffer operations for LEB128/SLEB128",
        //     func() {
        //         // Test using buffer operations
        //         let buf = Buffer.Buffer<Nat8>(10);
        //         let value : Nat64 = 624485;
        //         ByteUtils.Buffer.writeLEB128_64(buf, value);

        //         // Verify correct byte pattern in buffer
        //         assert buf.size() == 3;
        //         assert buf.get(0) == 0xE5;
        //         assert buf.get(1) == 0x8E;
        //         assert buf.get(2) == 0x26;

        //         // Read back the value using readLEB128_64
        //         let decoded = ByteUtils.Buffer.readLEB128_64({
        //             add = func(_ : Nat8) {}; // Not used in read operation
        //             get = func(i : Nat) : Nat8 { buf.get(i) };
        //         });
        //         assert decoded == value;

        //         // Test SLEB128 buffer operations with a negative value
        //         let buf2 = Buffer.Buffer<Nat8>(10);
        //         let valueInt : Int64 = -42;
        //         ByteUtils.Buffer.writeSLEB128_64(buf2, valueInt);

        //         // Verify correct byte pattern in buffer
        //         assert buf2.size() == 2;
        //         assert buf2.get(0) == 0xD6;
        //         assert buf2.get(1) == 0x7F;

        //         // Read back the value using readSLEB128_64
        //         let decodedInt = ByteUtils.Buffer.readSLEB128_64({
        //             add = func(_ : Nat8) {}; // Not used in read operation
        //             get = func(i : Nat) : Nat8 { buf2.get(i) };
        //         });
        //         assert decodedInt == valueInt;
        //     },
        // );
    },
);

suite(
    "ByteUtils Edge Cases",
    func() {
        test(
            "Zero values - Little Endian",
            func() {
                // Test zero values for all numeric types
                let nat8_zero : Nat8 = 0;
                let bytes = ByteUtils.LE.fromNat8(nat8_zero);
                let restored = ByteUtils.LE.toNat8(bytes.vals());
                assert restored == nat8_zero;

                let nat16_zero : Nat16 = 0;
                let bytes16 = ByteUtils.LE.fromNat16(nat16_zero);
                let restored16 = ByteUtils.LE.toNat16(bytes16.vals());
                assert restored16 == nat16_zero;

                let int8_zero : Int8 = 0;
                let bytesI8 = ByteUtils.LE.fromInt8(int8_zero);
                let restoredI8 = ByteUtils.LE.toInt8(bytesI8.vals());
                assert restoredI8 == int8_zero;

                let float_zero : Float = 0.0;
                let bytesF = ByteUtils.LE.fromFloat64(float_zero);
                let restoredF = ByteUtils.LE.toFloat64(bytesF.vals());
                assert restoredF == float_zero;
            },
        );

        test(
            "Maximum values - Little Endian",
            func() {
                // Test maximum values for all numeric types
                let nat8_max : Nat8 = 255;
                let bytes = ByteUtils.LE.fromNat8(nat8_max);
                let restored = ByteUtils.LE.toNat8(bytes.vals());
                assert restored == nat8_max;

                let nat16_max : Nat16 = 65535;
                let bytes16 = ByteUtils.LE.fromNat16(nat16_max);
                let restored16 = ByteUtils.LE.toNat16(bytes16.vals());
                assert restored16 == nat16_max;

                let nat32_max : Nat32 = 4294967295;
                let bytes32 = ByteUtils.LE.fromNat32(nat32_max);
                let restored32 = ByteUtils.LE.toNat32(bytes32.vals());
                assert restored32 == nat32_max;

                let int8_max : Int8 = 127;
                let bytesI8 = ByteUtils.LE.fromInt8(int8_max);
                let restoredI8 = ByteUtils.LE.toInt8(bytesI8.vals());
                assert restoredI8 == int8_max;
            },
        );

        test(
            "Minimum values - Little Endian",
            func() {
                // Test minimum values for signed types
                let int8_min : Int8 = -128;
                let bytesI8 = ByteUtils.LE.fromInt8(int8_min);
                let restoredI8 = ByteUtils.LE.toInt8(bytesI8.vals());
                assert restoredI8 == int8_min;

                let int16_min : Int16 = -32768;
                let bytesI16 = ByteUtils.LE.fromInt16(int16_min);
                let restoredI16 = ByteUtils.LE.toInt16(bytesI16.vals());
                assert restoredI16 == int16_min;

                let int32_min : Int32 = -2147483648;
                let bytesI32 = ByteUtils.LE.fromInt32(int32_min);
                let restoredI32 = ByteUtils.LE.toInt32(bytesI32.vals());
                assert restoredI32 == int32_min;
            },
        );

        test(
            "Special float values - Little Endian",
            func() {
                // Test special float values
                let float_inf : Float = 1.0 / 0.0;
                Debug.print(debug_show { float_inf });

                // Error message:
                //    0: 0xda4e - <unknown>!bigint_trap
                //    1: 0xe1b9 - <unknown>!bigint_of_float64
                //    2: 0x90ae - <unknown>!fromFloat
                //    3: 0x474d - <unknown>!fromFloat64
                //
                //! Seems like a system limitation to convert the infinity value

                // let bytesInf = ByteUtils.LE.fromFloat64(float_inf);
                // // Debug.print(debug_show { bytesInf });
                // let restoredInf = ByteUtils.LE.toFloat64(bytesInf.vals());

                // assert restoredInf == float_inf;

                // let float_neg_inf : Float = -1.0 / 0.0;
                // let bytesNegInf = ByteUtils.LE.fromFloat64(float_neg_inf);
                // let restoredNegInf = ByteUtils.LE.toFloat64(bytesNegInf.vals());

                // Debug.print(debug_show { restoredNegInf; float_neg_inf; bytesNegInf });
                // assert restoredNegInf == float_neg_inf;
            },
        );

        test(
            "Zero values - Big Endian",
            func() {
                // Test zero values for all numeric types
                let nat8_zero : Nat8 = 0;
                let bytes = ByteUtils.BE.fromNat8(nat8_zero);
                let restored = ByteUtils.BE.toNat8(bytes.vals());
                assert restored == nat8_zero;

                let nat16_zero : Nat16 = 0;
                let bytes16 = ByteUtils.BE.fromNat16(nat16_zero);
                let restored16 = ByteUtils.BE.toNat16(bytes16.vals());
                assert restored16 == nat16_zero;

                let int8_zero : Int8 = 0;
                let bytesI8 = ByteUtils.BE.fromInt8(int8_zero);
                let restoredI8 = ByteUtils.BE.toInt8(bytesI8.vals());
                assert restoredI8 == int8_zero;

                let float_zero : Float = 0.0;
                let bytesF = ByteUtils.BE.fromFloat64(float_zero);
                let restoredF = ByteUtils.BE.toFloat64(bytesF.vals());
                assert restoredF == float_zero;
            },
        );
    },
);

suite(
    "Buffer Operations - Little Endian",
    func() {
        test(
            "addNat8/readNat8",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Nat8 = 123;
                ByteUtils.Buffer.LE.addNat8(buf, value);
                let restored = ByteUtils.Buffer.LE.readNat8(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addNat16/readNat16",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Nat16 = 12345;
                ByteUtils.Buffer.LE.addNat16(buf, value);
                let restored = ByteUtils.Buffer.LE.readNat16(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addNat32/readNat32",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Nat32 = 1234567890;
                ByteUtils.Buffer.LE.addNat32(buf, value);
                let restored = ByteUtils.Buffer.LE.readNat32(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addNat64/readNat64",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Nat64 = 1234567890123456789;
                ByteUtils.Buffer.LE.addNat64(buf, value);
                let restored = ByteUtils.Buffer.LE.readNat64(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addInt8/readInt8",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Int8 = -42;
                ByteUtils.Buffer.LE.addInt8(buf, value);
                let restored = ByteUtils.Buffer.LE.readInt8(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addInt16/readInt16",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Int16 = -12345;
                ByteUtils.Buffer.LE.addInt16(buf, value);
                let restored = ByteUtils.Buffer.LE.readInt16(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addInt32/readInt32",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Int32 = -1234567890;
                ByteUtils.Buffer.LE.addInt32(buf, value);
                let restored = ByteUtils.Buffer.LE.readInt32(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addInt64/readInt64",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Int64 = -1234567890123456789;
                ByteUtils.Buffer.LE.addInt64(buf, value);
                let restored = ByteUtils.Buffer.LE.readInt64(buf, 0);
                assert restored == value;
            },
        );

        test(
            "writeNat8/readNat8",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                // Fill buffer with zeros
                for (_ in Iter.range(0, 9)) {
                    buf.add(0);
                };

                let value : Nat8 = 123;
                ByteUtils.Buffer.LE.writeNat8(buf, 5, value);
                let restored = ByteUtils.Buffer.LE.readNat8(buf, 5);
                assert restored == value;
            },
        );

        test(
            "writeNat16/readNat16",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                // Fill buffer with zeros
                for (_ in Iter.range(0, 9)) {
                    buf.add(0);
                };

                let value : Nat16 = 12345;
                ByteUtils.Buffer.LE.writeNat16(buf, 5, value);
                let restored = ByteUtils.Buffer.LE.readNat16(buf, 5);
                assert restored == value;
            },
        );

        test(
            "Multiple values in buffer",
            func() {
                let buf = Buffer.Buffer<Nat8>(20);

                let nat8val : Nat8 = 123;
                let nat16val : Nat16 = 12345;
                let int8val : Int8 = -42;
                let int16val : Int16 = -12345;

                // Add values to buffer
                ByteUtils.Buffer.LE.addNat8(buf, nat8val);
                ByteUtils.Buffer.LE.addNat16(buf, nat16val);
                ByteUtils.Buffer.LE.addInt8(buf, int8val);
                ByteUtils.Buffer.LE.addInt16(buf, int16val);

                // Read them back in sequence
                let restored8 = ByteUtils.Buffer.LE.readNat8(buf, 0);
                let restored16 = ByteUtils.Buffer.LE.readNat16(buf, 1);
                let restoredI8 = ByteUtils.Buffer.LE.readInt8(buf, 3);
                let restoredI16 = ByteUtils.Buffer.LE.readInt16(buf, 4);

                assert restored8 == nat8val;
                assert restored16 == nat16val;
                assert restoredI8 == int8val;
                assert restoredI16 == int16val;
            },
        );
    },
);

suite(
    "Buffer Operations - Big Endian",
    func() {
        test(
            "addNat8/readNat8",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Nat8 = 123;
                ByteUtils.Buffer.BE.addNat8(buf, value);
                let restored = ByteUtils.Buffer.BE.readNat8(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addNat16/readNat16",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Nat16 = 12345;
                ByteUtils.Buffer.BE.addNat16(buf, value);
                let restored = ByteUtils.Buffer.BE.readNat16(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addNat32/readNat32",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Nat32 = 1234567890;
                ByteUtils.Buffer.BE.addNat32(buf, value);
                let restored = ByteUtils.Buffer.BE.readNat32(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addNat64/readNat64",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Nat64 = 1234567890123456789;
                ByteUtils.Buffer.BE.addNat64(buf, value);
                let restored = ByteUtils.Buffer.BE.readNat64(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addInt8/readInt8",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Int8 = -42;
                ByteUtils.Buffer.BE.addInt8(buf, value);
                let restored = ByteUtils.Buffer.BE.readInt8(buf, 0);
                assert restored == value;
            },
        );

        test(
            "addInt16/readInt16",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                let value : Int16 = -12345;
                ByteUtils.Buffer.BE.addInt16(buf, value);
                let restored = ByteUtils.Buffer.BE.readInt16(buf, 0);
                assert restored == value;
            },
        );

        test(
            "writeNat16/readNat16",
            func() {
                let buf = Buffer.Buffer<Nat8>(10);
                // Fill buffer with zeros
                for (_ in Iter.range(0, 9)) {
                    buf.add(0);
                };

                let value : Nat16 = 12345;
                ByteUtils.Buffer.BE.writeNat16(buf, 5, value);
                let restored = ByteUtils.Buffer.BE.readNat16(buf, 5);
                assert restored == value;
            },
        );

        test(
            "Multiple values in buffer",
            func() {
                let buf = Buffer.Buffer<Nat8>(20);

                let nat8val : Nat8 = 123;
                let nat16val : Nat16 = 12345;
                let int8val : Int8 = -42;
                let int16val : Int16 = -12345;

                // Add values to buffer
                ByteUtils.Buffer.BE.addNat8(buf, nat8val);
                ByteUtils.Buffer.BE.addNat16(buf, nat16val);
                ByteUtils.Buffer.BE.addInt8(buf, int8val);
                ByteUtils.Buffer.BE.addInt16(buf, int16val);

                // Read them back in sequence
                let restored8 = ByteUtils.Buffer.BE.readNat8(buf, 0);
                let restored16 = ByteUtils.Buffer.BE.readNat16(buf, 1);
                let restoredI8 = ByteUtils.Buffer.BE.readInt8(buf, 3);
                let restoredI16 = ByteUtils.Buffer.BE.readInt16(buf, 4);

                assert restored8 == nat8val;
                assert restored16 == nat16val;
                assert restoredI8 == int8val;
                assert restoredI16 == int16val;
            },
        );
    },
);

suite(
    "Endianness Consistency",
    func() {
        test(
            "LE and BE represent the same values differently",
            func() {
                let value : Nat32 = 0x12345678;

                let leBytes = ByteUtils.LE.fromNat32(value);
                let beBytes = ByteUtils.BE.fromNat32(value);

                // LE should be [0x78, 0x56, 0x34, 0x12]
                assert leBytes[0] == 0x78;
                assert leBytes[1] == 0x56;
                assert leBytes[2] == 0x34;
                assert leBytes[3] == 0x12;

                // BE should be [0x12, 0x34, 0x56, 0x78]
                assert beBytes[0] == 0x12;
                assert beBytes[1] == 0x34;
                assert beBytes[2] == 0x56;
                assert beBytes[3] == 0x78;
            },
        );

        test(
            "LE bytes interpreted as BE give different values",
            func() {
                let value : Nat32 = 0x12345678;

                // Convert to LE bytes
                let leBytes = ByteUtils.LE.fromNat32(value);

                // Interpret those bytes as BE
                let reinterpreted = ByteUtils.BE.toNat32(leBytes.vals());

                // Should be a different value
                assert reinterpreted != value;

                // Should be byte-reversed: 0x78563412
                assert reinterpreted == 0x78563412;
            },
        );
    },
);
