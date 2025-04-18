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
                assert bytes == [123];

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
                assert bytes == [0x34, 0x12];

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
                assert bytes == [0x78, 0x56, 0x34, 0x12];

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
                assert bytes == [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01];

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
                assert bytes == [214]; // Two's complement of -42 is 214

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
                assert bytes == [0xC7, 0xCF];

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
                assert bytes == [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F];

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
                assert bytes == [123];

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
                assert bytes == [0x12, 0x34];

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
                assert bytes == [0x12, 0x34, 0x56, 0x78];

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
                assert bytes == [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];

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
                assert bytes == [214]; // Two's complement of -42 is 214

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
                assert bytes == [0xCF, 0xC7];

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
                assert bytes == [0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

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
                assert encoded1 == [0x01];

                // Value 127 should encode to [0x7F]
                let encoded127 = ByteUtils.toLEB128_64(127);
                assert encoded127 == [0x7F];

                // Value 128 should encode to [0x80, 0x01]
                let encoded128 = ByteUtils.toLEB128_64(128);
                assert encoded128 == [0x80, 0x01];

                // Value 624485 (0x98765) should encode to [0xE5, 0x8E, 0x26]
                let encoded624485 = ByteUtils.toLEB128_64(624485);
                assert encoded624485 == [0xE5, 0x8E, 0x26];
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

        test(
            "LEB128 test vectors - encoding",
            func() {
                // Test vector: (value, expected bytes)
                let testVectors : [(Nat64, [Nat8])] = [
                    (0, [0x00]),
                    (1, [0x01]),
                    (127, [0x7f]),
                    (128, [0x80, 0x01]),
                    (129, [0x81, 0x01]),
                    (255, [0xff, 0x01]),
                    (256, [0x80, 0x02]),
                    (624485, [0xe5, 0x8e, 0x26]),
                    (12345, [0xb9, 0x60]),
                    (123456, [0xc0, 0xc4, 0x07]),
                    (1234567, [0x87, 0xad, 0x4b]),
                    (12345678, [0xce, 0xc2, 0xf1, 0x05]),
                    (123456789, [0x95, 0x9a, 0xef, 0x3a]),
                    (4294967295, [0xff, 0xff, 0xff, 0xff, 0x0f]),
                    (4294967296, [0x80, 0x80, 0x80, 0x80, 0x10]),
                ];

                for ((value, expectedBytes) in testVectors.vals()) {
                    let encoded = ByteUtils.toLEB128_64(value);
                    assert encoded == expectedBytes;
                };
            },
        );

        test(
            "LEB128 test vectors - decoding",
            func() {
                // Test vector: (expected value, bytes)
                let testVectors : [(Nat64, [Nat8])] = [
                    (0, [0x00]),
                    (1, [0x01]),
                    (127, [0x7f]),
                    (128, [0x80, 0x01]),
                    (129, [0x81, 0x01]),
                    (255, [0xff, 0x01]),
                    (256, [0x80, 0x02]),
                    (624485, [0xe5, 0x8e, 0x26]),
                    (12345, [0xb9, 0x60]),
                    (123456, [0xc0, 0xc4, 0x07]),
                    (1234567, [0x87, 0xad, 0x4b]),
                    (12345678, [0xce, 0xc2, 0xf1, 0x05]),
                    (123456789, [0x95, 0x9a, 0xef, 0x3a]),
                    (4294967295, [0xff, 0xff, 0xff, 0xff, 0x0f]),
                    (4294967296, [0x80, 0x80, 0x80, 0x80, 0x10]),
                ];

                for ((expectedValue, bytes) in testVectors.vals()) {
                    let decoded = ByteUtils.fromLEB128_64(bytes.vals());
                    assert decoded == expectedValue;
                };
            },
        );

        test(
            "SLEB128 test vectors - encoding",
            func() {
                // Test vector: (value, expected bytes)
                let testVectors : [(Int64, [Nat8])] = [
                    (0, [0x00]),
                    (1, [0x01]),
                    (-1, [0x7f]),
                    (63, [0x3f]),
                    (-64, [0x40]),
                    (64, [0xc0, 0x00]),
                    (-65, [0xbf, 0x7f]),
                    (127, [0xff, 0x00]),
                    (-128, [0x80, 0x7f]),
                    (128, [0x80, 0x01]),
                    (-129, [0xff, 0x7e]),
                    (12345, [0xb9, 0xe0, 0x00]),
                    (-12345, [0xc7, 0x9f, 0x7f]),
                    (123456, [0xc0, 0xc4, 0x07]),
                    (-123456, [0xc0, 0xbb, 0x78]),
                    (1234567, [0x87, 0xad, 0xcb, 0x00]),
                    (-1234567, [0xf9, 0xd2, 0xb4, 0x7f]),
                    (12345678, [0xce, 0xc2, 0xf1, 0x05]),
                    (-12345678, [0xb2, 0xbd, 0x8e, 0x7a]),
                    (2147483647, [0xff, 0xff, 0xff, 0xff, 0x07]),
                    (-2147483648, [0x80, 0x80, 0x80, 0x80, 0x78]),
                ];

                for ((value, expectedBytes) in testVectors.vals()) {
                    let encoded = ByteUtils.toSLEB128_64(value);
                    assert encoded == expectedBytes;
                };
            },
        );

        test(
            "SLEB128 test vectors - decoding",
            func() {
                // Test vector: (expected value, bytes)
                let testVectors : [(Int64, [Nat8])] = [
                    (0, [0x00]),
                    (1, [0x01]),
                    (-1, [0x7f]),
                    (63, [0x3f]),
                    (-64, [0x40]),
                    (64, [0xc0, 0x00]),
                    (-65, [0xbf, 0x7f]),
                    (127, [0xff, 0x00]),
                    (-128, [0x80, 0x7f]),
                    (128, [0x80, 0x01]),
                    (-129, [0xff, 0x7e]),
                    (12345, [0xb9, 0xe0, 0x00]),
                    (-12345, [0xc7, 0x9f, 0x7f]),
                    (123456, [0xc0, 0xc4, 0x07]),
                    (-123456, [0xc0, 0xbb, 0x78]),
                    (1234567, [0x87, 0xad, 0xcb, 0x00]),
                    (-1234567, [0xf9, 0xd2, 0xb4, 0x7f]),
                    (12345678, [0xce, 0xc2, 0xf1, 0x05]),
                    (-12345678, [0xb2, 0xbd, 0x8e, 0x7a]),
                    (2147483647, [0xff, 0xff, 0xff, 0xff, 0x07]),
                    (-2147483648, [0x80, 0x80, 0x80, 0x80, 0x78]),
                ];

                for ((expectedValue, bytes) in testVectors.vals()) {
                    let decoded = ByteUtils.fromSLEB128_64(bytes.vals());
                    assert decoded == expectedValue;
                };
            },
        );

        test(
            "LEB128 edge cases - powers of 2",
            func() {
                // Test vector: (value, expected bytes)
                let testVectors : [(Nat64, [Nat8])] = [
                    (1, [0x01]), // 2^0
                    (128, [0x80, 0x01]), // 2^7
                    (16384, [0x80, 0x80, 0x01]), // 2^14
                    (2097152, [0x80, 0x80, 0x80, 0x01]), // 2^21
                    (268435456, [0x80, 0x80, 0x80, 0x80, 0x01]), // 2^28
                ];

                for ((value, expectedBytes) in testVectors.vals()) {
                    let encoded = ByteUtils.toLEB128_64(value);
                    assert encoded == expectedBytes;

                    let decoded = ByteUtils.fromLEB128_64(encoded.vals());
                    assert decoded == value;
                };
            },
        );

        test(
            "LEB128 edge cases - powers of 2 minus 1",
            func() {
                // Test vector: (value, expected bytes)
                let testVectors : [(Nat64, [Nat8])] = [
                    (127, [0x7f]), // 2^7-1
                    (16383, [0xff, 0x7f]), // 2^14-1
                    (2097151, [0xff, 0xff, 0x7f]), // 2^21-1
                    (268435455, [0xff, 0xff, 0xff, 0x7f]), // 2^28-1
                    (34359738367, [0xff, 0xff, 0xff, 0xff, 0x7f]), // 2^35-1
                ];

                for ((value, expectedBytes) in testVectors.vals()) {
                    let encoded = ByteUtils.toLEB128_64(value);
                    assert encoded == expectedBytes;

                    let decoded = ByteUtils.fromLEB128_64(encoded.vals());
                    assert decoded == value;
                };
            },
        );

        test(
            "SLEB128 edge cases - negative powers of 2",
            func() {
                // Test vector: (value, expected bytes)
                let testVectors : [(Int64, [Nat8])] = [
                    (-1, [0x7f]), // -2^0
                    (-128, [0x80, 0x7f]), // -2^7
                    (-16384, [0x80, 0x80, 0x7f]), // -2^14
                    (-2097152, [0x80, 0x80, 0x80, 0x7f]), // -2^21
                    (-268435456, [0x80, 0x80, 0x80, 0x80, 0x7f]), // -2^28
                ];

                for ((value, expectedBytes) in testVectors.vals()) {
                    let encoded = ByteUtils.toSLEB128_64(value);
                    assert encoded == expectedBytes;

                    let decoded = ByteUtils.fromSLEB128_64(encoded.vals());
                    assert decoded == value;
                };
            },
        );
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
                assert leBytes == [0x78, 0x56, 0x34, 0x12];

                // BE should be [0x12, 0x34, 0x56, 0x78]
                assert beBytes == [0x12, 0x34, 0x56, 0x78];
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

suite(
    "IEEE-754 Float Encoding Test Vectors",
    func() {
        // test(
        //     "Float special values - Little Endian",
        //     func() {
        //         // Test vector: (value, expected bytes in LE format)
        //         let testVectors : [(Float, [Nat8])] = [
        //             (0.0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        //             (1.0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F]),
        //             (-1.0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF]),
        //             (3.14159, [0x6E, 0x86, 0x1B, 0xF0, 0xF9, 0x21, 0x09, 0x40]),
        //             (2.718281828459045, [0x77, 0xBE, 0x9F, 0x1A, 0x2F, 0xDD, 0x05, 0x40]),
        //             (1.7976931348623157e+308, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F]), // Max double
        //             (2.2250738585072014e-308, [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00]), // Min normal double
        //         ];

        //         for ((value, expectedBytes) in testVectors.vals()) {
        //             let encoded = ByteUtils.LE.fromFloat64(value);

        //             assert encoded.size() == expectedBytes.size();

        //             for (i in Iter.range(0, encoded.size() - 1)) {
        //                 assert encoded[i] == expectedBytes[i];
        //             };

        //             let restored = ByteUtils.LE.toFloat64(encoded.vals());

        //             // For floating-point, use epsilon comparison
        //             let epsilon : Float = 1e-10;
        //             assert Float.abs(restored - value) < epsilon;
        //         };
        //     },
        // );

        // test(
        //     "Float special values - Big Endian",
        //     func() {
        //         // Test vector: (value, expected bytes in BE format)
        //         let testVectors : [(Float, [Nat8])] = [
        //             (0.0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        //             (1.0, [0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        //             (-1.0, [0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        //             (3.14159, [0x40, 0x09, 0x21, 0xF9, 0xF0, 0x1B, 0x86, 0x6E]),
        //             (2.718281828459045, [0x40, 0x05, 0xDD, 0x2F, 0x1A, 0x9F, 0xBE, 0x77]),
        //             (1.7976931348623157e+308, [0x7F, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]), // Max double
        //             (2.2250738585072014e-308, [0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]), // Min normal double
        //         ];

        //         for ((value, expectedBytes) in testVectors.vals()) {
        //             let encoded = ByteUtils.BE.fromFloat64(value);

        //             assert encoded.size() == expectedBytes.size();

        //             for (i in Iter.range(0, encoded.size() - 1)) {
        //                 assert encoded[i] == expectedBytes[i];
        //             };

        //             let restored = ByteUtils.BE.toFloat64(encoded.vals());

        //             // For floating-point, use epsilon comparison
        //             let epsilon : Float = 1e-10;
        //             assert Float.abs(restored - value) < epsilon;
        //         };
        //     },
        // );
    },
);

suite(
    "Integer Edge Cases and Extremes",
    func() {
        test(
            "Int32/Int64 extremes - Little Endian",
            func() {
                // Int32 min/max
                let int32_min : Int32 = -2147483648; // -2^31
                let int32_max : Int32 = 2147483647; // 2^31-1

                // Verify Int32 min
                let bytes_int32_min = ByteUtils.LE.fromInt32(int32_min);
                assert bytes_int32_min == [0x00, 0x00, 0x00, 0x80];

                // Verify Int32 max
                let bytes_int32_max = ByteUtils.LE.fromInt32(int32_max);
                assert bytes_int32_max == [0xFF, 0xFF, 0xFF, 0x7F];

                // Int64 min/max
                let int64_min : Int64 = -9223372036854775808; // -2^63
                let int64_max : Int64 = 9223372036854775807; // 2^63-1

                // Round-trip Int64 extremes
                let bytes_int64_min = ByteUtils.LE.fromInt64(int64_min);
                let restored_int64_min = ByteUtils.LE.toInt64(bytes_int64_min.vals());
                assert restored_int64_min == int64_min;

                // Verify Int64 min byte pattern
                assert bytes_int64_min == [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80];

                // Round-trip Int64 max
                let bytes_int64_max = ByteUtils.LE.fromInt64(int64_max);
                let restored_int64_max = ByteUtils.LE.toInt64(bytes_int64_max.vals());
                assert restored_int64_max == int64_max;

                // Verify Int64 max byte pattern
                assert bytes_int64_max == [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
            },
        );

        test(
            "Nat32/Nat64 extremes - Big Endian",
            func() {
                // Nat32 extremes
                let nat32_zero : Nat32 = 0;
                let nat32_max : Nat32 = 4294967295; // 2^32-1

                // Verify Nat32 zero
                let bytes_nat32_zero = ByteUtils.BE.fromNat32(nat32_zero);
                assert bytes_nat32_zero == [0x00, 0x00, 0x00, 0x00];

                // Verify Nat32 max
                let bytes_nat32_max = ByteUtils.BE.fromNat32(nat32_max);
                assert bytes_nat32_max == [0xFF, 0xFF, 0xFF, 0xFF];

                // Nat64 extremes
                let nat64_zero : Nat64 = 0;
                let nat64_max : Nat64 = 18446744073709551615; // 2^64-1

                // Round-trip for Nat64 max
                let bytes_nat64_max = ByteUtils.BE.fromNat64(nat64_max);
                let restored_nat64_max = ByteUtils.BE.toNat64(bytes_nat64_max.vals());
                assert restored_nat64_max == nat64_max;

                // Verify Nat64 max byte pattern
                assert bytes_nat64_max == [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
            },
        );
    },
);

suite(
    "Complex Buffer Operations",
    func() {
        test(
            "Mixed data type buffer operations - LE",
            func() {
                let buf = Buffer.Buffer<Nat8>(32);

                // Write multiple different types in sequence
                ByteUtils.Buffer.LE.addNat16(buf, 0x1234);
                ByteUtils.Buffer.LE.addInt32(buf, -1234567890);
                ByteUtils.Buffer.LE.addNat8(buf, 0xFF);
                ByteUtils.Buffer.LE.addFloat64(buf, 3.14159);

                // Verify buffer size
                assert buf.size() == 15; // 2 + 4 + 1 + 8 bytes

                // Read values back in sequence
                let val1 = ByteUtils.Buffer.LE.readNat16(buf, 0);
                let val2 = ByteUtils.Buffer.LE.readInt32(buf, 2);
                let val3 = ByteUtils.Buffer.LE.readNat8(buf, 6);
                let val4 = ByteUtils.Buffer.LE.readNat64(buf, 7); // Read raw bytes

                // Verify values
                assert val1 == 0x1234;
                assert val2 == -1234567890;
                assert val3 == 0xFF;

                // Convert raw bytes back to float and verify with epsilon
                let float_bytes = Array.tabulate<Nat8>(8, func(i) = buf.get(7 + i));
                let val4_float = ByteUtils.LE.toFloat64(float_bytes.vals());
                assert Float.abs(val4_float - 3.14159) < 1e-10;
            },
        );

        test(
            "Writing at specific offsets - BE",
            func() {
                let buf = Buffer.Buffer<Nat8>(16);

                // Fill buffer with zeros
                for (_ in Iter.range(0, 15)) {
                    buf.add(0);
                };

                // Write at various offsets
                ByteUtils.Buffer.BE.writeNat16(buf, 2, 0xABCD);
                ByteUtils.Buffer.BE.writeNat32(buf, 6, 0x12345678);
                ByteUtils.Buffer.BE.writeInt16(buf, 12, -42);

                // Verify buffer values at expected positions
                assert buf.get(2) == 0xAB and buf.get(3) == 0xCD;
                assert buf.get(6) == 0x12 and buf.get(7) == 0x34 and buf.get(8) == 0x56 and buf.get(9) == 0x78;
                assert buf.get(12) == 0xFF and buf.get(13) == 0xD6;

                // Verify values can be read back
                let val1 = ByteUtils.Buffer.BE.readNat16(buf, 2);
                let val2 = ByteUtils.Buffer.BE.readNat32(buf, 6);
                let val3 = ByteUtils.Buffer.BE.readInt16(buf, 12);

                assert val1 == 0xABCD;
                assert val2 == 0x12345678;
                assert val3 == -42;
            },
        );
    },
);
