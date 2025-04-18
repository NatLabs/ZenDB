import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Char "mo:base/Char";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";

import ZenDB "../src";
import Orchid "../src/Collection/Orchid";

let a = Orchid.Orchid.blobify.to_blob([#Nat(138)]);
let b = Orchid.Orchid.blobify.to_blob([#Nat(999_240)]);
Debug.print("138: " # debug_show (a));
Debug.print("999_240:  " # debug_show (b));

Debug.print("a > b: " # debug_show (a > b));

Debug.print("a < b: " # debug_show (("\aa\aa" : Blob) < ("\ab" : Blob)));

func get_prefix(blob : Blob, other : Blob) : Blob {
    let buffer = Buffer.Buffer<Nat8>(blob.size());

    label getting_prefix for ((a, b) in Itertools.zip(blob.vals(), other.vals())) {
        if (a != b) {
            break getting_prefix;
        };

        buffer.add(a);
    };

    Blob.fromArray(Buffer.toArray(buffer));
};

suite(
    "Orchid: serialized values are orderd correctly",
    func() {
        test(
            "Nat",
            func() {
                var a = Orchid.Orchid.blobify.to_blob([#Nat(138)]);
                var b = Orchid.Orchid.blobify.to_blob([#Nat(999_240)]);
                assert a < b;

                a := Orchid.Orchid.blobify.to_blob([#Nat(138)]);
                b := Orchid.Orchid.blobify.to_blob([#Nat(138)]);
                assert a == b;

                a := Orchid.Orchid.blobify.to_blob([#Nat(1234)]);
                b := Orchid.Orchid.blobify.to_blob([#Nat(321)]);
                assert a > b;

            },
        );

        test(
            "Int64",
            func() {
                var a = Orchid.Orchid.blobify.to_blob([#Int64(-138)]);
                var b = Orchid.Orchid.blobify.to_blob([#Int64(999_240)]);
                assert a < b;

                a := Orchid.Orchid.blobify.to_blob([#Int64(138)]);
                b := Orchid.Orchid.blobify.to_blob([#Int64(138)]);
                assert a == b;

                a := Orchid.Orchid.blobify.to_blob([#Int64(-138)]);
                b := Orchid.Orchid.blobify.to_blob([#Int64(138)]);
                assert a < b;

                a := Orchid.Orchid.blobify.to_blob([#Int64(1234)]);
                b := Orchid.Orchid.blobify.to_blob([#Int64(321)]);
                assert a > b;

                a := Orchid.Orchid.blobify.to_blob([#Int64(-1234)]);
                b := Orchid.Orchid.blobify.to_blob([#Int64(-321)]);
                assert a < b;
            },
        );

        test(
            "Float",
            func() {
                // Regular float comparisons
                var a = Orchid.Orchid.blobify.to_blob([#Float(1.23)]);
                var b = Orchid.Orchid.blobify.to_blob([#Float(4.56)]);
                assert a < b;

                a := Orchid.Orchid.blobify.to_blob([#Float(4.56)]);
                b := Orchid.Orchid.blobify.to_blob([#Float(4.56)]);
                assert a == b;

                a := Orchid.Orchid.blobify.to_blob([#Float(7.89)]);
                b := Orchid.Orchid.blobify.to_blob([#Float(4.56)]);

                assert a > b;

                // // Negative number comparisons
                // a := Orchid.Orchid.blobify.to_blob([#Float(-1.23)]);
                // b := Orchid.Orchid.blobify.to_blob([#Float(-4.56)]);
                // assert a > b;

                // // Positive vs negative
                // a := Orchid.Orchid.blobify.to_blob([#Float(-1.23)]);
                // b := Orchid.Orchid.blobify.to_blob([#Float(4.56)]);
                // assert a < b;

                // Zero handling
                a := Orchid.Orchid.blobify.to_blob([#Float(0.0)]);
                b := Orchid.Orchid.blobify.to_blob([#Float(-0.0)]);
                assert a >= b; // -0.0 and +0.0 may compare equal

                // Very small values
                a := Orchid.Orchid.blobify.to_blob([#Float(0.0000001)]);
                b := Orchid.Orchid.blobify.to_blob([#Float(0.00000001)]);
                assert a > b;

                // Very large values
                a := Orchid.Orchid.blobify.to_blob([#Float(1_000_000.0)]);
                b := Orchid.Orchid.blobify.to_blob([#Float(10_000_000.0)]);
                assert a < b;

                // // Special values
                // a := Orchid.Orchid.blobify.to_blob([#Float(0.0 / 0.0)]); // NaN
                // b := Orchid.Orchid.blobify.to_blob([#Float(1.0 / 0.0)]); // +Infinity
                // assert a != b;

                // a := Orchid.Orchid.blobify.to_blob([#Float(1.0 / 0.0)]); // +Infinity
                // b := Orchid.Orchid.blobify.to_blob([#Float(-1.0 / 0.0)]); // -Infinity
                // assert a > b;

                // Subnormal numbers
                a := Orchid.Orchid.blobify.to_blob([#Float(1.0e-308)]);
                b := Orchid.Orchid.blobify.to_blob([#Float(1.0e-309)]);
                assert a > b;
            },
        );
    },
);

suite(
    "Orchid: maintains prefix after serialization for better key compression",
    func() {
        test(
            "Blob",
            func() {
                let a = Orchid.Orchid.blobify.to_blob([#Blob("this might be orchid")]);
                let b = Orchid.Orchid.blobify.to_blob([#Blob("this might be candid")]);

                let prefix_bytes = get_prefix(a, b);
                let ?prefix = Text.decodeUtf8(prefix_bytes);
                Debug.print("prefix: " # debug_show (prefix));

                // assert Text.endsWith(prefix, #text("this might be "));

            },
        );

        test(
            "Int64",
            func() {
                var a = Orchid.Orchid.blobify.to_blob([#Int64(1234)]);
                var b = Orchid.Orchid.blobify.to_blob([#Int64(321)]);

                var prefix_bytes = get_prefix(a, b);
                assert prefix_bytes.size() == 7;

                a := Orchid.Orchid.blobify.to_blob([#Int64(-1234)]);
                b := Orchid.Orchid.blobify.to_blob([#Int64(-321)]);
                prefix_bytes := get_prefix(a, b);
                assert prefix_bytes.size() == 7;

                a := Orchid.Orchid.blobify.to_blob([#Int64(-1234)]);
                b := Orchid.Orchid.blobify.to_blob([#Int64(321)]);

                prefix_bytes := get_prefix(a, b);
                Debug.print("prefix_bytes: " # debug_show (prefix_bytes));
            },
        );

        test(
            "Float",
            func() {
                // // Test prefix preservation for floats with same exponent
                // var a = Orchid.Orchid.blobify.to_blob([#Float(123.456)]);
                // var b = Orchid.Orchid.blobify.to_blob([#Float(123.789)]);

                // var prefix_bytes = get_prefix(a, b);
                // assert prefix_bytes.size() > 0;

                // // Test prefix preservation for floats with different signs
                // a := Orchid.Orchid.blobify.to_blob([#Float(123.456)]);
                // b := Orchid.Orchid.blobify.to_blob([#Float(-123.456)]);

                // prefix_bytes := get_prefix(a, b);
                // Debug.print("Float +/- prefix_bytes: " # debug_show (prefix_bytes));

                // // Test prefix preservation for special values
                // a := Orchid.Orchid.blobify.to_blob([#Float(1.0 / 0.0)]); // +Infinity
                // b := Orchid.Orchid.blobify.to_blob([#Float(-1.0 / 0.0)]); // -Infinity

                // prefix_bytes := get_prefix(a, b);
                // Debug.print("Float inf prefix_bytes: " # debug_show (prefix_bytes));

                // // Test scientific notation values
                // a := Orchid.Orchid.blobify.to_blob([#Float(1.23e20)]);
                // b := Orchid.Orchid.blobify.to_blob([#Float(1.23e19)]);

                // prefix_bytes := get_prefix(a, b);
                // Debug.print("Float scientific prefix_bytes: " # debug_show (prefix_bytes));
            },
        );
    },
);

suite(
    "Edge Cases",
    func() {

        test(
            "Composite: [Blob, Nat]",
            func() {
                let a : [ZenDB.Types.CandidQuery] = [#Blob(Blob.fromArray([0xfe, 0x32, 0x00])), #Nat8(244)];
                let b : [ZenDB.Types.CandidQuery] = [#Blob(Blob.fromArray([0xfe, 0x32, 0x00, 0x01])), #Nat(1234)];

                let a_blob = Orchid.Orchid.blobify.to_blob(a);
                let b_blob = Orchid.Orchid.blobify.to_blob(b);

                Debug.print("a < b: " # debug_show (a_blob, b_blob, a_blob < b_blob));

                assert a_blob < b_blob;
            },
        );

        test(
            "Composite: [Text, Nat]",
            func() {

                let text_1 = Text.fromIter(
                    [
                        Char.fromNat32(0xFE),
                        Char.fromNat32(0x00),
                        Char.fromNat32(0xEE),
                    ].vals()
                );

                let text_2 = Text.fromIter(
                    [
                        Char.fromNat32(0xFE),
                        Char.fromNat32(0x00),
                        Char.fromNat32(0xEE),
                        Char.fromNat32(0x00),
                    ].vals()
                );

                let a : [ZenDB.Types.CandidQuery] = [#Text(text_1), #Nat8(244)];
                let b : [ZenDB.Types.CandidQuery] = [#Text(text_2), #Nat(1234)];
                let a_blob = Orchid.Orchid.blobify.to_blob(a);
                let b_blob = Orchid.Orchid.blobify.to_blob(b);

                Debug.print("a < b: " # debug_show (a_blob, b_blob, a_blob < b_blob));

                assert a_blob < b_blob;
            },
        )

    },
);
