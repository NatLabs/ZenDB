import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Orchid "../src/Collection/Orchid";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";

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
        )
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

                assert Text.endsWith(prefix, #text("this might be "));

            },
        );

        test(
            "Int64",
            func() {
                var a = Orchid.Orchid.blobify.to_blob([#Int64(1234)]);
                var b = Orchid.Orchid.blobify.to_blob([#Int64(321)]);

                var prefix_bytes = get_prefix(a, b);
                assert prefix_bytes.size() == 8;

                a := Orchid.Orchid.blobify.to_blob([#Int64(-1234)]);
                b := Orchid.Orchid.blobify.to_blob([#Int64(-321)]);
                prefix_bytes := get_prefix(a, b);
                assert prefix_bytes.size() == 8;

                a := Orchid.Orchid.blobify.to_blob([#Int64(-1234)]);
                b := Orchid.Orchid.blobify.to_blob([#Int64(321)]);

                prefix_bytes := get_prefix(a, b);
                Debug.print("prefix_bytes: " # debug_show (prefix_bytes));
            },
        );
    },
);
