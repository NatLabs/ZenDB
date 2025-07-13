// @testmode wasi
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Int8 "mo:base/Int8";
import Iter "mo:base/Iter";
import Int32 "mo:base/Int32";
import Int16 "mo:base/Int16";
import Int64 "mo:base/Int64";
import Float "mo:base/Float";
import Nat64 "mo:base/Nat64";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Int "mo:base/Int";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Option "mo:base/Option";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import PeekableIter "mo:itertools/PeekableIter";
import BpTree "mo:augmented-btrees/BpTree";
import Cmp "mo:augmented-btrees/Cmp";

import ZenDB "../../../src";
import Orchid "../../../src/Collection/Orchid";
import Fuzz "mo:fuzz";
import CandidMap "../../../src/CandidMap";
import SchemaMap "../../../src/Collection/SchemaMap";
import Schema "../../../src/Collection/Schema";
import Utils "../../../src/Utils";

let T = ZenDB.Types;

let fuzz = Fuzz.fromSeed(0x7eadbeef);

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

let limit = 1_000;

type MotokoTypes = {
    text : Text;
    nat : Nat;
    nat8 : Nat8;
    nat16 : Nat16;
    nat32 : Nat32;
    nat64 : Nat64;
    int : Int;
    int8 : Int8;
    int16 : Int16;
    int32 : Int32;
    int64 : Int64;
    float : Float;
    principal : Principal;
    blob : Blob;
    bool : Bool;
};

let MotokoTypesSchema = #Record([
    ("text", #Text),
    ("nat", #Nat),
    ("nat8", #Nat8),
    ("nat16", #Nat16),
    ("nat32", #Nat32),
    ("nat64", #Nat64),
    ("int", #Int),
    ("int8", #Int8),
    ("int16", #Int16),
    ("int32", #Int32),
    ("int64", #Int64),
    ("float", #Float),
    ("principal", #Principal),
    ("blob", #Blob),
    ("bool", #Bool),
]);
let schema_map = SchemaMap.new(MotokoTypesSchema);

let inputs = Buffer.Buffer<MotokoTypes>(limit);
let candid_maps = Buffer.Buffer<T.CandidMap>(limit);

let nat64_max = (2 ** 64 - 1);
let int64_max = (2 ** 63 - 1);
let int64_min = -(2 ** 63);

for (i in Itertools.range(0, limit)) {

    let record : MotokoTypes = {
        text = fuzz.text.randomAlphanumeric(
            fuzz.nat.randomRange(0, 20)
        );
        nat = fuzz.nat.randomRange(0, nat64_max);
        nat8 = fuzz.nat8.random();
        nat16 = fuzz.nat16.random();
        nat32 = fuzz.nat32.random();
        nat64 = fuzz.nat64.random();
        int = fuzz.int.randomRange(int64_min, int64_max);
        int8 = fuzz.int8.random();
        int16 = fuzz.int16.random();
        int32 = fuzz.int32.random();
        int64 = fuzz.int64.random();
        float = fuzz.float.random();
        principal = fuzz.principal.randomPrincipal(
            fuzz.nat.randomRange(0, 29)
        );
        blob = fuzz.blob.randomBlob(
            fuzz.nat.randomRange(0, 100)
        );
        bool = fuzz.bool.random();
    };

    inputs.add(record);

    let candid_map = CandidMap.new(
        schema_map,
        0,
        #Record([
            ("text", #Text(record.text)),
            ("nat", #Nat(record.nat)),
            ("nat8", #Nat8(record.nat8)),
            ("nat16", #Nat16(record.nat16)),
            ("nat32", #Nat32(record.nat32)),
            ("nat64", #Nat64(record.nat64)),
            ("int", #Int(record.int)),
            ("int8", #Int8(record.int8)),
            ("int16", #Int16(record.int16)),
            ("int32", #Int32(record.int32)),
            ("int64", #Int64(record.int64)),
            ("float", #Float(record.float)),
            ("principal", #Principal(record.principal)),
            ("blob", #Blob(record.blob)),
            ("bool", #Bool(record.bool)),
        ]),
    );
    candid_maps.add(candid_map);

};

suite(
    "Orchid: serialized values are orderd correctly",
    func() {
        test(
            "Bool",
            func() {
                let sorted_bools = BpTree.new<Bool, Nat>(null);
                let encoded_bools = BpTree.new<Blob, Nat>(null);

                let encoded_true = Orchid.Orchid.blobify.to_blob([#Bool(true)]);
                ignore BpTree.insert(sorted_bools, Cmp.Bool, true, 0);
                ignore BpTree.insert(encoded_bools, Orchid.Orchid.btree_cmp, encoded_true, 0);

                let encoded_false = Orchid.Orchid.blobify.to_blob([#Bool(false)]);
                ignore BpTree.insert(sorted_bools, Cmp.Bool, false, 1);
                ignore BpTree.insert(encoded_bools, Orchid.Orchid.btree_cmp, encoded_false, 1);

                assert Itertools.equal(
                    BpTree.vals(sorted_bools),
                    BpTree.vals(encoded_bools),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_bools),
                    Iter.map<Blob, Bool>(
                        BpTree.keys(encoded_bools),
                        func(b : Blob) : Bool {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Bool(bool)) { bool };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Bool.equal,
                );

            },
        );
        test(
            "Nat",
            func() {
                let sorted_nats = BpTree.new<Nat, Nat>(null);
                let encoded_nats = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_nats, Cmp.Nat, r.nat, i);

                    let encoded_nat = Orchid.Orchid.blobify.to_blob([#Nat(r.nat)]);
                    ignore BpTree.insert(encoded_nats, Orchid.Orchid.btree_cmp, encoded_nat, i);

                };

                assert Itertools.equal(
                    BpTree.vals(sorted_nats),
                    BpTree.vals(encoded_nats),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_nats),
                    Iter.map<Blob, Nat>(
                        BpTree.keys(encoded_nats),
                        func(b : Blob) : Nat {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Nat(nat)) { nat };
                                case (other) Debug.trap("error extracting candid value: " # debug_show (other));
                            };
                        },
                    ),
                    Nat.equal,
                );

            },
        );

        test(
            "Nat8",
            func() {
                let sorted_nat8s = BpTree.new<Nat8, Nat>(null);
                let encoded_nat8s = BpTree.new<Blob, Nat>(null);

                for ((i, n) in Itertools.enumerate(Iter.range(0, 255))) {

                    let n8 = Nat8.fromNat(n);
                    ignore BpTree.insert(sorted_nat8s, Cmp.Nat8, n8, i);

                    let encoded_nat8 = Orchid.Orchid.blobify.to_blob([#Nat8(n8)]);
                    ignore BpTree.insert(encoded_nat8s, Orchid.Orchid.btree_cmp, encoded_nat8, i);

                };

                assert Itertools.equal(
                    BpTree.vals(sorted_nat8s),
                    BpTree.vals(encoded_nat8s),
                    Nat.equal,
                );

                // round trip nat8
                assert Itertools.equal(
                    BpTree.keys(sorted_nat8s),
                    Iter.map<Blob, Nat8>(
                        BpTree.keys(encoded_nat8s),
                        func(b : Blob) : Nat8 {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Nat8(nat8)) { nat8 };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Nat8.equal,
                );
            },
        );

        test(
            "Nat16",
            func() {
                let sorted_nat16s = BpTree.new<Nat16, Nat>(null);
                let encoded_nat16s = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_nat16s, Cmp.Nat16, r.nat16, i);

                    let encoded_nat16 = Orchid.Orchid.blobify.to_blob([#Nat16(r.nat16)]);
                    ignore BpTree.insert(encoded_nat16s, Orchid.Orchid.btree_cmp, encoded_nat16, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_nat16s),
                    BpTree.vals(encoded_nat16s),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_nat16s),
                    Iter.map<Blob, Nat16>(
                        BpTree.keys(encoded_nat16s),
                        func(b : Blob) : Nat16 {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Nat16(nat16)) { nat16 };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Nat16.equal,
                );

            },
        );

        test(
            "Nat32",
            func() {
                let sorted_nat32s = BpTree.new<Nat32, Nat>(null);
                let encoded_nat32s = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_nat32s, Cmp.Nat32, r.nat32, i);

                    let encoded_nat32 = Orchid.Orchid.blobify.to_blob([#Nat32(r.nat32)]);
                    ignore BpTree.insert(encoded_nat32s, Orchid.Orchid.btree_cmp, encoded_nat32, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_nat32s),
                    BpTree.vals(encoded_nat32s),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_nat32s),
                    Iter.map(
                        BpTree.keys(encoded_nat32s),
                        func(b : Blob) : Nat32 {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Nat32(nat32)) { nat32 };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Nat32.equal,
                );
            },
        );

        test(
            "Nat64",
            func() {
                let sorted_nat64s = BpTree.new<Nat64, Nat>(null);
                let encoded_nat64s = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_nat64s, Cmp.Nat64, r.nat64, i);

                    let encoded_nat64 = Orchid.Orchid.blobify.to_blob([#Nat64(r.nat64)]);
                    ignore BpTree.insert(encoded_nat64s, Orchid.Orchid.btree_cmp, encoded_nat64, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_nat64s),
                    BpTree.vals(encoded_nat64s),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_nat64s),
                    Iter.map(
                        BpTree.keys(encoded_nat64s),
                        func(b : Blob) : Nat64 {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Nat64(nat64)) { nat64 };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Nat64.equal,
                );
            },
        );

        test(
            "Int8",
            func() {
                let sorted_int8s = BpTree.new<Int8, Nat>(null);
                let encoded_int8s = BpTree.new<Blob, Nat>(null);

                for ((i, n) in Itertools.enumerate(Iter.range(0, 255))) {

                    let int8 = Int8.fromInt(n - 128);

                    ignore BpTree.insert(sorted_int8s, Cmp.Int8, int8, i);

                    let encoded_int8 = Orchid.Orchid.blobify.to_blob([#Int8(int8)]);
                    ignore BpTree.insert(encoded_int8s, Orchid.Orchid.btree_cmp, encoded_int8, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_int8s),
                    BpTree.vals(encoded_int8s),
                    Nat.equal,
                );

                assert (
                    Orchid.Orchid.blobify.to_blob([#Int8(Int8.minimumValue)]) < Orchid.Orchid.blobify.to_blob([#Int8(0)])
                ) and (
                    Orchid.Orchid.blobify.to_blob([#Int8(0)]) < Orchid.Orchid.blobify.to_blob([#Int8(Int8.maximumValue)])
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_int8s),
                    Iter.map(
                        BpTree.keys(encoded_int8s),
                        func(b : Blob) : Int8 {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Int8(int8)) { int8 };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Int8.equal,
                );
            },
        );

        test(
            "Int16",
            func() {
                let sorted_int16s = BpTree.new<Int16, Nat>(null);
                let encoded_int16s = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_int16s, Cmp.Int16, r.int16, i);

                    let encoded_int16 = Orchid.Orchid.blobify.to_blob([#Int16(r.int16)]);
                    ignore BpTree.insert(encoded_int16s, Orchid.Orchid.btree_cmp, encoded_int16, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_int16s),
                    BpTree.vals(encoded_int16s),
                    Nat.equal,
                );

                assert (
                    Orchid.Orchid.blobify.to_blob([#Int16(Int16.minimumValue)]) < Orchid.Orchid.blobify.to_blob([#Int16(0)])
                ) and (
                    Orchid.Orchid.blobify.to_blob([#Int16(0)]) < Orchid.Orchid.blobify.to_blob([#Int16(Int16.maximumValue)])
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_int16s),
                    Iter.map(
                        BpTree.keys(encoded_int16s),
                        func(b : Blob) : Int16 {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Int16(int16)) { int16 };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Int16.equal,
                );

            },
        );

        test(
            "Int32",
            func() {
                let sorted_int32s = BpTree.new<Int32, Nat>(null);
                let encoded_int32s = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_int32s, Cmp.Int32, r.int32, i);

                    let encoded_int32 = Orchid.Orchid.blobify.to_blob([#Int32(r.int32)]);
                    ignore BpTree.insert(encoded_int32s, Orchid.Orchid.btree_cmp, encoded_int32, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_int32s),
                    BpTree.vals(encoded_int32s),
                    Nat.equal,
                );

                assert (
                    Orchid.Orchid.blobify.to_blob([#Int32(Int32.minimumValue)]) < Orchid.Orchid.blobify.to_blob([#Int32(0)])
                ) and (
                    Orchid.Orchid.blobify.to_blob([#Int32(0)]) < Orchid.Orchid.blobify.to_blob([#Int32(Int32.maximumValue)])
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_int32s),
                    Iter.map(
                        BpTree.keys(encoded_int32s),
                        func(b : Blob) : Int32 {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Int32(int32)) { int32 };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Int32.equal,
                );
            },
        );

        test(
            "Int64",
            func() {
                let sorted_int64s = BpTree.new<Int64, Nat>(null);
                let encoded_int64s = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_int64s, Cmp.Int64, r.int64, i);

                    let encoded_int64 = Orchid.Orchid.blobify.to_blob([#Int64(r.int64)]);
                    ignore BpTree.insert(encoded_int64s, Orchid.Orchid.btree_cmp, encoded_int64, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_int64s),
                    BpTree.vals(encoded_int64s),
                    Nat.equal,
                );

                assert (
                    Orchid.Orchid.blobify.to_blob([#Int64(Int64.minimumValue)]) < Orchid.Orchid.blobify.to_blob([#Int64(0)])
                ) and (
                    Orchid.Orchid.blobify.to_blob([#Int64(0)]) < Orchid.Orchid.blobify.to_blob([#Int64(Int64.maximumValue)])
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_int64s),
                    Iter.map(
                        BpTree.keys(encoded_int64s),
                        func(b : Blob) : Int64 {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Int64(int64)) { int64 };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Int64.equal,
                );
            },
        );

        test(
            "Int",
            func() {
                let sorted_ints = BpTree.new<Int, Nat>(null);
                let encoded_ints = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_ints, Cmp.Int, r.int, i);

                    let encoded_int = Orchid.Orchid.blobify.to_blob([#Int(r.int)]);
                    ignore BpTree.insert(encoded_ints, Orchid.Orchid.btree_cmp, encoded_int, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_ints),
                    BpTree.vals(encoded_ints),
                    Nat.equal,
                );
            },
        );

        test(
            "Principal",
            func() {
                let sorted_principals = BpTree.new<Principal, Nat>(null);
                let encoded_principals = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_principals, Cmp.Principal, r.principal, i);

                    let encoded_principal = Orchid.Orchid.blobify.to_blob([#Principal(r.principal)]);
                    ignore BpTree.insert(encoded_principals, Orchid.Orchid.btree_cmp, encoded_principal, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_principals),
                    BpTree.vals(encoded_principals),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_principals),
                    Iter.map(
                        BpTree.keys(encoded_principals),
                        func(b : Blob) : Principal {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Principal(principal)) { principal };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Principal.equal,
                );
            },
        );

        test(
            "Blob",
            func() {
                let sorted_blobs = BpTree.new<Blob, Nat>(null);
                let encoded_blobs = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_blobs, Cmp.Blob, r.blob, i);

                    let encoded_blob = Orchid.Orchid.blobify.to_blob([#Blob(r.blob)]);
                    ignore BpTree.insert(encoded_blobs, Orchid.Orchid.btree_cmp, encoded_blob, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_blobs),
                    BpTree.vals(encoded_blobs),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_blobs),
                    Iter.map(
                        BpTree.keys(encoded_blobs),
                        func(b : Blob) : Blob {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Blob(blob)) { blob };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Blob.equal,
                );
            },
        );

        test(
            "Float",
            func() {
                let sorted_floats = BpTree.new<Float, Nat>(null);
                let encoded_floats = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_floats, Cmp.Float, r.float, i);

                    let encoded_float = Orchid.Orchid.blobify.to_blob([#Float(r.float)]);
                    ignore BpTree.insert(encoded_floats, Orchid.Orchid.btree_cmp, encoded_float, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_floats),
                    BpTree.vals(encoded_floats),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_floats),
                    Iter.map(
                        BpTree.keys(encoded_floats),
                        func(b : Blob) : Float {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Float(float)) { float };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    func(x : Float, y : Float) : Bool = Float.equalWithin(x, y, 0.0001),
                );
            },
        );

        test(
            "Float 2",
            func() {

                /// Does not support encoding to these Special cases:
                let pos_inf = 1.0 / 0.0;
                let neg_inf = -1.0 / 0.0;
                let nan = 0.0 / 0.0;

                // Very small values
                var a = Orchid.Orchid.blobify.to_blob([#Float(0.0000001)]);
                var b = Orchid.Orchid.blobify.to_blob([#Float(0.0000002)]);
                assert a < b;

                // Very large values
                a := Orchid.Orchid.blobify.to_blob([#Float(1_000_000.0)]);
                b := Orchid.Orchid.blobify.to_blob([#Float(10_000_000.0)]);
                assert a < b;

                // Subnormal numbers
                a := Orchid.Orchid.blobify.to_blob([#Float(1.0e-308)]);
                b := Orchid.Orchid.blobify.to_blob([#Float(1.0e-309)]);
                assert a > b;
            },
        );

        test(
            "Text",
            func() {
                let sorted_texts = BpTree.new<Text, Nat>(null);
                let encoded_texts = BpTree.new<Blob, Nat>(null);

                for ((i, r) in Itertools.enumerate(inputs.vals())) {
                    ignore BpTree.insert(sorted_texts, Cmp.Text, r.text, i);

                    let encoded_text = Orchid.Orchid.blobify.to_blob([#Text(r.text)]);
                    ignore BpTree.insert(encoded_texts, Orchid.Orchid.btree_cmp, encoded_text, i);
                };

                assert Itertools.equal(
                    BpTree.vals(sorted_texts),
                    BpTree.vals(encoded_texts),
                    Nat.equal,
                );

                assert Itertools.equal(
                    BpTree.keys(sorted_texts),
                    Iter.map(
                        BpTree.keys(encoded_texts),
                        func(b : Blob) : Text {
                            let candid_values = Orchid.Orchid.blobify.from_blob(b);
                            switch (candid_values[0]) {
                                case (#Text(text)) { text };
                                case (other) Debug.trap("error extracting candid value");
                            };
                        },
                    ),
                    Text.equal,
                );
            },
        );

        // test(
        //     "Option(Text)",
        //     func() {
        //         let sorted_options = BpTree.new<?Text, Nat>(null);
        //         let encoded_options = BpTree.new<Blob, Nat>(null);

        //         for ()

        //     },
        // );

    },

);

suite(
    "Orchid: Composite keys starting with variable length types are serialized correctly",
    func() {
        let types = ["nat", "nat8", "nat16", "nat32", "nat64", "int", "int8", "int16", "int32", "int64", "text", "principal", "blob"];

        for (type_a in ["text", "blob", "principal"].vals()) {
            for (type_b in types.vals()) {

                test(
                    "Composite: [" # type_a # ", " # type_b # "]",
                    func() {

                        let sorted_candid = BpTree.new<[T.Candid], Nat>(null);
                        let encoded_candid = BpTree.new<Blob, Nat>(null);

                        for ((i, candid_map) in Itertools.enumerate(candid_maps.vals())) {
                            let ?a = CandidMap.get(candid_map, schema_map, type_a);
                            let ?b = CandidMap.get(candid_map, schema_map, type_b);
                            func cmp_candid_array(a : [T.Candid], b : [T.Candid]) : Int8 {

                                let iter = Itertools.peekable(
                                    Iter.map<(T.Candid, T.Candid), Int8>(
                                        Itertools.zip(a.vals(), b.vals()),
                                        func((a, b) : (T.Candid, T.Candid)) : Int8 {
                                            Schema.cmp_candid(MotokoTypesSchema, a, b);
                                        },
                                    )
                                );

                                PeekableIter.skipWhile(
                                    iter,
                                    func(a : Int8) : Bool { a == 0 },
                                );

                                Option.get(iter.next(), 0 : Int8);

                            };

                            ignore BpTree.insert<[T.Candid], Nat>(sorted_candid, cmp_candid_array, [a, b], i);

                            let encoded = Orchid.Orchid.blobify.to_blob([a, b]);
                            ignore BpTree.insert(encoded_candid, Orchid.Orchid.btree_cmp, encoded, i);
                        };

                        assert Itertools.equal(
                            BpTree.vals(sorted_candid),
                            BpTree.vals(encoded_candid),
                            Nat.equal,
                        );

                    },
                );

            };
        };

    },
);

func compare_entries<T>(
    entries : [Text],
    type_cmp : (Text, Text) -> Int8,
    conv_to_candid : (Text) -> ZenDB.Types.Candid,
) : [Text] {
    let btree_type = BpTree.new<Text, Nat>(null);
    let btree_blob = BpTree.new<Blob, Nat>(null);

    for ((i, entry) in Itertools.enumerate(entries.vals())) {
        ignore BpTree.insert(btree_type, type_cmp, entry, i);
        let encoded = Orchid.Orchid.blobify.to_blob([conv_to_candid(entry)]);
        ignore BpTree.insert(btree_blob, Orchid.Orchid.btree_cmp, encoded, i);
    };

    assert Itertools.equal(
        BpTree.vals(btree_type),
        BpTree.vals(btree_blob),
        Nat.equal,
    );

    return Iter.toArray<Text>(
        Iter.map<Blob, Text>(
            BpTree.keys(btree_blob),
            func(b : Blob) : Text {
                let candid_values = Orchid.Orchid.blobify.from_blob(b);
                switch (candid_values[0]) {
                    case (#Text(value)) { value };
                    case (other) Debug.trap("error extracting candid value: " # debug_show (other));
                };
            },

        )
    );
};

suite(
    "Adhoc: compare entries of different types using edge cases",
    func() {
        test(
            "Text ",
            func() {
                assert compare_entries<Text>(
                    // edge cases
                    [
                        "",
                        "\00",
                        "\n",
                        "\r",
                        "a",
                        "å",
                        "ab",
                    ],
                    Cmp.Text,
                    func(t : Text) : ZenDB.Types.Candid {
                        #Text(t);
                    },
                ) == [
                    "",
                    "\00",
                    "\n",
                    "\r",
                    "a",
                    "ab",
                    "å",
                ];
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
                let a = Orchid.Orchid.blobify.to_blob([#Text("this might be orchid")]);
                let b = Orchid.Orchid.blobify.to_blob([#Text("this might be candid")]);

                let prefix_bytes = get_prefix(a, b);
                assert prefix_bytes.size() >= 14 + 1; // 14 bytes for the prefix and 1 byte for the type encoding

                let ?prefix = Text.decodeUtf8(Utils.slice_blob(prefix_bytes, 1, prefix_bytes.size()));

                assert Text.endsWith(prefix, #text("this might be "));

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
                let a : [T.CandidQuery] = [#Blob(Blob.fromArray([0xfe, 0x00, 0xEE])), #Nat8(244)];
                let b : [T.CandidQuery] = [#Blob(Blob.fromArray([0xfe, 0x00, 0xEE, 0x00])), #Nat(1234)];

                let a_blob = Orchid.Orchid.blobify.to_blob(a);
                let b_blob = Orchid.Orchid.blobify.to_blob(b);

                Debug.print("a < b: " # debug_show (a_blob, b_blob, a_blob < b_blob));

                assert a_blob < b_blob;
            },
        );

    },
);
