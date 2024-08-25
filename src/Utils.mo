import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Result "mo:base/Result";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Hash "mo:base/Hash";
import Float "mo:base/Float";

import Int "mo:base/Int";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Tag "mo:candid/Tag";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "Types";

module {
    type Order = Order.Order;
    public func extract_schema_keys(schema : T.Schema) : [Text] {
        let buffer = Buffer.Buffer<Text>(8);

        func extract(schema : T.Schema) {
            switch (schema) {
                case (#Record(fields)) {
                    for ((name, value) in fields.vals()) {
                        buffer.add(name);
                        extract(value);
                    };
                };
                case (#Variant(variants)) {
                    for ((name, value) in variants.vals()) {
                        buffer.add(name);
                        extract(value);
                    };
                };
                case (#Tuple(types)) {
                    for (tuple_type in types.vals()) {
                        extract(tuple_type);
                    };
                };
                case (#Option(inner)) { extract(inner) };
                case (#Array(inner)) { extract(inner) };
                case (_) {};
            };
        };

        extract(schema);

        Buffer.toArray(buffer);
    };

    public func unwrap_or_err<A>(res : T.Result<A, Text>) : A {
        switch (res) {
            case (#ok(success)) success;
            case (#err(err)) Debug.trap("unwrap_or_err: " # err);
        };
    };

    public func assert_result<A>(res : T.Result<A, Text>) {
        switch (res) {
            case (#ok(_)) ();
            case (#err(err)) Debug.trap("assert_result: " # err);
        };
    };

    public func tuple_cmp<A, B>(cmp : (A, A) -> T.Order) : ((A, B), (A, B)) -> Order {
        func(a : (A, B), b : (A, B)) : T.Order {
            cmp(a.0, b.0);
        };
    };

    public func tuple_eq<A, B>(eq : (A, A) -> Bool) : ((A, B), (A, B)) -> Bool {
        func(a : (A, B), b : (A, B)) : Bool {
            eq(a.0, b.0);
        };
    };

    public let typeutils_nat_as_nat64 : TypeUtils.TypeUtils<Nat> = {
        // converts to Nat64 because pointers are 64-bit
        blobify = {
            from_blob = func(blob : Blob) : Nat {
                TypeUtils.BigEndian.Nat64.blobify.from_blob(blob) |> Nat64.toNat(_);
            };

            to_blob = func(nat : Nat) : Blob {
                let n64 = Nat64.fromNat(nat);
                TypeUtils.BigEndian.Nat64.blobify.to_blob(n64);
            };
        };

        cmp = #BlobCmp(Int8Cmp.Blob);

    };

};
