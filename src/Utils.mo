import Principal "mo:base@0.16.0/Principal";
import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Nat32 "mo:base@0.16.0/Nat32";
import Nat64 "mo:base@0.16.0/Nat64";
import Result "mo:base@0.16.0/Result";
import Order "mo:base@0.16.0/Order";
import Iter "mo:base@0.16.0/Iter";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Option "mo:base@0.16.0/Option";
import Hash "mo:base@0.16.0/Hash";
import Float "mo:base@0.16.0/Float";
import Blob "mo:base@0.16.0/Blob";
import Prelude "mo:base@0.16.0/Prelude";

import Int "mo:base@0.16.0/Int";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.3.2";
import Decoder "mo:serde@3.3.2/Candid/Blob/Decoder";
import Candid "mo:serde@3.3.2/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import Logger "Logger";

import _TypeUtils "mo:memory-collection@0.3.2/TypeUtils";
import Int8Cmp "mo:memory-collection@0.3.2/TypeUtils/Int8Cmp";

import T "Types";
import ByteUtils "mo:byte-utils@0.1.1";

module {
    type Order = Order.Order;

    public let TypeUtils = _TypeUtils;

    public func convert_to_internal_candify<A>(collection_name : Text, external_candify : T.Candify<A>) : T.InternalCandify<A> {
        {
            from_blob = func(blob : Blob) : A {
                switch (external_candify.from_blob(blob)) {
                    case (?document) document;
                    case (null) Debug.trap("
                        Could not convert candid blob (" # debug_show blob # ") to motoko using the '" # collection_name # "' collection's schema.
                        If the schema and the candify encoding function are correct, then the blob might be corrupted or not a valid candid blob.
                        Please report this issue to the developers by creating a new issue on the GitHub repository.  ");
                };
            };
            to_blob = external_candify.to_blob;
        };
    };

    let bounding_12_byte_nat = 0x1_0000_0000_0000_0000;

    public func convert_last_8_bytes_to_nat(blob : Blob) : Nat {
        let size = blob.size();

        let n64 = ByteUtils.BigEndian.toNat64(
            [
                blob.get(size - 8),
                blob.get(size - 7),
                blob.get(size - 6),
                blob.get(size - 5),
                blob.get(size - 4),
                blob.get(size - 3),
                blob.get(size - 2),
                blob.get(size - 1),
            ].vals()
        );

        Nat64.toNat(n64);
    };

    public func nat_from_12_byte_blob(blob : Blob) : Nat {
        let top_4_bytes = ByteUtils.BigEndian.toNat32(
            [
                blob.get(0),
                blob.get(1),
                blob.get(2),
                blob.get(3),
            ].vals()
        );

        let bottom_8_bytes = ByteUtils.BigEndian.toNat64(
            [
                blob.get(4),
                blob.get(5),
                blob.get(6),
                blob.get(7),
                blob.get(8),
                blob.get(9),
                blob.get(10),
                blob.get(11),
            ].vals()
        );

        // (top_4_bytes) << 64 | bottom_8_bytes
        // multiplying by  (2 ^ 64) is the same as shifting left by 64 bits
        // (2 ^ 64) == 0x1_0000_0000_0000_0000
        (Nat32.toNat(top_4_bytes) * bounding_12_byte_nat + Nat64.toNat(bottom_8_bytes))

    };

    public func nat_to_12_byte_blob(n : Nat) : Blob {
        // assert n < bounding_12_byte_nat;

        let nat32 = Nat32.fromNat(n / bounding_12_byte_nat);
        let nat64 = Nat64.fromNat(n % bounding_12_byte_nat);

        let top_4_bytes = ByteUtils.BigEndian.fromNat32(nat32);
        let bottom_8_bytes = ByteUtils.BigEndian.fromNat64(nat64);

        Blob.fromArray([
            // top 4 bytes
            top_4_bytes[0],
            top_4_bytes[1],
            top_4_bytes[2],
            top_4_bytes[3],
            // bottom 8 bytes
            bottom_8_bytes[0],
            bottom_8_bytes[1],
            bottom_8_bytes[2],
            bottom_8_bytes[3],
            bottom_8_bytes[4],
            bottom_8_bytes[5],
            bottom_8_bytes[6],
            bottom_8_bytes[7],
        ]);
    };

    public func big_endian_nat_from_blob(blob : Blob) : Nat {
        assert blob.size() > 0;

        var i = blob.size() - 1;
        var nat = 0;

        for (_ in Itertools.range(0, (blob.size() / 8))) {
            let n64 = ByteUtils.BigEndian.toNat64(
                (
                    Array.tabulate(
                        8,
                        func(j : Nat) : Nat8 {
                            if (i + j < 8) {
                                0;
                            } else {
                                blob.get(i - 8 + j);
                            };
                        },
                    )
                ).vals()
            );

            if (nat > 0) nat *= (Nat64.toNat(Nat64.maximumValue) + 1);
            nat += Nat64.toNat(n64);

            if (i >= 8) i -= 8;

        };

        nat

    };

    public func send_error<OldOk, NewOk, Error>(res : T.Result<OldOk, Error>) : T.Result<NewOk, Error> {
        switch (res) {
            case (#ok(_)) Prelude.unreachable();
            case (#err(errorMsg)) #err(errorMsg);
        };
    };

    public func ignore_this() : None {
        Debug.trap("trap caused by ignoreThis()");
    };

    public func concat_blob(blob1 : Blob, blob2 : Blob) : Blob {
        let size = blob1.size() + blob2.size();
        let res = Blob.fromArray(
            Array.tabulate(
                size,
                func(i : Nat) : Nat8 {
                    if (i < blob1.size()) {
                        blob1.get(i);
                    } else {
                        blob2.get(i - blob1.size());
                    };
                },
            )
        );
        res;
    };

    public func slice_blob(blob : Blob, start : Nat, end : Nat) : Blob {
        let size = end - start;

        Blob.fromArray(
            Array.tabulate(
                size,
                func(i : Nat) : Nat8 {
                    blob.get(start + i);
                },
            )
        );
    };

    /// Generic helper function to handle Result types with consistent error logging
    public func handle_result<T>(logger : T.Logger, res : T.Result<T, Text>, context : Text) : T.Result<T, Text> {
        switch (res) {
            case (#ok(success)) #ok(success);
            case (#err(errorMsg)) {
                Logger.lazyError(logger, func() = context # ": " # errorMsg);
                #err(errorMsg);
            };
        };
    };

    public func log2(n : Float) : Float {
        Float.log(n) / Float.log(2);
    };

    public func strip_start(text : Text, prefix : Text) : Text {
        switch (Text.stripStart(text, #text(prefix))) {
            case (?stripped) stripped;
            case (null) text;
        };
    };

    public func concat_freeze<A>(buffers : [Buffer.Buffer<A>]) : [A] {
        var i = 0;
        var total_size = 0;
        while (i < buffers.size()) {
            total_size += buffers[i].size();
            i += 1;
        };

        var buffer_index = 0;
        var acc_size = 0;

        Array.tabulate(
            total_size,
            func(i : Nat) : A {
                if ((i + 1) % (buffers[0].size() + 1) == 0) {
                    acc_size += buffers[buffer_index].size();
                    buffer_index += 1;
                };

                buffers[buffer_index].get(i - acc_size);

            },
        );

    };

    public func get_schema_keys(schema : T.Schema) : [Text] {
        let buffer = Buffer.Buffer<Text>(8);

        func extract(schema : T.Schema) {
            switch (schema) {
                case (#Record(fields) or #Map(fields)) {
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

    public func reverse_order(order : T.Order) : T.Order {
        switch (order) {
            case (#less) #greater;
            case (#greater) #less;
            case (#equal) #equal;
        };
    };

    public func unwrap_or_err<A>(res : T.Result<A, Text>) : A {
        switch (res) {
            case (#ok(success)) success;
            case (#err(err)) Debug.trap("unwrapOrErr: " # err);
        };
    };

    public func assert_result<A>(res : T.Result<A, Text>) {
        switch (res) {
            case (#ok(_)) ();
            case (#err(err)) Debug.trap("assertResult: " # err);
        };
    };

    public func log_error_msg<A>(logger : T.Logger, err_msg : Text) : T.Result<A, Text> {
        Logger.error(logger, err_msg);
        #err(err_msg);
    };

    public func log_error<A>(logger : T.Logger, res : T.Result<A, Text>, opt_prefix_msg : ?Text) : T.Result<A, Text> {
        switch (res) {
            case (#ok(success)) #ok(success);
            case (#err(errorMsg)) {
                Logger.lazyError(
                    logger,
                    func() {
                        switch (opt_prefix_msg) {
                            case (?prefix) prefix # ": " # errorMsg;
                            case (null) errorMsg;
                        };
                    },
                );
                #err(errorMsg);
            };
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

    public func add_all<A>(buffer : Buffer.Buffer<A>, iter : Iter.Iter<A>) {
        for (elem in iter) { buffer.add(elem) };
    };

    // add all elements from an iterator to a bufferlike object that has the add method
    public func add_all_like<A>(buffer : { add : (A) -> () }, iter : Iter.Iter<A>) {
        for (elem in iter) { buffer.add(elem) };
    };

    public class ReusableBuffer<A>(init_capacity : Nat) {
        var elems : [var ?A] = Array.init(init_capacity, null);
        var count : Nat = 0;

        public func size() : Nat = count;

        public func add(elem : A) {
            if (count == elems.size()) {
                elems := Array.tabulateVar(
                    elems.size() * 2,
                    func(i : Nat) : ?A {
                        if (i < count) {
                            elems[i];
                        } else {
                            null;
                        };
                    },
                );
            };

            elems[count] := ?elem;
            count += 1;
        };

        public func clear() {
            count := 0;
        };

        public func get(i : Nat) : A {
            switch (elems[i]) {
                case (?elem) elem;
                case (null) Debug.trap "CompositeIndex out of bounds";
            };
        };

        public func vals() : Iter.Iter<A> {
            var i = 0;

            object {
                public func next() : ?A {
                    if (i < count) {
                        let res = elems[i];
                        i += 1;
                        res;
                    } else {
                        null;
                    };
                };
            };
        };
    };

};
