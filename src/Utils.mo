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
import Blob "mo:base/Blob";

import Int "mo:base/Int";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Logger "Logger";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "Types";
import ByteUtils "ByteUtils";

module {
    type Order = Order.Order;

    /// Generic helper function to handle Result types with consistent error logging
    public func handleResult<T>(logger : T.Logger, res : T.Result<T, Text>, context : Text) : T.Result<T, Text> {
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

    public func text_strip_start(text : Text, prefix : Text) : Text {
        switch (Text.stripStart(text, #text(prefix))) {
            case (?stripped) stripped;
            case (null) text;
        };
    };

    public func buffer_concat_freeze<A>(buffers : [Buffer.Buffer<A>]) : [A] {
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

    public func log_error_msg<A>(logger : T.Logger, err_msg : Text) : T.Result<A, Text> {
        Logger.lazyError(logger, func() = err_msg);
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

    public let typeutils_nat_as_nat64 : TypeUtils.TypeUtils<Nat> = {
        // converts to Nat64 because pointers are 64-bit
        blobify = {
            from_blob = func(blob : Blob) : Nat {
                assert blob.size() == 8;

                // must be big-endian, because these keys are sorted in the BTree
                // and we need to be able to compare them correctly in their byte form
                let n64 = ByteUtils.BigEndian.toNat64(blob.vals());
                Nat64.toNat(n64);
            };

            to_blob = func(nat : Nat) : Blob {
                let n64 = Nat64.fromNat(nat);
                let bytes = ByteUtils.BigEndian.fromNat64(n64);
                let blob = Blob.fromArray(bytes);

                assert blob.size() == 8;
                blob;
            };
        };

        cmp = #BlobCmp(Int8Cmp.Blob);

    };

    public func buffer_add_all<A>(buffer : Buffer.Buffer<A>, iter : Iter.Iter<A>) {
        for (elem in iter) { buffer.add(elem) };
    };

    // add all elements from an iterator to a bufferlike object that has the add method
    public func buffer_like_add_all<A>(buffer : { add : (A) -> () }, iter : Iter.Iter<A>) {
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
                case (null) Debug.trap "Index out of bounds";
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
