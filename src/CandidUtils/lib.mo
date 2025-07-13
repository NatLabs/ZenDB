import Debug "mo:base/Debug";
import Array "mo:base/Array";
import Text "mo:base/Text";
import Nat32 "mo:base/Nat32";
import Result "mo:base/Result";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Hash "mo:base/Hash";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Int32 "mo:base/Int32";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Int16 "mo:base/Int16";
import Int64 "mo:base/Int64";
import Int8 "mo:base/Int8";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";
import Char "mo:base/Char";
import Principal "mo:base/Principal";

import CastModule "Cast";
import OpsModule "Ops";

import T "../Types";

module {
    // public let {
    //     cast;
    //     cast_to_nat;
    //     cast_to_int;
    //     cast_to_text;
    // } = Cast;

    public let Ops = OpsModule;
    public let MultiOps = OpsModule.Multi;
    public let Cast = CastModule;

    // todo: should consider moving this to the Orchid module so we can directly encode these values
    public func get_next_value(value : T.CandidQuery) : T.CandidQuery {
        switch (value) {
            case (#Nat(n)) #Nat(n + 1);
            case (#Nat8(n)) if (n == Nat8.maximumValue) #Maximum else #Nat8(n + 1);
            case (#Nat16(n)) if (n == Nat16.maximumValue) #Maximum else #Nat16(n + 1);
            case (#Nat32(n)) if (n == Nat32.maximumValue) #Maximum else #Nat32(n + 1);
            case (#Nat64(n)) if (n == Nat64.maximumValue) #Maximum else #Nat64(n + 1);
            case (#Int(n)) #Int(n + 1);
            case (#Int8(n)) if (n == Int8.maximumValue) #Maximum else #Int8(n + 1);
            case (#Int16(n)) if (n == Int16.maximumValue) #Maximum else #Int16(n + 1);
            case (#Int32(n)) if (n == Int32.maximumValue) #Maximum else #Int32(n + 1);
            case (#Int64(n)) if (n == Int64.maximumValue) #Maximum else #Int64(n + 1);
            case (#Float(n)) #Float(n + 1e-30);
            case (#Text(t)) #Text(t # Text.fromChar(Char.fromNat32(0 : Nat32)));
            case (#Blob(b)) #Blob(
                Blob.fromArray(
                    Array.append(
                        Blob.toArray(b),
                        [0 : Nat8],
                    )
                )
            );
            case (#Principal(p)) #Principal(
                Principal.fromBlob(
                    Blob.fromArray(
                        Array.append(
                            Blob.toArray(Principal.toBlob(p)),
                            [0 : Nat8],
                        )
                    )
                )
            );
            case (#Bool(b)) if (not b) #Bool(true) else #Maximum;
            case (#Null) #Option(#Null); // todo: need to ensure this is accurate
            case (#Option(inner_value)) get_next_value(inner_value);
            case (#Empty) #Maximum;
            case (#Maximum) #Maximum;
            case (#Minimum) #Minimum;
            case (candid_value) Debug.trap("get_next_value(): Does not support this type: " # debug_show (candid_value))

        };

    };

    func get_prev_blob_in_lexicographical_order(value : Blob) : Blob {
        let bytes = Blob.toArray(value);

        var j = bytes.size();

        let last_byte = bytes[bytes.size() - 1];

        if (last_byte == 0) {
            return Blob.fromArray(
                Array.subArray<Nat8>(
                    bytes,
                    0,
                    bytes.size() - 1,
                )
            );
        };

        let predecessor_byte = last_byte - 1;

        let new_bytes = Array.append<Nat8>(
            Array.subArray<Nat8>(
                bytes,
                0,
                bytes.size() - 1,
            ),
            Array.tabulate<Nat8>(30, func(_ : Nat) : Nat8 = predecessor_byte),
        );

        return Blob.fromArray(new_bytes);

    };

    public func get_prev_value(value : T.CandidQuery) : T.CandidQuery {
        switch (value) {
            case (#Nat(n)) if (n == 0) #Minimum else #Nat(n - 1);
            case (#Nat8(n)) if (n == 0) #Minimum else #Nat8(n - 1);
            case (#Nat16(n)) if (n == 0) #Minimum else #Nat16(n - 1);
            case (#Nat32(n)) if (n == 0) #Minimum else #Nat32(n - 1);
            case (#Nat64(n)) if (n == 0) #Minimum else #Nat64(n - 1);
            case (#Int(n)) #Int(n - 1);
            case (#Int8(n)) if (n == Int8.minimumValue) #Minimum else #Int8(n - 1);
            case (#Int16(n)) if (n == Int16.minimumValue) #Minimum else #Int16(n - 1);
            case (#Int32(n)) if (n == Int32.minimumValue) #Minimum else #Int32(n - 1);
            case (#Int64(n)) if (n == Int64.minimumValue) #Minimum else #Int64(n - 1);
            case (#Float(n)) #Float(n - 1e-30);
            case (#Text(t)) if (t == "") #Minimum else {

                let blob = get_prev_blob_in_lexicographical_order(Text.encodeUtf8(t));
                let opt_text = Text.decodeUtf8(blob);
                switch (opt_text) {
                    case (?new_text) #Text(new_text);
                    case (null) Debug.trap("get_prev_value(): Failed to decode text from blob: " # debug_show (blob));
                };
            };
            case (#Blob(b)) if (b.size() == 0) #Minimum else {
                let blob = get_prev_blob_in_lexicographical_order(b);
                #Blob(blob);
            };
            case (#Principal(p)) {
                let b = Principal.toBlob(p);

                if (b.size() == 0) #Minimum else {
                    let prev_blob = get_prev_blob_in_lexicographical_order(b);
                    #Principal(Principal.fromBlob(prev_blob));
                };
            };
            case (#Bool(b)) if (b) #Bool(false) else #Minimum;
            case (#Option(inner_value)) get_prev_value(inner_value);
            case (#Null) #Minimum;
            case (#Empty) #Minimum;
            case (#Maximum) #Maximum;
            case (#Minimum) #Minimum;
            case (_) Debug.trap("get_prev_value(): Does not support this type: " # debug_show (value));

        };

    };

    public func from_candid_query(value : T.CandidQuery) : T.Candid {
        switch (value) {
            case (#Nat(n)) #Nat(n);
            case (#Nat8(n)) #Nat8(n);
            case (#Nat16(n)) #Nat16(n);
            case (#Nat32(n)) #Nat32(n);
            case (#Nat64(n)) #Nat64(n);
            case (#Int(n)) #Int(n);
            case (#Int8(n)) #Int8(n);
            case (#Int16(n)) #Int16(n);
            case (#Int32(n)) #Int32(n);
            case (#Int64(n)) #Int64(n);
            case (#Float(f)) #Float(f);
            case (#Text(t)) #Text(t);
            case (#Blob(b)) #Blob(b);
            case (#Principal(p)) #Principal(p);
            case (#Bool(b)) #Bool(b);
            case (#Null) #Null;
            case (#Empty) #Empty;
            case (_candid_value) Debug.trap("from_candid_query(): Does not support this type: " # debug_show (_candid_value));
        };
    };

    public func to_candid_query(value : T.Candid) : T.CandidQuery {
        switch (value) {
            case (#Nat(n)) #Nat(n);
            case (#Nat8(n)) #Nat8(n);
            case (#Nat16(n)) #Nat16(n);
            case (#Nat32(n)) #Nat32(n);
            case (#Nat64(n)) #Nat64(n);
            case (#Int(n)) #Int(n);
            case (#Int8(n)) #Int8(n);
            case (#Int16(n)) #Int16(n);
            case (#Int32(n)) #Int32(n);
            case (#Int64(n)) #Int64(n);
            case (#Float(f)) #Float(f);
            case (#Text(t)) #Text(t);
            case (#Blob(b)) #Blob(b);
            case (#Principal(p)) #Principal(p);
            case (#Bool(b)) #Bool(b);
            case (#Null) #Null;
            case (#Empty) #Empty;
            case (_candid_value) Debug.trap("to_candid_query(): Does not support this type: " # debug_show (_candid_value));
        };
    };

    public func unwrap_option(value : T.Candid) : T.Candid {
        switch (value) {
            case (#Option(nested_value)) unwrap_option(nested_value);
            case (_) value;
        };
    };

    public func inherit_options_from_type(candid_type : T.CandidType, value : T.Candid) : T.Candid {
        switch (candid_type) {
            case (#Option(nested_type)) inherit_options_from_type(nested_type, #Option(value));
            case (_) value;
        };
    };

};
