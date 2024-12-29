import Principal "mo:base/Principal";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Debug "mo:base/Debug";
import Nat16 "mo:base/Nat16";
import Text "mo:base/Text";
import Blob "mo:base/Blob";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Buffer "mo:base/Buffer";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Candid "mo:serde/Candid";
import { TypeCode } "mo:serde/Candid/Types";
import Itertools "mo:itertools/Iter";

import T "Types";
import ByteUtils "ByteUtils";

module {
    let { nhash; thash } = Map;
    type Candid = Candid.Candid;
    type Iter<A> = Iter.Iter<A>;

    type NestedCandid = {
        #Candid : Candid.Candid;
        #CandidMap : Map.Map<Text, NestedCandid>;
    };

    /// Access nested fields faster than the default Candid datatype
    ///
    /// It converts the Candid record into a flat map where the keys are the paths to the nested fields
    public class CandidMap(candid_bytes : [Nat8]) {
        var candid_map_bytes = candid_bytes;
        let cache = Map.new<Text, Candid.Candid>();

        public func encode() : Blob {
            Blob.fromArray(candid_map_bytes);
        };

        public func encoded_bytes() : [Nat8] {
            candid_map_bytes;
        };

        public func reload(new_candid_map_bytes : [Nat8]) {
            Map.clear(cache);
            candid_map_bytes := new_candid_map_bytes;
        };

        public func get(key : Text) : ?Candid.Candid {

            func read_nat_16(i : Nat) : Nat16 {
                (
                    Nat16.fromNat8(candid_map_bytes[i]) << 8
                ) | (
                    Nat16.fromNat8(candid_map_bytes[i + 1])
                );
            };

            func read_nat_32(i : Nat) : Nat32 {
                (
                    Nat32.fromNat(Nat8.toNat(candid_map_bytes[i])) << 24
                ) | (
                    Nat32.fromNat(Nat8.toNat(candid_map_bytes[i + 1])) << 16
                ) | (
                    Nat32.fromNat(Nat8.toNat(candid_map_bytes[i + 2])) << 8
                ) | (
                    Nat32.fromNat(Nat8.toNat(candid_map_bytes[i + 3]))
                );
            };

            let num_fields = Nat16.toNat(read_nat_16(0));
            // Debug.print("num_fields: " # debug_show num_fields);

            if (num_fields == 0) return null;

            let start = 2;

            let KEY_POINTER = 4;
            let VALUE_POINTER = 4;

            func read_key_pointer(nth_record : Nat) : Nat {
                let key_pointer_index = start + (nth_record * (KEY_POINTER + VALUE_POINTER));
                let key_pointer = read_nat_32(key_pointer_index) |> Nat32.toNat(_);
                key_pointer;
            };

            func read_value_pointer(nth_record : Nat) : Nat {
                let value_pointer_index = start + (nth_record * (KEY_POINTER + VALUE_POINTER)) + KEY_POINTER;
                let value_pointer = read_nat_32(value_pointer_index) |> Nat32.toNat(_);
                value_pointer;
            };

            func read_key_iter(i : Nat) : Iter.Iter<Nat8> {
                let key_pointer = read_key_pointer(i);
                let next_key_pointer = if (i + 1 < num_fields) read_key_pointer(i + 1) else read_value_pointer(0);

                Itertools.fromArraySlice(candid_map_bytes, key_pointer, next_key_pointer);
            };

            func read_value_iter(i : Nat) : Iter.Iter<Nat8> {
                let value_pointer = read_value_pointer(i);
                let next_value_pointer = if (i + 1 < num_fields) read_value_pointer(i + 1) else candid_map_bytes.size();

                Itertools.fromArraySlice(candid_map_bytes, value_pointer, next_value_pointer);
            };

            let LESS : Int8 = -1;
            let EQUAL : Int8 = 0;
            let GREATER : Int8 = 1;
            let IS_PREFIX : Int8 = 2;
            let IS_BASE_KEY : Int8 = 3; // means that the other key is a prefix of this key

            func lexicographical_comparison(a : Iter.Iter<Nat8>, b : Iter.Iter<Nat8>) : Int8 {
                loop switch (a.next(), b.next()) {
                    case (?a_byte, ?b_byte) {
                        if (a_byte < b_byte) {
                            return LESS;
                        } else if (a_byte > b_byte) {
                            return GREATER;
                        };
                    };
                    case (null, null) return EQUAL;
                    case (null, ?_) return IS_PREFIX;
                    case (?_, null) return IS_BASE_KEY;
                };
            };

            let search_key_bytes = Text.encodeUtf8(key);

            var l = 0;
            var r = num_fields;

            while (l < r) {
                let mid = (l + r) / 2;
                // Debug.print("mid: " # debug_show mid);

                // Debug.print(
                //     "comparing (key, search_key): " # debug_show (
                //         Text.decodeUtf8(
                //             Blob.fromArray(Iter.toArray(read_key_iter(mid)))
                //         ),
                //         Text.decodeUtf8(
                //             Blob.fromArray(Iter.toArray(search_key_bytes.vals()))
                //         ),
                //     )
                // );

                let cmp = lexicographical_comparison(read_key_iter(mid), search_key_bytes.vals());
                // Debug.print("cmp: " # debug_show cmp);

                if (cmp == LESS) {
                    l := mid + 1;
                } else if (cmp == GREATER) {
                    r := mid;
                } else if (cmp == EQUAL) {
                    let candid = decode_candid_value(read_value_iter(mid));
                    // Debug.print("found candid: " # debug_show candid);
                    return ?candid;
                } else if (cmp == IS_PREFIX) {
                    // Debug.print("key is a prefix of the search key");
                    let candid = decode_candid_value(read_value_iter(mid));
                    assert candid == #Null or candid == #Option(#Null);
                    return ?candid;
                } else if (cmp == IS_BASE_KEY) {
                    Debug.trap(
                        "search key is a prefix of the mid key (mid key, search key): " # debug_show (
                            Text.decodeUtf8(
                                Blob.fromArray(Iter.toArray(read_key_iter(mid)))
                            ),
                            Text.decodeUtf8(
                                Blob.fromArray(Iter.toArray(search_key_bytes.vals()))
                            ),
                        )
                    );
                };
            };

            // Debug.print("could not find key: " # debug_show key);

            null;
        };

    };

    public func fromBlob(blob : Blob) : CandidMap {
        CandidMap(Blob.toArray(blob));
    };

    public func fromCandid(candid : Candid.Candid) : CandidMap {
        // Debug.print("fromCandid: " # debug_show candid);
        let flattened_records = flatten_nested_records(candid);
        // Debug.print("flatten_nested_records: " # debug_show Buffer.toArray(flattened_records));
        let candid_map_bytes = encode_flattened_records(flattened_records);
        CandidMap(candid_map_bytes);
    };

    func flatten_nested_records(candid : Candid.Candid) : Buffer.Buffer<(Text, Candid)> {
        let #Record(fields) = candid else return Debug.trap("CandidMap: Expected #Record type");
        let flattened_records = Buffer.Buffer<(Text, Candid)>(fields.size() * 2);

        func flatten(flattened_records : Buffer.Buffer<(Text, Candid)>, prefix : Text, fields : [(Text, Candid)]) {

            var i = 0;

            while (i < fields.size()) {
                let field = fields[i].0;
                let value = fields[i].1;

                let field_path = if (prefix == "") field else prefix # "." # field;

                switch (value) {
                    case (#Record(records) or #Map(records)) {
                        flatten(flattened_records, field_path, records);
                    };
                    case (#Option(#Record(records)) or #Option(#Map(records))) {
                        flatten(flattened_records, field_path, records);
                    };
                    case (_) {
                        flattened_records.add(field_path, value);
                    };
                };

                i += 1;
            };

        };

        flatten(flattened_records, "", fields);

        flattened_records;

    };

    func encode_flattened_records(flattened_records : Buffer.Buffer<(Text, Candid)>) : [Nat8] {
        flattened_records.sort(
            func((key, _) : (Text, Candid), (key2, _) : (Text, Candid)) : Order.Order {
                Text.compare(key, key2);
            }
        );

        // Debug.print("Sorted flattened records: " # debug_show Buffer.toArray(flattened_records));

        let key_sizes = Buffer.Buffer<Nat>(flattened_records.size());
        let value_sizes = Buffer.Buffer<Nat>(flattened_records.size());
        let keys = Buffer.Buffer<Nat8>(flattened_records.size() * 8);
        let values = Buffer.Buffer<Nat8>(flattened_records.size() * 8);

        func buffer_add_all<A>(buffer : Buffer.Buffer<A>, iter : Iter.Iter<A>) {
            label adding_item_to_buffer loop switch (iter.next()) {
                case (?val) buffer.add(val);
                case (null) break adding_item_to_buffer;
            };
        };

        var i = 0;

        while (i < flattened_records.size()) {
            let record = flattened_records.get(i);
            let key = record.0;
            let value = record.1;

            let key_bytes = Blob.toArray(Text.encodeUtf8(key));
            let value_bytes = encode_candid_value(value);

            key_sizes.add(key_bytes.size());
            value_sizes.add(value_bytes.1);

            buffer_add_all(keys, key_bytes.vals());
            buffer_add_all(values, value_bytes.0);

            i += 1;
        };

        let pointers = Buffer.Buffer<Nat8>(flattened_records.size() * 2 * 4);

        i := 0;
        var pointer_offset = 2; // 2 bytes for the number of fields
        let key_pointer_offset = pointer_offset;
        let keys_offset = pointer_offset + (flattened_records.size() * 2 * 4);
        let values_offset = keys_offset + keys.size();

        var acc_key_size = 0;
        var acc_value_size = 0;

        while (i < flattened_records.size()) {
            let key_size = key_sizes.get(i);
            let value_size = value_sizes.get(i);

            let key_pointer = keys_offset + acc_key_size;
            let value_pointer = values_offset + acc_value_size;

            acc_key_size += key_size;
            acc_value_size += value_size;

            let key_pointer_bytes = ByteUtils.from_nat32_be(Nat32.fromNat(key_pointer));
            let value_pointer_bytes = ByteUtils.from_nat32_be(Nat32.fromNat(value_pointer));

            buffer_add_all(pointers, key_pointer_bytes.vals());
            buffer_add_all(pointers, value_pointer_bytes.vals());

            i += 1;
        };

        let encoded = Buffer.Buffer<Nat8>(2 + pointers.size() + keys.size() + values.size());

        let num_fields_bytes = ByteUtils.from_nat16_be(Nat16.fromNat(flattened_records.size()));
        buffer_add_all(encoded, num_fields_bytes.vals());

        buffer_add_all(encoded, pointers.vals());
        buffer_add_all(encoded, keys.vals());
        buffer_add_all(encoded, values.vals());

        Buffer.toArray(encoded)

    };

    func decode_candid_value(value_iter : Iter.Iter<Nat8>) : Candid.Candid {

        let ?type_code = value_iter.next() else return Debug.trap("CandidMap: Expected type code");

        let candid_value : Candid.Candid = if (type_code == TypeCode.Null) {
            #Null;
        } else if (type_code == TypeCode.Bool) {
            let ?bool = value_iter.next() else return Debug.trap("CandidMap: Expected bool value");
            #Bool(bool == 1);
        } else if (type_code == TypeCode.Empty) {
            #Empty;
        } else if (type_code == TypeCode.Int) {
            #Int(ByteUtils.to_int(value_iter));
        } else if (type_code == TypeCode.Int8) {
            #Int8(ByteUtils.to_int8(value_iter));
        } else if (type_code == TypeCode.Int16) {
            #Int16(ByteUtils.to_int16(value_iter));
        } else if (type_code == TypeCode.Int32) {
            #Int32(ByteUtils.to_int32(value_iter));
        } else if (type_code == TypeCode.Int64) {
            #Int64(ByteUtils.to_int64(value_iter));
        } else if (type_code == TypeCode.Nat) {
            #Nat(ByteUtils.to_nat(value_iter));
        } else if (type_code == TypeCode.Nat8) {
            #Nat8(ByteUtils.to_nat8(value_iter));
        } else if (type_code == TypeCode.Nat16) {
            #Nat16(ByteUtils.to_nat16(value_iter));
        } else if (type_code == TypeCode.Nat32) {
            #Nat32(ByteUtils.to_nat32(value_iter));
        } else if (type_code == TypeCode.Nat64) {
            #Nat64(ByteUtils.to_nat64(value_iter));
        } else if (type_code == TypeCode.Float) {
            #Float(ByteUtils.to_float64(value_iter));
        } else if (type_code == TypeCode.Text) {
            let text_bytes = Blob.fromArray(Iter.toArray(value_iter));
            let ?text = Text.decodeUtf8(text_bytes) else Debug.trap("CandidMap: Invalid utf8 text");
            #Text(text);
        } else if (type_code == TypeCode.Array) {
            let ?nested_type_code = value_iter.next() else return Debug.trap("CandidMap: Expected nested type code");

            if (nested_type_code != TypeCode.Nat8) {
                Debug.trap("CandidMap: Expected nested type code to be Nat8");
            };

            let blob = Blob.fromArray(Iter.toArray(value_iter));
            #Blob(blob);
        } else if (type_code == TypeCode.Principal) {
            let principal = Principal.fromBlob(Blob.fromArray(Iter.toArray(value_iter)));
            #Principal(principal);
        } else if (type_code == TypeCode.Option) {
            let nested_candid = decode_candid_value(value_iter);
            #Option(nested_candid);
        } else {
            Debug.trap("CandidMap: Unsupported type code");
        };

        candid_value

    };

    func encode_candid_value(candid_value : Candid) : (Iter<Nat8>, Nat) {
        switch (candid_value) {
            case (#Null) {
                ([TypeCode.Null].vals(), 1);
            };
            case (#Bool(b)) {
                let arr = [TypeCode.Bool, if (b) 1 else 0] : [Nat8];
                (arr.vals(), arr.size());
            };
            case (#Empty) {
                let arr = [TypeCode.Empty];
                (arr.vals(), arr.size());
            };
            case (#Int(i)) {
                let bytes = ByteUtils.from_int(i);

                let iter = Itertools.prepend(
                    TypeCode.Int,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Int8(i)) {

                let bytes = ByteUtils.from_int8(i);

                let iter = Itertools.prepend(
                    TypeCode.Int8,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Int16(i)) {
                let bytes = ByteUtils.from_int16(i);

                let iter = Itertools.prepend(
                    TypeCode.Int16,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Int32(i)) {
                let bytes = ByteUtils.from_int32(i);

                let iter = Itertools.prepend(
                    TypeCode.Int32,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Int64(i)) {
                let bytes = ByteUtils.from_int64(i);

                let iter = Itertools.prepend(
                    TypeCode.Int64,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Nat(i)) {
                let bytes = ByteUtils.from_nat(i);

                let iter = Itertools.prepend(
                    TypeCode.Nat,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Nat8(i)) {
                let bytes = ByteUtils.from_nat8(i);

                let iter = Itertools.prepend(
                    TypeCode.Nat8,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Nat16(i)) {
                let bytes = ByteUtils.from_nat16_be(i);

                let iter = Itertools.prepend(
                    TypeCode.Nat16,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Nat32(i)) {
                let bytes = ByteUtils.from_nat32_be(i);

                let iter = Itertools.prepend(
                    TypeCode.Nat32,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Nat64(i)) {
                let bytes = ByteUtils.from_nat64_be(i);

                let iter = Itertools.prepend(
                    TypeCode.Nat64,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Float(f)) {
                let bytes = ByteUtils.from_float64(f);

                let iter = Itertools.prepend(
                    TypeCode.Float,
                    bytes.vals(),
                );

                (iter, bytes.size() + 1);
            };
            case (#Text(t)) {

                let text_bytes = Text.encodeUtf8(t);
                let iter = Itertools.prepend(
                    TypeCode.Text,
                    text_bytes.vals(),
                );

                (iter, text_bytes.size() + 1);
            };
            case (#Blob(b)) {
                let iter = Itertools.chain(
                    [TypeCode.Array, TypeCode.Nat8].vals(),
                    Blob.toArray(b).vals(),
                );

                (iter, 2 + b.size());

            };
            case (#Principal(p)) {
                let p_bytes = Principal.toBlob(p);

                let iter = Itertools.prepend(
                    TypeCode.Principal,
                    Blob.toArray(Principal.toBlob(p)).vals(),
                );

                (iter, 1 + p_bytes.size());
            };
            case (#Option(candid_value)) {
                let (nested_iter, size) = encode_candid_value(candid_value);
                let iter = Itertools.prepend(
                    TypeCode.Option,
                    nested_iter,
                );

                (iter, size + 1);
            };
            case (_) {
                Debug.trap("CandidMap: Does not support encoding of collection types : " # debug_show candid_value);
            };
        };

    };

};
