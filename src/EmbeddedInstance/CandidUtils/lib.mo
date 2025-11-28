import Debug "mo:base@0.16.0/Debug";
import Array "mo:base@0.16.0/Array";
import Text "mo:base@0.16.0/Text";
import Nat32 "mo:base@0.16.0/Nat32";
import Result "mo:base@0.16.0/Result";
import Order "mo:base@0.16.0/Order";
import Iter "mo:base@0.16.0/Iter";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Option "mo:base@0.16.0/Option";
import Hash "mo:base@0.16.0/Hash";
import Float "mo:base@0.16.0/Float";
import Int "mo:base@0.16.0/Int";
import Int32 "mo:base@0.16.0/Int32";
import Blob "mo:base@0.16.0/Blob";
import Nat64 "mo:base@0.16.0/Nat64";
import Int16 "mo:base@0.16.0/Int16";
import Int64 "mo:base@0.16.0/Int64";
import Int8 "mo:base@0.16.0/Int8";
import Nat16 "mo:base@0.16.0/Nat16";
import Nat8 "mo:base@0.16.0/Nat8";
import Char "mo:base@0.16.0/Char";
import Principal "mo:base@0.16.0/Principal";
import Bool "mo:base@0.16.0/Bool";

import Itertools "mo:itertools@0.2.2/Iter";
import { sort_candid_type } "mo:serde@3.4.0/Candid/Blob/CandidUtils";

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
    public func getNextValue(value : T.CandidQuery) : T.CandidQuery {
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
            case (#Option(inner_value)) getNextValue(inner_value);
            case (#Empty) #Maximum;
            case (#Maximum) #Maximum;
            case (#Minimum) #Minimum;
            case (candid_value) Debug.trap("getNextValue(): Does not support this type: " # debug_show (candid_value))

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

    public func getPrevValue(value : T.CandidQuery) : T.CandidQuery {
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
                    case (null) Debug.trap("getPrevValue(): Failed to decode text from blob: " # debug_show (blob));
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
            case (#Option(inner_value)) getPrevValue(inner_value);
            case (#Null) #Minimum;
            case (#Empty) #Minimum;
            case (#Maximum) #Maximum;
            case (#Minimum) #Minimum;
            case (_) Debug.trap("getPrevValue(): Does not support this type: " # debug_show (value));

        };

    };

    public func fromCandidQuery(value : T.CandidQuery) : T.Candid {
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
            case (#Option(n)) #Option(fromCandidQuery(n));
            case (#Array(arr)) #Array(Array.tabulate<T.Candid>(arr.size(), func(i : Nat) : T.Candid = fromCandidQuery(arr[i])));
            case (#Record(fields)) #Record(
                Array.tabulate<(Text, T.Candid)>(
                    fields.size(),
                    func(i : Nat) : (Text, T.Candid) {
                        let (field_name, field_value) = fields[i];
                        (field_name, fromCandidQuery(field_value));
                    },
                )
            );
            case (#Variant(field_name, field_value)) #Variant(
                field_name,
                fromCandidQuery(field_value),
            );
            case (_candid_value) Debug.trap("fromCandidQuery(): Does not support this type: " # debug_show (_candid_value));
        };
    };

    public func toCandidQuery(value : T.Candid) : T.CandidQuery {
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
            case (_candid_value) Debug.trap("toCandidQuery(): Does not support this type: " # debug_show (_candid_value));
        };
    };

    public func unwrapOption(value : T.Candid) : T.Candid {
        switch (value) {
            case (#Option(nested_value)) unwrapOption(nested_value);
            case (_) value;
        };
    };

    public func inheritOptionsFromType(candid_type : T.CandidType, value : T.Candid) : T.Candid {
        switch (candid_type) {
            case (#Option(nested_type)) inheritOptionsFromType(nested_type, #Option(value));
            case (_) value;
        };
    };

    public func min(schema : T.Schema, a : T.CandidQuery, b : T.CandidQuery) : T.CandidQuery {
        let cmp = compare_ignore_option(schema, a, b);
        if (cmp == #less or cmp == #equal) a else b;
    };

    public func max(schema : T.Schema, a : T.CandidQuery, b : T.CandidQuery) : T.CandidQuery {
        let cmp = compare_ignore_option(schema, a, b);
        if (cmp == #greater or cmp == #equal) a else b;
    };

    // schema is added here to get the order of the #Variant type
    public func compare(schema : T.Schema, a : T.CandidQuery, b : T.CandidQuery) : T.Order {

        switch (schema, a, b) {
            // The #Minimum variant is used in queries to represent the minimum value
            case (_, #Minimum, _) #less;
            case (_, _, #Minimum) #greater;

            // The #Maximum variant is used in queries to represent the maximum value
            case (_, #Maximum, _) #greater;
            case (_, _, #Maximum) #less;

            case (_, #Null, #Null) #equal;
            case (_, #Empty, #Empty) #equal;

            case (_, _, #Null) #greater;
            case (_, #Null, _) #less;

            case (_, #Text(a), #Text(b)) Text.compare(a, b);
            case (_, #Blob(a), #Blob(b)) Blob.compare(a, b);
            case (_, #Nat(a), #Nat(b)) Nat.compare(a, b);
            case (_, #Nat8(a), #Nat8(b)) Nat8.compare(a, b);
            case (_, #Nat16(a), #Nat16(b)) Nat16.compare(a, b);
            case (_, #Nat32(a), #Nat32(b)) Nat32.compare(a, b);
            case (_, #Nat64(a), #Nat64(b)) Nat64.compare(a, b);
            case (_, #Principal(a), #Principal(b)) Principal.compare(a, b);
            case (_, #Float(a), #Float(b)) Float.compare(a, b);
            case (_, #Bool(a), #Bool(b)) Bool.compare(a, b);
            case (_, #Int(a), #Int(b)) Int.compare(a, b);
            case (_, #Int8(a), #Int8(b)) Int8.compare(a, b);
            case (_, #Int16(a), #Int16(b)) Int16.compare(a, b);
            case (_, #Int32(a), #Int32(b)) Int32.compare(a, b);
            case (_, #Int64(a), #Int64(b)) Int64.compare(a, b);

            case (_, #Option(a), #Option(b)) {
                switch (a, b) {
                    case (#Null, #Null) #equal;
                    case (#Null, _) #less;
                    case (_, #Null) #greater;
                    case (_, _) compare(schema, a, b);
                };
            };
            case (#Variant(schema), #Variant(a), #Variant(b)) {

                let ?i = Array.indexOf<(Text, Any)>(
                    a,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("compare: variant not found in schema");

                let ?j = Array.indexOf<(Text, Any)>(
                    b,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("compare: variant not found in schema");

                let res = Nat.compare(i, j);

                if (res == #equal) {
                    compare(schema[i].1, a.1, b.1);
                } else {
                    res;
                };

            };

            // case (_, #Array(a), #Array(b)) {
            //     // compare the length of the arrays
            //     let len_cmp = Nat.compare(a.size(), b.size());
            //     if (len_cmp != #equal) return len_cmp;

            //     let min_len = Nat.min(a.size(), b.size());
            //     for (i in Iter.range(0, min_len - 1)) {
            //         let cmp_result = compare(a[i], b[i]);
            //         if (cmp_result != #equal) return cmp_result;
            //     };
            //     Nat.compare(a.size(), b.size());
            // };

            case (schema, a, b) {
                // Debug.print(debug_show (a, b));
                Debug.trap("compare: unexpected candid type " # debug_show { schema; a; b });
            };
        };
    };

    public func compare_ignore_option(schema : T.Schema, a : T.CandidQuery, b : T.CandidQuery) : T.Order {

        switch (schema, a, b) {
            // The #Minimum variant is used in queries to represent the minimum value
            case (_, #Minimum, _) #less;
            case (_, _, #Minimum) #greater;

            // The #Maximum variant is used in queries to represent the maximum value
            case (_, #Maximum, _) #greater;
            case (_, _, #Maximum) #less;

            case (_, #Null, #Null) #equal;
            case (_, #Empty, #Empty) #equal;

            case (_, _, #Null) #greater;
            case (_, #Null, _) #less;

            case (_, #Text(a), #Text(b)) Text.compare(a, b);
            case (_, #Blob(a), #Blob(b)) Blob.compare(a, b);
            case (_, #Nat(a), #Nat(b)) Nat.compare(a, b);
            case (_, #Nat8(a), #Nat8(b)) Nat8.compare(a, b);
            case (_, #Nat16(a), #Nat16(b)) Nat16.compare(a, b);
            case (_, #Nat32(a), #Nat32(b)) Nat32.compare(a, b);
            case (_, #Nat64(a), #Nat64(b)) Nat64.compare(a, b);
            case (_, #Principal(a), #Principal(b)) Principal.compare(a, b);
            case (_, #Float(a), #Float(b)) Float.compare(a, b);
            case (_, #Bool(a), #Bool(b)) Bool.compare(a, b);
            case (_, #Int(a), #Int(b)) Int.compare(a, b);
            case (_, #Int8(a), #Int8(b)) Int8.compare(a, b);
            case (_, #Int16(a), #Int16(b)) Int16.compare(a, b);
            case (_, #Int32(a), #Int32(b)) Int32.compare(a, b);
            case (_, #Int64(a), #Int64(b)) Int64.compare(a, b);

            case (_, #Option(a), #Option(b)) {
                switch (a, b) {
                    case (#Null, #Null) #equal;
                    case (#Null, _) #less;
                    case (_, #Null) #greater;
                    case (_, _) compare(schema, a, b);
                };
            };
            case (_, #Option(a), b) compare_ignore_option(schema, a, b);
            case (_, a, #Option(b)) compare_ignore_option(schema, a, b);
            case (#Variant(schema), #Variant(a), #Variant(b)) {

                let ?i = Array.indexOf<(Text, Any)>(
                    a,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("compare: variant not found in schema");

                let ?j = Array.indexOf<(Text, Any)>(
                    b,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("compare: variant not found in schema");

                let res = Nat.compare(i, j);

                if (res == #equal) {
                    compare(schema[i].1, a.1, b.1);
                } else {
                    res;
                };

            };

            // case (_, #Array(a), #Array(b)) {
            //     // compare the length of the arrays
            //     let len_cmp = Nat.compare(a.size(), b.size());
            //     if (len_cmp != #equal) return len_cmp;

            //     let min_len = Nat.min(a.size(), b.size());
            //     for (i in Iter.range(0, min_len - 1)) {
            //         let cmp_result = compare(a[i], b[i]);
            //         if (cmp_result != #equal) return cmp_result;
            //     };
            //     Nat.compare(a.size(), b.size());
            // };

            case (schema, a, b) {
                // Debug.print(debug_show (a, b));
                Debug.trap("compare: unexpected candid type " # debug_show { schema; a; b });
            };
        };
    };

    public func generate_default_value(schema : T.Schema) : T.Result<T.Candid, Text> {
        let candid : T.Candid = switch (schema) {
            case (#Empty) #Empty;
            case (#Null) #Null;
            case (#Text) #Text("");
            case (#Nat) #Nat(0);
            case (#Nat8) #Nat8(0);
            case (#Nat16) #Nat16(0);
            case (#Nat32) #Nat32(0);
            case (#Nat64) #Nat64(0);
            case (#Int) #Int(0);
            case (#Int8) #Int8(0);
            case (#Int16) #Int16(0);
            case (#Int32) #Int32(0);
            case (#Int64) #Int64(0);
            case (#Float) #Float(0.0);
            case (#Bool) #Bool(false);
            case (#Principal) #Principal(Principal.fromBlob("\04")); // anonymous principal
            case (#Blob) #Blob("");
            case (#Option(inner)) switch (generate_default_value(inner)) {
                case (#ok(value)) #Option(value);
                case (#err(err)) return #err(err);
            };
            case (#Array(inner)) switch (generate_default_value(inner)) {
                case (#ok(val)) #Array([val]);
                case (#err(err)) return #err(err);
            };
            case (#Tuple(tuples)) {

                let buffer = Buffer.Buffer<T.Candid>(tuples.size());

                for (i in Itertools.range(0, tuples.size())) {
                    let tuple_type = tuples[i];
                    let value = switch (generate_default_value(tuple_type)) {
                        case (#ok(value)) value;
                        case (#err(err)) return #err(err);
                    };

                    buffer.add(value);
                };

                #Tuple(Buffer.toArray(buffer))

            };
            case (#Record(fields) or #Map(fields)) {

                let buffer = Buffer.Buffer<(Text, T.Candid)>(fields.size());
                for (i in Itertools.range(0, fields.size())) {
                    let (name, record_type) = fields[i];
                    let value = switch (generate_default_value(record_type)) {
                        case (#ok(value)) value;
                        case (#err(err)) return #err(err);
                    };

                    buffer.add((name, value));
                };

                #Record(Buffer.toArray(buffer));

            };

            case (#Variant(variants)) {
                let (name, variant_type) = variants[0];

                switch (generate_default_value(variant_type)) {
                    case (#ok(value)) #Variant((name, value));
                    case (#err(err)) return #err(err);
                };
            };

            case (_) return #err("generate_default_value: unexpected schema type " # debug_show (schema));
        };

        #ok(candid);
    };

};
