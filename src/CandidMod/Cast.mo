import Array "mo:base/Array";
import Debug "mo:base/Debug";
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

import T "../Types";

module Cast {
    type CandidType = T.CandidType;
    type Candid = T.Candid;
    type Result<A, B> = T.Result<A, B>;
    type Schema = T.Schema;

    public func cast_to_nat(candid : Candid) : Result<Candid, Text> {

        func int_to_nat_if_positive(int : Int) : Result<Candid, Text> {
            if (int >= 0) {
                #ok(#Nat(Int.abs(int)));
            } else {
                return #err("Cannot convert " # debug_show candid # " with negative value to #Nat()");
            };
        };

        let converted = switch (candid) {
            case (#Nat(n)) candid;
            case (#Nat8(nat8)) #Nat(Nat8.toNat(nat8));
            case (#Nat16(nat16)) #Nat(Nat16.toNat(nat16));
            case (#Nat32(nat32)) #Nat(Nat32.toNat(nat32));
            case (#Nat64(nat64)) #Nat(Nat64.toNat(nat64));

            case (#Int(int)) return int_to_nat_if_positive(int);
            case (#Int8(int8)) return int_to_nat_if_positive(Int8.toInt(int8));
            case (#Int16(int16)) return int_to_nat_if_positive(Int16.toInt(int16));
            case (#Int32(int32)) return int_to_nat_if_positive(Int32.toInt(int32));
            case (#Int64(int64)) return int_to_nat_if_positive(Int64.toInt(int64));

            case (#Float(f)) return int_to_nat_if_positive(Float.toInt(f));

            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat()"
            );

        };

        #ok(converted);
    };

    public func cast_to_nat8(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Nat8(n)) candid;
            case (#Nat(n)) #Nat8(Nat8.fromNat(n));
            case (#Int(int)) #Nat8(Nat8.fromNat(Int.abs(int)));

            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat8()"
            );

        };

        #ok(converted);

    };

    public func cast_to_nat16(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Nat16(n)) candid;
            case (#Nat(n)) #Nat16(Nat16.fromNat(n));
            case (#Int(int)) #Nat16(Nat16.fromNat(Int.abs(int)));
            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat16()"
            );
        };

        #ok(converted);
    };

    public func cast_to_nat32(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Nat32(n)) candid;
            case (#Nat(n)) #Nat32(Nat32.fromNat(n));
            case (#Int(int)) #Nat32(Nat32.fromNat(Int.abs(int)));
            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat32()"
            );
        };

        #ok(converted);
    };

    public func cast_to_nat64(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Nat64(n)) candid;
            case (#Nat(n)) #Nat64(Nat64.fromNat(n));
            case (#Int(int)) #Nat64(Nat64.fromNat(Int.abs(int)));
            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat64()"
            );
        };

        #ok(converted);
    };

    public func cast_to_int8(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Int8(n)) candid;
            case (#Int(n)) #Int8(Int8.fromInt(n));
            case (#Nat(n)) #Int8(Int8.fromInt(n));
            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int8()"
            );
        };

        #ok(converted);

    };

    public func cast_to_int16(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Int16(n)) candid;
            case (#Int(n)) #Int16(Int16.fromInt(n));
            case (#Nat(n)) #Int16(Int16.fromInt(n));
            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int16()"
            );
        };

        #ok(converted);

    };

    public func cast_to_int32(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Int32(n)) candid;
            case (#Int(n)) #Int32(Int32.fromInt(n));
            case (#Nat(n)) #Int32(Int32.fromInt(n));
            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int32()"
            );
        };

        #ok(converted)

    };

    public func cast_to_int64(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Int64(n)) candid;
            case (#Int(n)) #Int64(Int64.fromInt(n));
            case (#Nat(n)) #Int64(Int64.fromInt(n));
            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int64()"
            );
        };

        #ok(converted)

    };

    public func cast_to_float(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Float(n)) candid;
            case (#Int(n)) #Float(Float.fromInt(n));
            case (#Nat(n)) #Float(Float.fromInt(n));
            case (unsupported_float_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_float_cast_types) # " to #Float()"
            );
        };

        #ok(converted)

    };

    public func cast_to_int(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Int(n)) candid;
            case (#Int8(n)) #Int(Int8.toInt(n));
            case (#Int16(n)) #Int(Int16.toInt(n));
            case (#Int32(n)) #Int(Int32.toInt(n));
            case (#Int64(n)) #Int(Int64.toInt(n));

            case (#Nat(n)) #Int(n);
            case (#Nat8(n)) #Int(Nat8.toNat(n));
            case (#Nat16(n)) #Int(Nat16.toNat(n));
            case (#Nat32(n)) #Int(Nat32.toNat(n));
            case (#Nat64(n)) #Int(Nat64.toNat(n));

            case (#Float(n)) #Int(Float.toInt(n));

            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int()"
            );

        };

        #ok(converted)

    };

    public func cast_to_text(candid : Candid) : Result<Candid, Text> {
        let converted = switch (candid) {
            case (#Text(_)) candid;
            case (#Blob(b)) switch (Text.decodeUtf8(b)) {
                case (?t) #Text(t);
                case (null) return #err("cast_to_text: Could not decode blob to utf8");
            };
            case (_) return #err("cast_to_text: Can't convert " # debug_show candid # " to #Text");

        };

        #ok(converted);

    };

    public func cast_to_blob(candid : Candid) : Result<Candid, Text> {
        let converted = switch (candid) {
            case (#Blob(b)) candid;
            case (#Text(t)) #Blob(Text.encodeUtf8(t));
            case (_) return #err("cast_to_blob: Can't convert " # debug_show candid # " to #Blob");
        };

        #ok(converted);

    };

    public func cast(candid_type : CandidType, value_to_cast : Candid) : Result<Candid, Text> {

        switch (candid_type) {
            case (#Nat(_)) cast_to_nat(value_to_cast);
            case (#Nat8(_)) cast_to_nat8(value_to_cast);
            case (#Nat16(_)) cast_to_nat16(value_to_cast);
            case (#Nat32(_)) cast_to_nat32(value_to_cast);
            case (#Nat64(_)) cast_to_nat64(value_to_cast);
            case (#Int8(_)) cast_to_int8(value_to_cast);
            case (#Int16(_)) cast_to_int16(value_to_cast);
            case (#Int32(_)) cast_to_int32(value_to_cast);
            case (#Int64(_)) cast_to_int64(value_to_cast);
            case (#Int(_)) cast_to_int(value_to_cast);
            case (#Float(_)) cast_to_float(value_to_cast);
            case (#Text(_)) cast_to_text(value_to_cast);
            case (#Blob(_)) cast_to_blob(value_to_cast);
            case (#Option(inner)) {
                switch (value_to_cast) {
                    case (#Null) #ok(#Null);
                    case (_) switch (cast(inner, value_to_cast)) {
                        case (#ok(c)) #ok(#Option(c));
                        case (#err(e)) #err(e);
                    };
                };
            };
            // case (schema, #Option(inner)) {
            //     if (inner == #Null) return #ok;

            //     validate(schema, inner);
            // };
            // case (#Tuple(tuples), #Record(records)) {
            //     if (records.size() != tuples.size()) return #err("Tuple size mismatch: expected " # debug_show (tuples.size()) # ", got " # debug_show (records.size()));

            //     for ((i, (key, _)) in Itertools.enumerate(records.vals())) {
            //         if (key != Nat.toText(i)) return #err("Tuple key mismatch: expected " # Nat.toText(i) # ", got " # debug_show (key));
            //     };

            //     for ((i, (key, value)) in Itertools.enumerate(records.vals())) {
            //         let res = validate(tuples[i], value);
            //         let #ok(_) = res else return send_error(res);
            //     };

            //     #ok;

            // };
            case (#Record(fields)) {
                let #Record(records) = value_to_cast else return #err("Expected a record");

                if (fields.size() != records.size()) {
                    return #err("Record size mismatch: " # debug_show (("schema", fields.size()), ("record", records.size())));
                };

                let sorted_fields = Array.sort(
                    fields,
                    func(a : (Text, Schema), b : (Text, Schema)) : T.Order {
                        Text.compare(a.0, b.0);
                    },
                );

                let sorted_records = Array.sort(
                    records,
                    func(a : (Text, Candid), b : (Text, Candid)) : T.Order {
                        Text.compare(a.0, b.0);
                    },
                );

                let buffer = Buffer.Buffer<(Text, Candid)>(records.size());

                // should sort fields and records
                var i = 0;
                while (i < fields.size()) {
                    let field = sorted_fields[i];
                    let record = sorted_records[i];

                    if (field.0 != record.0) return #err("Record field mismatch: " # debug_show (("field", field.0), ("record", record.0)) # debug_show (fields, records));

                    let value = switch (cast(field.1, record.1)) {
                        case (#ok(c)) c;
                        case (#err(e)) return #err(e);
                    };

                    buffer.add((field.0, value));

                    i += 1;
                };

                #ok(#Record(Buffer.toArray(buffer)));
            };
            case (#Array(inner)) {
                let #Array(records) = value_to_cast else return #err("Expected an array");
                var i = 0;
                let buffer = Buffer.Buffer<Candid>(records.size());
                while (i < records.size()) {
                    let val = switch (cast(inner, records[i])) {
                        case (#ok(c)) c;
                        case (#err(e)) return #err(e);
                    };
                    buffer.add(val);

                    i += 1;
                };

                #ok(#Array(Buffer.toArray(buffer)));

            };
            case (#Variant(variants)) {
                let #Variant((record_key, nested_record)) = value_to_cast else return #err("Expected a variant");

                let result = Array.find<(Text, T.Schema)>(
                    variants,
                    func((variant_name, _) : (Text, Schema)) : Bool {
                        variant_name == record_key;
                    },
                );

                // Debug.print("schema: " # debug_show (schema));
                // Debug.print("record: " # debug_show (record));

                switch (result) {
                    case (null) return #err("Variant not found in schema");
                    case (?(name, variant)) switch (cast(variant, nested_record)) {
                        case (#ok(c)) #ok(#Variant((name, c)));
                        case (#err(e)) return #err(e);
                    };
                };
            };
            case (t) Debug.trap("need to implement cast for " # debug_show t);
        };

    };

};
