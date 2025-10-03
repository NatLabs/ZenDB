import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Nat32 "mo:base@0.16.0/Nat32";
import Result "mo:base@0.16.0/Result";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Float "mo:base@0.16.0/Float";
import Int "mo:base@0.16.0/Int";
import Int32 "mo:base@0.16.0/Int32";
import Nat64 "mo:base@0.16.0/Nat64";
import Int16 "mo:base@0.16.0/Int16";
import Int64 "mo:base@0.16.0/Int64";
import Int8 "mo:base@0.16.0/Int8";
import Nat16 "mo:base@0.16.0/Nat16";
import Nat8 "mo:base@0.16.0/Nat8";
// Additional imports for new casting functionality
import Principal "mo:base@0.16.0/Principal";
import Bool "mo:base@0.16.0/Bool";
import Char "mo:base@0.16.0/Char";

import T "../Types";

module Cast {
    type CandidType = T.CandidType;
    type Candid = T.Candid;
    type Result<A, B> = T.Result<A, B>;
    type Schema = T.Schema;

    // Helper function to convert bool to numeric value
    private func bool_to_int(b : Bool) : Int {
        if (b) 1 else 0;
    };

    // Helper function to convert numeric types to text
    private func numeric_to_text(candid : Candid) : Text {
        switch (candid) {
            case (#Nat(n)) Nat.toText(n);
            case (#Nat8(n)) Nat8.toText(n);
            case (#Nat16(n)) Nat16.toText(n);
            case (#Nat32(n)) Nat32.toText(n);
            case (#Nat64(n)) Nat64.toText(n);
            case (#Int(n)) Int.toText(n);
            case (#Int8(n)) Int8.toText(n);
            case (#Int16(n)) Int16.toText(n);
            case (#Int32(n)) Int32.toText(n);
            case (#Int64(n)) Int64.toText(n);
            case (#Float(f)) Float.toText(f);
            case (_) Debug.trap("numeric_to_text: Unsupported numeric type");
        };
    };

    public func cast_to_bool(candid : Candid) : T.Result<Candid, Text> {
        let converted = switch (candid) {
            case (#Bool(_)) candid;
            // Convert numeric types: 0 = false, anything else = true
            case (#Nat(n)) #Bool(n != 0);
            case (#Nat8(n)) #Bool(n != 0);
            case (#Nat16(n)) #Bool(n != 0);
            case (#Nat32(n)) #Bool(n != 0);
            case (#Nat64(n)) #Bool(n != 0);
            case (#Int(n)) #Bool(n != 0);
            case (#Int8(n)) #Bool(n != 0);
            case (#Int16(n)) #Bool(n != 0);
            case (#Int32(n)) #Bool(n != 0);
            case (#Int64(n)) #Bool(n != 0);
            case (#Float(f)) #Bool(f != 0.0);
            // Convert text: "true", "1" = true; "false", "0", "" = false
            case (#Text(t)) {
                let lower = Text.toLowercase(t);
                #Bool(lower == "true" or lower == "1");
            };
            case (unsupported_types) return #err(
                "Can't convert " # debug_show (unsupported_types) # " to #Bool()"
            );
        };
        #ok(converted);
    };

    public func cast_to_principal(candid : Candid) : T.Result<Candid, Text> {
        let converted = switch (candid) {
            case (#Principal(_)) candid;
            case (#Text(t)) {
                switch (Principal.fromText(t)) {
                    case (principal) #Principal(principal);
                };
            };
            case (#Blob(b)) #Principal(Principal.fromBlob(b));
            case (unsupported_types) return #err(
                "Can't convert " # debug_show (unsupported_types) # " to #Principal()"
            );
        };
        #ok(converted);
    };

    public func cast_to_nat(candid : Candid) : T.Result<Candid, Text> {

        func int_to_nat_if_positive(int : Int) : T.Result<Candid, Text> {
            if (int >= 0) {
                #ok(#Nat(Int.abs(int)));
            } else {
                return #err("Cannot convert " # debug_show candid # " with negative value to #Nat()");
            };
        };

        let converted = switch (candid) {
            case (#Nat(_)) candid;
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

            // Convert bool to nat (true=1, false=0)
            case (#Bool(b)) #Nat(Int.abs(bool_to_int(b)));

            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat()"
            );

        };

        #ok(converted);
    };

    public func cast_to_nat8(candid : Candid) : T.Result<Candid, Text> {

        func int_to_nat8_if_within_bounds(int : Int) : T.Result<Candid, Text> {
            if (int >= 0 and int <= 255) {
                #ok(#Nat8(Nat8.fromNat(Int.abs(int))));
            } else {
                return #err("Cannot convert " # debug_show candid # " with value " # Int.toText(int) # " to #Nat8()");
            };
        };

        let converted = switch (candid) {
            case (#Nat8(_)) candid;
            case (#Nat16(nat16)) return int_to_nat8_if_within_bounds(Nat16.toNat(nat16));
            case (#Nat32(nat32)) return int_to_nat8_if_within_bounds(Nat32.toNat(nat32));
            case (#Nat64(nat64)) return int_to_nat8_if_within_bounds(Nat64.toNat(nat64));
            case (#Nat(n)) return int_to_nat8_if_within_bounds(n);
            case (#Int(int)) return int_to_nat8_if_within_bounds(int);
            case (#Int8(int8)) return int_to_nat8_if_within_bounds(Int8.toInt(int8));
            case (#Int16(int16)) return int_to_nat8_if_within_bounds(Int16.toInt(int16));
            case (#Int32(int32)) return int_to_nat8_if_within_bounds(Int32.toInt(int32));
            case (#Int64(int64)) return int_to_nat8_if_within_bounds(Int64.toInt(int64));
            case (#Float(f)) return int_to_nat8_if_within_bounds(Float.toInt(f));

            // Convert bool to nat8 (true=1, false=0)
            case (#Bool(b)) return int_to_nat8_if_within_bounds(bool_to_int(b));

            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat8()"
            );

        };

        #ok(converted);

    };

    public func cast_to_nat16(candid : Candid) : T.Result<Candid, Text> {
        func int_to_nat16_if_within_bounds(int : Int) : T.Result<Candid, Text> {
            if (int >= 0 and int <= Nat16.toNat(Nat16.maximumValue)) {
                #ok(#Nat16(Nat16.fromNat(Int.abs(int))));
            } else {
                return #err("Cannot convert " # debug_show candid # " with value " # Int.toText(int) # " to #Nat16()");
            };
        };

        let converted = switch (candid) {
            case (#Nat16(_)) candid;
            case (#Nat8(nat8)) return int_to_nat16_if_within_bounds(Nat8.toNat(nat8));
            case (#Nat32(nat32)) return int_to_nat16_if_within_bounds(Nat32.toNat(nat32));
            case (#Nat64(nat64)) return int_to_nat16_if_within_bounds(Nat64.toNat(nat64));
            case (#Nat(n)) return int_to_nat16_if_within_bounds(n);
            case (#Int(int)) return int_to_nat16_if_within_bounds(int);
            case (#Int8(int8)) return int_to_nat16_if_within_bounds(Int8.toInt(int8));
            case (#Int16(int16)) return int_to_nat16_if_within_bounds(Int16.toInt(int16));
            case (#Int32(int32)) return int_to_nat16_if_within_bounds(Int32.toInt(int32));
            case (#Int64(int64)) return int_to_nat16_if_within_bounds(Int64.toInt(int64));
            case (#Float(f)) return int_to_nat16_if_within_bounds(Float.toInt(f));
            case (#Bool(b)) return int_to_nat16_if_within_bounds(bool_to_int(b));
            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat16()"
            );
        };

        #ok(converted);
    };

    public func cast_to_nat32(candid : Candid) : T.Result<Candid, Text> {
        func int_to_nat32_if_within_bounds(int : Int) : T.Result<Candid, Text> {
            if (int >= 0 and int <= Nat32.toNat(Nat32.maximumValue)) {
                #ok(#Nat32(Nat32.fromNat(Int.abs(int))));
            } else {
                return #err("Cannot convert " # debug_show candid # " with value " # Int.toText(int) # " to #Nat32()");
            };
        };

        let converted = switch (candid) {
            case (#Nat32(n)) candid;
            case (#Nat8(nat8)) return int_to_nat32_if_within_bounds(Nat8.toNat(nat8));
            case (#Nat16(nat16)) return int_to_nat32_if_within_bounds(Nat16.toNat(nat16));
            case (#Nat64(nat64)) return int_to_nat32_if_within_bounds(Nat64.toNat(nat64));
            case (#Nat(n)) return int_to_nat32_if_within_bounds(n);
            case (#Int(int)) return int_to_nat32_if_within_bounds(int);
            case (#Int8(int8)) return int_to_nat32_if_within_bounds(Int8.toInt(int8));
            case (#Int16(int16)) return int_to_nat32_if_within_bounds(Int16.toInt(int16));
            case (#Int32(int32)) return int_to_nat32_if_within_bounds(Int32.toInt(int32));
            case (#Int64(int64)) return int_to_nat32_if_within_bounds(Int64.toInt(int64));
            case (#Float(f)) return int_to_nat32_if_within_bounds(Float.toInt(f));
            case (#Bool(b)) return int_to_nat32_if_within_bounds(bool_to_int(b));

            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat32()"
            );
        };

        #ok(converted);
    };

    public func cast_to_nat64(candid : Candid) : T.Result<Candid, Text> {

        func int_to_nat64_if_positive(int : Int) : T.Result<Candid, Text> {
            if (int >= 0 and int <= Nat64.toNat(Nat64.maximumValue)) {
                #ok(#Nat64(Nat64.fromNat(Int.abs(int))));
            } else {
                return #err("Cannot convert " # debug_show candid # " with negative value to #Nat64()");
            };
        };

        let converted = switch (candid) {
            case (#Nat64(n)) candid;
            case (#Nat(n)) #Nat64(Nat64.fromNat(n));
            case (#Nat8(n)) #Nat64(Nat64.fromNat(Nat8.toNat(n)));
            case (#Nat16(n)) #Nat64(Nat64.fromNat(Nat16.toNat(n)));
            case (#Nat32(n)) #Nat64(Nat64.fromNat(Nat32.toNat(n)));
            case (#Int(int)) return int_to_nat64_if_positive(int);
            case (#Int8(int8)) return int_to_nat64_if_positive(Int8.toInt(int8));
            case (#Int16(int16)) return int_to_nat64_if_positive(Int16.toInt(int16));
            case (#Int32(int32)) return int_to_nat64_if_positive(Int32.toInt(int32));
            case (#Int64(int64)) return int_to_nat64_if_positive(Int64.toInt(int64));
            case (#Float(f)) return int_to_nat64_if_positive(Float.toInt(f));
            case (#Bool(b)) return int_to_nat64_if_positive(bool_to_int(b));
            case (unsupported_nat_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_nat_cast_types) # " to #Nat64()"
            );
        };

        #ok(converted);
    };

    public func cast_to_int8(candid : Candid) : T.Result<Candid, Text> {

        func int_to_int8_if_within_bounds(int : Int) : T.Result<Candid, Text> {
            if (int >= -128 and int <= 127) {
                #ok(#Int8(Int8.fromInt(int)));
            } else {
                return #err("Cannot convert " # debug_show candid # " with value " # Int.toText(int) # " to #Int8()");
            };
        };

        let converted = switch (candid) {
            case (#Int8(n)) candid;
            case (#Int(n)) return int_to_int8_if_within_bounds(n);
            case (#Int16(n)) return int_to_int8_if_within_bounds(Int16.toInt(n));
            case (#Int32(n)) return int_to_int8_if_within_bounds(Int32.toInt(n));
            case (#Int64(n)) return int_to_int8_if_within_bounds(Int64.toInt(n));
            case (#Nat(n)) return int_to_int8_if_within_bounds(n);
            case (#Nat8(n)) return int_to_int8_if_within_bounds(Nat8.toNat(n));
            case (#Nat16(n)) return int_to_int8_if_within_bounds(Nat16.toNat(n));
            case (#Nat32(n)) return int_to_int8_if_within_bounds(Nat32.toNat(n));
            case (#Nat64(n)) return int_to_int8_if_within_bounds(Nat64.toNat(n));
            case (#Float(f)) return int_to_int8_if_within_bounds(Float.toInt(f));
            case (#Bool(b)) return int_to_int8_if_within_bounds(bool_to_int(b));
            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int8()"
            );
        };

        #ok(converted);

    };

    public func cast_to_int16(candid : Candid) : T.Result<Candid, Text> {

        func int_to_int16_if_within_bounds(int : Int) : T.Result<Candid, Text> {
            if (int >= Int16.toInt(Int16.minimumValue) and int <= Int16.toInt(Int16.maximumValue)) {
                #ok(#Int16(Int16.fromInt(int)));
            } else {
                return #err("Cannot convert " # debug_show candid # " with value " # Int.toText(int) # " to #Int16()");
            };
        };

        let converted = switch (candid) {
            case (#Int16(n)) candid;
            case (#Int(n)) return int_to_int16_if_within_bounds(n);
            case (#Int8(n)) return int_to_int16_if_within_bounds(Int8.toInt(n));
            case (#Int32(n)) return int_to_int16_if_within_bounds(Int32.toInt(n));
            case (#Int64(n)) return int_to_int16_if_within_bounds(Int64.toInt(n));
            case (#Nat(n)) return int_to_int16_if_within_bounds(n);
            case (#Nat8(n)) return int_to_int16_if_within_bounds(Nat8.toNat(n));
            case (#Nat16(n)) return int_to_int16_if_within_bounds(Nat16.toNat(n));
            case (#Nat32(n)) return int_to_int16_if_within_bounds(Nat32.toNat(n));
            case (#Nat64(n)) return int_to_int16_if_within_bounds(Nat64.toNat(n));
            case (#Float(f)) return int_to_int16_if_within_bounds(Float.toInt(f));
            case (#Bool(b)) return int_to_int16_if_within_bounds(bool_to_int(b));
            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int16()"
            );
        };

        #ok(converted);

    };

    public func cast_to_int32(candid : Candid) : T.Result<Candid, Text> {

        func int_to_int32_if_within_bounds(int : Int) : T.Result<Candid, Text> {
            if (int >= Int32.toInt(Int32.minimumValue) and int <= Int32.toInt(Int32.maximumValue)) {
                #ok(#Int32(Int32.fromInt(int)));
            } else {
                return #err("Cannot convert " # debug_show candid # " with value " # Int.toText(int) # " to #Int32()");
            };
        };

        let converted = switch (candid) {
            case (#Int32(n)) candid;
            case (#Int(n)) return int_to_int32_if_within_bounds(n);
            case (#Int8(n)) return int_to_int32_if_within_bounds(Int8.toInt(n));
            case (#Int16(n)) return int_to_int32_if_within_bounds(Int16.toInt(n));
            case (#Int64(n)) return int_to_int32_if_within_bounds(Int64.toInt(n));
            case (#Nat(n)) return int_to_int32_if_within_bounds(n);
            case (#Nat8(n)) return int_to_int32_if_within_bounds(Nat8.toNat(n));
            case (#Nat16(n)) return int_to_int32_if_within_bounds(Nat16.toNat(n));
            case (#Nat32(n)) return int_to_int32_if_within_bounds(Nat32.toNat(n));
            case (#Nat64(n)) return int_to_int32_if_within_bounds(Nat64.toNat(n));
            case (#Float(f)) return int_to_int32_if_within_bounds(Float.toInt(f));
            case (#Bool(b)) return int_to_int32_if_within_bounds(bool_to_int(b));
            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int32()"
            );
        };

        #ok(converted)

    };

    public func cast_to_int64(candid : Candid) : T.Result<Candid, Text> {

        func int_to_int64_if_within_bounds(int : Int) : T.Result<Candid, Text> {
            if (int >= Int64.toInt(Int64.minimumValue) and int <= Int64.toInt(Int64.maximumValue)) {
                #ok(#Int64(Int64.fromInt(int)));
            } else {
                return #err("Cannot convert " # debug_show candid # " with value " # Int.toText(int) # " to #Int64()");
            };
        };

        let converted = switch (candid) {
            case (#Int64(n)) candid;
            case (#Int(n)) return int_to_int64_if_within_bounds(n);
            case (#Int8(n)) return int_to_int64_if_within_bounds(Int8.toInt(n));
            case (#Int16(n)) return int_to_int64_if_within_bounds(Int16.toInt(n));
            case (#Int32(n)) return int_to_int64_if_within_bounds(Int32.toInt(n));
            case (#Nat(n)) return int_to_int64_if_within_bounds(n);
            case (#Nat8(n)) return int_to_int64_if_within_bounds(Nat8.toNat(n));
            case (#Nat16(n)) return int_to_int64_if_within_bounds(Nat16.toNat(n));
            case (#Nat32(n)) return int_to_int64_if_within_bounds(Nat32.toNat(n));
            case (#Nat64(n)) return int_to_int64_if_within_bounds(Nat64.toNat(n));
            case (#Float(f)) return int_to_int64_if_within_bounds(Float.toInt(f));
            case (#Bool(b)) return int_to_int64_if_within_bounds(bool_to_int(b));
            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int64()"
            );
        };

        #ok(converted)

    };

    public func cast_to_float(candid : Candid) : T.Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Float(n)) candid;
            case (#Int(n)) #Float(Float.fromInt(n));
            case (#Nat(n)) #Float(Float.fromInt(n));
            case (#Int8(n)) #Float(Float.fromInt(Int8.toInt(n)));
            case (#Int16(n)) #Float(Float.fromInt(Int16.toInt(n)));
            case (#Int32(n)) #Float(Float.fromInt(Int32.toInt(n)));
            case (#Int64(n)) #Float(Float.fromInt(Int64.toInt(n)));
            case (#Nat8(n)) #Float(Float.fromInt(Nat8.toNat(n)));
            case (#Nat16(n)) #Float(Float.fromInt(Nat16.toNat(n)));
            case (#Nat32(n)) #Float(Float.fromInt(Nat32.toNat(n)));
            case (#Nat64(n)) #Float(Float.fromInt(Nat64.toNat(n)));

            // Convert bool to float (true=1.0, false=0.0)
            case (#Bool(b)) #Float(Float.fromInt(bool_to_int(b)));

            case (unsupported_float_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_float_cast_types) # " to #Float()"
            );
        };

        #ok(converted)

    };

    public func cast_to_int(candid : Candid) : T.Result<Candid, Text> {

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

            // Convert bool to int (true=1, false=0)
            case (#Bool(b)) #Int(bool_to_int(b));

            case (unsupported_int_cast_types) return #err(
                "Can't convert " # debug_show (unsupported_int_cast_types) # " to #Int()"
            );

        };

        #ok(converted)

    };

    public func cast_to_text(candid : Candid) : T.Result<Candid, Text> {
        let converted = switch (candid) {
            case (#Text(_)) candid;
            case (#Blob(b)) switch (Text.decodeUtf8(b)) {
                case (?t) #Text(t);
                case (null) return #err("cast_to_text: Could not decode blob to utf8");
            };
            // Convert numeric types to text representation
            case (
                #Nat(_) or #Nat8(_) or #Nat16(_) or #Nat32(_) or #Nat64(_) or
                #Int(_) or #Int8(_) or #Int16(_) or #Int32(_) or #Int64(_) or #Float(_)
            ) {
                #Text(numeric_to_text(candid));
            };
            // Convert bool to text
            case (#Bool(b)) #Text(Bool.toText(b));
            // Convert principal to text
            case (#Principal(p)) #Text(Principal.toText(p));
            case (_) return #err("cast_to_text: Can't convert " # debug_show candid # " to #Text");

        };

        #ok(converted);

    };

    public func cast_to_blob(candid : Candid) : T.Result<Candid, Text> {
        let converted = switch (candid) {
            case (#Blob(b)) candid;
            case (#Text(t)) #Blob(Text.encodeUtf8(t));
            // Convert principal to blob
            case (#Principal(p)) #Blob(Principal.toBlob(p));
            case (_) return #err("cast_to_blob: Can't convert " # debug_show candid # " to #Blob");
        };

        #ok(converted);

    };

    public func cast(candid_type : CandidType, value_to_cast : Candid) : T.Result<Candid, Text> {

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
            case (#Bool(_)) cast_to_bool(value_to_cast);
            case (#Principal(_)) cast_to_principal(value_to_cast);
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
            // case (#Tuple(tuples), #Record(documents)) {
            //     if (documents.size() != tuples.size()) return #err("Tuple size mismatch: expected " # debug_show (tuples.size()) # ", got " # debug_show (documents.size()));

            //     for ((i, (key, _)) in Itertools.enumerate(documents.vals())) {
            //         if (key != Nat.toText(i)) return #err("Tuple key mismatch: expected " # Nat.toText(i) # ", got " # debug_show (key));
            //     };

            //     for ((i, (key, value)) in Itertools.enumerate(documents.vals())) {
            //         let res = validate(tuples[i], value);
            //         let #ok(_) = res else return send_error(res);
            //     };

            //     #ok;

            // };
            case (#Record(fields)) {
                let #Record(documents) = value_to_cast else return #err("Expected a document");

                if (fields.size() != documents.size()) {
                    return #err("Record size mismatch: " # debug_show (("schema", fields.size()), ("document", documents.size())));
                };

                let sorted_fields = Array.sort(
                    fields,
                    func(a : (Text, Schema), b : (Text, Schema)) : T.Order {
                        Text.compare(a.0, b.0);
                    },
                );

                let sorted_records = Array.sort(
                    documents,
                    func(a : (Text, Candid), b : (Text, Candid)) : T.Order {
                        Text.compare(a.0, b.0);
                    },
                );

                let buffer = Buffer.Buffer<(Text, Candid)>(documents.size());

                // should sort fields and documents
                var i = 0;
                while (i < fields.size()) {
                    let field = sorted_fields[i];
                    let document = sorted_records[i];

                    if (field.0 != document.0) return #err("Record field mismatch: " # debug_show (("field", field.0), ("document", document.0)) # debug_show (fields, documents));

                    let value = switch (cast(field.1, document.1)) {
                        case (#ok(c)) c;
                        case (#err(e)) return #err(e);
                    };

                    buffer.add((field.0, value));

                    i += 1;
                };

                #ok(#Record(Buffer.toArray(buffer)));
            };
            case (#Array(inner)) {
                let #Array(documents) = value_to_cast else return #err("Expected an array");
                var i = 0;
                let buffer = Buffer.Buffer<Candid>(documents.size());
                while (i < documents.size()) {
                    let val = switch (cast(inner, documents[i])) {
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
                // Debug.print("document: " # debug_show (document));

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
