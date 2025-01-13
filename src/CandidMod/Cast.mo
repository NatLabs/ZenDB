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

    public func cast_to_int(candid : Candid) : Result<Candid, Text> {

        let converted = switch (candid) {
            case (#Int(n)) candid;
            case (#Int8(n)) #Int(Int8.toInt(n));
            case (#Int16(n)) #Int(Int16.toInt(n));
            case (#Int32(n)) #Int(Int32.toInt(n));
            case (#Int63(n)) #Int(Int64.toInt(n));

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

    // public func cast_to_nat8(candid : Candid) : Result<Candid, Text> {

    //     switch (candid) {
    //         case ()

    //     }

    // };

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

    public func cast(candid_type : CandidType, value_to_cast : Candid) : Result<Candid, Text> {

        switch (candid_type) {
            case (#Nat(_)) cast_to_nat(value_to_cast);
            case (#Int(_)) cast_to_int(value_to_cast);
            case (#Option(inner)) {
                switch (value_to_cast) {
                    case (#Null) #ok(#Null);
                    case (_) switch (cast(inner, value_to_cast)) {
                        case (#ok(c)) #ok(#Option(c));
                        case (#err(e)) #err(e);
                    };
                };
            };
            case (t) Debug.trap("need to implement cast for " # debug_show t);
        };

    };

};
