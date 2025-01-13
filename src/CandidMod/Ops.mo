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

import Itertools "mo:itertools/Iter";

import T "../Types";

/// Handles numeric update operations to candid values
///
/// Floats are used as the base type for each of these operations because
/// every other type can be converted to a Float without losing any information.
/// Or more specifically the Float type cannot be converted to any other type without losing some information to be able to perform numeric operations in that type.
///
/// Side effects of using a Floats as the base numeric value is that values are limited to 64 bits

module CandidOps {

    type CandidType = T.CandidType;
    type Candid = T.Candid;
    type Result<A, B> = T.Result<A, B>;
    type Iter<A> = T.Iter<A>;

    public func to_float(candid : Candid) : Float {
        switch (candid) {
            case (#Nat(nat)) Float.fromInt(nat);
            case (#Int(int)) Float.fromInt(int);
            case (#Float(float)) float;
            case (#Option(opt)) to_float(opt);
            case (#Null) 0.0;
            case (#Nat8(nat8)) Float.fromInt(Nat8.toNat(nat8));
            case (#Nat16(nat16)) Float.fromInt(Nat16.toNat(nat16));
            case (#Nat32(nat32)) Float.fromInt(Nat32.toNat(nat32));
            case (#Nat64(nat64)) Float.fromInt(Nat64.toNat(nat64));
            case (#Int8(int8)) Float.fromInt(Int8.toInt(int8));
            case (#Int16(int16)) Float.fromInt(Int16.toInt(int16));
            case (#Int32(int32)) Float.fromInt(Int32.toInt(int32));
            case (#Int64(int64)) Float.fromInt(Int64.toInt(int64));
            case (#Text(text)) Debug.trap("Can't convert text to float");
            case (#Bool(bool)) Debug.trap("Can't convert bool to float");
            case (compound_candid) Debug.trap("Can't convert compound candid '" # debug_show compound_candid # "' to float");
        };
    };

    public func from_float(self : Candid, float : Float) : Candid {

        switch (self) {
            case (#Nat(_)) #Nat(Int.abs(Float.toInt(float)));
            case (#Int(_)) #Int(Float.toInt(float));
            case (#Float(_)) #Float(float);
            case (#Option(opt)) #Option(from_float(opt, float));
            case (#Null) Debug.trap("Can't convert null to float. Need to pass in the candid type as well");
            case (#Nat8(nat8)) #Nat8(Nat8.fromNat(Int.abs(Float.toInt(float))));
            case (#Nat16(nat16)) #Nat16(Nat16.fromNat(Int.abs(Float.toInt(float))));
            case (#Nat32(nat32)) #Nat32(Nat32.fromNat(Int.abs(Float.toInt(float))));
            case (#Nat64(nat64)) #Nat64(Nat64.fromNat(Int.abs(Float.toInt(float))));
            case (#Int8(int8)) #Int8(Int8.fromInt(Float.toInt(float)));
            case (#Int16(int16)) #Int16(Int16.fromInt(Float.toInt(float)));
            case (#Int32(int32)) #Int32(Int32.fromInt(Float.toInt(float)));
            case (#Int64(int64)) #Int64(Int64.fromInt(Float.toInt(float)));
            case (#Text(text)) Debug.trap("Can't convert float to text");
            case (#Bool(bool)) Debug.trap("Can't convert float to bool");
            case (compound_candid) Debug.trap("Can't convert from float to compound type '" # debug_show compound_candid # "'");
        };

    };

    public func add(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        let c = a + b;

        let candid = from_float(self, c);

        #ok(candid);

    };

    public func sub(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        if (a < b) {
            return #err("Cannot complete #sub operation because " # debug_show (self) # " < " # debug_show (other));
        };

        let c = a - b;

        let res = from_float(self, c);
        #ok(res)

    };

    public func mul(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        let c = a * b;

        let res = from_float(self, c);

        #ok(res)

    };

    public func div(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        if (b == 0) {
            return #err("Cannot complete #div operation because " # debug_show (self) # " cannot be divided by zero - " # debug_show (other));
        };

        let c = a / b;

        let res = from_float(self, c);
        #ok(res);

    };

    public module Multi {
        public func add(values : Iter<Candid>) : Result<Candid, Text> {

            let floats = Iter.toArray(Iter.map(values, to_float));
         //    Debug.print("floats: " # debug_show floats);
            let res = Itertools.fold(floats.vals(), 0.0, Float.add);

            #ok(#Float(res))

        };

        public func sub(values : Iter<Candid>) : Result<Candid, Text> {

            let floats = Iter.map(values, to_float);
            let ?first = floats.next() else return #err("expected at least one value in #sub");
            let res = Itertools.fold(floats, first, Float.sub);

            #ok(#Float(res))

        };

        public func mul(values : Iter<Candid>) : Result<Candid, Text> {

            let floats = Iter.map(values, to_float);
            let res = Itertools.fold(floats, 1.0, Float.mul);

            #ok(#Float(res))

        };

        public func div(values : Iter<Candid>) : Result<Candid, Text> {

            let floats = Iter.map(values, to_float);
            let ?first = floats.next() else return #err("expected at least one value in #div");
            let res = Itertools.fold(floats, first, Float.div);

            #ok(#Float(res))

        };

    };

};
