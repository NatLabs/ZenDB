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
import Func "mo:base/Func";
import Char "mo:base/Char";

import Itertools "mo:itertools/Iter";

import T "../Types";
import Utils "../Utils";

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

    public func toFloat(candid : Candid) : Float {
        switch (candid) {
            case (#Nat(nat)) Float.fromInt(nat);
            case (#Int(int)) Float.fromInt(int);
            case (#Float(float)) float;
            case (#Option(opt)) toFloat(opt);
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

    public func fromFloat(self : Candid, float : Float) : Candid {

        switch (self) {
            case (#Nat(_)) #Nat(Int.abs(Float.toInt(float)));
            case (#Int(_)) #Int(Float.toInt(float));
            case (#Float(_)) #Float(float);
            case (#Option(opt)) #Option(fromFloat(opt, float));
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

        let a = toFloat(self);
        let b = toFloat(other);

        let c = a + b;

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func sub(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = toFloat(self);
        let b = toFloat(other);

        if (a < b) {
            return #err("Cannot complete #sub operation because " # debug_show (self) # " < " # debug_show (other));
        };

        let c = a - b;

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func mul(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = toFloat(self);
        let b = toFloat(other);

        let c = a * b;

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func div(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = toFloat(self);
        let b = toFloat(other);

        if (b == 0) {
            return #err("Cannot complete #div operation because " # debug_show (self) # " cannot be divided by zero - " # debug_show (other));
        };

        let c = a / b;

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func mod(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = toFloat(self);
        let b = toFloat(other);

        if (b == 0) {
            return #err("Cannot complete #mod operation because " # debug_show (self) # " cannot be divided by zero - " # debug_show (other));
        };

        let c = a % b;

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func pow(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = toFloat(self);
        let b = toFloat(other);

        let c = a ** b;

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func sqrt(self : Candid) : Result<Candid, Text> {

        let a = toFloat(self);

        if (a < 0) {
            return #err("Cannot complete #sqrt operation because " # debug_show (self) # " is negative");
        };

        let c = Float.sqrt(a);

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func abs(self : Candid) : Result<Candid, Text> {

        let a = toFloat(self);

        let c = Float.abs(a);

        let res = fromFloat(self, c);
        #ok(res);

    };

    public func neg(self : Candid) : Result<Candid, Text> {

        let a = toFloat(self);

        let c = Float.neg(a);

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func floor(self : Candid) : Result<Candid, Text> {

        let a = toFloat(self);

        let c = Float.floor(a);

        let res = fromFloat(self, c);
        #ok(res);

    };

    public func ceil(self : Candid) : Result<Candid, Text> {

        let a = toFloat(self);

        let c = Float.ceil(a);

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func min(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = toFloat(self);
        let b = toFloat(other);

        let c = Float.min(a, b);

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func max(self : Candid, other : Candid) : Result<Candid, Text> {

        let a = toFloat(self);
        let b = toFloat(other);

        let c = Float.max(a, b);

        // let candid = fromFloat(self, c);
        #ok(#Float(c));

    };

    public func trim(self : Candid, toTrim : Text) : Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let trimmed = Text.trim(text, #text(toTrim));
                #ok(#Text(trimmed));
            };
            case (other) {
                return #err("Cannot complete #trim operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func lowercase(self : Candid) : Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let lower = Text.toLowercase(text);
                #ok(#Text(lower));
            };
            case (other) {
                return #err("Cannot complete #lowercase operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func uppercase(self : Candid) : Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let upper = Text.toUppercase(text);
                #ok(#Text(upper));
            };
            case (other) {
                return #err("Cannot complete #uppercase operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func trimStart(self : Candid, toTrim : Text) : Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let trimmed = Text.trimStart(text, #text(toTrim));
                #ok(#Text(trimmed));
            };
            case (other) {
                return #err("Cannot complete #trim_start operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func trimEnd(self : Candid, toTrim : Text) : Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let trimmed = Text.trimEnd(text, #text(toTrim));
                #ok(#Text(trimmed));
            };
            case (other) {
                return #err("Cannot complete #trim_end operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func replaceSubText(self : Candid, toReplace : Text, replacement : Text) : Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let replaced = Text.replace(text, #text(toReplace), replacement);
                #ok(#Text(replaced));
            };
            case (other) {
                return #err("Cannot complete #replace_sub_texts operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func slice(self : Candid, start : Nat, end : Nat) : Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let chars_iter = text.chars();
                let chars = Array.tabulate(
                    text.size(),
                    func(_ : Nat) : Char {
                        switch (chars_iter.next()) {
                            case (?char) char;
                            case (none) Debug.trap("Unexpected end of chars iterator");
                        };
                    },
                );

                let sub_chars = Iter.map(Array.slice(chars, start, end), Text.fromChar);
                let sub_text = Text.join("", sub_chars);

                #ok(#Text(sub_text));
            };
            case (#Blob(blob)) {
                let bytes = Blob.toArray(blob);
                let sub_bytes_iter = Array.slice(bytes, start, end);
                let sub_bytes_array = Iter.toArray(sub_bytes_iter);
                let sub_blob = Blob.fromArray(sub_bytes_array);
                #ok(#Blob(sub_blob));
            };
            case (other) {
                return #err("Cannot complete #slice operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func concat(self : Candid, other : Candid) : Result<Candid, Text> {
        switch (self, other) {
            case (#Text(text), #Text(other_text)) {
                let concatenated = text # other_text;
                #ok(#Text(concatenated));
            };
            case (other) {
                return #err("Cannot complete #concat operation on " # debug_show (self, other) # ". Only text is supported");
            };
        };

    };

    public func concatBytes(self : Candid, bytes : Blob) : Result<Candid, Text> {
        switch (self) {
            case (#Blob(blob)) {
                let concatenated = Utils.concatBlob(blob, bytes);
                #ok(#Blob(concatenated));
            };
            case (#Text(text)) {
                let other_text = Text.fromIter(
                    Iter.map<Nat8, Char>(
                        bytes.vals(),
                        Func.compose(Char.fromNat32, Func.compose(Nat32.fromNat, Nat8.toNat)),
                    )
                );
                let concatenated = text # other_text;
                #ok(#Text(concatenated));
            };
            case (other) {
                return #err("Cannot complete #concat operation on " # debug_show (self, other) # ". Only blob is supported");
            };
        };

    };

    public func compare(self : Candid, other : Candid) : T.Order {
        let a = toFloat(self);
        let b = toFloat(other);

        Float.compare(a, b);
    };

    public func size(self : Candid) : Nat {
        switch (self) {
            case (#Text(text)) text.size();
            case (#Blob(blob)) blob.size();
            case (#Array(array)) array.size();
            case (#Option(inner)) size(inner);
            case (#Null) 0;
            case (other) Debug.trap("Cannot get size of " # debug_show (self));
        };
    };

    public module Multi {
        public func add(values : Iter<Candid>) : Result<Candid, Text> {

            let floats = Iter.toArray(Iter.map(values, toFloat));
            //    Debug.print("floats: " # debug_show floats);
            let res = Itertools.fold(floats.vals(), 0.0, Float.add);

            #ok(#Float(res))

        };

        public func sub(values : Iter<Candid>) : Result<Candid, Text> {

            let floats = Iter.map(values, toFloat);
            let ?first = floats.next() else return #err("expected at least one value in #sub");
            let res = Itertools.fold(floats, first, Float.sub);

            #ok(#Float(res))

        };

        public func mul(values : Iter<Candid>) : Result<Candid, Text> {

            let floats = Iter.map(values, toFloat);
            let res = Itertools.fold(floats, 1.0, Float.mul);

            #ok(#Float(res))

        };

        public func div(values : Iter<Candid>) : Result<Candid, Text> {

            let floats = Iter.map(values, toFloat);
            let ?first = floats.next() else return #err("expected at least one value in #div");
            let res = Itertools.fold(floats, first, Float.div);

            #ok(#Float(res))

        };

        public func concat(values : Iter<Candid>) : Result<Candid, Text> {

            let ?res = Itertools.reduce(
                values,
                func(acc : Candid, curr : Candid) : Candid {
                    let #ok(concatenated) = CandidOps.concat(acc, curr) else Debug.trap("Failed to concatenate " # debug_show (acc, curr) # " in #concat");
                    concatenated;
                },
            ) else return #err("Failed to reduce values in #concat");

            #ok(res);

        };

    };

};
