import Array "mo:core@2.4/Array";
import Debug "mo:core@2.4/Debug";
import Text "mo:core@2.4/Text";
import Nat32 "mo:core@2.4/Nat32";
import Result "mo:core@2.4/Result";
import Order "mo:core@2.4/Order";
import Iter "mo:core@2.4/Iter";
import Buffer "mo:base@0.16/Buffer";
import Nat "mo:core@2.4/Nat";
import Option "mo:core@2.4/Option";
import Hash "mo:base@0.16/Hash";
import Float "mo:core@2.4/Float";
import Int "mo:core@2.4/Int";
import Int32 "mo:core@2.4/Int32";
import Blob "mo:core@2.4/Blob";
import Nat64 "mo:core@2.4/Nat64";
import Int16 "mo:core@2.4/Int16";
import Int64 "mo:core@2.4/Int64";
import Int8 "mo:core@2.4/Int8";
import Nat16 "mo:core@2.4/Nat16";
import Nat8 "mo:core@2.4/Nat8";
import Func "mo:core@2.4/Func";
import Char "mo:core@2.4/Char";

import Itertools "mo:itertools@0.2/Iter";

import T "../Types";
import Utils "../Utils";
import Runtime "mo:core@2.4/Runtime";

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
            case (#Text(text)) Runtime.trap("Can't convert text to float");
            case (#Bool(bool)) Runtime.trap("Can't convert bool to float");
            case (compound_candid) Runtime.trap("Can't convert compound candid '" # debug_show compound_candid # "' to float");
        };
    };

    public func from_float(self : Candid, float : Float) : Candid {

        switch (self) {
            case (#Nat(_)) #Nat(Int.abs(Float.toInt(float)));
            case (#Int(_)) #Int(Float.toInt(float));
            case (#Float(_)) #Float(float);
            case (#Option(opt)) #Option(from_float(opt, float));
            case (#Null) Runtime.trap("Can't convert null to float. Need to pass in the candid type as well");
            case (#Nat8(nat8)) #Nat8(Nat8.fromNat(Int.abs(Float.toInt(float))));
            case (#Nat16(nat16)) #Nat16(Nat16.fromNat(Int.abs(Float.toInt(float))));
            case (#Nat32(nat32)) #Nat32(Nat32.fromNat(Int.abs(Float.toInt(float))));
            case (#Nat64(nat64)) #Nat64(Nat64.fromNat(Int.abs(Float.toInt(float))));
            case (#Int8(int8)) #Int8(Int8.fromInt(Float.toInt(float)));
            case (#Int16(int16)) #Int16(Int16.fromInt(Float.toInt(float)));
            case (#Int32(int32)) #Int32(Int32.fromInt(Float.toInt(float)));
            case (#Int64(int64)) #Int64(Int64.fromInt(Float.toInt(float)));
            case (#Text(text)) Runtime.trap("Can't convert float to text");
            case (#Bool(bool)) Runtime.trap("Can't convert float to bool");
            case (compound_candid) Runtime.trap("Can't convert from float to compound type '" # debug_show compound_candid # "'");
        };

    };

    public func add(self : Candid, other : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        let c = a + b;

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func sub(self : Candid, other : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        if (a < b) {
            return #err("Cannot complete #sub operation because " # debug_show (self) # " < " # debug_show (other));
        };

        let c = a - b;

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func mul(self : Candid, other : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        let c = a * b;

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func div(self : Candid, other : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        if (b == 0) {
            return #err("Cannot complete #div operation because " # debug_show (self) # " cannot be divided by zero - " # debug_show (other));
        };

        let c = a / b;

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func mod(self : Candid, other : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        if (b == 0) {
            return #err("Cannot complete #mod operation because " # debug_show (self) # " cannot be divided by zero - " # debug_show (other));
        };

        let c = a % b;

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func pow(self : Candid, other : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        let c = a ** b;

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func sqrt(self : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);

        if (a < 0) {
            return #err("Cannot complete #sqrt operation because " # debug_show (self) # " is negative");
        };

        let c = Float.sqrt(a);

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func abs(self : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);

        let c = Float.abs(a);

        let res = from_float(self, c);
        #ok(res);

    };

    public func neg(self : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);

        let c = Float.neg(a);

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func floor(self : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);

        let c = Float.floor(a);

        let res = from_float(self, c);
        #ok(res);

    };

    public func ceil(self : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);

        let c = Float.ceil(a);

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func min(self : Candid, other : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        let c = Float.min(a, b);

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func max(self : Candid, other : Candid) : T.Result<Candid, Text> {

        let a = to_float(self);
        let b = to_float(other);

        let c = Float.max(a, b);

        // let candid = from_float(self, c);
        #ok(#Float(c));

    };

    public func trim(self : Candid, toTrim : Text) : T.Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let trimmed = Text.trim(text, #text(toTrim));
                #ok(#Text(trimmed));
            };
            case (#Null) #ok(#Null);
            case (#Option(#Null)) #ok(#Null);
            case (#Option(inner)) trim(inner, toTrim);
            case (other) {
                return #err("Cannot complete #trim operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func lowercase(self : Candid) : T.Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let lower = Text.toLower(text);
                #ok(#Text(lower));
            };
            case (#Null) #ok(#Null);
            case (#Option(#Null)) #ok(#Null);
            case (#Option(inner)) lowercase(inner);
            case (other) {
                return #err("Cannot complete #lowercase operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func uppercase(self : Candid) : T.Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let upper = Text.toUpper(text);
                #ok(#Text(upper));
            };
            case (#Null) #ok(#Null);
            case (#Option(#Null)) #ok(#Null);
            case (#Option(inner)) uppercase(inner);
            case (other) {
                return #err("Cannot complete #uppercase operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func trim_start(self : Candid, toTrim : Text) : T.Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let trimmed = Text.trimStart(text, #text(toTrim));
                #ok(#Text(trimmed));
            };
            case (#Null) #ok(#Null);
            case (#Option(#Null)) #ok(#Null);
            case (#Option(inner)) trim_start(inner, toTrim);
            case (other) {
                return #err("Cannot complete #trim_start operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func trim_end(self : Candid, toTrim : Text) : T.Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let trimmed = Text.trimEnd(text, #text(toTrim));
                #ok(#Text(trimmed));
            };
            case (#Null) #ok(#Null);
            case (#Option(#Null)) #ok(#Null);
            case (#Option(inner)) trim_end(inner, toTrim);
            case (other) {
                return #err("Cannot complete #trim_end operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func replaceSubText(self : Candid, toReplace : Text, replacement : Text) : T.Result<Candid, Text> {
        switch (self) {
            case (#Text(text)) {
                let replaced = Text.replace(text, #text(toReplace), replacement);
                #ok(#Text(replaced));
            };
            case (#Null) #ok(#Null);
            case (#Option(#Null)) #ok(#Null);
            case (#Option(inner)) replaceSubText(inner, toReplace, replacement);
            case (other) {
                return #err("Cannot complete #replaceSubTexts operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func slice(self : Candid, start : Nat, end : Nat) : T.Result<Candid, Text> {
        switch (self) {
            case (#Null) return #ok(#Null);
            case (#Option(#Null)) return #ok(#Null);
            case (#Option(inner)) return slice(inner, start, end);
            case (#Text(text)) {
                let chars_iter = text.chars();
                let chars = Array.tabulate(
                    text.size(),
                    func(_ : Nat) : Char {
                        switch (chars_iter.next()) {
                            case (?char) char;
                            case (none) Runtime.trap("Unexpected end of chars iterator");
                        };
                    },
                );

                let sub_chars = Array.sliceToArray(chars, start, end).vals();
                let sub_chars_mapped = Iter.map(sub_chars, Text.fromChar);
                let sub_text = Text.join(sub_chars_mapped, "");

                #ok(#Text(sub_text));
            };
            case (#Blob(blob)) {
                let bytes = Blob.toArray(blob);
                let sub_bytes_iter = Array.sliceToArray(bytes, start, end).vals();
                let sub_bytes_array = Iter.toArray(sub_bytes_iter);
                let sub_blob = Blob.fromArray(sub_bytes_array);
                #ok(#Blob(sub_blob));
            };
            case (other) {
                return #err("Cannot complete #slice operation on " # debug_show (self) # ". Only text is supported");
            };
        };

    };

    public func concat(self : Candid, other : Candid) : T.Result<Candid, Text> {
        switch (self, other) {
            case (#Text(text), #Text(other_text)) {
                let concatenated = text # other_text;
                #ok(#Text(concatenated));
            };
            case (#Null, _) #ok(#Null);
            case (_, #Null) #ok(#Null);
            case (#Option(#Null), _) #ok(#Null);
            case (_, #Option(#Null)) #ok(#Null);
            case (#Option(inner), other_val) concat(inner, other_val);
            case (self_val, #Option(inner)) concat(self_val, inner);
            case (other) {
                return #err("Cannot complete #concat operation on " # debug_show (self, other) # ". Only text is supported");
            };
        };

    };

    public func concatBytes(self : Candid, bytes : Blob) : T.Result<Candid, Text> {
        switch (self) {
            case (#Null) #ok(#Null);
            case (#Option(#Null)) #ok(#Null);
            case (#Option(inner)) concatBytes(inner, bytes);
            case (#Blob(blob)) {
                let concatenated = Utils.concat_blob(blob, bytes);
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
        let a = to_float(self);
        let b = to_float(other);

        Float.compare(a, b);
    };

    public func size(self : Candid) : Nat {
        switch (self) {
            case (#Text(text)) text.size();
            case (#Blob(blob)) blob.size();
            case (#Array(array)) array.size();
            case (#Option(inner)) size(inner);
            case (#Null) 0;
            case (other) Runtime.trap("Cannot get size of " # debug_show (self));
        };
    };

    public module Multi {
        public func add(values : Iter<Candid>) : T.Result<Candid, Text> {

            let floats = Iter.toArray(Iter.map(values, to_float));
            //    Debug.print("floats: " # debug_show floats);
            let res = Itertools.fold(floats.vals(), 0.0, Float.add);

            #ok(#Float(res))

        };

        public func sub(values : Iter<Candid>) : T.Result<Candid, Text> {

            let floats = Iter.map(values, to_float);
            let ?first = floats.next() else return #err("expected at least one value in #sub");
            let res = Itertools.fold(floats, first, Float.sub);

            #ok(#Float(res))

        };

        public func mul(values : Iter<Candid>) : T.Result<Candid, Text> {

            let floats = Iter.map(values, to_float);
            let res = Itertools.fold(floats, 1.0, Float.mul);

            #ok(#Float(res))

        };

        public func div(values : Iter<Candid>) : T.Result<Candid, Text> {

            let floats = Iter.map(values, to_float);
            let ?first = floats.next() else return #err("expected at least one value in #div");
            let res = Itertools.fold(floats, first, Float.div);

            #ok(#Float(res))

        };

        public func concat(values : Iter<Candid>) : T.Result<Candid, Text> {

            let ?res = Itertools.reduce(
                values,
                func(acc : Candid, curr : Candid) : Candid {
                    let #ok(concatenated) = CandidOps.concat(acc, curr) else Runtime.trap("Failed to concatenate " # debug_show (acc, curr) # " in #concat");
                    concatenated;
                },
            ) else return #err("Failed to reduce values in #concat");

            #ok(res);

        };

    };

};
