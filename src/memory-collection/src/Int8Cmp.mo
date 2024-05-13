import Prim "mo:prim";
import FloatModule "mo:base/Float";

/// Int8 Comparators
/// Comparators that return an `Int8` type instead of an `Order` variant
/// Comparator functions adapted from the base module
module Cmp {

    public func Blob(a : Blob, b : Blob) : Int8 {
        Prim.blobCompare(a, b);
    };

    public func Bool(a : Bool, b : Bool) : Int8 {
        if (a == b) { 0 } else if (a) { 1 } else { -1 };
    };

    public func Char(a : Char, b : Char) : Int8 {
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 };
    };

    func isFloatNegative(number : Float) : Bool {
        FloatModule.copySign(1.0, number) < 0.0;
    };

    public func Float(a : Float, b : Float) : Int8 {
        if (FloatModule.isNaN(a)) {
            if (isFloatNegative(a)) {
                if (FloatModule.isNaN(b) and isFloatNegative(b)) { 0 } else {
                    -1;
                };
            } else {
                if (FloatModule.isNaN(b) and not isFloatNegative(b)) { 0 } else {
                    +1;
                };
            };
        } else if (FloatModule.isNaN(b)) {
            if (isFloatNegative(b)) {
                +1;
            } else {
                -1;
            };
        } else {
            if (a == b) { 0 } else if (a < b) { -1 } else { +1 };
        };
    };

    public func Int(a: Int, b: Int) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };  

    public func Int8(a: Int8, b: Int8) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };  

    public func Int16(a: Int16, b: Int16) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };  

    public func Int32(a: Int32, b: Int32) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };

    public func Int64(a: Int64, b: Int64) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };

    public func Nat(a : Nat, b : Nat) : Int8 {
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };

    public func Nat8(a: Nat8, b: Nat8) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };  

    public func Nat16(a: Nat16, b: Nat16) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };  

    public func Nat32(a: Nat32, b: Nat32) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };

    public func Nat64(a: Nat64, b: Nat64) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };

    public func Principal(a: Principal, b: Principal) : Int8{
        if (a < b) { -1 } else if (a > b) { 1 } else { 0 }
    };

    public func Text(a: Text, b: Text): Int8 {
        Prim.textCompare(a, b)
    };

};
