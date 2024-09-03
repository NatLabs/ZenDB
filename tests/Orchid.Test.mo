import Debug "mo:base/Debug";
import Collection "../src/Collection";

let a = Collection.Orchid.blobify.to_blob([#Nat(138)]);
let b = Collection.Orchid.blobify.to_blob([#Nat(999_240)]);
Debug.print("138: " # debug_show (a));
Debug.print("999_240:  " # debug_show (b));

Debug.print("a > b: " # debug_show (a > b));
