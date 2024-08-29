import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Char "mo:base/Char";
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

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Tag "mo:candid/Tag";
import BitMap "mo:bit-map";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

module {
    public type BitMap = BitMap.BitMap;

    // public type HydraDB = HydraDB.HydraDB;

    public type Candid = Serde.Candid or {
        #Minimum;
        #Maximum;
    };

    public type Candify<A> = {
        from_blob : Blob -> A;
        to_blob : A -> Blob;
    };
    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    public type Order = Order.Order;

    // public type MemoryBTree = MemoryBTree.VersionedMemoryBTree;
    public type BTreeUtils<K, V> = MemoryBTree.BTreeUtils<K, V>;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type RecordPointer = Nat;

    public type Tuple<A, B> = { _0_ : A; _1_ : B };
    public func Tuple<A, B>(a : A, b : B) : Tuple<A, B> {
        { _0_ = a; _1_ = b };
    };

    public type Triple<A, B, C> = { _0_ : A; _1_ : B; _2_ : C };
    public func Triple<A, B, C>(a : A, b : B, c : C) : Triple<A, B, C> {
        { _0_ = a; _1_ = b; _2_ = c };
    };

    public type Quadruple<A, B, C, D> = { _0_ : A; _1_ : B; _2_ : C; _3_ : D };
    public func Quadruple<A, B, C, D>(a : A, b : B, c : C, d : D) : Quadruple<A, B, C, D> {
        { _0_ = a; _1_ = b; _2_ = c; _3_ = d };
    };

    public type SortDirection = {
        #Asc;
        #Desc;
    };

    public type Index = {
        name : Text;
        key_details : [(Text, SortDirection)];
        data : MemoryBTree.StableMemoryBTree;
    };

    public type StableCollection = {
        var schema : Schema;
        schema_keys : [Text];
        schema_keys_set : Set<Text>;
        main : MemoryBTree.StableMemoryBTree;
        indexes : Map<Text, Index>;
    };

    public type HydraDB = {
        collections : Map<Text, StableCollection>;
    };

    public type IndexKeyFields = [(Text, Candid)];

    public type HqlOperators = {
        #eq : Candid;
        #gte : Candid;
        #lte : Candid;
        #lt : Candid;
        #gt : Candid;
        // #exists : Text;
        #In : [Candid];

        #Not : HqlOperators;

    };

    public type HydraQueryLang = {

        #Operation : (Text, HqlOperators);
        #And : [HydraQueryLang];
        #Or : [HydraQueryLang];

        // #Limit : (Nat, HydraQueryLang);
        // #Skip : (Nat, HydraQueryLang);
        // #BatchSize : (Nat, HydraQueryLang);

        // #Regex : (Text, Text);
        // #Not : HydraQueryLang;

        // #In : (Text, [Candid]);
        // #Between : (Text, Candid, Candid);
        // #All : (Text, HydraQueryLang);
        // #Intersect : (HydraQueryLang, HydraQueryLang);
        // #Union : (HydraQueryLang, HydraQueryLang);
    };

    public type Operator = {
        #Eq;
        #Gt;
        #Lt;
    };

    public type WrapId<Record> = (Nat, Record);

    public type State<T> = {
        #True : T;
        #False : T;
    };

    public type CandidQuery = State<Candid>;
};
