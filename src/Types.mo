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
import Vector "mo:vector";
import Ids "mo:incremental-ids";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

module T {
    public type BitMap = BitMap.BitMap;

    // public type ZenDB = ZenDB.ZenDB;

    public type Candid = Serde.Candid or {
        #Minimum;
        #Maximum;
    };

    public type CandidType = Serde.CandidType;

    public type Interval = (Nat, Nat);

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

    public type RecordId = Nat;
    // public type RecordId = Blob;
    public type CandidRecord = [(Text, Candid)];
    public type CandidBlob = Blob;

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
        #Ascending;
        #Descending;
    };

    public type Index = {
        name : Text;
        key_details : [(Text, SortDirection)];
        data : MemoryBTree.StableMemoryBTree;
    };

    public type StableCollection = {
        ids : Ids.Generator;
        var schema : Schema;
        schema_keys : [Text];
        schema_keys_set : Set<Text>;
        main : MemoryBTree.StableMemoryBTree;
        indexes : Map<Text, Index>;

        // reference to the freed btrees to the same variable in
        // the ZenDB database record
        freed_btrees : Vector.Vector<MemoryBTree.StableMemoryBTree>;
    };

    public type ZenDB = {
        id_store : Ids.Ids;
        collections : Map<Text, StableCollection>;
        freed_btrees : Vector.Vector<MemoryBTree.StableMemoryBTree>;
    };

    public type IndexKeyFields = [(Text, Candid)];

    public type FieldLimit = (Text, ?State<Candid>);
    public type RecordLimits = [(Text, ?State<Candid>)];
    public type Bounds = (RecordLimits, RecordLimits);

    public type ZqlOperators = {
        #eq : Candid;
        #gte : Candid;
        #lte : Candid;
        #lt : Candid;
        #gt : Candid;

        #In : [Candid];
        #Not : ZqlOperators;

        #between : (Candid, Candid);
        #exists;
        #startsWith : Candid;

        // #regex : Candid;

    };

    public type ZenQueryLang = {

        #Operation : (Text, ZqlOperators);
        #And : [ZenQueryLang];
        #Or : [ZenQueryLang];

    };

    public type Cursor = Nat;

    public type PaginationDirection = {
        #Forward;
        #Backward;
    };

    public type StableQueryPagination = {
        cursor : ?(Nat, PaginationDirection);
        limit : ?Nat;
        skip : ?Nat;
    };
    public type StableQuery = {
        query_operations : ZenQueryLang;
        pagination : StableQueryPagination;
        sort_by : ?(Text, SortDirection);
    };
    public type Operator = {
        #Eq;
        #Gt;
        #Lt;
    };

    public type WrapId<Record> = (Nat, Record);

    public type State<T> = {
        #Inclusive : T;
        #Exclusive : T;
    };

    public type CandidQuery = State<Candid>;

    public type FullScanDetails = {
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        scan_bounds : Bounds;
        filter_bounds : Bounds;
    };

    public type IndexScanDetails = {
        index : Index;
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        sorted_in_reverse : Bool;
        interval : (Nat, Nat);
        scan_bounds : Bounds;
        filter_bounds : Bounds;
        simple_operations : [(Text, T.ZqlOperators)];
    };

    public type ScanDetails = {
        #IndexScan : IndexScanDetails;
        #FullScan : FullScanDetails;
    };

    public type QueryPlan = {
        is_and_operation : Bool;
        subplans : [QueryPlan]; // result of nested #And/#Or operations
        simple_operations : [(Text, T.ZqlOperators)];
        scans : [ScanDetails]; // scan results from simple #Operation
    };

    public type MemoryStats = {
        metadata_bytes : Nat;
        actual_data_bytes : Nat;
    };

    public type IndexStats = {
        columns : [Text];
        stable_memory : MemoryStats;
    };

    public type CollectionStats = {
        records : Nat;
        indexes : [IndexStats];

        main_btree_index : {
            stable_memory : MemoryStats;
        };

    };

    public type EvalResult = {
        #Empty;
        #Ids : Iter<Nat>;
        #BitMap : T.BitMap;
        #Interval : (index : Text, interval : [(Nat, Nat)], is_reversed : Bool);
    };

    public type BestIndexResult = {
        index : T.Index;
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        sorted_in_reverse : Bool;
        fully_covered_equality_and_range_fields : Set.Set<Text>;
        score : Float;

    };

};
