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
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import BitMap "mo:bit-map";
import Vector "mo:vector";
import Ids "mo:incremental-ids";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";
import BpTree "mo:augmented-btrees/BpTree";
import BpTreeTypes "mo:augmented-btrees/BpTree/Types";

module T {
    public type BitMap = BitMap.BitMap;

    public type Candid = Serde.Candid;

    public type CandidQuery = Serde.Candid or {
        #Minimum;
        #Maximum;
    };

    public type CandidType = Serde.CandidType;

    public type Interval = (Nat, Nat);

    public type Candify<A> = {
        from_blob : Blob -> ?A;
        to_blob : A -> Blob;
    };

    public type InternalCandify<A> = {
        from_blob : Blob -> A;
        to_blob : A -> Blob;
    };

    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    public let { thash; bhash; nhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    public type Order = Order.Order;

    public type MemoryBTree = MemoryBTree.StableMemoryBTree;
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

        data : BTree<[CandidQuery], RecordId>; // using CandidQuery here for comparison only, the actual data stored here is Candid
        used_internally : Bool; // if true, the index cannot be deleted by user if true
        is_unique : Bool; // if true, the index is unique and the record ids are not concatenated with the index key values to make duplicate values appear unique
    };

    public type SchemaMap = Map<Text, Schema>;

    public type ResolvedConstraints = {

    };

    public type BTree<K, V> = {
        #stableMemory : T.MemoryBTree;
        #heap : BpTree.BpTree<Blob, V>;
    };

    public type BpTreeUtils<K> = {
        blobify : TypeUtils.Blobify<K>;
        cmp : BpTreeTypes.CmpFn<Blob>;
    };

    public type BTreeUtils<K, V> = {
        #stableMemory : MemoryBTree.BTreeUtils<K, V>;
        #heap : BpTreeUtils<K>;
    };

    public type StableCollection = {
        ids : Ids.Generator;
        name : Text;
        schema : Schema;
        schema_map : SchemaMap;
        schema_keys : [Text];
        schema_keys_set : Set<Text>;

        main : BTree<Nat, Blob>;
        indexes : Map<Text, Index>;

        field_constraints : Map<Text, [SchemaFieldConstraint]>;
        unique_constraints : [([Text], Index)];
        fields_with_unique_constraints : Map<Text, Set<Nat>>; // the value is the index of the unique constraint in the unique_constraints list

        // reference to the freed btrees to the same variable in
        // the ZenDB database record
        freed_btrees : Vector.Vector<MemoryBTree.StableMemoryBTree>;
        logger : Logger;
        memory_type : MemoryType;
    };

    public type MemoryType = {
        #heap;
        #stableMemory;
    };

    public type StableDatabase = {
        collections : Map<Text, StableCollection>;
        memory_type : MemoryType;

        // reference to the freed btrees to the same variable in
        // the ZenDB database record
        freed_btrees : Vector.Vector<MemoryBTree.StableMemoryBTree>;
        logger : Logger;
        id_store : Ids.Ids;
    };

    public type StableStore = {
        id_store : Ids.Ids;
        databases : Map<Text, StableDatabase>;
        memory_type : MemoryType;
        freed_btrees : Vector.Vector<MemoryBTree.StableMemoryBTree>;
        logger : Logger;
    };

    public type LogLevel = {
        #Debug;
        #Info;
        #Warn;
        #Error;
        #Trap;
    };

    public type Logger = {
        var log_level : LogLevel;
        var next_thread_id : Nat;
        var is_running_locally : Bool;
    };

    public type IndexKeyFields = [(Text, Candid)];

    public type FieldLimit = (Text, ?State<CandidQuery>);
    public type RecordLimits = [(Text, ?State<CandidQuery>)];
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

    public type CandidInclusivityQuery = State<CandidQuery>;

    public type FullScanDetails = {
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        scan_bounds : Bounds;
        filter_bounds : Bounds;
    };

    public type IndexScanDetails = {
        index_name : Text;
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

    public type FieldUpdateOperations = {
        #currValue : (); // refers to the current (prior to the update) of the field you are updating
        #get : (Text);

        // multi-value operations
        #addAll : [FieldUpdateOperations];
        #subAll : [FieldUpdateOperations];
        #mulAll : [FieldUpdateOperations];
        #divAll : [FieldUpdateOperations];

        // Number operations
        #add : (FieldUpdateOperations, FieldUpdateOperations);
        #sub : (FieldUpdateOperations, FieldUpdateOperations);
        #mul : (FieldUpdateOperations, FieldUpdateOperations);
        #div : (FieldUpdateOperations, FieldUpdateOperations);
        #abs : (FieldUpdateOperations);
        #neg : (FieldUpdateOperations);
        #floor : (FieldUpdateOperations);
        #ceil : (FieldUpdateOperations);
        #sqrt : (FieldUpdateOperations);
        #pow : (FieldUpdateOperations, FieldUpdateOperations);
        #min : (FieldUpdateOperations, FieldUpdateOperations);
        #max : (FieldUpdateOperations, FieldUpdateOperations);
        #mod : (FieldUpdateOperations, FieldUpdateOperations);

        // Text operations
        #trim : (FieldUpdateOperations, Text);
        #lowercase : (FieldUpdateOperations);
        #uppercase : (FieldUpdateOperations);
        #replaceSubText : (FieldUpdateOperations, Text, Text);
        #slice : (FieldUpdateOperations, Nat, Nat);
        #concat : (FieldUpdateOperations, FieldUpdateOperations);

    } or Candid;

    public type SchemaFieldConstraint = {
        #Min : Float;
        #Max : Float;
        #Size : (min_size : Nat, max_size : Nat);
        #MinSize : Nat;
        #MaxSize : Nat;
    };

    public type SchemaConstraint = {
        #Unique : [Text];
        #Field : (Text, [SchemaFieldConstraint]);
    };

    public type CandidHeapStorageType = {
        // stored by both memory types
        #candid_blob : CandidBlob;

        // heap only
        #candid_variant : Candid;
        #candid_map : { get : () -> () }; // placeholder for map type

    };
};
