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
import Vector "mo:vector";
import Ids "mo:incremental-ids";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import Collection "Collection";
import Database "Database";
import Query "Query";
import ZT "Types";

module {
    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    // public type MemoryBTree = MemoryBTree.VersionedMemoryBTree;
    public type BTreeUtils<K, V> = MemoryBTree.BTreeUtils<K, V>;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;
    public type StableQuery = ZT.StableQuery;

    public type ZenQueryLang = ZT.ZenQueryLang;
    public type ZqlOperators = ZT.ZqlOperators;

    public module Schema {
        public func Tuple(a : Schema, b : Schema) : Schema {
            #Tuple([a, b]);
        };
        public func Triple(a : Schema, b : Schema, c : Schema) : Schema {
            #Tuple([a, b, c]);
        };
        public func Quadruple(a : Schema, b : Schema, c : Schema, d : Schema) : Schema {
            #Tuple([a, b, c, d]);
        };
        // public func MultiTuple(schemas: [Schema]) : Schema {
        //     let fields = Array.map<Nat, (Text, Schema)>(schemas, func(i: Nat, schema: Schema) : (Text, Schema) {
        //         (Text.fromNat(i), schema)
        //     });
        //     #Record(fields)
        // };
    };

    public type Tuple<A, B> = ZT.Tuple<A, B>;
    public func Tuple<A, B>(a : A, b : B) : ZT.Tuple<A, B> = {
        _0_ = a;
        _1_ = b;
    };

    public type Triple<A, B, C> = ZT.Triple<A, B, C>;
    public func Triple<A, B, C>(a : A, b : B, c : C) : ZT.Triple<A, B, C> = {
        _0_ = a;
        _1_ = b;
        _2_ = c;
    };

    public type Quadruple<A, B, C, D> = ZT.Quadruple<A, B, C, D>;
    public func Quadruple<A, B, C, D>(a : A, b : B, c : C, d : D) : Quadruple<A, B, C, D> = {
        _0_ = a;
        _1_ = b;
        _2_ = c;
        _3_ = d;
    };

    public type SortDirection = ZT.SortDirection;

    public type Index = ZT.Index;

    public type Collection<Record> = Collection.Collection<Record>;

    public type ZenDB = ZT.ZenDB;

    public let DEFAULT_BTREE_ORDER = 256;

    public type Candid = Serde.Candid;

    public type Candify<A> = {
        from_blob : Blob -> A;
        to_blob : A -> Blob;
    };

    // func eq_candid(a : Candid, b : Candid) : Bool {
    //     cmp_candid(a, b) == 0;
    // };
    public type StableCollection = ZT.StableCollection;

    func get_collection(hydra_db : ZenDB, collection_name : Text) : ?StableCollection {
        Map.get<Text, StableCollection>(hydra_db.collections, thash, collection_name);
    };

    func reverse_order(order : Order) : Order {
        switch (order) {
            case (#less) #greater;
            case (#greater) #less;
            case (#equal) #equal;
        };
    };

    public type IndexKeyFields = [(Text, Candid)];

    // func get_btree_utils() : (BT) {
    //     MemoryBTree.createUtils(TypeUtils.Nat, TypeUtils.Blob);
    // };

    public type WrapId<Record> = (Nat, Record);

    type State<T> = {
        #Inclusive : T;
        #Exclusive : T;
    };

    public func newStableStore() : ZenDB {
        let hydra_db = {
            id_store = Ids.new();
            collections = Map.new<Text, StableCollection>();
            freed_btrees = Vector.new<MemoryBTree.StableMemoryBTree>();
        };
    };

    public func new() : ZenDB {
        newStableStore();
    };

    public func launch(sstore : ZenDB) : Database.Database {
        Database.Database(sstore);
    };

    public type Database = Database.Database;

    public let QueryBuilder = Query.QueryBuilder;
    public type QueryBuilder = Query.QueryBuilder;

    public type CollectionStats = ZT.CollectionStats;
    public type IndexStats = ZT.IndexStats;
    public type MemoryStats = ZT.MemoryStats;

};
