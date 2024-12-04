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
import Vector "mo:vector";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import Collection "Collection";
import Database "Database";
import Query "Query";
import T "Types";

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
    public type StableQuery = T.StableQuery;

    public type RecordPointer = Nat;

    public type ZenQueryLang = T.ZenQueryLang;
    public type ZqlOperators = T.ZqlOperators;

    public module Schema {
        public func Tuple(a : Schema, b : Schema) : Schema {
            #Record([
                ("0", a),
                ("1", b),
            ]);
        };
        public func Triple(a : Schema, b : Schema, c : Schema) : Schema {
            #Record([
                ("0", a),
                ("1", b),
                ("2", c),
            ]);
        };
        public func Quadruple(a : Schema, b : Schema, c : Schema, d : Schema) : Schema {
            #Record([
                ("0", a),
                ("1", b),
                ("2", c),
                ("3", d),
            ]);
        };
        // public func MultiTuple(schemas: [Schema]) : Schema {
        //     let fields = Array.map<Nat, (Text, Schema)>(schemas, func(i: Nat, schema: Schema) : (Text, Schema) {
        //         (Text.fromNat(i), schema)
        //     });
        //     #Record(fields)
        // };
    };

    public type Tuple<A, B> = T.Tuple<A, B>;
    public func Tuple<A, B>(a : A, b : B) : T.Tuple<A, B> = { _0_ = a; _1_ = b };

    public type Triple<A, B, C> = T.Triple<A, B, C>;
    public func Triple<A, B, C>(a : A, b : B, c : C) : T.Triple<A, B, C> = {
        _0_ = a;
        _1_ = b;
        _2_ = c;
    };

    public type Quadruple<A, B, C, D> = T.Quadruple<A, B, C, D>;
    public func Quadruple<A, B, C, D>(a : A, b : B, c : C, d : D) : Quadruple<A, B, C, D> = {
        _0_ = a;
        _1_ = b;
        _2_ = c;
        _3_ = d;
    };

    public type SortDirection = T.SortDirection;

    public type Index = T.Index;

    public type Collection<Record> = Collection.Collection<Record>;

    public type ZenDB = T.ZenDB;

    public let DEFAULT_BTREE_ORDER = 256;

    public type Candid = Serde.Candid;

    public type Candify<A> = {
        from_blob : Blob -> A;
        to_blob : A -> Blob;
    };

    // func eq_candid(a : Candid, b : Candid) : Bool {
    //     cmp_candid(a, b) == 0;
    // };
    public type StableCollection = T.StableCollection;

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
        #True : T;
        #False : T;
    };

    public func newStableStore() : ZenDB {
        let hydra_db = {
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

    public let QueryBuilder = Query.QueryBuilder;
    public type QueryBuilder = Query.QueryBuilder;

    public type CollectionStats = T.CollectionStats;
    public type IndexStats = T.IndexStats;
    public type MemoryStats = T.MemoryStats;

};
