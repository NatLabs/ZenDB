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

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import Collection "Collection";
import Utils "Utils";
import T "Types";

module {

    public type Candify<T> = T.Candify<T>;
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
        #Ascending;
        #Descending;
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

    // public type ZenDB = {
    //     collections : Map<Text, Collection>;
    // };

    public type Collection<Record> = Collection.Collection<Record>;

    public let DEFAULT_BTREE_ORDER = 256;

    public class Database(hydra_db : T.ZenDB) = self {

        public func create_collection<Record>(name : Text, schema : Schema, blobify : T.Candify<Record>) : Result<Collection<Record>, Text> {

            switch (Map.get<Text, StableCollection>(hydra_db.collections, thash, name)) {
                case (?collection) {
                    if (collection.schema != schema) {
                        return #err("Schema error: collection already exists with different schema");
                    };

                    return #ok(
                        Collection.Collection<Record>(
                            name,
                            collection,
                            blobify,
                        )
                    );
                };
                case (null) ();
            };

            let #Record(_) = schema else return #err("Schema error: schema type is not a record");

            let schema_keys = Utils.extract_schema_keys(schema);

            let stable_collection = {
                var schema = schema;
                schema_keys;
                schema_keys_set = Set.fromIter(schema_keys.vals(), thash);
                main = MemoryBTree.new(?DEFAULT_BTREE_ORDER);
                indexes = Map.new<Text, Index>();
            };

            ignore Map.put<Text, StableCollection>(hydra_db.collections, thash, name, stable_collection);

            #ok(
                Collection.Collection<Record>(
                    name,
                    stable_collection,
                    blobify,
                )
            );
        };

        public func get_collection<Record>(
            name : Text,
            blobify : T.Candify<Record>,
        ) : Result<Collection<Record>, Text> {
            let stable_collection = switch (Map.get<Text, StableCollection>(hydra_db.collections, thash, name)) {
                case (?collection) (collection);
                case (null) return #err("Collection not found");
            };

            #ok(
                Collection.Collection<Record>(
                    name,
                    stable_collection,
                    blobify,
                )
            );
        };

        public func get_or_create_collection<Record>(
            name : Text,
            schema : Schema,
            blobify : T.Candify<Record>,
        ) : Result<Collection<Record>, Text> {

            switch (create_collection(name, schema, blobify)) {
                case (#ok(collection)) #ok(collection);
                case (#err(msg)) {
                    switch (get_collection(name, blobify)) {
                        case (#ok(collection)) #ok(collection);
                        case (#err(_)) #err(msg);
                    };
                };
            };

        };

    };

};
