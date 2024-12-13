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
import Ids "mo:incremental-ids";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import Collection "../Collection";
import Utils "../Utils";
import ZT "../Types";
import C "../Constants";

module {

    public type Candify<T> = ZT.Candify<T>;
    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    public type StableCollection = ZT.StableCollection;

    public func create_collection(zendb : ZT.ZenDB, name : Text, schema : ZT.Schema) : Result<StableCollection, Text> {

        switch (Map.get<Text, StableCollection>(zendb.collections, thash, name)) {
            case (?stable_collection) {
                if (stable_collection.schema != schema) {
                    return #err("Schema error: collection already exists with different schema");
                };

                return #ok(stable_collection);
            };
            case (null) ();
        };

        let #Record(_) = schema else return #err("Schema error: schema type is not a record");

        let schema_keys = Utils.extract_schema_keys(schema);

        let stable_collection = {
            ids = Ids.create(zendb.id_store, name);
            var schema = schema;
            schema_keys;
            schema_keys_set = Set.fromIter(schema_keys.vals(), thash);
            main = MemoryBTree.new(?C.DEFAULT_BTREE_ORDER);
            indexes = Map.new<Text, ZT.Index>();
            freed_btrees = zendb.freed_btrees;
        };

        ignore Map.put<Text, StableCollection>(zendb.collections, thash, name, stable_collection);

        #ok(stable_collection);

    };

    public func get_collection(zendb : ZT.ZenDB, name : Text) : Result<StableCollection, Text> {
        let stable_collection = switch (Map.get<Text, StableCollection>(zendb.collections, thash, name)) {
            case (?collection) (collection);
            case (null) return #err("ZenDB Database.get_collection(): Collection " # debug_show name # " not found");
        };

        #ok(stable_collection);
    };

    public func get_or_create_collection<Record>(
        zendb : ZT.ZenDB,
        name : Text,
        schema : ZT.Schema,
    ) : Result<StableCollection, Text> {

        switch (create_collection(zendb, name, schema)) {
            case (#ok(collection)) #ok(collection);
            case (#err(msg)) {
                switch (get_collection(zendb, name)) {
                    case (#ok(collection)) #ok(collection);
                    case (#err(_)) #err(msg);
                };
            };
        };

    };

};
