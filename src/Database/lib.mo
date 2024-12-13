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

import Collection "../Collection";
import Utils "../Utils";
import T "../Types";

import StableDatabase "StableDatabase";
module {

    public type Candify<T> = T.Candify<T>;
    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    public type StableCollection = T.StableCollection;

    // public type ZenDB = {
    //     collections : Map<Text, Collection>;
    // };

    public type Collection<Record> = Collection.Collection<Record>;

    public class Database(zendb : T.ZenDB) = self {

        public func create_collection<Record>(name : Text, schema : T.Schema, blobify : T.Candify<Record>) : Result<Collection<Record>, Text> {
            switch (StableDatabase.create_collection(zendb, name, schema)) {
                case (#ok(stable_collection)) {
                    #ok(
                        Collection.Collection<Record>(
                            name,
                            stable_collection,
                            blobify,
                        )
                    );
                };
                case (#err(msg)) #err(msg);
            };
        };

        public func get_collection<Record>(
            name : Text,
            blobify : T.Candify<Record>,
        ) : Result<Collection<Record>, Text> {

            switch (StableDatabase.get_collection(zendb, name)) {
                case (#ok(stable_collection)) {
                    #ok(
                        Collection.Collection<Record>(
                            name,
                            stable_collection,
                            blobify,
                        )
                    );
                };
                case (#err(msg)) #err(msg);
            };
        };

        public func get_or_create_collection<Record>(
            name : Text,
            schema : T.Schema,
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
