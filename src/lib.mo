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
import Ids "Ids";

import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import Collection "Collection";
import Database "Database";
import Query "Query";
import Logger "Logger";

import T "Types";
import C "Constants";

module {

    public let Types = T;
    public let Constants = C;

    public type Collection<T> = Collection.Collection<T>;
    public type Database = Database.Database;

    public module Schema {
        public func Tuple(a : T.Schema, b : T.Schema) : T.Schema {
            #Tuple([a, b]);
        };
        public func Triple(a : T.Schema, b : T.Schema, c : T.Schema) : T.Schema {
            #Tuple([a, b, c]);
        };
        public func Quadruple(a : T.Schema, b : T.Schema, c : T.Schema, d : T.Schema) : T.Schema {
            #Tuple([a, b, c, d]);
        };
        // public func MultiTuple(schemas: [T.Schema]) : T.Schema {
        //     let fields = Array.map<Nat, (Text, T.Schema)>(schemas, func(i: Nat, schema: T.Schema) : (Text, T.Schema) {
        //         (Text.fromNat(i), schema)
        //     });
        //     #Record(fields)
        // };
    };

    public type Tuple<A, B> = T.Tuple<A, B>;
    public func Tuple<A, B>(a : A, b : B) : T.Tuple<A, B> = {
        _0_ = a;
        _1_ = b;
    };

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

    public type Settings = {
        logging : ?{
            log_level : Logger.LogLevel;
            is_running_locally : Bool;
        };
        memory_type : ?T.MemoryType;
    };

    public let defaultSettings : Settings = {
        logging = ?{
            log_level = #Error;
            is_running_locally = false;
        };
        memory_type = ?(#stableMemory);
    };

    public func newStableStore(opt_settings : ?Settings) : T.StableStore {
        let settings = Option.get(opt_settings, defaultSettings);

        let zendb : T.StableStore = {
            databases = Map.new<Text, T.StableDatabase>();
            memory_type = Option.get(settings.memory_type, #heap);

            freed_btrees = Vector.new<T.MemoryBTree>();
            logger = Logger.init(#Error, false);
        };

        let default_db : T.StableDatabase = {
            collections = Map.new<Text, T.StableCollection>();
            freed_btrees = zendb.freed_btrees;
            logger = zendb.logger;
            memory_type = zendb.memory_type;
        };

        ignore Map.put(zendb.databases, T.thash, "default", default_db);

        ignore do ? {
            let log_settings = settings.logging!;

            Logger.setLogLevel(zendb.logger, log_settings.log_level);
            Logger.setIsRunLocally(zendb.logger, log_settings.is_running_locally);
        };

        zendb;
    };

    public func launchDefaultDB(sstore : T.StableStore) : Database.Database {
        let ?default_db = Map.get<Text, T.StableDatabase>(sstore.databases, T.thash, "default") else Debug.trap("Default database not found");
        Database.Database(default_db);
    };

    public func createDatabase(sstore : T.StableStore, db_name : Text) : T.Result<Database.Database, Text> {

        switch (Map.get<Text, T.StableDatabase>(sstore.databases, T.thash, db_name)) {
            case (?db) return #err("Database with name '" # db_name # "' already exists");
            case (null) {};
        };

        let db : T.StableDatabase = {
            collections = Map.new<Text, T.StableCollection>();
            freed_btrees = sstore.freed_btrees;
            logger = sstore.logger;
            memory_type = sstore.memory_type;
        };

        ignore Map.put(sstore.databases, T.thash, db_name, db);

        #ok(Database.Database(db));
    };

    public func getDatabase(sstore : T.StableStore, db_name : Text) : ?Database.Database {
        switch (Map.get<Text, T.StableDatabase>(sstore.databases, T.thash, db_name)) {
            case (?db) return ?Database.Database(db);
            case (null) return null;
        };
    };

    public func setIsRunLocally(sstore : T.StableStore, is_running_locally : Bool) {
        Logger.setIsRunLocally(sstore.logger, is_running_locally);
    };

    public func setLogLevel(sstore : T.StableStore, log_level : Logger.LogLevel) {
        Logger.setLogLevel(sstore.logger, log_level);
    };

    public let QueryBuilder = Query.QueryBuilder;
    public type QueryBuilder = Query.QueryBuilder;

};
