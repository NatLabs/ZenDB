import Prim "mo:prim";

import Principal "mo:base@0.16.0/Principal";
import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Nat32 "mo:base@0.16.0/Nat32";
import Result "mo:base@0.16.0/Result";
import Order "mo:base@0.16.0/Order";
import Iter "mo:base@0.16.0/Iter";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Option "mo:base@0.16.0/Option";
import Hash "mo:base@0.16.0/Hash";
import Float "mo:base@0.16.0/Float";
import Int "mo:base@0.16.0/Int";
import Blob "mo:base@0.16.0/Blob";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.4.0";
import Decoder "mo:serde@3.4.0/Candid/Blob/Decoder";
import Candid "mo:serde@3.4.0/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import Vector "mo:vector@0.4.2";
import ByteUtils "mo:byte-utils@0.1.1";
import Ids "Ids";

import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";
import Int8Cmp "mo:memory-collection@0.3.2/TypeUtils/Int8Cmp";

import Collection "Collection";
import Database "Database";
import Query "Query";
import Logger "Logger";
import StableDatabase "Database/StableDatabase";

import T "Types";
import C "Constants";

import TypeMigrations "TypeMigrations";

module {

    public let Types = T;
    public let Constants = C;

    public type Collection<T> = Collection.Collection<T>;
    public type Database = Database.Database;
    public type Schema = T.Schema;
    public type Candify<T> = T.Candify<T>;

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
        public func Quintuple(a : T.Schema, b : T.Schema, c : T.Schema, d : T.Schema, e : T.Schema) : T.Schema {
            #Tuple([a, b, c, d, e]);
        };
    };

    public type Tuple<A, B> = T.Tuple<A, B>;
    public func Tuple<A, B>(a : A, b : B) : T.Tuple<A, B> = {
        _0_ = a;
        _1_ = b;
    };
    public func fromTuple<A, B>(t : T.Tuple<A, B>) : (A, B) {
        (t._0_, t._1_);
    };

    public type Triple<A, B, C> = T.Triple<A, B, C>;
    public func Triple<A, B, C>(a : A, b : B, c : C) : T.Triple<A, B, C> = {
        _0_ = a;
        _1_ = b;
        _2_ = c;
    };
    public func fromTriple<A, B, C>(t : T.Triple<A, B, C>) : (A, B, C) {
        (t._0_, t._1_, t._2_);
    };

    public type Quadruple<A, B, C, D> = T.Quadruple<A, B, C, D>;
    public func Quadruple<A, B, C, D>(a : A, b : B, c : C, d : D) : Quadruple<A, B, C, D> = {
        _0_ = a;
        _1_ = b;
        _2_ = c;
        _3_ = d;
    };
    public func fromQuadruple<A, B, C, D>(t : T.Quadruple<A, B, C, D>) : (A, B, C, D) {
        (t._0_, t._1_, t._2_, t._3_);
    };

    public type Quintuple<A, B, C, D, E> = T.Quintuple<A, B, C, D, E>;
    public func Quintuple<A, B, C, D, E>(a : A, b : B, c : C, d : D, e : E) : T.Quintuple<A, B, C, D, E> = {
        _0_ = a;
        _1_ = b;
        _2_ = c;
        _3_ = d;
        _4_ = e;
    };
    public func fromQuintuple<A, B, C, D, E>(t : T.Quintuple<A, B, C, D, E>) : (A, B, C, D, E) {
        (t._0_, t._1_, t._2_, t._3_, t._4_);
    };

    public type Settings = {
        logging : ?{
            log_level : Logger.LogLevel;
            is_running_locally : Bool;
        };
        memory_type : ?T.MemoryType;
    };

    public let DefaultMemoryType = #stableMemory;

    public let defaultSettings : Settings = {
        logging = ?{
            log_level = #Warn;
            is_running_locally = false;
        };
        memory_type = ?(DefaultMemoryType);
    };

    public func newStableStore(canister_id : Principal, opt_settings : ?Settings) : T.VersionedStableStore {
        let settings = Option.get(opt_settings, defaultSettings);

        let canister_id_blob = Principal.toBlob(canister_id);
        let instance_id = Blob.fromArray([
            canister_id_blob[0],
            canister_id_blob[1],
            canister_id_blob[2],
            canister_id_blob[3],
        ]);

        let zendb : T.StableStore = {
            canister_id;
            instance_id;
            ids = Ids.new();
            databases = Map.new<Text, T.StableDatabase>();
            memory_type = Option.get(settings.memory_type, DefaultMemoryType);
            freed_btrees = Vector.new<T.MemoryBTree>();
            logger = Logger.init(#Error, false);
            is_running_locally = Option.get(
                Option.map(settings.logging, func(s : { is_running_locally : Bool }) : Bool = s.is_running_locally),
                false,
            );
        };

        let default_db : T.StableDatabase = {
            name = "default";
            ids = zendb.ids;
            instance_id = zendb.instance_id;
            collections = Map.new<Text, T.StableCollection>();
            freed_btrees = zendb.freed_btrees;
            logger = zendb.logger;
            memory_type = zendb.memory_type;
            is_running_locally = zendb.is_running_locally;
        };

        ignore Map.put(zendb.databases, T.thash, "default", default_db);

        ignore do ? {
            let log_settings = settings.logging!;

            Logger.setLogLevel(zendb.logger, log_settings.log_level);
            Logger.setIsRunLocally(zendb.logger, log_settings.is_running_locally);
        };

        TypeMigrations.share_version(zendb);
    };

    public func upgrade(versioned_sstore : T.VersionedStableStore) : T.VersionedStableStore {
        TypeMigrations.upgrade(versioned_sstore);
    };

    public func launchDefaultDB(versioned_sstore : T.VersionedStableStore) : Database.Database {
        let sstore = TypeMigrations.get_current_state(versioned_sstore);
        let ?default_db = Map.get<Text, T.StableDatabase>(sstore.databases, T.thash, "default") else Debug.trap("Default database not found");
        Database.Database(default_db);
    };

    public func createDB(versioned_sstore : T.VersionedStableStore, db_name : Text) : T.Result<Database.Database, Text> {

        let sstore = TypeMigrations.get_current_state(versioned_sstore);

        switch (Map.get<Text, T.StableDatabase>(sstore.databases, T.thash, db_name)) {
            case (?db) return #err("Database with name '" # db_name # "' already exists");
            case (null) {};
        };

        let db : T.StableDatabase = {
            name = db_name;
            ids = sstore.ids;
            instance_id = sstore.instance_id;
            collections = Map.new<Text, T.StableCollection>();
            freed_btrees = sstore.freed_btrees;
            logger = sstore.logger;
            memory_type = sstore.memory_type;
            is_running_locally = sstore.is_running_locally;
        };

        ignore Map.put(sstore.databases, T.thash, db_name, db);

        #ok(Database.Database(db));
    };

    public func getDB(versioned_sstore : T.VersionedStableStore, db_name : Text) : ?Database.Database {

        let sstore = TypeMigrations.get_current_state(versioned_sstore);

        switch (Map.get<Text, T.StableDatabase>(sstore.databases, T.thash, db_name)) {
            case (?db) return ?Database.Database(db);
            case (null) return null;
        };
    };

    public func setIsRunLocally(versioned_sstore : T.VersionedStableStore, is_running_locally : Bool) {
        let sstore = TypeMigrations.get_current_state(versioned_sstore);
        Logger.setIsRunLocally(sstore.logger, is_running_locally);
    };

    public func setLogLevel(versioned_sstore : T.VersionedStableStore, log_level : Logger.LogLevel) {
        let sstore = TypeMigrations.get_current_state(versioned_sstore);
        Logger.setLogLevel(sstore.logger, log_level);
    };

    public let QueryBuilder = Query.QueryBuilder;
    public type QueryBuilder = Query.QueryBuilder;

    public func stats(versioned_sstore : T.VersionedStableStore) : T.InstanceStats {
        let sstore = TypeMigrations.get_current_state(versioned_sstore);

        let dbStats = Buffer.Buffer<T.DatabaseStats>(0);
        var totalAllocated : Nat = 0;
        var totalUsed : Nat = 0;
        var totalFree : Nat = 0;
        var totalData : Nat = 0;
        var totalMetadata : Nat = 0;

        var totalDocumentStoreBytes : Nat = 0;
        var totalIndexData : Nat = 0;

        for ((name, db) in Map.entries(sstore.databases)) {
            let dbStat = StableDatabase.stats(db);
            dbStats.add(dbStat);

            totalAllocated += dbStat.total_allocated_bytes;
            totalUsed += dbStat.total_used_bytes;
            totalFree += dbStat.total_free_bytes;
            totalData += dbStat.total_data_bytes;
            totalMetadata += dbStat.total_metadata_bytes;
            totalDocumentStoreBytes += dbStat.total_document_store_bytes;
            totalIndexData += dbStat.total_index_store_bytes;
        };

        {
            memory_type = sstore.memory_type;
            databases = Map.size(sstore.databases);
            database_stats = Buffer.toArray(dbStats);
            total_allocated_bytes = totalAllocated;
            total_used_bytes = totalUsed;
            total_free_bytes = totalFree;
            total_data_bytes = totalData;
            total_metadata_bytes = totalMetadata;
            total_document_store_bytes = totalDocumentStoreBytes;
            total_index_store_bytes = totalIndexData;
        };
    };

};
