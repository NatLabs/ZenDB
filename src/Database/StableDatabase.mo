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
import Ids "mo:incremental-ids";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import Collection "../Collection";
import Utils "../Utils";
import ZT "../Types";
import C "../Constants";
import Schema "../Collection/Schema";
import Logger "../Logger";

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
        Logger.info(zendb.logger, "StableDatabase.create_collection(): Creating collection '" # name # "'");

        let processed_schema = Schema.process_schema(schema);

        switch (Map.get<Text, StableCollection>(zendb.collections, thash, name)) {
            case (?stable_collection) {
                Logger.log(zendb.logger, "StableDatabase.create_collection(): Collection '" # name # "' already exists, checking schema compatibility");
                if (stable_collection.schema != processed_schema) {
                    Logger.error(zendb.logger, "StableDatabase.create_collection(): Schema mismatch for existing collection '" # name # "'");
                    return #err("Schema error: collection already exists with different schema");
                };

                Logger.info(zendb.logger, "StableDatabase.create_collection(): Returning existing collection '" # name # "'");
                return #ok(stable_collection);
            };
            case (null) {
                Logger.log(zendb.logger, "StableDatabase.create_collection(): Collection '" # name # "' does not exist, creating new one");
            };
        };

        let #Record(_) = processed_schema else return #err("Schema error: schema type is not a record");

        let schema_keys = Utils.extract_schema_keys(processed_schema);

        let stable_collection = {
            ids = Ids.create(zendb.id_store, name);
            var schema = processed_schema;
            var formatted_schema = Candid.formatCandidType([schema], null)[0];
            schema_keys;
            schema_keys_set = Set.fromIter(schema_keys.vals(), thash);
            main = MemoryBTree.new(?C.DEFAULT_BTREE_ORDER);
            indexes = Map.new<Text, ZT.Index>();

            // zendb references
            freed_btrees = zendb.freed_btrees;
            logger = zendb.logger;
        };

        ignore Map.put<Text, StableCollection>(zendb.collections, thash, name, stable_collection);
        Logger.info(zendb.logger, "StableDatabase.create_collection(): Created collection '" # name # "' successfully");
        Logger.log(zendb.logger, "StableDatabase.create_collection(): Schema for collection '" # name # "': " # debug_show schema);

        #ok(stable_collection);

    };

    public func get_collection(zendb : ZT.ZenDB, name : Text) : Result<StableCollection, Text> {
        Logger.log(zendb.logger, "StableDatabase.get_collection(): Getting collection '" # name # "'");

        let stable_collection = switch (Map.get<Text, StableCollection>(zendb.collections, thash, name)) {
            case (?collection) {
                Logger.log(zendb.logger, "StableDatabase.get_collection(): Found collection '" # name # "'");
                collection;
            };
            case (null) {
                Logger.warn(zendb.logger, "StableDatabase.get_collection(): Collection '" # name # "' not found");
                return #err("ZenDB Database.get_collection(): Collection " # debug_show name # " not found");
            };
        };

        #ok(stable_collection);
    };

    public func get_or_create_collection<Record>(
        zendb : ZT.ZenDB,
        name : Text,
        schema : ZT.Schema,
    ) : Result<StableCollection, Text> {
        Logger.info(zendb.logger, "StableDatabase.get_or_create_collection(): Getting or creating collection '" # name # "'");

        switch (create_collection(zendb, name, schema)) {
            case (#ok(collection)) {
                Logger.info(zendb.logger, "StableDatabase.get_or_create_collection(): Created collection '" # name # "'");
                #ok(collection);
            };
            case (#err(msg)) {
                Logger.log(
                    zendb.logger,
                    "StableDatabase.get_or_create_collection(): Failed to create collection '" #
                    name # "', trying to get existing collection. Error: " # msg,
                );

                switch (get_collection(zendb, name)) {
                    case (#ok(collection)) {
                        Logger.info(zendb.logger, "StableDatabase.get_or_create_collection(): Found existing collection '" # name # "'");
                        #ok(collection);
                    };
                    case (#err(_)) {
                        Logger.error(zendb.logger, "StableDatabase.get_or_create_collection(): Failed to get or create collection '" # name # "': " # msg);
                        #err(msg);
                    };
                };
            };
        };

    };

};
