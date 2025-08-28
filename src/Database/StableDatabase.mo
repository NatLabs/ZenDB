import Prim "mo:prim";

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
import Ids "../Ids";

import Vector "mo:vector";

import Collection "../Collection";
import StableCollection "../Collection/StableCollection";
import Utils "../Utils";
import T "../Types";
import C "../Constants";
import Schema "../Collection/Schema";
import Logger "../Logger";
import SchemaMap "../Collection/SchemaMap";
import BTree "../BTree";

module {

    let { logErrorMsg } = Utils;

    public type InternalCandify<T> = T.InternalCandify<T>;
    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    public type StableCollection = T.StableCollection;

    public func size(db : T.StableDatabase) : Nat {
        let size = Map.size<Text, StableCollection>(db.collections);
        Logger.lazyInfo(db.logger, func() = "StableDatabase.size(): Number of collections: " # debug_show size);
        size;
    };

    // public type CollectionOptions = T.CollectionOptions;

    public type CreateCollectionOptions = {
        schemaConstraints : [T.SchemaConstraint];
    };

    public func createCollection(db : T.StableDatabase, name : Text, schema : T.Schema, options : ?T.CreateCollectionOptions) : Result<StableCollection, Text> {

        Logger.lazyInfo(
            db.logger,
            func() = "StableDatabase.createCollection(): Creating collection '" # name # "'",
        );

        switch (Schema.validateSchema(schema)) {
            case (#ok(_)) {
                Logger.lazyInfo(
                    db.logger,
                    func() = "StableDatabase.createCollection(): Schema validation passed for collection '" # name # "'",
                );
            };
            case (#err(msg)) {
                let error_msg = "StableDatabase.createCollection(): Schema validation failed: " # msg;
                return logErrorMsg(db.logger, error_msg);
            };
        };

        let processed_schema = Schema.processSchema(schema);

        switch (Map.get<Text, StableCollection>(db.collections, thash, name)) {
            case (?stable_collection) {
                Logger.lazyDebug(
                    db.logger,
                    func() = "StableDatabase.createCollection(): Collection '" # name # "' already exists, checking schema compatibility",
                );

                if (stable_collection.schema != processed_schema) {
                    Logger.lazyError(
                        db.logger,
                        func() = "StableDatabase.createCollection(): Schema mismatch for existing collection '" # name # "'",
                    );
                    return logErrorMsg(db.logger, "Schema error: collection already exists with different schema");
                };

                Logger.lazyInfo(
                    db.logger,
                    func() = "StableDatabase.createCollection(): Returning existing collection '" # name # "'",
                );
                return #ok(stable_collection);
            };
            case (null) {
                Logger.lazyDebug(
                    db.logger,
                    func() = "StableDatabase.createCollection(): Collection '" # name # "' does not exist, creating new one",
                );
            };
        };

        let schema_map = SchemaMap.new(processed_schema);

        let schema_constraints = switch (options) {
            case (?options) { options.schemaConstraints };
            case (null) { [] };
        };

        // Validate schema constraints
        let { field_constraints; unique_constraints } = switch (SchemaMap.validateSchemaConstraints(schema_map, schema_constraints)) {
            case (#ok(res)) res;
            case (#err(msg)) {
                let error_msg = "StableDatabase.createCollection(): Schema constraints validation failed: " # msg;
                return logErrorMsg(db.logger, error_msg);
            };
        };

        let schema_keys = Utils.getSchemaKeys(processed_schema);

        var stable_collection : T.StableCollection = {
            ids = db.ids;
            name;
            schema = processed_schema;
            schema_map = SchemaMap.new(processed_schema);
            schema_keys;
            schema_keys_set = Set.fromIter(schema_keys.vals(), thash);
            documents = switch (db.memory_type) {
                case (#heap) { BTree.newHeap() };
                case (#stableMemory) {
                    switch (Vector.removeLast(db.freed_btrees)) {
                        case (?memory_btree) {
                            #stableMemory(memory_btree);
                        };
                        case (null) {
                            BTree.newStableMemory();
                        };
                    };
                };
            };

            indexes = Map.new<Text, T.Index>();

            field_constraints;
            unique_constraints = [];
            fields_with_unique_constraints = Map.new();

            // db references
            freed_btrees = db.freed_btrees;
            logger = db.logger;
            memory_type = db.memory_type;
        };

        let unique_constraints_buffer = Buffer.Buffer<([Text], T.Index)>(8);

        for (unique_field_names in unique_constraints.vals()) {

            let unique_field_names_with_direction = Array.map<Text, (Text, T.SortDirection)>(
                unique_field_names,
                func(field_name : Text) : (Text, T.SortDirection) = (field_name, #Ascending),
            );

            let index_res = StableCollection.createIndexInternal(
                stable_collection,
                "internal_index_" # debug_show (Map.size(stable_collection.indexes)) # "_unique",
                unique_field_names_with_direction,
                true,
                true,
            );

            let index : T.Index = switch (index_res) {
                case (#ok(index)) {
                    Logger.lazyInfo(
                        db.logger,
                        func() = "StableDatabase.createCollection(): Created index for unique constraint on fields: " # debug_show unique_field_names,
                    );

                    index;
                };
                case (#err(msg)) {
                    let error_msg = "StableDatabase.createCollection(): Failed to create index for unique constraint on fields: " # debug_show unique_field_names # ", error: " # msg;
                    return logErrorMsg(db.logger, error_msg);
                };
            };

            let unique_constraint_index = unique_constraints_buffer.size();
            unique_constraints_buffer.add((unique_field_names, index));

            for (unique_field_name in unique_field_names.vals()) {
                let set = switch (Map.get(stable_collection.fields_with_unique_constraints, T.thash, unique_field_name)) {
                    case (?set) set;
                    case (null) {
                        let set = Set.new<Nat>();
                        ignore Map.put(stable_collection.fields_with_unique_constraints, thash, unique_field_name, set);
                        set;
                    };
                };

                Set.add(set, T.nhash, unique_constraint_index);

            };

        };

        stable_collection := {
            stable_collection with
            unique_constraints = Buffer.toArray(unique_constraints_buffer);
        };

        ignore Map.put<Text, StableCollection>(db.collections, thash, name, stable_collection);

        Logger.lazyInfo(
            db.logger,
            func() = "StableDatabase.createCollection(): Created collection '" # name # "' successfully",
        );
        Logger.lazyDebug(
            db.logger,
            func() = "StableDatabase.createCollection(): Schema for collection '" # name # "': " # debug_show schema,
        );

        #ok(stable_collection);

    };

    public func getCollection(db : T.StableDatabase, name : Text) : Result<StableCollection, Text> {
        Logger.lazyDebug(
            db.logger,
            func() = "StableDatabase.getCollection(): Getting collection '" # name # "'",
        );

        let stable_collection = switch (Map.get<Text, StableCollection>(db.collections, thash, name)) {
            case (?collection) {
                Logger.lazyDebug(
                    db.logger,
                    func() = "StableDatabase.getCollection(): Found collection '" # name # "'",
                );
                collection;
            };
            case (null) {
                Logger.lazyWarn(
                    db.logger,
                    func() = "StableDatabase.getCollection(): Collection '" # name # "' not found",
                );
                return logErrorMsg(db.logger, "ZenDB Database.getCollection(): Collection " # debug_show name # " not found");
            };
        };

        #ok(stable_collection);
    };

};
