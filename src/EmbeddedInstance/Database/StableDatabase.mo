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

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import Ids "../Ids";
import Candid "mo:serde@3.4.0/Candid";

import Vector "mo:vector@0.4.2";

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

    let LOGGER_NAMESPACE = "StableDatabase";

    let { log_error_msg } = Utils;

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
        let log = Logger.NamespacedLogger(db.logger, LOGGER_NAMESPACE).subnamespace("size");
        let size = Map.size<Text, StableCollection>(db.collections);
        log.lazyInfo(func() = "Number of collections: " # debug_show size);
        size;
    };

    // public type CollectionOptions = T.CollectionOptions;

    public type CreateCollectionOptions = {
        schemaConstraints : [T.SchemaConstraint];
    };

    public func create_collection(db : T.StableDatabase, name : Text, schema : T.Schema, options : ?T.CreateCollectionOptions) : T.Result<StableCollection, Text> {

        let log = Logger.NamespacedLogger(db.logger, LOGGER_NAMESPACE).subnamespace("create_collection");

        log.lazyInfo(
            func() = "Creating collection '" # name # "'"
        );

        switch (Schema.validate_schema(schema)) {
            case (#ok(_)) {
                log.lazyInfo(
                    func() = "Schema validation passed for collection '" # name # "'"
                );
            };
            case (#err(msg)) {
                let error_msg = "StableDatabase.create_collection(): Schema validation failed: " # msg;
                return log_error_msg(db.logger, error_msg);
            };
        };

        let processed_schema = Schema.process_schema(schema);

        switch (Map.get<Text, StableCollection>(db.collections, Map.thash, name)) {
            case (?stable_collection) {
                log.lazyDebug(
                    func() = "Collection '" # name # "' already exists, checking schema compatibility"
                );

                if (stable_collection.schema != processed_schema) {
                    log.lazyError(
                        func() = "Schema mismatch for existing collection '" # name # "'"
                    );
                    return log_error_msg(db.logger, "Schema error: collection already exists with different schema");
                };

                log.lazyInfo(
                    func() = "Returning existing collection '" # name # "'"
                );
                return #ok(stable_collection);
            };
            case (null) {
                log.lazyDebug(
                    func() = "Collection '" # name # "' does not exist, creating new one"
                );
            };
        };

        let schema_map = SchemaMap.new(processed_schema);

        let schema_constraints = switch (options) {
            case (?options) { options.schemaConstraints };
            case (null) { [] };
        };

        // Validate schema constraints
        let { field_constraints; unique_constraints } = switch (SchemaMap.validate_schema_constraints(schema_map, schema_constraints)) {
            case (#ok(res)) res;
            case (#err(msg)) {
                let error_msg = "StableDatabase.create_collection(): Schema constraints validation failed: " # msg;
                return log_error_msg(db.logger, error_msg);
            };
        };

        let schema_keys = Utils.get_schema_keys(processed_schema);

        var stable_collection : T.StableCollection = {

            name;
            schema = processed_schema;
            schema_map = SchemaMap.new(processed_schema);
            schema_keys;
            schema_keys_set = Set.fromIter(schema_keys.vals(), Map.thash);
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
            indexes_in_batch_operations = Map.new<Text, T.Index>();
            populate_index_batches = Map.new<Nat, T.BatchPopulateIndex>();
            candid_serializer = Candid.TypedSerializer.new(
                [processed_schema],
                ?{ Candid.defaultOptions with types = ?[processed_schema] },
            );

            field_constraints;
            unique_constraints = [];
            fields_with_unique_constraints = Map.new();

            // db references
            ids = db.ids;
            instance_id = db.instance_id;
            freed_btrees = db.freed_btrees;
            candid_map_cache = db.candid_map_cache;
            logger = db.logger;
            memory_type = db.memory_type;
            is_running_locally = db.is_running_locally;
        };

        let unique_constraints_buffer = Buffer.Buffer<([Text], T.CompositeIndex)>(8);

        for (unique_field_names in unique_constraints.vals()) {

            let unique_field_names_with_direction = Array.map<Text, (Text, T.SortDirection)>(
                unique_field_names,
                func(field_name : Text) : (Text, T.SortDirection) = (field_name, #Ascending),
            );

            let index_res = StableCollection.create_index_internal(
                stable_collection,
                "internal_index_" # debug_show (Map.size(stable_collection.indexes)) # "_unique",
                unique_field_names_with_direction,
                true,
                true,
            );

            let index : T.CompositeIndex = switch (index_res) {
                case (#ok(index)) {
                    log.lazyInfo(
                        func() = "Created index for unique constraint on fields: " # debug_show unique_field_names
                    );

                    index;
                };
                case (#err(msg)) {
                    let error_msg = "StableDatabase.create_collection(): Failed to create index for unique constraint on fields: " # debug_show unique_field_names # ", error: " # msg;
                    return log_error_msg(db.logger, error_msg);
                };
            };

            let unique_constraint_index = unique_constraints_buffer.size();
            unique_constraints_buffer.add((unique_field_names, index));

            for (unique_field_name in unique_field_names.vals()) {
                let set = switch (Map.get(stable_collection.fields_with_unique_constraints, T.thash, unique_field_name)) {
                    case (?set) set;
                    case (null) {
                        let set = Set.new<Nat>();
                        ignore Map.put(stable_collection.fields_with_unique_constraints, Map.thash, unique_field_name, set);
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

        ignore Map.put<Text, StableCollection>(db.collections, Map.thash, name, stable_collection);

        log.lazyInfo(func() = "Created collection '" # name # "' successfully");
        log.lazyDebug(func() = "Schema for collection '" # name # "': " # debug_show schema);

        #ok(stable_collection);

    };

    public func get_collection(db : T.StableDatabase, name : Text) : T.Result<StableCollection, Text> {
        let log = Logger.NamespacedLogger(db.logger, LOGGER_NAMESPACE).subnamespace("get_collection");
        log.lazyDebug(func() = "Getting collection '" # name # "'");

        let stable_collection = switch (Map.get<Text, StableCollection>(db.collections, Map.thash, name)) {
            case (?collection) {
                log.lazyDebug(func() = "Found collection '" # name # "'");
                collection;
            };
            case (null) {
                log.lazyWarn(func() = "Collection '" # name # "' not found");
                return log_error_msg(db.logger, "ZenDB Database.get_collection(): Collection " # debug_show name # " not found");
            };
        };

        #ok(stable_collection);
    };

    public func rename_collection(db : T.StableDatabase, old_name : Text, new_name : Text) : T.Result<(), Text> {
        let log = Logger.NamespacedLogger(db.logger, LOGGER_NAMESPACE).subnamespace("rename_collection");
        log.lazyInfo(func() = "Renaming collection '" # old_name # "' to '" # new_name # "'");

        if (old_name == new_name) {
            log.lazyInfo(func() = "Old name and new name are the same, no action taken");
            return #ok(());
        };

        let stable_collection = switch (Map.get<Text, StableCollection>(db.collections, Map.thash, old_name)) {
            case (?collection) collection;
            case (null) {
                let error_msg = "StableDatabase.rename_collection(): Collection '" # old_name # "' not found";
                return log_error_msg(db.logger, error_msg);
            };
        };

        switch (Map.get<Text, StableCollection>(db.collections, Map.thash, new_name)) {
            case (?_) {
                let error_msg = "StableDatabase.rename_collection(): Collection with new name '" # new_name # "' already exists";
                return log_error_msg(db.logger, error_msg);
            };
            case (null) {};
        };

        ignore Map.remove<Text, StableCollection>(db.collections, Map.thash, old_name);

        let renamed_collection = { stable_collection with name = new_name };
        ignore Map.put<Text, StableCollection>(db.collections, Map.thash, new_name, renamed_collection);

        log.lazyInfo(func() = "Renamed collection '" # old_name # "' to '" # new_name # "' successfully");

        #ok(());
    };

    public func delete_collection(db : T.StableDatabase, name : Text) : T.Result<(), Text> {
        let log = Logger.NamespacedLogger(db.logger, LOGGER_NAMESPACE).subnamespace("delete_collection");
        log.lazyInfo(func() = "Deleting collection '" # name # "'");

        let collection = switch (Map.get<Text, StableCollection>(db.collections, Map.thash, name)) {
            case (?collection) { collection };
            case (null) {
                let error_msg = "StableDatabase.delete_collection(): Collection '" # name # "' not found";
                return log_error_msg(db.logger, error_msg);
            };
        };

        StableCollection.deallocate(collection);
        ignore Map.remove<Text, StableCollection>(db.collections, Map.thash, name);

        log.lazyInfo(func() = "Deleted collection '" # name # "' successfully");

        #ok(());
    };

    public func list_collections(db : T.StableDatabase) : [Text] {
        Iter.toArray(Map.keys(db.collections));
    };

    public func stats(db : T.StableDatabase) : T.DatabaseStats {
        let collections = Map.toArray<Text, T.StableCollection>(db.collections);

        let collection_stats = Array.map(
            collections,
            func((collection_name, stable_collection) : (Text, T.StableCollection)) : T.CollectionStats {
                StableCollection.stats(stable_collection);
            },
        );

        var total_allocated_bytes : Nat = 0;
        var total_free_bytes : Nat = 0;
        var total_used_bytes : Nat = 0;
        var total_data_bytes : Nat = 0;
        var total_metadata_bytes : Nat = 0;
        var total_document_store_bytes : Nat = 0;
        var total_index_store_bytes : Nat = 0;

        for (stats in collection_stats.vals()) {
            total_allocated_bytes += stats.total_allocated_bytes;
            total_free_bytes += stats.total_free_bytes;
            total_used_bytes += stats.total_used_bytes;
            total_data_bytes += stats.total_document_size;
            total_metadata_bytes += stats.total_metadata_bytes;

            total_document_store_bytes += stats.total_document_store_bytes;
            total_index_store_bytes += stats.total_index_store_bytes;
        };

        {
            name = db.name;
            memoryType = db.memory_type;
            collections = collections.size();
            collection_stats = collection_stats;

            total_allocated_bytes = total_allocated_bytes;
            total_used_bytes = total_used_bytes;
            total_free_bytes = total_free_bytes;
            total_data_bytes = total_data_bytes;
            total_metadata_bytes = total_metadata_bytes;

            total_document_store_bytes = total_document_store_bytes;
            total_index_store_bytes = total_index_store_bytes;
        };
    };

};
