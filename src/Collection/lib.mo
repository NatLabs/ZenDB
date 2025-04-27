/// A collection is a set of records of the same type.

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
import Int32 "mo:base/Int32";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Int16 "mo:base/Int16";
import Int64 "mo:base/Int64";
import Int8 "mo:base/Int8";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";
import InternetComputer "mo:base/ExperimentalInternetComputer";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import BitMap "mo:bit-map";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import ByteUtils "../ByteUtils";
import C "../Constants";

import Index "Index";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "Utils";
import QueryPlan "QueryPlan";
import QueryExecution "QueryExecution";
import StableCollection "StableCollection";
import Logger "../Logger";

module {

    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; nhash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    type QueryBuilder = Query.QueryBuilder;

    // public type MemoryBTree = MemoryBTree.VersionedMemoryBTree;
    public type BTreeUtils<K, V> = MemoryBTree.BTreeUtils<K, V>;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type InternalCandify<A> = T.InternalCandify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;

    let DEFAULT_BTREE_ORDER = 256;
    let MAX_QUERY_INSTRUCTIONS : Nat64 = 5_000_000_000;
    let MAX_UPDATE_INSTRUCTIONS : Nat64 = 40_000_000_000;

    public class Collection<Record>(
        collection_name : Text,
        collection : StableCollection,
        blobify : T.InternalCandify<Record>,
    ) = self {

        /// Generic helper function to handle Result types with consistent error logging
        private func handleResult<T>(res : Result<T, Text>, context : Text) : Result<T, Text> {
            switch (res) {
                case (#ok(success)) #ok(success);
                case (#err(errorMsg)) {
                    Logger.lazyError(collection.logger, func() = context # ": " # errorMsg);
                    #err(errorMsg);
                };
            };
        };

        /// for debugging
        public func _get_schema() : T.Schema { collection.schema };
        public func _get_schema_map() : T.SchemaMap { collection.schema_map };
        public func _get_indexes() : Map<Text, Index> { collection.indexes };
        public func _get_stable_state() : StableCollection { collection };

        /// Returns the collection name.
        public func name() : Text = collection_name;

        /// Returns the number of records in the collection.
        public func size() : Nat = MemoryBTree.size(collection.main);

        let main_btree_utils : MemoryBTree.BTreeUtils<Nat, Blob> = CollectionUtils.get_main_btree_utils();

        /// Returns an iterator over all the record ids in the collection.
        public func keys() : Iter<Nat> {
            MemoryBTree.keys(collection.main, main_btree_utils);
        };

        /// Returns an iterator over all the records in the collection.
        public func vals() : Iter<Record> {
            let iter = MemoryBTree.vals(collection.main, main_btree_utils);
            let records = Iter.map<Blob, Record>(
                iter,
                func(candid_blob : Blob) {
                    blobify.from_blob(candid_blob);
                },
            );
            records;
        };

        /// Returns an iterator over a tuple containing the id and record for all entries in the collection.
        public func entries() : Iter<(Nat, Record)> {
            let iter = MemoryBTree.entries(collection.main, main_btree_utils);

            let records = Iter.map<(Nat, Blob), (Nat, Record)>(
                iter,
                func((id, candid_blob) : (Nat, Blob)) {
                    (id, blobify.from_blob(candid_blob));
                },
            );
            records;
        };

        public func filter_iter(condition : (Record) -> Bool) : Iter<Record> {

            let iter = MemoryBTree.vals(collection.main, main_btree_utils);
            let records = Iter.map<Blob, Record>(
                iter,
                func(candid_blob : Blob) {
                    blobify.from_blob(candid_blob);
                },
            );
            let filtered = Iter.filter<Record>(records, condition);

        };

        public func filter(condition : (Record) -> Bool) : [Record] {
            Iter.toArray(filter_iter(condition));
        };

        /// Clear all the data in the collection.
        public func clear() {
            MemoryBTree.clear(collection.main);

            for (index in Map.vals(collection.indexes)) {
                MemoryBTree.clear(index.data);
            };
        };

        // public func update_schema(schema : Schema) : Result<(), Text> {
        //     handleResult(StableCollection.update_schema(collection, schema), "Failed to update schema");
        // };

        /// Creates a new index with the given index keys.
        /// If `is_unique_on_index_keys` is true, the index will be unique on the index keys and records with duplicate index keys will be rejected.
        public func create_index(name : Text, index_key_details : [(Text, SortDirection)], is_unique_on_index_keys : Bool) : Result<(), Text> {
            switch (StableCollection.create_index(collection, main_btree_utils, name, index_key_details, is_unique_on_index_keys)) {
                case (#ok(success)) #ok();
                case (#err(errorMsg)) {
                    return Utils.log_error_msg(collection.logger, "Failed to create index (" # name # "): " # errorMsg);
                };
            };

        };

        /// Deletes an index from the collection that is not used internally.
        public func delete_index(name : Text) : Result<(), Text> {
            handleResult(
                StableCollection.delete_index(collection, main_btree_utils, name),
                "Failed to delete index: " # name,
            );
        };

        /// Clears an index from the collection that is not used internally.
        public func clear_index(name : Text) : Result<(), Text> {
            handleResult(
                StableCollection.clear_index(collection, main_btree_utils, name),
                "Failed to clear index: " # name,
            );
        };

        public func create_and_populate_index(name : Text, index_key_details : [(Text, SortDirection)]) : Result<(), Text> {
            handleResult(
                StableCollection.create_and_populate_index(collection, main_btree_utils, name, index_key_details),
                "Failed to create and populate index: " # name,
            );
        };

        public func populate_index(name : Text) : Result<(), Text> {
            handleResult(
                StableCollection.populate_index(collection, main_btree_utils, name),
                "Failed to populate index: " # name,
            );
        };

        public func populate_indexes(names : [Text]) : Result<(), Text> {
            handleResult(
                StableCollection.populate_indexes(collection, main_btree_utils, names),
                "Failed to populate indexes: " # debug_show (names),
            );
        };

        public func insert_with_id(id : Nat, record : Record) : Result<(), Text> {
            put_with_id(id, record);
        };

        public func put_with_id(id : Nat, record : Record) : Result<(), Text> {
            let candid_blob = blobify.to_blob(record);
            handleResult(
                StableCollection.put_with_id(collection, main_btree_utils, id, candid_blob),
                "Failed to put record with id: " # debug_show (id),
            );
        };

        public func insert(record : Record) : Result<(Nat), Text> {
            put(record);
        };

        public func put(record : Record) : Result<(Nat), Text> {
            let candid_blob = blobify.to_blob(record);
            handleResult(
                StableCollection.put(collection, main_btree_utils, candid_blob),
                "Failed to put record",
            );
        };

        public func get(id : Nat) : Result<Record, Text> {
            switch (
                handleResult(
                    StableCollection.get(collection, main_btree_utils, id),
                    "Failed to get record with id: " # debug_show (id),
                )
            ) {
                case (#err(err)) return #err(err);
                case (#ok(record_details)) {
                    let record = blobify.from_blob(record_details);
                    #ok(record);
                };
            };
        };

        type RecordLimits = [(Text, ?State<T.CandidQuery>)];
        type FieldLimit = (Text, ?State<T.CandidQuery>);

        type Bounds = (RecordLimits, RecordLimits);

        type IndexDetails = {
            var sorted_in_reverse : ?Bool;
            intervals : Buffer.Buffer<(Nat, Nat)>;
        };

        type Iter<A> = Iter.Iter<A>;

        public func search_iter(query_builder : QueryBuilder) : Result<Iter<T.WrapId<Record>>, Text> {
            switch (
                handleResult(
                    StableCollection.internal_search(collection, query_builder),
                    "Failed to execute search",
                )
            ) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = StableCollection.id_to_record_iter(collection, blobify, record_ids_iter);
                    #ok(record_iter);
                };
            };
        };

        public func search(query_builder : QueryBuilder) : Result<[T.WrapId<Record>], Text> {
            switch (
                handleResult(
                    StableCollection.internal_search(collection, query_builder),
                    "Failed to execute search",
                )
            ) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = StableCollection.id_to_record_iter(collection, blobify, record_ids_iter);
                    let records = Iter.toArray(record_iter);
                    #ok(records);
                };
            };
        };

        // func async_skip_helper(iter : Iter<Nat>, skip : Nat) : async (Nat) {
        //     let performance_start = InternetComputer.performanceCounter(0);

        //     func instructions() : Nat64 {
        //         InternetComputer.performanceCounter(0) - performance_start;
        //     };

        //     var i = 0;
        //     while ((instructions() + 10_000_000 < MAX_UPDATE_INSTRUCTIONS) and i < skip) {
        //         ignore iter.next();
        //         i += 1;
        //     };

        //     i;
        // };

        // func async_skip(iter : Iter<Nat>, skip : Nat, performance_start : Nat64) : async* () {

        //     var skipped = 0;
        //     // Debug.print("starting async_skip: " # debug_show skip);
        //     while (skipped < skip) {
        //         skipped += await async_skip_helper(iter, skip - skipped);
        //         // Debug.print("skipped: " # debug_show skipped);
        //     };

        // };

        // public func async_find(query_builder : QueryBuilder, buffer : Buffer<T.WrapId<Record>>) : async* Result<(), Text> {
        //     switch (search_iter(query_builder)) {
        //         case (#err(err)) #err(err);
        //         case (#ok(records)) {
        //             for (record in records) {
        //                 buffer.add(record);
        //             };
        //             #ok(());
        //         };
        //     };
        // };

        public func stats() : T.CollectionStats {
            StableCollection.stats(collection);
        };

        /// Returns the total number of records that match the query.
        /// This ignores the limit and skip parameters.
        public func count(query_builder : QueryBuilder) : Result<Nat, Text> {
            handleResult(
                StableCollection.count(collection, query_builder),
                "Failed to count records",
            );
        };

        public func replaceRecord(id : Nat, record : Record) : Result<(), Text> {
            handleResult(
                StableCollection.replace_record_by_id(collection, main_btree_utils, id, blobify.to_blob(record)),
                "Failed to replace record with id: " # debug_show (id),
            );
        };

        public func replaceRecords(records : [(Nat, Record)]) : Result<(), Text> {
            for ((id, record) in records.vals()) {
                switch (replaceRecord(id, record)) {
                    case (#ok(_)) {};
                    case (#err(err)) return #err(err);
                };
            };

            #ok();
        };

        public func updateById(id : Nat, update_operations : [(Text, T.FieldUpdateOperations)]) : Result<(), Text> {
            handleResult(
                StableCollection.update_by_id(collection, main_btree_utils, id, update_operations),
                "Failed to update record with id: " # debug_show (id),
            );
        };

        public func update(query_builder : QueryBuilder, update_operations : [(Text, T.FieldUpdateOperations)]) : Result<(), Text> {
            let records_iter = switch (
                handleResult(
                    StableCollection.internal_search(collection, query_builder),
                    "Failed to find records to update",
                )
            ) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            for (id in records_iter) {
                switch (StableCollection.update_by_id(collection, main_btree_utils, id, update_operations)) {
                    case (#ok(_)) {};
                    case (#err(err)) {
                        Logger.lazyError(collection.logger, func() = "Failed to update record with id: " # debug_show (id) # ": " # err);
                        return #err("Failed to update record with id: " # debug_show (id) # ": " # err);
                    };
                };
            };

            #ok;
        };

        public func deleteById(id : Nat) : Result<Record, Text> {
            switch (
                handleResult(
                    StableCollection.delete_by_id(collection, main_btree_utils, id),
                    "Failed to delete record with id: " # debug_show (id),
                )
            ) {
                case (#err(err)) return #err(err);
                case (#ok(record_details)) {
                    let record = blobify.from_blob(record_details);
                    #ok(record);
                };
            };
        };

        public func delete(query_builder : QueryBuilder) : Result<[Record], Text> {
            let internal_search_res = handleResult(
                StableCollection.internal_search(collection, query_builder),
                "Failed to find records to delete",
            );

            let results_iter = switch (internal_search_res) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            let buffer = Buffer.Buffer<Record>(8);
            for ((id) in results_iter) {
                switch (deleteById(id)) {
                    case (#ok(record)) buffer.add(record);
                    case (#err(err)) return #err(err);
                };
            };

            #ok(Buffer.toArray(buffer));
        };

    };

};
