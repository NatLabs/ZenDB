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

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import ByteUtils "../ByteUtils";
import C "../Constants";
import BTree "../BTree";

import Index "Index";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "Utils";
import QueryPlan "QueryPlan";
import QueryExecution "QueryExecution";
import StableCollection "StableCollection";
import Logger "../Logger";
import DocumentStore "DocumentStore";

module {

    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; nhash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    type QueryBuilder = Query.QueryBuilder;

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
        public func _get_stable_state() : StableCollection { collection };
        public func _get_schema() : T.Schema { collection.schema };
        public func _get_schema_map() : T.SchemaMap { collection.schema_map };
        public func _get_indexes() : Map<Text, Index> { collection.indexes };
        public func _get_index(name : Text) : Index = switch (Map.get(collection.indexes, T.thash, name)) {
            case (?(index)) return index;
            case (null) Debug.trap("Internal function error '_get_index()': You shouldn't be using this function anyway");
        };

        /// Returns the collection name.
        public func name() : Text = collection_name;

        /// Returns the total number of records in the collection.
        public func size() : Nat = StableCollection.size(collection);

        let main_btree_utils : T.BTreeUtils<Nat, T.Document> = DocumentStore.get_btree_utils(collection.documents);

        /// Returns an iterator over all the record ids in the collection.
        public func keys() : Iter<Nat> {
            StableCollection.keys(collection, main_btree_utils);
        };

        /// Returns an iterator over all the records in the collection.
        public func vals() : Iter<Record> {
            let iter = StableCollection.vals(collection, main_btree_utils);
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
            let iter = StableCollection.entries(collection, main_btree_utils);

            let records = Iter.map<(Nat, Blob), (Nat, Record)>(
                iter,
                func((id, candid_blob) : (Nat, Blob)) {
                    (id, blobify.from_blob(candid_blob));
                },
            );
            records;
        };

        /// Insert a record that matches the collection's schema.
        /// If the record passes the schema validation and schema constraints, it will be inserted into the collection and a unique id will be assigned to it and returned.
        ///
        /// Example:
        /// ```motoko
        /// let #ok(id) = collection.insert(record);
        /// ```
        ///
        /// If the record does not pass the schema validation or schema constraints, an error will be returned.
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

        /// Retrieves a record by its id.
        public func get(id : Nat) : ?Record {
            Option.map(
                StableCollection.get(collection, main_btree_utils, id),
                blobify.from_blob,
            );
        };

        type RecordLimits = [(Text, ?State<T.CandidQuery>)];
        type FieldLimit = (Text, ?State<T.CandidQuery>);

        type Bounds = (RecordLimits, RecordLimits);

        type IndexDetails = {
            var sorted_in_reverse : ?Bool;
            intervals : Buffer.Buffer<(Nat, Nat)>;
        };

        type Iter<A> = Iter.Iter<A>;

        public func searchIter(query_builder : QueryBuilder) : Result<Iter<T.WrapId<Record>>, Text> {
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

        /// This function is used to search for records in the collection by using a query builder.
        /// The query builder takes a set of queries or filters on the fields in the records and uses this as instructions to search for the specified records.
        ///
        /// Example:
        /// - Search for all records with a field "name" equal to "John":
        /// ```motoko
        ///
        /// let #ok(records_named_john) = collection.search(
        ///     ZenDB.QueryBuilder().Where("name", #eq("John"))
        /// );
        /// ```
        ///
        /// - Search for all records with a field "age" greater than 18, sorted by "age" in descending order:
        /// ```motoko
        /// let #ok(records_older_than_18) = collection.search(
        ///     ZenDB.QueryBuilder().Where("age", #gt(18)).Sort("age", #Descending)
        /// );
        /// ```
        ///
        /// - Search for all records with name "John" or "Jane", and age greater than 18:
        /// ```motoko
        /// let #ok(records_named_john_or_jane) = collection.search(
        ///     ZenDB.QueryBuilder()
        ///         .Where("name", #anyOf([#Text("John"), #Text("Jane")]))
        ///         .And("age", #gt(18))
        /// );
        /// ```
        ///
        /// Could also be written as:
        /// ```motoko
        /// let #ok(records_named_john_or_jane) = collection.search(
        ///     ZenDB.QueryBuilder()
        ///         .Where("name", #eq(#Text("John")))
        ///            .Or("name", #eq(#Text("Jane")))
        ///         .And("age", #gt(18))
        /// );
        /// ```
        ///
        /// Or as nested queries:
        /// ```motoko
        /// let #ok(records_named_john_or_jane) = collection.search(
        ///     ZenDB.QueryBuilder()
        ///          .Where("age", #gt(18))
        ///          .AndQuery(
        ///              ZenDB.QueryBuilder()
        ///                  .Where("name", #eq(#Text("John")))
        ///                  .Or("name", #eq(#Text("Jane")))
        ///          )
        /// );
        /// ```
        ///
        /// @returns A Result containing an array of tuples containing the id and the record for all matching records.
        /// If the search fails, an error message will be returned.

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

        // public func stats() : T.CollectionStats {
        //     StableCollection.stats(collection);
        // };

        /// Returns the total number of records that match the query.
        /// This ignores the limit and skip parameters.
        public func count(query_builder : QueryBuilder) : Result<Nat, Text> {
            handleResult(
                StableCollection.count(collection, query_builder),
                "Failed to count records",
            );
        };

        public func replace(id : Nat, record : Record) : Result<(), Text> {
            handleResult(
                StableCollection.replace_record_by_id(collection, main_btree_utils, id, blobify.to_blob(record)),
                "Failed to replace record with id: " # debug_show (id),
            );
        };

        public func replaceDocs(records : [(Nat, Record)]) : Result<(), Text> {
            for ((id, record) in records.vals()) {
                switch (replace(id, record)) {
                    case (#ok(_)) {};
                    case (#err(err)) return #err(err);
                };
            };

            #ok();
        };

        /// Updates a record by its id with the given update operations.
        public func updateById(id : Nat, update_operations : [(Text, T.FieldUpdateOperations)]) : Result<(), Text> {
            handleResult(
                StableCollection.update_by_id(collection, main_btree_utils, id, update_operations),
                "Failed to update record with id: " # debug_show (id),
            );
        };

        public func update(query_builder : QueryBuilder, update_operations : [(Text, T.FieldUpdateOperations)]) : Result<Nat, Text> {
            let records_iter = switch (
                handleResult(
                    StableCollection.internal_search(collection, query_builder),
                    "Failed to find records to update",
                )
            ) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            var total_updated = 0;

            for (id in records_iter) {
                switch (StableCollection.update_by_id(collection, main_btree_utils, id, update_operations)) {
                    case (#ok(_)) total_updated += 1;
                    case (#err(err)) {
                        Logger.lazyError(collection.logger, func() = "Failed to update record with id: " # debug_show (id) # ": " # err);
                        return #err("Failed to update record with id: " # debug_show (id) # ": " # err);
                    };
                };
            };

            #ok(total_updated);
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

        public func delete(query_builder : QueryBuilder) : Result<[(T.RecordId, Record)], Text> {
            let internal_search_res = handleResult(
                StableCollection.internal_search(collection, query_builder),
                "Failed to find records to delete",
            );

            let results_iter = switch (internal_search_res) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            // need to convert the iterator to an array before deleting
            // to avoid invalidating the iterator as its reference in the btree
            // might slide when elements are deleted.

            let results = Iter.toArray(results_iter);

            let buffer = Buffer.Buffer<(T.RecordId, Record)>(8);
            for ((id) in results.vals()) {
                switch (deleteById(id)) {
                    case (#ok(record)) buffer.add(id, record);
                    case (#err(err)) return #err(err);
                };
            };

            #ok(Buffer.toArray(buffer));
        };

        public func filterIter(condition : (Record) -> Bool) : Iter<Record> {

            let iter = StableCollection.vals(collection, main_btree_utils);
            let records = Iter.map<Blob, Record>(
                iter,
                func(candid_blob : Blob) {
                    blobify.from_blob(candid_blob);
                },
            );
            let filtered = Iter.filter<Record>(records, condition);

        };

        public func filter(condition : (Record) -> Bool) : [Record] {
            Iter.toArray(filterIter(condition));
        };

        /// Clear all the data in the collection.
        public func clear() {
            StableCollection.clear(collection);
        };

        // public func update_schema(schema : Schema) : Result<(), Text> {
        //     handleResult(StableCollection.update_schema(collection, schema), "Failed to update schema");
        // };

        type CreateIndexOptions = {
            isUnique : Bool;
        };

        /// Creates a new index with the given index keys.
        /// If `isUnique` is true, the index will be unique on the index keys and records with duplicate index keys will be rejected.
        public func createIndex(name : Text, index_key_details : [(Text, SortDirection)], options : ?CreateIndexOptions) : Result<(), Text> {

            let isUnique = switch (options) {
                case (?options) options.isUnique;
                case (null) false;
            };

            switch (StableCollection.create_index(collection, main_btree_utils, name, index_key_details, isUnique)) {
                case (#ok(success)) #ok();
                case (#err(errorMsg)) {
                    return Utils.log_error_msg(collection.logger, "Failed to create index (" # name # "): " # errorMsg);
                };
            };

        };

        /// Deletes an index from the collection that is not used internally.
        public func deleteIndex(name : Text) : Result<(), Text> {
            handleResult(
                StableCollection.delete_index(collection, main_btree_utils, name),
                "Failed to delete index: " # name,
            );
        };

        /// Clears an index from the collection that is not used internally.
        public func clearIndex(name : Text) : Result<(), Text> {
            handleResult(
                StableCollection.clear_index(collection, main_btree_utils, name),
                "Failed to clear index: " # name,
            );
        };

        public func createAndPopulateIndex(name : Text, index_key_details : [(Text, SortDirection)]) : Result<(), Text> {
            handleResult(
                StableCollection.create_and_populate_index(collection, main_btree_utils, name, index_key_details),
                "Failed to create and populate index: " # name,
            );
        };

        public func populateIndex(name : Text) : Result<(), Text> {
            handleResult(
                StableCollection.populate_index(collection, main_btree_utils, name),
                "Failed to populate index: " # name,
            );
        };

        public func populateIndexes(names : [Text]) : Result<(), Text> {
            handleResult(
                StableCollection.populate_indexes(collection, main_btree_utils, names),
                "Failed to populate indexes: " # debug_show (names),
            );
        };

    };

};
