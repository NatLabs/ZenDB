/// A collection is a set of documents of the same type.

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
import Int32 "mo:base@0.16.0/Int32";
import Blob "mo:base@0.16.0/Blob";
import Nat64 "mo:base@0.16.0/Nat64";
import Int16 "mo:base@0.16.0/Int16";
import Int64 "mo:base@0.16.0/Int64";
import Int8 "mo:base@0.16.0/Int8";
import Nat16 "mo:base@0.16.0/Nat16";
import Nat8 "mo:base@0.16.0/Nat8";
import InternetComputer "mo:base@0.16.0/ExperimentalInternetComputer";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.4.0";
import Decoder "mo:serde@3.4.0/Candid/Blob/Decoder";
import Candid "mo:serde@3.4.0/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import C "../Constants";
import BTree "../BTree";

import CompositeIndex "Index/CompositeIndex";
import Index "Index";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "CollectionUtils";
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

    type QueryBuilder = Query.QueryBuilder;

    public class Collection<Record>(
        collection_name : Text,
        collection : T.StableCollection,
        blobify : T.InternalCandify<Record>,
    ) = self {
        let LOGGER_NAMESPACE = "Collection";

        /// Generic helper function to handle Result types with consistent error logging
        private func handleResult<T>(res : T.Result<T, Text>, context : Text) : T.Result<T, Text> {
            let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("handleResult");
            switch (res) {
                case (#ok(success)) #ok(success);
                case (#err(errorMsg)) {
                    log.lazyError(func() = context # ": " # errorMsg);
                    #err(errorMsg);
                };
            };
        };

        /// for debugging
        public func _get_stable_state() : T.StableCollection { collection };
        public func _get_schema() : T.Schema { collection.schema };
        public func _get_schema_map() : T.SchemaMap { collection.schema_map };
        public func _get_indexes() : Map<Text, T.Index> {
            collection.indexes;
        };
        public func _get_index(name : Text) : T.Index = switch (Map.get(collection.indexes, T.thash, name)) {
            case (?(index)) return index;
            case (null) Debug.trap("Internal function error '_get_index()': use getIndex() instead");
        };

        /// Returns the collection name.
        public func name() : Text = collection_name;
        public func getSchema() : T.Schema { collection.schema };

        public func get_schema() : T.Schema { collection.schema };

        /// Returns the total number of documents in the collection.
        public func size() : Nat = StableCollection.size(collection);
        public func isEmpty() : Bool = StableCollection.size(collection) == 0;

        /// Returns an iterator over all the document ids in the collection.
        public func keys() : Iter<T.DocumentId> {
            StableCollection.keys(collection);
        };

        /// Returns an iterator over all the documents in the collection.
        public func vals() : Iter<Record> {
            let iter = StableCollection.vals(collection);
            let documents = Iter.map<Blob, Record>(
                iter,
                func(candid_blob : Blob) {
                    blobify.from_blob(candid_blob);
                },
            );
            documents;
        };

        /// Returns an iterator over a tuple containing the id and document for all entries in the collection.
        public func entries() : Iter<(T.DocumentId, Record)> {
            let iter = StableCollection.entries(collection);

            let documents = Iter.map<(T.DocumentId, Blob), (T.DocumentId, Record)>(
                iter,
                func((id, candid_blob) : (T.DocumentId, Blob)) {
                    (id, blobify.from_blob(candid_blob));
                },
            );
            documents;
        };

        /// Insert a document that matches the collection's schema.
        /// If the document passes the schema validation and schema constraints, it will be inserted into the collection and a unique id will be assigned to it and returned.
        ///
        /// Example:
        /// ```motoko
        /// let #ok(id) = collection.insert(document);
        /// ```
        ///
        /// If the document does not pass the schema validation or schema constraints, an error will be returned.
        public func insert(document : Record) : Result<(T.DocumentId), Text> {
            let candid_blob = blobify.to_blob(document);

            handleResult(
                StableCollection.insert(collection, candid_blob),
                "Failed to insert document",
            );
        };

        public func insertDocs(documents : [Record]) : Result<[T.DocumentId], Text> {
            StableCollection.insert_docs(collection, Array.map<Record, Blob>(documents, blobify.to_blob));
        };

        /// Retrieves a document by its id.
        public func get(id : T.DocumentId) : ?Record {
            Option.map(
                StableCollection.get(collection, id),
                blobify.from_blob,
            );
        };

        // public func exists(db_query : QueryBuilder) : T.Result<Bool, Text> {
        //     let internal_search_res = handleResult(
        //         StableCollection.exists(collection, db_query),
        //         "Failed to find documents to check existence",
        //     );

        //     let results_iter = switch (internal_search_res) {
        //         case (#err(err)) return #err(err);
        //         case (#ok(documents_iter)) documents_iter;
        //     };

        //     #ok(
        //         Option.isSome(
        //             results_iter.next()
        //         )
        //     );
        // };

        type DocumentLimits = [(Text, ?T.State<T.CandidQuery>)];
        type FieldLimit = (Text, ?T.State<T.CandidQuery>);

        type Bounds = (DocumentLimits, DocumentLimits);

        type IndexDetails = {
            var sorted_in_reverse : ?Bool;
            intervals : Buffer.Buffer<T.Interval>;
        };

        type Iter<A> = Iter.Iter<A>;

        public func searchIter(query_builder : QueryBuilder) : T.Result<Iter<T.WrapId<Record>>, Text> {
            switch (
                handleResult(
                    StableCollection.internal_search(collection, query_builder.build()),
                    "Failed to execute search",
                )
            ) {
                case (#err(err)) return #err(err);
                case (#ok(document_ids_iter)) {
                    let document_iter = StableCollection.ids_to_documents(collection, blobify, document_ids_iter);
                    #ok(document_iter);
                };
            };
        };

        /// This function is used to search for documents in the collection by using a query builder.
        /// The query builder takes a set of queries or filters on the fields in the documents and uses this as instructions to search for the specified documents.
        ///
        /// Example:
        /// - Search for all documents with a field "name" equal to "John":
        /// ```motoko
        ///
        /// let #ok(documents_named_john) = collection.search(
        ///     ZenDB.QueryBuilder().Where("name", #eq("John"))
        /// );
        /// ```
        ///
        /// - Search for all documents with a field "age" greater than 18, sorted by "age" in descending order:
        /// ```motoko
        /// let #ok(documents_older_than_18) = collection.search(
        ///     ZenDB.QueryBuilder().Where("age", #gt(18)).Sort("age", #Descending)
        /// );
        /// ```
        ///
        /// - Search for all documents with name "John" or "Jane", and age greater than 18:
        /// ```motoko
        /// let #ok(documents_named_john_or_jane) = collection.search(
        ///     ZenDB.QueryBuilder()
        ///         .Where("name", #anyOf([#Text("John"), #Text("Jane")]))
        ///         .And("age", #gt(18))
        /// );
        /// ```
        ///
        /// Could also be written as:
        /// ```motoko
        /// let #ok(documents_named_john_or_jane) = collection.search(
        ///     ZenDB.QueryBuilder()
        ///         .Where("name", #eq(#Text("John")))
        ///            .Or("name", #eq(#Text("Jane")))
        ///         .And("age", #gt(18))
        /// );
        /// ```
        ///
        /// Or as nested queries:
        /// ```motoko
        /// let #ok(documents_named_john_or_jane) = collection.search(
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
        /// @returns A Result containing an array of tuples containing the id and the document for all matching documents.
        /// If the search fails, an error message will be returned.

        public func search(query_builder : QueryBuilder) : T.Result<T.SearchResult<Record>, Text> {
            switch (
                StableCollection.search(collection, query_builder.build())
            ) {
                case (#err(err)) return #err(err);
                case (#ok(result)) {
                    let documents = Array.map<T.WrapId<T.CandidBlob>, T.WrapId<Record>>(
                        result.documents,
                        func((id, candid_blob) : T.WrapId<T.CandidBlob>) : T.WrapId<Record> {
                            (id, blobify.from_blob(candid_blob));
                        },
                    );

                    #ok({ result with documents = documents });
                };
            };
        };

        public func searchForOne(query_builder : QueryBuilder) : T.Result<T.SearchOneResult<Record>, Text> {
            switch (StableCollection.search_for_one(collection, query_builder.build())) {
                case (#err(err)) #err(err);
                case (#ok({ document; instructions })) {
                    #ok({
                        document = switch (document) {
                            case (null) null;
                            case (?doc) ?((doc.0, blobify.from_blob(doc.1)));
                        };
                        instructions;
                    });
                };
            };
        };

        public func stats() : T.CollectionStats {
            StableCollection.stats(collection);
        };

        /// Returns the total number of documents that match the query.
        /// This ignores the limit and skip parameters.
        public func count(query_builder : QueryBuilder) : T.Result<T.CountResult, Text> {
            handleResult(
                StableCollection.count(collection, query_builder.build()),
                "Failed to count documents",
            );
        };

        public func replace(id : T.DocumentId, document : Record) : T.Result<T.ReplaceByIdResult, Text> {
            handleResult(
                StableCollection.replace_by_id(collection, id, blobify.to_blob(document)),
                "Failed to replace document with id: " # debug_show (id),
            );
        };

        public func replaceDocs(documents : [(T.DocumentId, Record)]) : T.Result<T.ReplaceDocsResult, Text> {
            handleResult(
                StableCollection.replace_docs(
                    collection,
                    Array.map<(T.DocumentId, Record), (T.DocumentId, Blob)>(
                        documents,
                        func((id, doc) : (T.DocumentId, Record)) : (T.DocumentId, Blob) {
                            (id, blobify.to_blob(doc));
                        },
                    ),
                ),
                "Failed to replace documents",
            );
        };

        /// Updates a document by its id with the given update operations.
        public func updateById(id : T.DocumentId, update_operations : [(Text, T.FieldUpdateOperations)]) : T.Result<T.UpdateByIdResult, Text> {
            handleResult(
                StableCollection.update_by_id(collection, id, update_operations),
                "Failed to update document with id: " # debug_show (id),
            );
        };

        public func update(query_builder : QueryBuilder, update_operations : [(Text, T.FieldUpdateOperations)]) : T.Result<T.UpdateResult, Text> {
            handleResult(
                StableCollection.update_documents(collection, query_builder.build(), update_operations),
                "Failed to update documents",
            );
        };

        public func deleteById(id : T.DocumentId) : T.Result<T.DeleteByIdResult<Record>, Text> {
            switch (
                handleResult(
                    StableCollection.delete_by_id(collection, id),
                    "Failed to delete document with id: " # debug_show (id),
                )
            ) {
                case (#err(err)) return #err(err);
                case (#ok(result)) {
                    let document = blobify.from_blob(result.deleted_document);
                    #ok({
                        deleted_document = document;
                        instructions = result.instructions;
                    });
                };
            };
        };

        public func delete(query_builder : QueryBuilder) : T.Result<T.DeleteResult<Record>, Text> {
            handleResult(
                StableCollection.delete_documents(collection, blobify, query_builder.build()),
                "Failed to delete documents",
            );
        };

        public func deleteIndexes(index_names : [Text]) : T.Result<(), Text> {
            handleResult(
                StableCollection.delete_indexes(collection, index_names),
                "Failed to delete indexes",
            );
        };

        public func filterIter(condition : (Record) -> Bool) : Iter<Record> {

            let iter = StableCollection.vals(collection);
            let documents = Iter.map<Blob, Record>(
                iter,
                func(candid_blob : Blob) {
                    blobify.from_blob(candid_blob);
                },
            );
            let filtered = Iter.filter<Record>(documents, condition);

        };

        public func filter(condition : (Record) -> Bool) : [Record] {
            Utils.iter_to_array(filterIter(condition));
        };

        /// Clear all the data in the collection.
        public func clear() {
            StableCollection.clear(collection);
        };

        // public func update_schema(schema : T.Schema) : T.Result<(), Text> {
        //     handleResult(StableCollection.update_schema(collection, schema), "Failed to update schema");
        // };

        // Index Fns

        public func listIndexNames() : [Text] {
            StableCollection.list_index_names(collection);
        };

        public func getIndexes() : [(Text, T.IndexStats)] {
            StableCollection.get_indexes(collection);
        };

        public func getIndex(name : Text) : ?T.IndexStats {
            StableCollection.get_index(collection, name);
        };

        /// Creates a new index with the given index keys.
        /// If `is_unique` is true, the index will be unique on the index keys and documents with duplicate index keys will be rejected.
        public func createIndex(name : Text, index_key_details : [(Text, T.CreateIndexSortDirection)], opt_options : ?T.CreateIndexOptions) : T.Result<(), Text> {

            StableCollection.create_and_populate_index_in_one_call(
                collection,
                name,
                index_key_details,
                T.CreateIndexOptions.internal_from_opt(opt_options),
            );
        };

        public func batchCreateIndexes(index_configs : [T.CreateIndexParams]) : T.Result<(batch_id : Nat), Text> {

            let internal_index_configs = Array.map<T.CreateIndexParams, T.CreateInternalIndexParams>(
                index_configs,
                func(config : T.CreateIndexParams) : T.CreateInternalIndexParams {
                    (
                        config.0,
                        config.1,
                        T.CreateIndexOptions.internal_from_opt(config.2),
                    );
                },
            );

            handleResult(
                StableCollection.batch_create_indexes(collection, internal_index_configs),
                "Failed to create index batch",
            );
        };

        public func batchPopulateIndexes(index_names : [Text]) : T.Result<(batch_id : Nat), Text> {
            handleResult(
                StableCollection.batch_populate_indexes_from_names(collection, index_names),
                "Failed to create populate index batch",
            );
        };

        public func processIndexBatch(batch_id : Nat) : T.Result<(done_processing : Bool), Text> {
            handleResult(
                StableCollection.populate_indexes_in_batch(collection, batch_id, null),
                "Failed to process index batch with id: " # debug_show (batch_id),
            );
        };

        /// Deletes an index from the collection that is not used internally.
        public func deleteIndex(name : Text) : T.Result<(), Text> {
            handleResult(
                StableCollection.delete_index(collection, name),
                "Failed to delete index: " # name,
            );
        };

        /// Hide indexes from query planning.
        public func hideIndexes(index_names : [Text]) : T.Result<(), Text> {
            handleResult(
                StableCollection.hide_indexes(collection, index_names),
                "Failed to hide indexes: " # debug_show (index_names),
            );
        };

        /// Unhide indexes and make them available for query planning again.
        public func unhideIndexes(index_names : [Text]) : T.Result<(), Text> {
            handleResult(
                StableCollection.unhide_indexes(collection, index_names),
                "Failed to unhide indexes: " # debug_show (index_names),
            );
        };

        // /// Clears an index from the collection that is not used internally.
        // public func clearIndex(name : Text) : T.Result<(), Text> {
        //     handleResult(
        //         StableCollection.clear_index(collection, name),
        //         "Failed to clear index: " # name,
        //     );
        // };

        // public func repopulateIndex(name : Text) : T.Result<(), Text> {
        //     handleResult(
        //         StableCollection.repopulate_index(collection, name),
        //         "Failed to populate index: " # name,
        //     );
        // };

        // public func repopulateIndexes(names : [Text]) : T.Result<(), Text> {
        //     handleResult(
        //         StableCollection.repopulate_indexes(collection, names),
        //         "Failed to populate indexes: " # debug_show (names),
        //     );
        // };

        // public func createTextIndex(index_name : Text, name : Text, tokenizer : T.Tokenizer) : T.Result<(), Text> {
        //     let res = handleResult(
        //         StableCollection.create_text_index(collection, index_name, name, tokenizer),
        //         "Failed to create text index: " # name,
        //     );

        //     Result.mapOk<T.TextIndex, (), Text>(res, func(_) : () {});

        // };

    };

};
