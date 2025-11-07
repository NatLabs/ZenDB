import Array "mo:base@0.16.0/Array";
import Option "mo:base@0.16.0/Option";
import Debug "mo:base@0.16.0/Debug";

import ZT "../../EmbeddedInstance/Types";

import ClusterTypes "../Types";
import Utils "../../EmbeddedInstance/Utils";

module {

    public class CollectionClient<Record>(
        canister_db : ClusterTypes.ClusterApiService,
        db_name : Text,
        collection_name : Text,
        candify : ZT.Candify<Record>,
    ) {

        let internal_candify = Utils.convert_to_internal_candify(collection_name, candify);

        public func name() : async* Text {
            collection_name;
        };

        public func size() : async* (Nat) {
            await canister_db.zendb_collection_size(db_name, collection_name);
        };

        /// Returns the total number of documents that match the query.
        /// This ignores the limit and skip parameters.
        public func count(stable_query : ZT.StableQuery) : async* (ZT.CountResult) {
            await canister_db.zendb_collection_count(db_name, collection_name, stable_query);
        };

        /// Returns the total number of documents that match the query (query).
        /// This ignores the limit and skip parameters.
        public func count_query(stable_query : ZT.StableQuery) : async* (ZT.CountResult) {
            await canister_db.zendb_collection_count_query(db_name, collection_name, stable_query);
        };

        public func insert(record : Record) : async* (ZT.Result<ZT.DocumentId, Text>) {
            let blob = candify.to_blob(record);
            await canister_db.zendb_collection_insert_document(db_name, collection_name, blob);
        };

        public func insertDocs(records : [Record]) : async* (ZT.Result<[ZT.DocumentId], Text>) {
            let blobs = Array.map<Record, Blob>(records, candify.to_blob);
            let res = await canister_db.zendb_collection_insert_documents(db_name, collection_name, blobs);
            return res;
        };

        public func get(document_id : ZT.DocumentId) : async* (ZT.Result<Blob, Text>) {
            await canister_db.zendb_collection_get_document(db_name, collection_name, document_id);
        };

        public func from_blob(blob : Blob) : Record {
            internal_candify.from_blob(blob);
        };

        public func to_blob(record : Record) : Blob {
            internal_candify.to_blob(record);
        };

        public func from_get(get_response : ZT.Result<ZT.CandidBlob, Text>) : ZT.Result<Record, Text> {
            switch (get_response) {
                case (#ok(blob)) { #ok(internal_candify.from_blob(blob)) };
                case (#err(e)) { #err(e) };
            };
        };

        func search_results_from_blobs(results : [(ZT.DocumentId, Blob)]) : [(ZT.DocumentId, Record)] {
            Array.map<(ZT.DocumentId, Blob), (ZT.DocumentId, Record)>(
                results,
                func(response_tuple : (ZT.DocumentId, Blob)) : (ZT.DocumentId, Record) {
                    (
                        response_tuple.0,
                        internal_candify.from_blob(response_tuple.1),
                    );
                },
            );
        };

        public func from_search(search_response : ZT.Result<ZT.SearchResult<Blob>, Text>) : ZT.Result<ZT.SearchResult<Record>, Text> {
            switch (search_response) {
                case (#ok(result)) {
                    #ok({
                        documents = search_results_from_blobs(result.documents);
                        instructions = result.instructions;
                    });
                };
                case (#err(e)) { #err(e) };
            };
        };

        public func search(stable_query : ZT.StableQuery) : async* (ZT.Result<ZT.SearchResult<Blob>, Text>) {
            await canister_db.zendb_collection_search(db_name, collection_name, stable_query);
        };

        public func search_query(stable_query : ZT.StableQuery) : async* (ZT.Result<ZT.SearchResult<Blob>, Text>) {
            await canister_db.zendb_collection_search_query(db_name, collection_name, stable_query);
        };

        public func replace(document_id : ZT.DocumentId, record : Record) : async* (ZT.Result<ZT.ReplaceByIdResult, Text>) {
            let blob = candify.to_blob(record);
            await canister_db.zendb_collection_replace_document(db_name, collection_name, document_id, blob);
        };

        public func delete_by_id(document_id : ZT.DocumentId) : async* (ZT.Result<ZT.DeleteByIdResult<Blob>, Text>) {
            await canister_db.zendb_collection_delete_document_by_id(db_name, collection_name, document_id);
        };

        public func delete(db_query : ZT.StableQuery) : async* (ZT.Result<ZT.DeleteResult<Blob>, Text>) {
            await canister_db.zendb_collection_delete_documents(db_name, collection_name, db_query);
        };

        public func update_by_id(document_id : ZT.DocumentId, updates : [(Text, ZT.FieldUpdateOperations)]) : async* (ZT.Result<ZT.UpdateByIdResult, Text>) {
            await canister_db.zendb_collection_update_document_by_id(db_name, collection_name, document_id, updates);
        };

        public func update(db_query : ZT.StableQuery, updates : [(Text, ZT.FieldUpdateOperations)]) : async* (ZT.Result<ZT.UpdateResult, Text>) {
            await canister_db.zendb_collection_update_documents(db_name, collection_name, db_query, updates);
        };

        public func get_schema() : async* (ZT.Result<ZT.Schema, Text>) {
            await canister_db.zendb_collection_get_schema(db_name, collection_name);
        };

        public func create_index(index_name : Text, index_fields : [(Text, ZT.SortDirection)], options : ?ZT.CreateIndexOptions) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_create_index(db_name, collection_name, index_name, index_fields, options);
        };

        public func batch_create_indexes(index_configs : [ZT.CreateIndexParams]) : async* (ZT.Result<Nat, Text>) {
            await canister_db.zendb_collection_batch_create_indexes(db_name, collection_name, index_configs);
        };

        public func batch_populate_indexes(index_names : [Text]) : async* (ZT.Result<Nat, Text>) {
            await canister_db.zendb_collection_batch_populate_indexes(db_name, collection_name, index_names);
        };

        public func process_index_batch(batch_id : Nat) : async* (ZT.Result<Bool, Text>) {
            await canister_db.zendb_collection_process_index_batch(db_name, collection_name, batch_id);
        };

        public func delete_index(index_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_delete_index(db_name, collection_name, index_name);
        };

        public func repopulate_index(index_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_repopulate_index(db_name, collection_name, index_name);
        };

        /// Get collection statistics
        public func stats() : async* ZT.CollectionStats {
            await canister_db.zendb_collection_stats(db_name, collection_name);
        };

    };
};
