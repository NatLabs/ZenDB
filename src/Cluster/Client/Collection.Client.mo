import Array "mo:base@0.16.0/Array";
import Option "mo:base@0.16.0/Option";
import Debug "mo:base@0.16.0/Debug";

import ZT "../../Types";

import ClusterTypes "../Types";
import Utils "../../Utils";

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

        public func insert(record : Record) : async* (ZT.Result<ZT.DocumentId, Text>) {
            let blob = candify.to_blob(record);
            await canister_db.zendb_collection_insert_document(db_name, collection_name, blob);
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

        public func from_search(search_response : ZT.Result<[(ZT.DocumentId, Blob)], Text>) : ZT.Result<[(ZT.DocumentId, Record)], Text> {
            switch (search_response) {
                case (#ok(results)) { #ok(search_results_from_blobs(results)) };
                case (#err(e)) { #err(e) };
            };
        };

        public func replace(document_id : ZT.DocumentId, record : Record) : async* (ZT.Result<(), Text>) {
            let blob = candify.to_blob(record);
            await canister_db.zendb_collection_replace_document(db_name, collection_name, document_id, blob);
        };

        public func delete_by_id(document_id : ZT.DocumentId) : async* (ZT.Result<Blob, Text>) {
            await canister_db.zendb_collection_delete_document_by_id(db_name, collection_name, document_id);
        };

        // public func delete(db_query : ZT.StableQuery) : async* (ZT.Result<Nat, Text>) {
        //     await canister_db.zendb_collection_delete_documents(db_name, collection_name, db_query);
        // };

        public func update_by_id(document_id : ZT.DocumentId, updates : [(Text, ZT.FieldUpdateOperations)]) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_update_document_by_id(db_name, collection_name, document_id, updates);
        };

        // public func update(db_query : ZT.StableQuery, updates : [(Text, ZT.FieldUpdateOperations)]) : async* (ZT.Result<[ZT.DocumentId], Text>) {
        //     await canister_db.zendb_collection_update_documents(db_name, collection_name, db_query, updates);
        // };

        public func get_schema() : async* (ZT.Result<ZT.Schema, Text>) {
            await canister_db.zendb_collection_get_schema(db_name, collection_name);
        };

        public func create_index(index_name : Text, index_fields : [(Text, ZT.SortDirection)], options : ?ZT.CreateIndexOptions) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_create_index(db_name, collection_name, index_name, index_fields, options);
        };

        public func delete_index(index_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_delete_index(db_name, collection_name, index_name);
        };

        public func repopulate_index(index_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_repopulate_index(db_name, collection_name, index_name);
        };

    };
};
