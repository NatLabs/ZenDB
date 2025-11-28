import ZT "../../EmbeddedInstance/Types";

import CollectionClient "Collection.Client";
import ClusterTypes "../Types";

module {

    // public type CollectionClient<Record> = CollectionClient.CollectionClient<Record>;

    /// The database connects to a database canister.
    public class DatabaseClient(canister_db : ClusterTypes.ClusterApiService, db_name : Text) {

        public func name() : async* Text {
            db_name;
        };

        public func createCollection<Record>(collection_name : Text, schema : ZT.Schema, opt_options : ?ZT.CreateCollectionOptions) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_v1_create_collection(db_name, collection_name, schema, opt_options);
        };

        public func deleteCollection(collection_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_v1_delete_collection(db_name, collection_name);
        };

        public func getCollection<Record>(collection_name : Text, candify : ZT.Candify<Record>) : CollectionClient.CollectionClient<Record> {
            CollectionClient.CollectionClient<Record>(canister_db, db_name, collection_name, candify);
        };

        /// Get database statistics
        public func stats() : async* ZT.DatabaseStats {
            await canister_db.zendb_v1_database_stats(db_name);
        };

        /// List all collection names in the database
        public func listCollectionNames() : async* [Text] {
            await canister_db.zendb_v1_database_list_collection_names(db_name);
        };

        /// Get statistics for a specific collection
        public func getCollectionStats(collection_name : Text) : async* ?ZT.CollectionStats {
            await canister_db.zendb_v1_database_get_collection_stats(db_name, collection_name);
        };

        /// Get statistics for all collections in the database
        public func getAllCollectionsStats() : async* [(Text, ZT.CollectionStats)] {
            await canister_db.zendb_v1_database_get_all_collections_stats(db_name);
        };

    };
};
