import CanisterDB_types "../Types";
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

        public func create_collection<Record>(collection_name : Text, schema : ZT.Schema, opt_options : ?ZT.CreateCollectionOptions) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_create_collection(db_name, collection_name, schema, opt_options);
        };

        public func delete_collection(collection_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_delete_collection(db_name, collection_name);
        };

        public func get_collection<Record>(collection_name : Text, candify : ZT.Candify<Record>) : CollectionClient.CollectionClient<Record> {
            CollectionClient.CollectionClient<Record>(canister_db, db_name, collection_name, candify);
        };

        /// Get database statistics
        public func stats() : async* ZT.DatabaseStats {
            await canister_db.zendb_database_stats(db_name);
        };

    };
};
