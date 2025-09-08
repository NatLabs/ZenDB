import CanisterDB_types "../Types";
import ZT "../../Types";

import CollectionClient "Collection.Client";
import ClusterTypes "../Types";

module {

    // public type CollectionClient<Record> = CollectionClient.CollectionClient<Record>;

    /// The database connects to a database canister.
    public class DatabaseClient(canister_db : ClusterTypes.ClusterApiService, db_name : Text) {

        public func name() : async* Text {
            db_name;
        };

        public func create_collection<Record>(collection_name : Text, schema : ZT.Schema, collection_client : CollectionClient.CollectionClient<Record>) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_create_collection(db_name, collection_name, schema);
        };

        public func get_collection<Record>(collection_name : Text, candify : ZT.Candify<Record>) : CollectionClient.CollectionClient<Record> {
            CollectionClient.CollectionClient<Record>(canister_db, db_name, collection_name, candify);
        };

    };
};
