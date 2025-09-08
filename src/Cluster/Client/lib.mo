import CanisterDB_types "../Types";
import ZT "../../Types";

import CollectionClient "Collection.Client";
import DatabaseClient "Database.Client";

import ClusterTypes "../Types";

module ClusterClient {

    public type CollectionClient<Record> = CollectionClient.CollectionClient<Record>;

    public class ClusterClient(canister_id : Text) {
        let canister_db : ClusterTypes.ClusterApiService = actor (canister_id);

        public func api_version() : async* Text {
            await canister_db.zendb_api_version();
        };

        public func get_database(db_name : Text) : DatabaseClient.DatabaseClient {
            DatabaseClient.DatabaseClient(canister_db, db_name);
        };

        public func launchDefaultDB() : DatabaseClient.DatabaseClient {
            DatabaseClient.DatabaseClient(canister_db, "default");
        };

    };

};
