import CanisterDB_types "../Types";
import ZT "../../Types";

import CollectionClient "../CollectionClient";

module {

    public type CanisterDB = CanisterDB_types.Service;
    public type CollectionClient<Record> = CollectionClient.CollectionClient<Record>;

    /// The database connects to a database canister.
    public class DatabaseClient(canister_id : Text) {

        let canister_db : CanisterDB = actor (canister_id);

        public func api_version() : async* Nat {
            await canister_db.zendb_api_version();
        };

        public func get_database_name() : async* Text {
            await canister_db.zendb_get_database_name();
        };

        public func create_collection<Record>(collection_name : Text, schema : ZT.Schema, collection_client : CollectionClient<Record>) : async* (ZT.Result<(), Text>) {
            let res = await canister_db.zendb_create_collection(collection_name, schema);

            switch (res) {
                case (#err(msg)) #err(msg);
                case (#ok(collection_name)) {
                    collection_client.set_canister_db(canister_db);
                    collection_client.set_collection_name(collection_name);
                    #ok();
                };
            };
        };

        public func delete_collection(collection_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_delete_collection(collection_name);
        };

    };
};
