import Map "mo:map@9.0.1/Map";
import Iter "mo:base@0.16.0/Iter";
import Candid "mo:serde@3.4.0/Candid";

import V0_2_1 "../v0.2.1/types";
import V0_2_2 "./types";

module {

    public func upgrade(prev : V0_2_1.StableStore) : V0_2_2.StableStore {

        let migrated_databases = Map.map<Text, V0_2_1.StableDatabase, V0_2_2.StableDatabase>(
            prev.databases,
            Map.thash,
            func(db_name : Text, db : V0_2_1.StableDatabase) : V0_2_2.StableDatabase {
                {
                    db with
                    collections = migrate_collections(db.collections);
                };
            },
        );

        {
            prev with
            databases = migrated_databases;
        };
    };

    func migrate_collections(collections : Map.Map<Text, V0_2_1.StableCollection>) : Map.Map<Text, V0_2_2.StableCollection> {
        Map.map<Text, V0_2_1.StableCollection, V0_2_2.StableCollection>(
            collections,
            Map.thash,
            func(collection_name : Text, collection : V0_2_1.StableCollection) : V0_2_2.StableCollection {
                // Remove candid_serializer field from v0.2.1 to create v0.2.2
                {
                    ids = collection.ids;
                    instance_id = collection.instance_id;
                    name = collection.name;
                    schema = collection.schema;
                    schema_map = collection.schema_map;
                    schema_keys = collection.schema_keys;
                    schema_keys_set = collection.schema_keys_set;

                    candid_serializer = Candid.TypedSerializer.new(
                        [collection.schema],
                        ?{
                            Candid.defaultOptions with types = ?[collection.schema]
                        },
                    );

                    documents = collection.documents;
                    indexes = collection.indexes;
                    indexes_in_batch_operations = collection.indexes_in_batch_operations;
                    populate_index_batches = collection.populate_index_batches;

                    field_constraints = collection.field_constraints;
                    unique_constraints = collection.unique_constraints;
                    fields_with_unique_constraints = collection.fields_with_unique_constraints;

                    freed_btrees = collection.freed_btrees;
                    logger = collection.logger;
                    memory_type = collection.memory_type;
                    is_running_locally = collection.is_running_locally;
                };
            },
        );
    };

};
