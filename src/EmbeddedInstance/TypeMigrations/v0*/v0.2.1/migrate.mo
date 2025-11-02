import Map "mo:map@9.0.1/Map";
import Iter "mo:base@0.16.0/Iter";

import V0_2_0 "../v0.2.0/types";
import V0_2_1 "./types";

module {

    public func upgrade(prev : V0_2_0.StableStore) : V0_2_1.StableStore {

        let migrated_databases = Map.map<Text, V0_2_0.StableDatabase, V0_2_1.StableDatabase>(
            prev.databases,
            Map.thash,
            func(db_name : Text, db : V0_2_0.StableDatabase) : V0_2_1.StableDatabase {
                {
                    db with
                    collections = migrate_collections(db.collections);
                    is_running_locally = false; // Add new field with default value
                };
            },
        );

        {
            prev with
            databases = migrated_databases;
            is_running_locally = false; // Add new field with default value
        };
    };

    func migrate_collections(collections : Map.Map<Text, V0_2_0.StableCollection>) : Map.Map<Text, V0_2_1.StableCollection> {
        Map.map<Text, V0_2_0.StableCollection, V0_2_1.StableCollection>(
            collections,
            Map.thash,
            func(collection_name : Text, collection : V0_2_0.StableCollection) : V0_2_1.StableCollection {
                {
                    collection with
                    indexes = migrate_indexes(collection.indexes);
                    unique_constraints = migrate_unique_constraints(collection.unique_constraints);
                    indexes_in_batch_operations = Map.new<Text, V0_2_1.Index>(); // Add new empty map
                    populate_index_batches = Map.new<Nat, V0_2_1.BatchPopulateIndex>(); // Add new empty map
                    is_running_locally = false; // Add new field with default value
                };
            },
        );
    };

    func migrate_indexes(indexes : Map.Map<Text, V0_2_0.Index>) : Map.Map<Text, V0_2_1.Index> {
        Map.map<Text, V0_2_0.Index, V0_2_1.Index>(
            indexes,
            Map.thash,
            func(index_name : Text, index : V0_2_0.Index) : V0_2_1.Index {
                // Wrap the old Index type with #composite_index variant
                #composite_index({
                    name = index.name;
                    key_details = index.key_details;
                    data = index.data;
                    used_internally = index.used_internally;
                    is_unique = index.is_unique;
                });
            },
        );
    };

    func migrate_unique_constraints(
        constraints : [([Text], V0_2_0.Index)]
    ) : [([Text], V0_2_1.CompositeIndex)] {
        // Convert the Index type in unique_constraints to CompositeIndex
        // Since v0.2.0 Index is now v0.2.1 CompositeIndex
        Iter.toArray(
            Iter.map<([Text], V0_2_0.Index), ([Text], V0_2_1.CompositeIndex)>(
                constraints.vals(),
                func((fields, index) : ([Text], V0_2_0.Index)) : ([Text], V0_2_1.CompositeIndex) {
                    (
                        fields,
                        {
                            name = index.name;
                            key_details = index.key_details;
                            data = index.data;
                            used_internally = index.used_internally;
                            is_unique = index.is_unique;
                        },
                    );
                },
            )
        );
    };

};
