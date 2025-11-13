import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Iter "mo:base@0.16.0/Iter";
import Array "mo:base@0.16.0/Array";

import V0_2_3 "../v0.2.3/types";
import V0_2_4 "types";

module {

    public func upgrade(prev : V0_2_3.StableStore) : V0_2_4.StableStore {

        let migrated_databases = Map.map<Text, V0_2_3.StableDatabase, V0_2_4.StableDatabase>(
            prev.databases,
            Map.thash,
            func(db_name : Text, db : V0_2_3.StableDatabase) : V0_2_4.StableDatabase {
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

    func migrate_collections(collections : Map.Map<Text, V0_2_3.StableCollection>) : Map.Map<Text, V0_2_4.StableCollection> {
        Map.map<Text, V0_2_3.StableCollection, V0_2_4.StableCollection>(
            collections,
            Map.thash,
            func(collection_name : Text, collection : V0_2_3.StableCollection) : V0_2_4.StableCollection {
                {
                    collection with
                    hidden_indexes = Set.new<Text>();
                };
            },
        );
    };

};
