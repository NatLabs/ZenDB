import Map "mo:map@9.0.1/Map";
import Iter "mo:base@0.16.0/Iter";
import Array "mo:base@0.16.0/Array";
import LruCache "mo:lru-cache";

import V0_2_2 "../v0.2.2/types";
import V0_2_3 "types";

module {

    func new_candid_map_cache<K, V>(capacity : Nat) : V0_2_3.TwoQueueCache<K, V> {

        let max_main_cache_size = capacity / 2; // 50%
        let max_ghost_cache_size = max_main_cache_size / 2; // 25%
        let max_admission_cache_size = capacity - max_main_cache_size - max_ghost_cache_size : Nat; // 25%

        {
            var main_cache = LruCache.new<K, V>(max_main_cache_size);
            var ghost_cache = LruCache.new<K, V>(max_ghost_cache_size);
            var admission_cache = LruCache.new<K, ()>(max_admission_cache_size);
        } : V0_2_3.TwoQueueCache<K, V>;

    };

    public func upgrade(prev : V0_2_2.StableStore) : V0_2_3.StableStore {

        let candid_map_cache = new_candid_map_cache<V0_2_3.DocumentId, V0_2_3.CandidMap>(1_000_000);

        let migrated_databases = Map.map<Text, V0_2_2.StableDatabase, V0_2_3.StableDatabase>(
            prev.databases,
            Map.thash,
            func(db_name : Text, db : V0_2_2.StableDatabase) : V0_2_3.StableDatabase {
                {
                    db with
                    collections = migrate_collections(db.collections, candid_map_cache);
                    candid_map_cache = candid_map_cache;
                };
            },
        );

        {
            prev with
            databases = migrated_databases;
            candid_map_cache;
        };
    };

    func migrate_collections(collections : Map.Map<Text, V0_2_2.StableCollection>, candid_map_cache : V0_2_3.TwoQueueCache<V0_2_3.DocumentId, V0_2_3.CandidMap>) : Map.Map<Text, V0_2_3.StableCollection> {
        Map.map<Text, V0_2_2.StableCollection, V0_2_3.StableCollection>(
            collections,
            Map.thash,
            func(collection_name : Text, collection : V0_2_2.StableCollection) : V0_2_3.StableCollection {
                {
                    collection with
                    candid_map_cache = candid_map_cache;
                };
            },
        );
    };

};
