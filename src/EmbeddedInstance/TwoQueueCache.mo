import LruCache "mo:lru-cache";
import Map "mo:map/Map";

import T "Types";

module TwoQueueCache {

    func admission_utils<K, V>(
        cache : T.TwoQueueCache<K, V>,
        hash : Map.HashUtils<K>,
        get_value : (K) -> V,
    ) : LruCache.LruCacheUtils<K, ()> {
        LruCache.utils<K, ()>(
            hash,
            func(evicted_key : K, _ : ()) {
                // When admission cache evicts, we don't have the value yet (keys only)
                // So we fetch it and store in ghost cache
                let value = get_value(evicted_key);
                let ghost_utils = LruCache.defaultUtils<K, V>(hash);
                LruCache.put(cache.ghost_cache, ghost_utils, evicted_key, value);
            },
        );
    };

    public func new<K, V>(capacity : Nat) : T.TwoQueueCache<K, V> {

        let max_main_cache_size = capacity / 2; // 50%
        let max_ghost_cache_size = max_main_cache_size / 2; // 25%
        let max_admission_cache_size = capacity - max_main_cache_size - max_ghost_cache_size : Nat; // 25%

        let main_cache = LruCache.new<K, V>(max_main_cache_size);
        let ghost_cache = LruCache.new<K, V>(max_ghost_cache_size);
        let admission_cache = LruCache.new<K, ()>(max_admission_cache_size);

        {
            var main_cache = main_cache;
            var ghost_cache = ghost_cache;
            var admission_cache = admission_cache;
        };

    };

    public func resize<K, V>(cache : T.TwoQueueCache<K, V>, new_capacity : Nat) {

        let max_main_cache_size = new_capacity / 2; // 50%
        let max_ghost_cache_size = max_main_cache_size / 2; // 25%
        let max_admission_cache_size = new_capacity - max_main_cache_size - max_ghost_cache_size : Nat; // 25%

        let old_capacity = capacity(cache);

        if (new_capacity >= old_capacity) {
            cache.main_cache.capacity := max_main_cache_size;
            cache.ghost_cache.capacity := max_ghost_cache_size;
            cache.admission_cache.capacity := max_admission_cache_size;
            return;
        };

        cache.main_cache := LruCache.new<K, V>(max_main_cache_size);
        cache.ghost_cache := LruCache.new<K, V>(max_ghost_cache_size);
        cache.admission_cache := LruCache.new<K, ()>(max_admission_cache_size);

    };

    public func size<K, V>(cache : T.TwoQueueCache<K, V>) : Nat {
        LruCache.size(cache.main_cache) + LruCache.size(cache.ghost_cache) + LruCache.size(cache.admission_cache);
    };

    public func capacity<K, V>(cache : T.TwoQueueCache<K, V>) : Nat {
        LruCache.capacity(cache.main_cache) + LruCache.capacity(cache.ghost_cache) + LruCache.capacity(cache.admission_cache);
    };

    public func get<K, V>(cache : T.TwoQueueCache<K, V>, hash : Map.HashUtils<K>, key : K, get_value : (K) -> V) : ?V {
        let main_utils = LruCache.defaultUtils<K, V>(hash);
        let ghost_utils = LruCache.defaultUtils<K, V>(hash);
        let a_utils = admission_utils<K, V>(cache, hash, get_value);

        switch (LruCache.get<K, V>(cache.main_cache, main_utils, key)) {
            case (?value) return ?value;
            case (null) {};
        };

        switch (LruCache.remove(cache.ghost_cache, ghost_utils, key)) {
            case (?value) {
                LruCache.put(cache.main_cache, main_utils, key, value);
                return ?value;
            };
            case (null) {};
        };

        switch (LruCache.remove(cache.admission_cache, a_utils, key)) {
            case (?()) {
                let value = get_value(key);
                LruCache.put(cache.main_cache, main_utils, key, value);
                return ?value;
            };
            case (null) {};
        };

        // this works like a fifo queue because we are not moving any of the elements back to the
        // top of the queue because we never access them (get()) we only add (put()) or remove them
        // from the queue. Which makes the least recently used element the top element in a fifo
        // queue that will be removed when the queue is full.
        LruCache.put(cache.admission_cache, a_utils, key, ());

        null;
    };

    // peek without updating the cache state
    public func peek<K, V>(cache : T.TwoQueueCache<K, V>, hash : Map.HashUtils<K>, key : K) : ?V {
        let utils = LruCache.defaultUtils<K, V>(hash);

        switch (LruCache.peek(cache.main_cache, utils, key)) {
            case (?value) return ?value;
            case (null) {};
        };

        switch (LruCache.peek(cache.ghost_cache, utils, key)) {
            case (?value) return ?value;
            case (null) {};
        };

        // Not checking admission cache because it only stores keys, no values to return

        null;
    };

    public func put<K, V>(cache : T.TwoQueueCache<K, V>, hash : Map.HashUtils<K>, key : K, value : V) : () {
        let main_utils = LruCache.defaultUtils<K, V>(hash);
        let ghost_utils = LruCache.defaultUtils<K, V>(hash);
        let admission_utils = LruCache.defaultUtils<K, ()>(hash);

        switch (LruCache.peek(cache.main_cache, main_utils, key)) {
            case (?prev_value) return LruCache.put(cache.main_cache, main_utils, key, value);
            case (null) {};
        };

        switch (LruCache.remove(cache.ghost_cache, ghost_utils, key)) {
            case (?prev_value) return LruCache.put(cache.main_cache, main_utils, key, value);
            case (null) {};
        };

        switch (LruCache.remove(cache.admission_cache, admission_utils, key)) {
            case (?()) return LruCache.put(cache.main_cache, main_utils, key, value);
            case (null) {};
        };

        // add to admission_cache
        LruCache.put(cache.admission_cache, admission_utils, key, ());
    };

    // remove from the cache completely - the entry is likely being removed from the main data store
    public func remove<K, V>(cache : T.TwoQueueCache<K, V>, hash : Map.HashUtils<K>, key : K) : ?V {
        let main_utils = LruCache.defaultUtils<K, V>(hash);
        let ghost_utils = LruCache.defaultUtils<K, V>(hash);
        let admission_utils = LruCache.defaultUtils<K, ()>(hash);

        ignore LruCache.remove(cache.admission_cache, admission_utils, key);

        let main_entry = LruCache.remove(cache.main_cache, main_utils, key);
        let ghost_entry = LruCache.remove(cache.ghost_cache, ghost_utils, key);

        switch (main_entry, ghost_entry) {
            case (?main, _) return ?main;
            case (_, ?ghost) return ?ghost;
            case (_, _) {};
        };

        null

    };

};
