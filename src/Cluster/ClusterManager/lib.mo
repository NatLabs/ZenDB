import Debug "mo:base@0.16.0/Debug";
import Order "mo:base@0.16.0/Order";
import Array "mo:base@0.16.0/Array";
import Option "mo:base@0.16.0/Option";
import Principal "mo:base@0.16.0/Principal";
import Nat "mo:base@0.16.0/Nat";
import Float "mo:base@0.16.0/Float";
import Cycles "mo:base@0.16.0/ExperimentalCycles";
import Iter "mo:base@0.16.0/Iter";
import Timer "mo:base@0.16.0/Timer";

import Itertools "mo:itertools@0.2.2/Iter";
import Map "mo:map@9.0.1/Map";
import Buffer "mo:base@0.16.0/Buffer";
import Vector "mo:vector@0.4.2";
import BpTree "mo:augmented-btrees@0.7.1/BpTree";
import BpTreeCmp "mo:augmented-btrees@0.7.1/Cmp";

import ZT "../../Types";
import ClusterTypes "../Types";
import CanisterDB "../CanisterDB";
import Utils "../../Utils";

shared ({ caller = owner }) persistent actor class ClusterManager(
    init_canister_dbs : [Principal],
    opt_sharding_strategy : ?ClusterTypes.ShardingStrategy,
) {

    var sharding_strategy = Option.get(opt_sharding_strategy, ClusterTypes.DefaultShardingStrategy);

    func get_max_canister_size_in_bytes(sharding_strategy : ClusterTypes.ShardingStrategy) : Nat {
        switch (sharding_strategy) {
            case (#fill_first(s)) { s.max_canister_size_in_bytes };
        };
    };

    func get_min_threshold_percent(sharding_strategy : ClusterTypes.ShardingStrategy) : Float {
        switch (sharding_strategy) {
            case (#fill_first(s)) { s.threshold_percent };
        };
    };

    module CanistersMap {
        public type CanistersMap = {
            principal_map : Map.Map<Principal, ClusterTypes.CanisterInfo>;
            size_map : BpTree.BpTree<(Nat, Principal), Principal>;
        };

        public func new() : CanistersMap {
            {
                principal_map = Map.new<Principal, ClusterTypes.CanisterInfo>();
                size_map = BpTree.new<(Nat, Principal), Principal>(null);
            };
        };

        public func size_map_compare(a : (Nat, Principal), b : (Nat, Principal)) : Int8 {
            let order = switch (Nat.compare(a.0, b.0)) {
                case (#equal) Principal.compare(a.1, b.1);
                case (ord) ord;
            };

            switch (order) {
                case (#less) return -1;
                case (#equal) return 0;
                case (#greater) return 1;
            };
        };

        public func put(canisters_map : CanistersMap, canister_id : Principal, canister_db_stats : ClusterTypes.CanisterStats) : ?ClusterTypes.CanisterInfo {

            let is_full = (
                canister_db_stats.total_allocated_bytes >= get_max_canister_size_in_bytes(sharding_strategy)
            ) and (
                (Float.fromInt(canister_db_stats.total_used_bytes) / Float.fromInt(canister_db_stats.total_allocated_bytes)) > get_min_threshold_percent(sharding_strategy)
            );

            let opt_existing_info = Map.get<Principal, ClusterTypes.CanisterInfo>(canisters_map.principal_map, Map.phash, canister_id);

            switch (opt_existing_info) {
                case (?existing_info) {
                    // Remove old size entry
                    assert canister_id == BpTree.remove<(Nat, Principal), Principal>(
                        canisters_map.size_map,
                        size_map_compare,
                        (existing_info.total_allocated_bytes, canister_id),
                    );
                };
                case (null) {};
            };

            let canister_info = {
                id = canister_id;
                status = if (is_full) { #full } else { #active };
                total_allocated_bytes = canister_db_stats.total_allocated_bytes;
                total_used_bytes = canister_db_stats.total_used_bytes;
                total_free_bytes = canister_db_stats.total_free_bytes;
                total_data_bytes = canister_db_stats.total_data_bytes;
                total_metadata_bytes = canister_db_stats.total_metadata_bytes;
                total_index_data_bytes = canister_db_stats.total_index_store_bytes;
            };

            ignore Map.put<Principal, ClusterTypes.CanisterInfo>(canisters_map.principal_map, Map.phash, canister_id, canister_info);
            ignore BpTree.insert<(Nat, Principal), Principal>(
                canisters_map.size_map,
                size_map_compare,
                (canister_info.total_allocated_bytes, canister_id),
                canister_id,
            );

            return opt_existing_info;
        };

        public func get(canisters_map : CanistersMap, canister_id : Principal) : ?ClusterTypes.CanisterInfo {
            Map.get<Principal, ClusterTypes.CanisterInfo>(canisters_map.principal_map, Map.phash, canister_id);
        };

        public func get_min_size_canister(canisters_map : CanistersMap) : ?(Nat, Principal) {
            switch (BpTree.min<(Nat, Principal), Principal>(canisters_map.size_map)) {
                case (?((size, canister_id), _canister_id)) return ?(size, canister_id);
                case (null) return null;
            };
        };

        public func vals(canisters_map : CanistersMap) : Iter.Iter<ClusterTypes.CanisterInfo> {
            Map.vals(canisters_map.principal_map);
        };

    };

    let databases = Map.new<Text, ClusterTypes.DatabaseLayout>();
    let canisters = CanistersMap.new();

    public shared query ({ caller }) func get_canisters() : async [(ClusterTypes.CanisterInfo)] {
        Iter.toArray(CanistersMap.vals(canisters));
    };

    public shared ({ caller }) func set_sharding_strategy(new_strategy : ClusterTypes.ShardingStrategy) : async () {
        sharding_strategy := new_strategy;
    };

    var init_has_been_executed = false;

    func init_from_canister_dbs(init_canister_dbs : [Principal]) : async* () {
        for (canister_id in Itertools.unique(init_canister_dbs.vals(), Principal.hash, Principal.equal)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister_id));
            let canister_db_stats = await canister_db.zendb_stats();
            Debug.print("Initializing from canister: " # Principal.toText(canister_id) # " with stats: " # debug_show (canister_db_stats));

            ignore CanistersMap.put(canisters, canister_id, canister_db_stats);

            for (db_stat in canister_db_stats.database_stats.vals()) {

                for (collection_stat in db_stat.collection_stats.vals()) {
                    let collection_layout : ClusterTypes.CollectionLayout = {
                        name = collection_stat.name;
                        schema = collection_stat.schema;
                        canisters = Vector.fromArray([canister_id]);
                    };

                    let db_layout = switch (Map.get(databases, Map.thash, db_stat.name)) {
                        case (?db) { db };
                        case (null) {
                            let db_layout = {
                                name = db_stat.name;
                                collections = Map.new<Text, ClusterTypes.CollectionLayout>();
                            };

                            ignore Map.put(databases, Map.thash, db_stat.name, db_layout);

                            db_layout;
                        };

                    };

                    let opt_prev_collection = Map.put(db_layout.collections, Map.thash, collection_stat.name, collection_layout);

                    switch (opt_prev_collection) {
                        case (?prev_collection_layout) {
                            // Ensure that the schema for the collection in a different canister has the same schema
                            // which means that it is either a replica or a shard of the same collection
                            assert prev_collection_layout.schema == collection_layout.schema;
                        };
                        case (null) {};
                    };

                };

            };

        };
    };

    let TRILLION = 1_000_000_000_000;

    func create_new_canister_db() : async* Principal {
        Cycles.add(10 * TRILLION);
        let canister_db = await CanisterDB.CanisterDB();
        return Principal.fromActor(canister_db);
    };

    public func init() : async () {
        assert not init_has_been_executed;

        if (init_canister_dbs.size() > 0) {
            await* init_from_canister_dbs(init_canister_dbs);
        } else {

            let canister_db_principal = await* create_new_canister_db();
            await* init_from_canister_dbs([canister_db_principal]);

        };

        init_has_been_executed := true;
    };

    func get_canister_db_from_principal(canister_id : Principal) : CanisterDB.CanisterDB {
        actor (Principal.toText(canister_id));
    };

    public func zendb_create_database(db_name : Text) : async (ZT.Result<(), Text>) {
        assert init_has_been_executed;

        switch (Map.get(databases, Map.thash, db_name)) {
            case (?d) return #err("Database with name '" # db_name # "' already exists");
            case (null) {
                let db_layout = {
                    name = db_name;
                    collections = Map.new<Text, ClusterTypes.CollectionLayout>();
                };

                ignore Map.put(databases, Map.thash, db_name, db_layout);

                // Create database in all canisters in parallel
                let async_calls : Buffer.Buffer<async (ZT.Result<(), Text>)> = Buffer.Buffer<async (ZT.Result<(), Text>)>(8);
                let results : Buffer.Buffer<ZT.Result<(), Text>> = Buffer.Buffer<ZT.Result<(), Text>>(8);

                for (canister_info in CanistersMap.vals(canisters)) {
                    let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister_info.id));
                    let call = canister_db.zendb_create_database(db_name);
                    async_calls.add(call);
                };

                // await* parallel(Buffer.toArray(async_calls), results);
                for (call in async_calls.vals()) {
                    let result = await call;
                    results.add(result);
                };

                var error_messages : Text = "";
                var has_errors = false;

                for ((i, result) in Itertools.enumerate(results.vals())) {
                    switch (result) {
                        case (#ok(())) {};
                        case (#err(error)) {
                            has_errors := true;
                            error_messages #= "canister [" # debug_show (i) # "] error: " # error # "\n";
                        };
                    };
                };

                if (has_errors) {
                    return #err("Failed to create database in some canisters: \n" # error_messages);
                } else {
                    return #ok(());
                };
            };
        };
    };

    func get_collection_layout(db_name : Text, collection_name : Text) : ?ClusterTypes.CollectionLayout {
        switch (Map.get(databases, Map.thash, db_name)) {
            case (?db) {
                switch (Map.get(db.collections, Map.thash, collection_name)) {
                    case (?collection) return ?collection;
                    case (null) return null;
                };
            };
            case (null) return null;
        };
    };

    func allocate_canister_for_new_collection(db_name : Text) : async* Principal {
        Debug.print("Allocating canister for new collection in database: " # db_name);

        // Find an active canister with lowest allocated bytes
        switch (CanistersMap.get_min_size_canister(canisters)) {
            case (?(alocated_size, canister_id)) {
                Debug.print("Found canister with min allocated size: " # Principal.toText(canister_id) # " with size: " # debug_show (alocated_size));
                let scaling_threshold = get_min_threshold_percent(sharding_strategy) * Float.fromInt(get_max_canister_size_in_bytes(sharding_strategy));

                if (Float.fromInt(alocated_size) < scaling_threshold) {
                    return canister_id;
                };
            };
            case (null) {};
        };

        Debug.print("No active canister with enough space found, creating a new canister...");

        // If no active canister found, create a new one
        let canister_db_principal = await* create_new_canister_db();
        let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister_db_principal));
        Debug.print("Created new canister: " # Principal.toText(canister_db_principal) # ", now initializing database...");

        // create the database in the new canister
        let db_creation_res = await canister_db.zendb_create_database(db_name);

        switch (db_creation_res) {
            case (#ok(())) {};
            case (#err(error)) {
                // If database creation failed, we should not add the canister to the cluster
                return Debug.trap("Failed to create database in new canister: " # error);
            };
        };

        let canister_db_stats = await canister_db.zendb_stats();

        Debug.print("Created new canister: " # Principal.toText(canister_db_principal) # " with stats: " # debug_show (canister_db_stats));
        ignore CanistersMap.put(canisters, canister_db_principal, canister_db_stats);
        return canister_db_principal;

    };

    public shared ({ caller }) func zendb_create_collection(db_name : Text, collection_name : Text, schema : ZT.Schema) : async (ZT.Result<(), Text>) {
        Debug.print("Creating collection: " # collection_name # " in database: " # db_name);
        assert init_has_been_executed;

        switch (get_collection_layout(db_name, collection_name)) {
            case (?c) return #err("Collection with name '" # collection_name # "' already exists in database '" # db_name # "'");
            case (null) {};
        };

        Debug.print("Allocating canister for new collection: " # collection_name # " in database: " # db_name);
        let canister_db_principal = await* allocate_canister_for_new_collection(db_name);

        let collection_layout : ClusterTypes.CollectionLayout = {
            name = collection_name;
            schema = schema;
            canisters = Vector.fromArray([canister_db_principal]);
        };

        let ?db = Map.get(databases, Map.thash, db_name) else return #err("Database with name '" # db_name # "' does not exist");

        ignore Map.put(db.collections, Map.thash, collection_name, collection_layout);

        let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister_db_principal));

        return await canister_db.zendb_create_collection(db_name, collection_name, schema);

    };

    public shared ({ caller }) func zendb_collection_insert_document(db_name : Text, collection_name : Text, candid_document_blob : Blob) : async (ZT.Result<ZT.DocumentId, Text>) {
        let ?collection = get_collection_layout(db_name, collection_name) else return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");

        switch (sharding_strategy) {
            case (#fill_first(s)) {
                for (canister in Vector.vals(collection.canisters)) {
                    switch (CanistersMap.get(canisters, canister)) {
                        case (?(canister_info)) {
                            if (canister_info.status == #active) {
                                let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
                                let insert_res = await canister_db.zendb_collection_insert_document(db_name, collection_name, candid_document_blob);

                                switch (insert_res) {
                                    case (#ok(doc_id)) return #ok(doc_id);
                                    case (#err(_)) {};
                                };
                            };
                        };
                        case (null) {};
                    };
                };
            };
        };

        #err("All canisters for collection '" # collection_name # "' in database '" # db_name # "' are full or unavailable");

    };

    // func parallel<T>(async_calls : [async T], results : Buffer.Buffer<T>) : async* () {

    // };

    public shared ({ caller }) func zendb_collection_get_document(db_name : Text, collection_name : Text, document_id : ZT.DocumentId) : async (ZT.Result<ZT.CandidBlob, Text>) {
        assert init_has_been_executed;

        switch (get_collection_layout(db_name, collection_name)) {
            case (?collection_layout) {
                // Check all canisters for the document
                let async_calls : Buffer.Buffer<async (ZT.Result<ZT.CandidBlob, Text>)> = Buffer.Buffer<async (ZT.Result<ZT.CandidBlob, Text>)>(8);
                let results : Buffer.Buffer<ZT.Result<ZT.CandidBlob, Text>> = Buffer.Buffer<ZT.Result<ZT.CandidBlob, Text>>(8);

                for (canister in Vector.vals(collection_layout.canisters)) {
                    let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
                    let call = canister_db.zendb_collection_get_document(db_name, collection_name, document_id);
                    async_calls.add(call);
                };

                for (call in async_calls.vals()) {
                    let result = await call;
                    results.add(result);
                };

                var error_messages : Text = "";

                for ((i, result) in Itertools.enumerate(results.vals())) {
                    switch (result) {
                        // return the first matching document
                        case (#ok(document)) return #ok(document);
                        case (#err(error_message)) {
                            error_messages #= "canister [" # debug_show (Vector.get(collection_layout.canisters, 8)) # "] error: "
                            # error_message # "\n";
                        };
                    };
                };

                return #err("Document with id '" # debug_show (document_id) # "' not found. Errors from canisters: \n" # error_messages);
            };
            case (null) {
                return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");
            };
        };
    };

    public shared ({ caller }) func zendb_collection_search(db_name : Text, collection_name : Text, db_query : ZT.StableQuery) : async (ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>) {
        assert init_has_been_executed;

        switch (get_collection_layout(db_name, collection_name)) {
            case (?collection_layout) {
                let async_calls : Buffer.Buffer<async (ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>)> = Buffer.Buffer<async (ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>)>(8);
                let results : Buffer.Buffer<ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>> = Buffer.Buffer<ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>>(8);

                // Create parallel async calls
                for (canister in Vector.vals(collection_layout.canisters)) {
                    let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
                    let call = canister_db.zendb_collection_search(db_name, collection_name, db_query);
                    async_calls.add(call);
                };

                // Execute all calls in parallel
                // await* parallel<(ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>)>(Buffer.toArray(async_calls), results);
                for (call in async_calls.vals()) {
                    let result = await call;
                    results.add(result);
                };

                var aggregated_results : [(ZT.DocumentId, ZT.CandidBlob)] = [];
                var error_messages : Text = "";

                // Process results
                for ((i, result) in Itertools.enumerate(results.vals())) {
                    switch (result) {
                        case (#ok(documents)) {
                            aggregated_results := Array.append(aggregated_results, documents);
                        };
                        case (#err(error)) {
                            error_messages #= "canister [" # debug_show (Vector.get(collection_layout.canisters, i)) # "] error: " # error # "\n";
                        };
                    };
                };

                // Return aggregated results if any, otherwise return errors
                if (aggregated_results.size() > 0) {
                    return #ok(aggregated_results);
                } else {
                    return #err("No search results found. Errors from canisters: \n" # error_messages);
                };
            };
            case (null) {
                return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");
            };
        };
    };

    // public shared query ({ caller }) func zendb_stats() : async ZT.InstanceStats {
    //     assert init_has_been_executed;

    //     let async_calls : Buffer.Buffer<async ZT.InstanceStats> = Buffer.Buffer<async ZT.InstanceStats>(8);
    //     let results : Buffer.Buffer<ZT.InstanceStats> = Buffer.Buffer<ZT.InstanceStats>(8);

    //     for (canister_info in Map.vals(canisters)) {
    //         let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister_info.id));
    //         let call = canister_db.zendb_stats();
    //         async_calls.add(call);
    //     };

    //     for (call in async_calls.vals()) {
    //         let result = await call;
    //         results.add(result);
    //     };

    //     // Aggregate stats from all canisters
    //     var total_allocated_bytes : Nat = 0;
    //     var total_used_bytes : Nat = 0;
    //     let aggregated_db_stats = Buffer.Buffer<ZT.DatabaseStats>(8);

    //     for (stats in results.vals()) {
    //         total_allocated_bytes += stats.totalAllocatedBytes;
    //         total_used_bytes += stats.totalUsedBytes;

    //         for (db_stat in stats.databaseStats.vals()) {
    //             aggregated_db_stats.add(db_stat);
    //         };
    //     };

    //     {
    //         totalAllocatedBytes = total_allocated_bytes;
    //         totalUsedBytes = total_used_bytes;
    //         databaseStats = Buffer.toArray(aggregated_db_stats);
    //     };
    // };

    public shared query ({ caller }) func zendb_collection_get_schema(db_name : Text, collection_name : Text) : async ZT.Result<ZT.Schema, Text> {
        assert init_has_been_executed;

        switch (get_collection_layout(db_name, collection_name)) {
            case (?collection_layout) {
                #ok(collection_layout.schema);
            };
            case (null) {
                #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");
            };
        };
    };

    public shared ({ caller }) func zendb_collection_replace_document(db_name : Text, collection_name : Text, id : ZT.DocumentId, document_blob : Blob) : async ZT.Result<(), Text> {
        assert init_has_been_executed;

        let ?collection = get_collection_layout(db_name, collection_name) else return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");

        for (canister in Vector.vals(collection.canisters)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
            let result = await canister_db.zendb_collection_replace_document(db_name, collection_name, id, document_blob);

            switch (result) {
                case (#ok(())) return #ok(());
                case (#err(_)) {};
            };
        };

        #err("Document with id '" # debug_show (id) # "' not found in any canister");
    };

    public shared ({ caller }) func zendb_collection_delete_document_by_id(db_name : Text, collection_name : Text, id : ZT.DocumentId) : async ZT.Result<Blob, Text> {
        assert init_has_been_executed;

        let ?collection = get_collection_layout(db_name, collection_name) else return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");

        for (canister in Vector.vals(collection.canisters)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
            let result = await canister_db.zendb_collection_delete_document_by_id(db_name, collection_name, id);

            switch (result) {
                case (#ok(blob)) return #ok(blob);
                case (#err(_)) {};
            };
        };

        #err("Document with id '" # debug_show (id) # "' not found in any canister");
    };

    public shared ({ caller }) func zendb_collection_update_document_by_id(db_name : Text, collection_name : Text, id : ZT.DocumentId, update_operations : [(Text, ZT.FieldUpdateOperations)]) : async ZT.Result<(), Text> {
        assert init_has_been_executed;

        let ?collection = get_collection_layout(db_name, collection_name) else return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");

        for (canister in Vector.vals(collection.canisters)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
            let result = await canister_db.zendb_collection_update_document_by_id(db_name, collection_name, id, update_operations);

            switch (result) {
                case (#ok(())) return #ok(());
                case (#err(_)) {};
            };
        };

        #err("Document with id '" # debug_show (id) # "' not found in any canister");
    };

    // public shared ({ caller }) func zendb_collection_update_documents(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery, update_operations : [(Text, ZT.FieldUpdateOperations)]) : async ZT.Result<[ZT.DocumentId], Text> {
    //     assert init_has_been_executed;

    //     let ?collection = get_collection_layout(db_name, collection_name) else return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");

    //     let async_calls : Buffer.Buffer<async (ZT.Result<[ZT.DocumentId], Text>)> = Buffer.Buffer<async (ZT.Result<[ZT.DocumentId], Text>)>(8);
    //     let results : Buffer.Buffer<ZT.Result<[ZT.DocumentId], Text>> = Buffer.Buffer<ZT.Result<[ZT.DocumentId], Text>>(8);

    //     for (canister in Vector.vals(collection.canisters)) {
    //         let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
    //         let call = canister_db.zendb_collection_update_documents(db_name, collection_name, stable_query, update_operations);
    //         async_calls.add(call);
    //     };

    //     for (call in async_calls.vals()) {
    //         let result = await call;
    //         results.add(result);
    //     };

    //     var aggregated_ids : [ZT.DocumentId] = [];
    //     var error_messages : Text = "";

    //     for ((i, result) in Itertools.enumerate(results.vals())) {
    //         switch (result) {
    //             case (#ok(ids)) {
    //                 aggregated_ids := Array.append(aggregated_ids, ids);
    //             };
    //             case (#err(error)) {
    //                 error_messages #= "canister [" # debug_show (Vector.get(collection.canisters, i)) # "] error: " # error # "\n";
    //             };
    //         };
    //     };

    //     if (aggregated_ids.size() > 0) {
    //         #ok(aggregated_ids);
    //     } else {
    //         #err("No documents updated. Errors from canisters: \n" # error_messages);
    //     };
    // };

    public shared ({ caller }) func zendb_collection_create_index(db_name : Text, collection_name : Text, index_name : Text, index_fields : [(Text, ZT.SortDirection)], options : ?ZT.CreateIndexOptions) : async ZT.Result<(), Text> {
        assert init_has_been_executed;

        let ?collection = get_collection_layout(db_name, collection_name) else return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");

        let async_calls : Buffer.Buffer<async (ZT.Result<(), Text>)> = Buffer.Buffer<async (ZT.Result<(), Text>)>(8);
        let results : Buffer.Buffer<ZT.Result<(), Text>> = Buffer.Buffer<ZT.Result<(), Text>>(8);

        for (canister in Vector.vals(collection.canisters)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
            let call = canister_db.zendb_collection_create_index(db_name, collection_name, index_name, index_fields, options);
            async_calls.add(call);
        };

        for (call in async_calls.vals()) {
            let result = await call;
            results.add(result);
        };

        var error_messages : Text = "";
        var has_errors = false;

        for ((i, result) in Itertools.enumerate(results.vals())) {
            switch (result) {
                case (#ok(())) {};
                case (#err(error)) {
                    has_errors := true;
                    error_messages #= "canister [" # debug_show (Vector.get(collection.canisters, i)) # "] error: " # error # "\n";
                };
            };
        };

        if (has_errors) {
            #err("Failed to create index in some canisters: \n" # error_messages);
        } else {
            #ok(());
        };
    };

    public shared ({ caller }) func zendb_collection_delete_index(db_name : Text, collection_name : Text, index_name : Text) : async ZT.Result<(), Text> {
        assert init_has_been_executed;

        let ?collection = get_collection_layout(db_name, collection_name) else return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");

        let async_calls : Buffer.Buffer<async (ZT.Result<(), Text>)> = Buffer.Buffer<async (ZT.Result<(), Text>)>(8);
        let results : Buffer.Buffer<ZT.Result<(), Text>> = Buffer.Buffer<ZT.Result<(), Text>>(8);

        for (canister in Vector.vals(collection.canisters)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
            let call = canister_db.zendb_collection_delete_index(db_name, collection_name, index_name);
            async_calls.add(call);
        };

        for (call in async_calls.vals()) {
            let result = await call;
            results.add(result);
        };

        var error_messages : Text = "";
        var has_errors = false;

        for ((i, result) in Itertools.enumerate(results.vals())) {
            switch (result) {
                case (#ok(())) {};
                case (#err(error)) {
                    has_errors := true;
                    error_messages #= "canister [" # debug_show (Vector.get(collection.canisters, i)) # "] error: " # error # "\n";
                };
            };
        };

        if (has_errors) {
            #err("Failed to delete index in some canisters: \n" # error_messages);
        } else {
            #ok(());
        };
    };

    public shared ({ caller }) func zendb_collection_repopulate_index(db_name : Text, collection_name : Text, index_name : Text) : async ZT.Result<(), Text> {
        assert init_has_been_executed;

        let ?collection = get_collection_layout(db_name, collection_name) else return #err("Collection '" # collection_name # "' not found in database '" # db_name # "'");

        let async_calls : Buffer.Buffer<async (ZT.Result<(), Text>)> = Buffer.Buffer<async (ZT.Result<(), Text>)>(8);
        let results : Buffer.Buffer<ZT.Result<(), Text>> = Buffer.Buffer<ZT.Result<(), Text>>(8);

        for (canister in Vector.vals(collection.canisters)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister));
            let call = canister_db.zendb_collection_repopulate_index(db_name, collection_name, index_name);
            async_calls.add(call);
        };

        for (call in async_calls.vals()) {
            let result = await call;
            results.add(result);
        };

        var error_messages : Text = "";
        var has_errors = false;

        for ((i, result) in Itertools.enumerate(results.vals())) {
            switch (result) {
                case (#ok(())) {};
                case (#err(error)) {
                    has_errors := true;
                    error_messages #= "canister [" # debug_show (Vector.get(collection.canisters, i)) # "] error: " # error # "\n";
                };
            };
        };

        if (has_errors) {
            #err("Failed to repopulate index in some canisters: \n" # error_messages);
        } else {
            #ok(());
        };
    };

    public shared query func zendb_api_version() : async Text {
        "0.0.1";
    };

    public shared query func zendb_list_canisters() : async [ClusterTypes.CanisterInfo] {
        Iter.toArray(CanistersMap.vals(canisters));
    };

    public shared query func zendb_canister_stats() : async [ZT.CanisterStats] {
        let buffer = Buffer.Buffer<ZT.CanisterStats>(8);

        for (canister_info in CanistersMap.vals(canisters)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister_info.id));
            let canister_db_stats = await canister_db.zendb_stats();
            buffer.add(canister_db_stats);
        };

        Buffer.toArray(buffer);
    };

    func update_all_canister_info() : async () {
        for (canister in CanistersMap.vals(canisters)) {
            let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister.id));
            let canister_db_stats = await canister_db.zendb_stats();

            Debug.print("Updating canister info for canister: " # Principal.toText(canister.id) # " with stats: " # debug_show (canister_db_stats));
            ignore CanistersMap.put(canisters, canister.id, canister_db_stats);
        };
    };

    func scale_collections() : async () {

        for (database in Map.vals(databases)) {

            label collections_loop for (collection in Map.vals(database.collections)) {

                for (canister_id in Vector.vals(collection.canisters)) {
                    switch (CanistersMap.get(canisters, canister_id)) {
                        case (?canister_info) {
                            if (canister_info.status == #active) {
                                continue collections_loop;
                            };
                        };
                        case (null) {};
                    };
                };

                // all canisters for the collection are full, need to allocate a new canister
                let canister_db_principal = await* allocate_canister_for_new_collection(database.name);
                Vector.add(collection.canisters, canister_db_principal);

                let canister_db : CanisterDB.CanisterDB = actor (Principal.toText(canister_db_principal));
                let collection_creation_res = await canister_db.zendb_create_collection(database.name, collection.name, collection.schema);

            };

        };
    };

    ignore Timer.recurringTimer<system>(#seconds(60 * 60), update_all_canister_info);
    ignore Timer.recurringTimer<system>(#seconds(60 * 60 * 6), scale_collections);

};
