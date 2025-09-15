import Map "mo:map@9.0.1/Map";
import Vector "mo:vector@0.4.2";

import ZenDB_types "../Types";

module {
    let ZT = ZenDB_types;

    public module Roles {
        public let MANAGER = "manager";
        public let USER = "user";
        public let GUEST = "guest";
    };

    public module Permissions {
        public let MANAGE = "manage"; // create/delete collections, create/delete indexes
        public let READ = "read"; // read documents
        public let WRITE = "write"; // insert/update/delete documents
    };

    public type CanisterStats = ZenDB_types.InstanceStats;

    public type ShardingStrategy = {
        #fill_first : {
            max_canister_size_in_bytes : Nat; // Max storage size before creating new canister
            threshold_percent : Float; // 0.8 = create new canister at 80% capacity
        };
    };

    public let DefaultShardingStrategy : ShardingStrategy = #fill_first({
        max_canister_size_in_bytes = 429_496_729_600; // ~ 400 GiB
        threshold_percent = 0.9;
    });

    public type CollectionLayout = {
        name : Text;
        schema : ZT.Schema;
        // todo:should store the memory infor of the collection in each canister as well
        canisters : Vector.Vector<Principal>;
    };

    public type DatabaseLayout = {
        name : Text;
        collections : Map.Map<Text, CollectionLayout>;
    };

    public type CanisterInfo = {
        id : Principal;
        status : { #active; #low_memory; #full }; // Need to know if canister can accept new data
        total_allocated_bytes : Nat; // Total allocated bytes in the canister
        total_used_bytes : Nat; // Total used bytes in the canister
        total_free_bytes : Nat; // Total free bytes in the canister
        total_data_bytes : Nat; // Total bytes used for data storage
        total_metadata_bytes : Nat; // Total bytes used for metadata storage (indexes, etc
        total_index_data_bytes : Nat; // Total bytes used for index data storage
    };

    public type CanisterMemoryInfo = {
        total_allocated_bytes : Nat;
        total_used_bytes : Nat;
        total_free_bytes : Nat;
        total_data_bytes : Nat;
        total_metadata_bytes : Nat;
        total_index_data_bytes : Nat;
    };

    public type ClusterLayout = {
        dbs : [DatabaseLayout];
        canisters : [CanisterInfo];
        // Essential: How to decide where new documents go
        sharding_strategy : ShardingStrategy;
    };

    public type ClusterSettings = {
        canister_dbs : [Principal];
        sharding_strategy : ShardingStrategy;
    };

    public type CrossCanisterRecordsCursor = {
        collection_name : Text;
        collection_query : ZT.StableQuery;
        results : ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>;
    };

    public type ClusterApiService = actor {
        zendb_api_version : shared query () -> async Text;

        // /// Get cluster layout.
        // get_cluster_layout : shared query () -> async ClusterLayout;

        /// Creates a new database and returns the database name.
        zendb_create_database : shared (Text) -> async (ZT.Result<(), Text>);

        /// Creates a new collection and returns the collection name.
        zendb_create_collection : shared (Text, Text, ZT.Schema) -> async (ZT.Result<(), Text>);

        /// Insert document into a collection in the cluster.
        zendb_collection_insert_document : shared (Text, Text, ZT.CandidBlob) -> async (ZT.Result<ZT.DocumentId, Text>);

        /// Get document from a collection in the cluster.
        zendb_collection_get_document : shared query (Text, Text, ZT.DocumentId) -> async (ZT.Result<ZT.CandidBlob, Text>);

        /// Search documents in a collection in the cluster.
        zendb_collection_search : shared query (Text, Text, ZT.StableQuery) -> async (ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>);

        // /// Get instance statistics.
        // zendb_stats : shared query () -> async ZT.InstanceStats;

        /// Get collection schema.
        zendb_collection_get_schema : shared query (Text, Text) -> async (ZT.Result<ZT.Schema, Text>);

        /// Replace document in a collection.
        zendb_collection_replace_document : shared (Text, Text, ZT.DocumentId, Blob) -> async (ZT.Result<(), Text>);

        /// Delete document by id from a collection.
        zendb_collection_delete_document_by_id : shared (Text, Text, ZT.DocumentId) -> async (ZT.Result<(Blob), Text>);

        /// Update document by id in a collection.
        zendb_collection_update_document_by_id : shared (Text, Text, ZT.DocumentId, [(Text, ZT.FieldUpdateOperations)]) -> async (ZT.Result<(), Text>);

        /// Update documents matching query in a collection.
        zendb_collection_update_documents : shared (Text, Text, ZT.StableQuery, [(Text, ZT.FieldUpdateOperations)]) -> async (ZT.Result<[ZT.DocumentId], Text>);

        /// Create index on a collection.
        zendb_collection_create_index : shared (Text, Text, Text, [(Text, ZT.SortDirection)], ?ZT.CreateIndexOptions) -> async (ZT.Result<(), Text>);

        /// Delete index from a collection.
        zendb_collection_delete_index : shared (Text, Text, Text) -> async (ZT.Result<(), Text>);

        /// Repopulate index in a collection.
        zendb_collection_repopulate_index : shared query (Text, Text, Text) -> async (ZT.Result<(), Text>);

        /// List Canisters in the cluster
        zendb_list_canisters : shared query () -> async [CanisterInfo];
        zendb_canister_stats : shared query () -> async ([ZT.InstanceStats]);
    };

    /// The canister acts as a database service.
    ///
    /// no intercanister query calls since they can't be called in update calls
    /// and these functions (query and update) might be called in a function which
    /// is only allowed in update calls.
    public type Service = actor {
        zendb_api_version : shared query () -> async Text;

        /// Get database name.
        // zendb_get_database_name : shared query () -> async Text;

        // /// Deletes a collection.
        // zendb_delete_collection : shared Text -> async (ZT.Result<(), Text>);

        // /// Get collection size.
        // zendb_collection_size : shared query Text -> async (Nat);

        // /// Get collection schema.
        // zendb_collection_schema : shared query Text -> async ZT.Result<(ZT.Schema), Text>;

        // /// Clear a collection.
        // zendb_collection_clear : shared Text -> async (ZT.Result<(), Text>);

        // /// Get collection stats
        // zendb_collection_stats : shared query Text -> async (ZT.Result<ZT.CollectionStats, Text>);

        // /// Get collection indexes.
        // zendb_collection_get_indexes : shared query Text -> async ([(indexed_keys : [Text])]);

        /// Create an index on a collection.
        // zendb_create_collection_index : shared (Text, [Text]) -> async (ZT.Result<(), Text>);

        // /// Delete an index from a collection.
        // zendb_collection_delete_index : shared (Text, [Text]) -> async (ZT.Result<(), Text>);

        // /// Inserts a record into a collection.
        // zendb_collection_insert_record : shared (Text, ZT.Candid) -> async (ZT.Result<ZT.DocumentId, Text>);

        // /// Insers a record with a specific id into a collection.
        // zendb_collection_insert_record_with_id : shared (Text, ZT.DocumentId, ZT.Candid) -> async (ZT.Result<(), Text>);

        // /// Deletes a record from a collection.
        // zendb_collection_delete_record_by_id : shared (Text, ZT.DocumentId) -> async (ZT.Result<(), Text>);

        // /// Updates a record in a collection.
        // zendb_collection_update_record_by_id : shared (Text, ZT.DocumentId, ZT.Candid) -> async (ZT.Result<(), Text>);

        // /// Get a record from a collection.
        // zendb_collection_get_record : shared query (Text, ZT.DocumentId) -> async (ZT.Result<ZT.CandidBlob, Text>);

        // /// Find records that match a query.
        // zendb_collection_find_records : shared query (Text, ZT.StableQuery) -> async (ZT.Result<CrossCanisterRecordsCursor, Text>);

        // /// Find one record that matches a query.
        // zendb_collection_find_one_record : shared query (Text, ZT.StableQuery) -> async (ZT.Result<(ZT.DocumentId, ZT.CandidBlob), Text>);

        // /// Updates all records that match a query.
        // zendb_collection_update_all_records : shared (Text, ZT.StableQuery, ZT.Candid) -> async (ZT.Result<[ZT.DocumentId], Text>);

        // /// Deletes all records that match a query.
        // zendb_collection_delete_all_record : shared (Text, ZT.StableQuery) -> async (ZT.Result<[ZT.DocumentId], Text>);

        // /// Count all records that match a query.
        // zendb_collection_count_records : shared query (Text, ZT.StableQuery) -> async (ZT.Result<Nat, Text>);
    };
};
