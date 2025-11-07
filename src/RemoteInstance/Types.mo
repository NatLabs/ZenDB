import Map "mo:map@9.0.1/Map";
import Vector "mo:vector@0.4.2";

import ZenDB_types "../EmbeddedInstance/Types";

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

        /// Grant a role to a principal
        grant_role : shared (Principal, Text) -> async (ZT.Result<(), Text>);

        /// Grant multiple roles to a principal
        grant_roles : shared (Principal, [Text]) -> async (ZT.Result<(), Text>);

        /// Revoke a role from a principal
        revoke_role : shared (Principal, Text) -> async (ZT.Result<(), Text>);

        /// Creates a new database and returns the database name.
        zendb_create_database : shared (Text) -> async (ZT.Result<(), Text>);

        /// Creates a new collection and returns the collection name.
        zendb_create_collection : shared (Text, Text, ZT.Schema) -> async (ZT.Result<(), Text>);

        /// Deletes a collection from the database.
        zendb_delete_collection : shared (Text, Text) -> async (ZT.Result<(), Text>);

        /// Insert document into a collection in the cluster.
        zendb_collection_insert_document : shared (Text, Text, ZT.CandidBlob) -> async (ZT.Result<ZT.DocumentId, Text>);

        /// Insert multiple documents into a collection in the cluster.
        zendb_collection_insert_documents : shared (Text, Text, [ZT.CandidBlob]) -> async (ZT.Result<[ZT.DocumentId], Text>);

        /// Get document from a collection in the cluster.
        zendb_collection_get_document : shared query (Text, Text, ZT.DocumentId) -> async (ZT.Result<ZT.CandidBlob, Text>);

        /// Get document from a collection in the cluster (composite query).
        zendb_collection_get_document_composite_query : shared composite query (Text, Text, ZT.DocumentId) -> async (ZT.Result<ZT.CandidBlob, Text>);

        /// Search documents in a collection in the cluster.
        zendb_collection_search : shared (Text, Text, ZT.StableQuery) -> async (ZT.Result<ZT.SearchResult<ZT.CandidBlob>, Text>);

        /// Search documents in a collection in the cluster (query).
        zendb_collection_search_query : shared query (Text, Text, ZT.StableQuery) -> async (ZT.Result<ZT.SearchResult<ZT.CandidBlob>, Text>);

        /// Search documents in a collection in the cluster (composite query).
        zendb_collection_search_composite_query : shared composite query (Text, Text, ZT.StableQuery) -> async (ZT.Result<ZT.SearchResult<ZT.CandidBlob>, Text>);

        zendb_collection_size : shared query (Text, Text) -> async (Nat);

        /// Get collection size (composite query).
        zendb_collection_size_composite_query : shared composite query (Text, Text) -> async (Nat);

        /// Returns the total number of documents that match the query.
        /// This ignores the limit and skip parameters.
        zendb_collection_count : shared (Text, Text, ZT.StableQuery) -> async (ZT.CountResult);

        /// Returns the total number of documents that match the query (query).
        /// This ignores the limit and skip parameters.
        zendb_collection_count_query : shared query (Text, Text, ZT.StableQuery) -> async (ZT.CountResult);

        /// Returns the total number of documents that match the query (composite query).
        /// This ignores the limit and skip parameters.
        zendb_collection_count_composite_query : shared composite query (Text, Text, ZT.StableQuery) -> async (ZT.CountResult);

        /// Get collection schema.
        zendb_collection_get_schema : shared query (Text, Text) -> async (ZT.Result<ZT.Schema, Text>);

        /// Get collection schema (composite query).
        zendb_collection_get_schema_composite_query : shared composite query (Text, Text) -> async (ZT.Result<ZT.Schema, Text>);

        /// Replace document in a collection.
        zendb_collection_replace_document : shared (Text, Text, ZT.DocumentId, Blob) -> async (ZT.Result<ZT.ReplaceByIdResult, Text>);

        /// Delete document by id from a collection.
        zendb_collection_delete_document_by_id : shared (Text, Text, ZT.DocumentId) -> async (ZT.Result<ZT.DeleteByIdResult<Blob>, Text>);

        /// Delete documents matching query from a collection.
        zendb_collection_delete_documents : shared (Text, Text, ZT.StableQuery) -> async (ZT.Result<ZT.DeleteResult<Blob>, Text>);

        /// Update document by id in a collection.
        zendb_collection_update_document_by_id : shared (Text, Text, ZT.DocumentId, [(Text, ZT.FieldUpdateOperations)]) -> async (ZT.Result<ZT.UpdateByIdResult, Text>);

        /// Update documents matching query in a collection.
        zendb_collection_update_documents : shared (Text, Text, ZT.StableQuery, [(Text, ZT.FieldUpdateOperations)]) -> async (ZT.Result<[ZT.DocumentId], Text>);

        /// Create index on a collection.
        zendb_collection_create_index : shared (Text, Text, Text, [(Text, ZT.SortDirection)], ?ZT.CreateIndexOptions) -> async (ZT.Result<(), Text>);

        /// Delete index from a collection.
        zendb_collection_delete_index : shared (Text, Text, Text) -> async (ZT.Result<(), Text>);

        /// Repopulate index in a collection.
        zendb_collection_repopulate_index : shared query (Text, Text, Text) -> async (ZT.Result<(), Text>);

        /// Repopulate index in a collection (composite query).
        zendb_collection_repopulate_index_composite_query : shared composite query (Text, Text, Text) -> async (ZT.Result<(), Text>);

        /// Batch create indexes on a collection.
        zendb_collection_batch_create_indexes : shared (Text, Text, [ZT.CreateIndexBatchConfig]) -> async (ZT.Result<Nat, Text>);

        /// Batch populate indexes on a collection.
        zendb_collection_batch_populate_indexes : shared (Text, Text, [Text]) -> async (ZT.Result<Nat, Text>);

        /// Process index batch operation.
        zendb_collection_process_index_batch : shared (Text, Text, Nat) -> async (ZT.Result<Bool, Text>);

        /// List Canisters in the cluster
        zendb_list_canisters : shared query () -> async [CanisterInfo];

        /// List Canisters in the cluster (composite query)
        zendb_list_canisters_composite_query : shared composite query () -> async [CanisterInfo];

        zendb_canister_stats : shared query () -> async ([ZT.InstanceStats]);

        /// Get canister stats (composite query)
        zendb_canister_stats_composite_query : shared composite query () -> async ([ZT.InstanceStats]);

        /// Get instance statistics
        zendb_stats : shared query () -> async ZT.InstanceStats;

        /// Get instance statistics (composite query)
        zendb_stats_composite_query : shared composite query () -> async ZT.InstanceStats;

        /// Get database statistics
        zendb_database_stats : shared query (Text) -> async ZT.DatabaseStats;

        /// Get database statistics (composite query)
        zendb_database_stats_composite_query : shared composite query (Text) -> async ZT.DatabaseStats;

        /// Get collection statistics
        zendb_collection_stats : shared query (Text, Text) -> async ZT.CollectionStats;

        /// Get collection statistics (composite query)
        zendb_collection_stats_composite_query : shared composite query (Text, Text) -> async ZT.CollectionStats;
    };

};
