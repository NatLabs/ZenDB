import ZenDB_types "../Types";

module {
    let ZT = ZenDB_types;

    public type CrossCanisterRecordsCursor = {
        collection_name : Text;
        collection_query : ZT.StableQuery;
        results : ZT.Result<[(ZT.RecordId, ZT.CandidBlob)], Text>;
    };

    /// The canister acts as a database service.
    ///
    /// no intercanister query calls since they can't be called in update calls
    /// and these functions (query and update) might be called in a function which
    /// is only allowed in update calls.
    public type Service = actor {
        zendb_api_version : shared query () -> async Nat;

        // / Creates a new database and returns the database name.
        // zendb_create_database : shared (Text) -> async (ZT.Result<(canister_id : Principal), Text>);

        /// Get database name.
        zendb_get_database_name : shared query () -> async Text;

        /// Creates a new collection and returns the collection name.
        zendb_create_collection : shared (Text, ZT.Schema) -> async (ZT.Result<Text, Text>);

        /// Deletes a collection.
        zendb_delete_collection : shared Text -> async (ZT.Result<(), Text>);

        /// Get collection size.
        zendb_collection_size : shared query Text -> async (Nat);

        /// Get collection schema.
        zendb_collection_schema : shared query Text -> async ZT.Result<(ZT.Schema), Text>;

        /// Clear a collection.
        zendb_collection_clear : shared Text -> async (ZT.Result<(), Text>);

        /// Get collection stats
        zendb_collection_stats : shared query Text -> async (ZT.Result<ZT.CollectionStats, Text>);

        /// Get collection indexes.
        zendb_collection_get_indexes : shared query Text -> async ([(indexed_keys : [Text])]);

        /// Create an index on a collection.
        zendb_collection_create_index : shared (Text, [Text]) -> async (ZT.Result<(), Text>);

        /// Delete an index from a collection.
        zendb_collection_delete_index : shared (Text, [Text]) -> async (ZT.Result<(), Text>);

        /// Inserts a record into a collection.
        zendb_collection_insert_record : shared (Text, ZT.Candid) -> async (ZT.Result<ZT.RecordId, Text>);

        /// Insers a record with a specific id into a collection.
        zendb_collection_insert_record_with_id : shared (Text, ZT.RecordId, ZT.Candid) -> async (ZT.Result<(), Text>);

        /// Deletes a record from a collection.
        zendb_collection_delete_record_by_id : shared (Text, ZT.RecordId) -> async (ZT.Result<(), Text>);

        /// Updates a record in a collection.
        zendb_collection_update_record_by_id : shared (Text, ZT.RecordId, ZT.Candid) -> async (ZT.Result<(), Text>);

        /// Get a record from a collection.
        zendb_collection_get_record : shared query (Text, ZT.RecordId) -> async (ZT.Result<ZT.CandidBlob, Text>);

        /// Find records that match a query.
        zendb_collection_find_records : shared query (Text, ZT.StableQuery) -> async (ZT.Result<CrossCanisterRecordsCursor, Text>);

        /// Find one record that matches a query.
        zendb_collection_find_one_record : shared query (Text, ZT.StableQuery) -> async (ZT.Result<(ZT.RecordId, ZT.CandidBlob), Text>);

        /// Updates all records that match a query.
        zendb_collection_update_all_records : shared (Text, ZT.StableQuery, ZT.Candid) -> async (ZT.Result<[ZT.RecordId], Text>);

        /// Deletes all records that match a query.
        zendb_collection_delete_all_record : shared (Text, ZT.StableQuery) -> async (ZT.Result<[ZT.RecordId], Text>);

        /// Count all records that match a query.
        zendb_collection_count_records : shared query (Text, ZT.StableQuery) -> async (ZT.Result<Nat, Text>);
    };
};
