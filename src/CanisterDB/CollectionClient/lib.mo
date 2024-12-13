import Array "mo:base/Array";
import Option "mo:base/Option";

import ZT "../../Types";
import Query "../../Query";

import CanisterDB_types "../Types";

module {
    public type CanisterDB = CanisterDB_types.Service;

    public class CollectionClient<Record>(candify : ZT.Candify<Record>) {
        var canister_db : CanisterDB = actor ("aaaaa-aa");
        var collection_name : Text = "collection_name";

        public func set_canister_db(_canister_db : CanisterDB) {
            canister_db := _canister_db;
        };

        public func set_collection_name(_collection_name : Text) {
            collection_name := _collection_name;
        };

        public func get_collection_name() : Text {
            collection_name;
        };

        public func size() : async* (Nat) {
            await canister_db.zendb_collection_size(collection_name);
        };

        public func schema() : async* (ZT.Result<ZT.Schema, Text>) {
            await canister_db.zendb_collection_schema(collection_name);
        };

        public func clear() : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_clear(collection_name);
        };

        public func stats() : async* (ZT.Result<ZT.CollectionStats, Text>) {
            await canister_db.zendb_collection_stats(collection_name);
        };

        public func get_indexes() : async* ([(indexed_keys : [Text])]) {
            await canister_db.zendb_collection_get_indexes(collection_name);
        };

        public func create_index(index_keys : [Text]) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_create_index(collection_name, index_keys);
        };

        public func delete_index(index_keys : [Text]) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_delete_index(collection_name, index_keys);
        };

        public func insert(record : ZT.Candid) : async* (ZT.Result<ZT.RecordId, Text>) {
            await canister_db.zendb_collection_insert_record(collection_name, record);
        };

        public func insert_with_id(record_id : ZT.RecordId, record : ZT.Candid) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_insert_record_with_id(collection_name, record_id, record);
        };

        public func delete_by_id(record_id : ZT.RecordId) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_delete_record_by_id(collection_name, record_id);
        };

        public func update_by_id(record_id : ZT.RecordId, record : ZT.Candid) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_collection_insert_record_with_id(collection_name, record_id, record);
        };

        public func get(record_id : ZT.RecordId, getter : (Record) -> ()) : async* ZT.Result<(), Text> {
            let res = await canister_db.zendb_collection_get_record(collection_name, record_id);

            switch (res) {
                case (#err(msg)) #err(msg);
                case (#ok(candid_blob)) {
                    let record = candify.from_blob(candid_blob);
                    getter(record);
                    #ok();
                };
            };
        };

        func candid_blob_to_record(candid_blob : ZT.CandidBlob) : Record {
            candify.from_blob(candid_blob);
        };

        func convert_candid_blobs_to_records(candid_blobs : [ZT.CandidBlob]) : [Record] {
            Array.map<ZT.CandidBlob, Record>(
                candid_blobs,
                func(candid_blob : ZT.CandidBlob) : Record {
                    candify.from_blob(candid_blob);
                },
            );
        };

        func convert_candid_blobs_paired_with_ids_to_records(candid_blobs : [(ZT.RecordId, ZT.CandidBlob)]) : [(ZT.RecordId, Record)] {
            Array.map<(ZT.RecordId, ZT.CandidBlob), (ZT.RecordId, Record)>(
                candid_blobs,
                func(id : ZT.RecordId, candid_blob : ZT.CandidBlob) : (ZT.RecordId, Record) {
                    let record = candify.from_blob(candid_blob);
                    (id, record);
                },
            );
        };

        public type BatchCursor<Record> = {
            next : ([(ZT.RecordId, Record)] -> ()) -> async* ?(ZT.Result<(), Text>);
        };

        public func BatchCursor(cross_canister_cursor : CanisterDB_types.CrossCanisterRecordsCursor) : BatchCursor<Record> {
            let { collection_query } = cross_canister_cursor;
            let { pagination } = collection_query;

            let initial_results = cross_canister_cursor.results;

            var terminated = false;
            let initial_offset = Option.get(pagination.skip, 0);
            var offset = initial_offset;
            let limit : Nat = Option.get(pagination.limit, 2 ** 64);

            func analyze_batch_response(
                getter : ([(ZT.RecordId, Record)]) -> (),
                result : ZT.Result<[(ZT.RecordId, ZT.CandidBlob)], Text>,
            ) : ?ZT.Result<(), Text> {
                switch (result) {
                    case (#err(msg)) {
                        terminated := true;
                        ? #err(msg);
                    };
                    case (#ok(candid_blobs)) {
                        if (candid_blobs.size() < limit) { terminated := true };

                        let records = convert_candid_blobs_paired_with_ids_to_records(candid_blobs);

                        getter(records);

                        ? #ok();
                    };
                };
            };

            return {
                next = func(getter : ([(ZT.RecordId, Record)]) -> ()) : async* ?(ZT.Result<(), Text>) {

                    if (terminated) {
                        return null;
                    } else if (initial_offset == offset) {
                        offset += limit;

                        return analyze_batch_response(getter, cross_canister_cursor.results);

                    } else {

                        let res = await canister_db.zendb_collection_find_records(
                            collection_name,
                            {
                                collection_query with pagination = {
                                    pagination with skip = ?offset
                                } : ZT.StableQueryPagination
                            } : ZT.StableQuery,
                        );

                        switch (res) {
                            case (#err(msg)) {
                                terminated := true;
                                return ? #err(msg);
                            };
                            case (#ok(cross_canister_cursor)) {
                                return analyze_batch_response(getter, cross_canister_cursor.results);
                            };
                        }

                    };

                };
            }

        };

        public func find(query_builder : Query.QueryBuilder, getter : (batch_cursor : BatchCursor<Record>) -> ()) : async* ZT.Result<(), Text> {

            let collection_query = query_builder.build();
            let res = await canister_db.zendb_collection_find_records(collection_name, collection_query);

            switch (res) {
                case (#err(msg)) #err(msg);
                case (#ok(cross_canister_batch_records_cursor)) {
                    let batch_records_cursor = BatchCursor(cross_canister_batch_records_cursor);
                    getter(batch_records_cursor);
                    #ok();
                };
            };
        };

        // public func update_all(query_builder : Query.QueryBuilder, record : ZT.Candid) : async* (ZT.Result<(), Text>) {
        //     let collection_query = query_builder.build();
        //     await canister_db.zendb_collection_update_records(collection_name, collection_query, record);
        // };

        public func count(query_builder : Query.QueryBuilder) : async* ZT.Result<Nat, Text> {
            let collection_query = query_builder.build();
            await canister_db.zendb_collection_count_records(collection_name, collection_query);
        };

    };
};
