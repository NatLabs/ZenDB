/// A collection is a set of records of the same type.

import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Result "mo:base/Result";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Hash "mo:base/Hash";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Int32 "mo:base/Int32";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Int16 "mo:base/Int16";
import Int64 "mo:base/Int64";
import Int8 "mo:base/Int8";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";
import InternetComputer "mo:base/ExperimentalInternetComputer";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Tag "mo:candid/Tag";
import BitMap "mo:bit-map";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import ByteUtils "../ByteUtils";
import LegacyCandidMap "../LegacyCandidMap";
import C "../Constants";

import Index "Index";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "Utils";
import QueryPlan "QueryPlan";
import QueryExecution "QueryExecution";
import StableCollection "StableCollection";

module {

    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; nhash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    type QueryBuilder = Query.QueryBuilder;

    // public type MemoryBTree = MemoryBTree.VersionedMemoryBTree;
    public type BTreeUtils<K, V> = MemoryBTree.BTreeUtils<K, V>;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type RecordPointer = Nat;
    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type Candify<A> = T.Candify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;

    let DEFAULT_BTREE_ORDER = 256;
    let MAX_QUERY_INSTRUCTIONS : Nat64 = 5_000_000_000;
    let MAX_UPDATE_INSTRUCTIONS : Nat64 = 40_000_000_000;

    public class Collection<Record>(collection_name : Text, collection : StableCollection, blobify : T.Candify<Record>) = self {

        public func name() : Text = collection_name;
        public func size() : Nat = MemoryBTree.size(collection.main);

        let main_btree_utils : MemoryBTree.BTreeUtils<Nat, (Blob, [Nat8])> = CollectionUtils.get_main_btree_utils();

        public func filter_iter(condition : (Record) -> Bool) : Iter<Record> {

            let iter = MemoryBTree.vals(collection.main, main_btree_utils);
            let records = Iter.map<(Blob, [Nat8]), Record>(
                iter,
                func((candid_blob, _) : (Blob, [Nat8])) {
                    blobify.from_blob(candid_blob);
                },
            );
            let filtered = Iter.filter<Record>(records, condition);

        };

        public func filter(condition : (Record) -> Bool) : [Record] {
            Iter.toArray(filter_iter(condition));
        };

        /// Clear all the data in the collection.
        public func clear() {
            MemoryBTree.clear(collection.main);

            for (index in Map.vals(collection.indexes)) {
                MemoryBTree.clear(index.data);
            };
        };

        public func update_schema(schema : Schema) : Result<(), Text> {

            let is_compatible = Schema.is_schema_backward_compatible(collection.schema, schema);
            if (not is_compatible) return #err("Schema is not backward compatible");

            collection.schema := schema;
            // Debug.print("Schema Updated: Ensure to update your Record type as well.");
            #ok;
        };

        public func create_index(index_key_details : [(Text)]) : Result<(), Text> {
            StableCollection.create_index(collection, main_btree_utils, index_key_details);
        };

        public func delete_index(index_key_details : [Text]) : Result<(), Text> {
            StableCollection.delete_index(collection, main_btree_utils, index_key_details);
        };

        public func insert_with_id(id : Nat, record : Record) : Result<(), Text> {
            put_with_id(id, record);
        };

        public func put_with_id(id : Nat, record : Record) : Result<(), Text> {

            let candid_blob = blobify.to_blob(record);
            let candid = CollectionUtils.decode_candid_blob(collection, candid_blob);

            switch (candid) {
                case (#Record(_)) {};
                case (_) return #err("Values inserted into the collection must be #Records");
            };

            // Debug.print("validate: " # debug_show (collection.schema) #debug_show (candid));
            Utils.assert_result(Schema.validate_record(collection.schema, candid));

            let candid_map = CandidMap.fromCandid(candid);
            let main_btree_value = (candid_blob, candid_map.encoded_bytes());

            // if this fails, it means the id already exists
            // insert() - should to used to update existing records
            //
            // also note that although we have already inserted the value into the main btree
            // the inserted value will be discarded because the call fails
            // meaning the canister state will not be updated
            // at least that's what I think - need to confirm
            let opt_prev = MemoryBTree.insert<Nat, (Blob, [Nat8])>(collection.main, main_btree_utils, id, main_btree_value);

            switch (opt_prev) {
                case (null) {};
                case (?prev) {
                    ignore MemoryBTree.insert<Nat, (Blob, [Nat8])>(collection.main, main_btree_utils, id, prev);
                    return #err("Record with id (" # debug_show id # ") already exists");
                };
            };

            // should change getId to getPointer
            // let ?ref_pointer = MemoryBTree.getId(collection.main, main_btree_utils, id);
            // assert MemoryBTree.getId(collection.main, main_btree_utils, id) == ?id;

            if (Map.size(collection.indexes) == 0) return #ok();

            for (index in Map.vals(collection.indexes)) {

                let buffer = Buffer.Buffer<Candid>(8);

                for ((index_key, dir) in index.key_details.vals()) {

                    if (index_key == C.RECORD_ID_FIELD) {
                        buffer.add(#Nat(id));
                    } else {
                        let ?value = candid_map.get(index_key) else return #err("Couldn't get value for index key: " # debug_show index_key);

                        buffer.add(value);
                    };

                };

                let index_key_values = Buffer.toArray(buffer);

                let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);
                ignore MemoryBTree.insert(index.data, index_data_utils, index_key_values, id);
            };

            #ok();

        };

        public func insert(record : Record) : Result<(Nat), Text> {
            put(record);
        };

        public func put(record : Record) : Result<(Nat), Text> {
            let id = MemoryBTree.size(collection.main);

            switch (put_with_id(id, record)) {
                case (#err(msg)) return #err(msg);
                case (#ok(_)) {};
            };

            #ok(id);
        };

        public func get(id : Nat) : Result<Record, Text> {

            let record = CollectionUtils.lookup_record(collection, blobify, id);

            #ok(record);
        };

        type RecordLimits = [(Text, ?State<Candid>)];
        type FieldLimit = (Text, ?State<Candid>);

        type Bounds = (RecordLimits, RecordLimits);

        type IndexDetails = {
            var sorted_in_reverse : ?Bool;
            intervals : Buffer.Buffer<(Nat, Nat)>;
        };

        type Iter<A> = Iter.Iter<A>;

        public func find_iter(query_builder : QueryBuilder) : Result<Iter<T.WrapId<Record>>, Text> {
            switch (StableCollection.internal_find(collection, query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = StableCollection.id_to_record_iter(collection, blobify, record_ids_iter);
                    #ok(record_iter);
                };
            };
        };

        public func find(query_builder : QueryBuilder) : Result<[T.WrapId<Record>], Text> {
            switch (StableCollection.internal_find(collection, query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = StableCollection.id_to_record_iter(collection, blobify, record_ids_iter);
                    let records = Iter.toArray(record_iter);
                    #ok(records);
                };
            };
        };

        func async_skip_helper(iter : Iter<Nat>, skip : Nat) : async (Nat) {
            let performance_start = InternetComputer.performanceCounter(0);

            func instructions() : Nat64 {
                InternetComputer.performanceCounter(0) - performance_start;
            };

            var i = 0;
            while ((instructions() + 10_000_000 < MAX_UPDATE_INSTRUCTIONS) and i < skip) {
                ignore iter.next();
                i += 1;
            };

            i;
        };

        func async_skip(iter : Iter<Nat>, skip : Nat, performance_start : Nat64) : async* () {

            var skipped = 0;
            Debug.print("starting async_skip: " # debug_show skip);
            while (skipped < skip) {
                skipped += await async_skip_helper(iter, skip - skipped);
                Debug.print("skipped: " # debug_show skipped);
            };

        };

        public func async_find(query_builder : QueryBuilder, buffer : Buffer<T.WrapId<Record>>) : async* Result<(), Text> {
            switch (find_iter(query_builder)) {
                case (#err(err)) #err(err);
                case (#ok(records)) {
                    for (record in records) {
                        buffer.add(record);
                    };
                    #ok(());
                };
            };
        };

        public func stats() : T.CollectionStats {
            let main_btree_index = {
                stable_memory = {
                    metadata_bytes = MemoryBTree.metadataBytes(collection.main);
                    actual_data_bytes = MemoryBTree.bytes(collection.main);
                };
            };

            let indexes : [T.IndexStats] = Iter.toArray(
                Iter.map<(Text, Index), T.IndexStats>(
                    Map.entries(collection.indexes),
                    func((index_name, index) : (Text, Index)) : T.IndexStats {
                        let columns : [Text] = Array.map<(Text, Any), Text>(
                            index.key_details,
                            func((key, direction) : (Text, Any)) : Text = key,
                        );

                        let stable_memory : T.MemoryStats = {
                            metadata_bytes = MemoryBTree.metadataBytes(index.data);
                            actual_data_bytes = MemoryBTree.bytes(index.data);
                        };

                        { columns; stable_memory };

                    },
                )
            );

            { indexes; main_btree_index; records = size() };

        };

        // public func getBestIndex(db_query : QueryBuilder) : ?Index { };

        /// Returns the total number of records that match the query.
        /// This ignores the limit and skip parameters.
        public func count(query_builder : QueryBuilder) : Result<Nat, Text> {
            let stable_query = query_builder.build();

            let query_plan = QueryPlan.create_query_plan(
                collection,
                stable_query.query_operations,
                null,
                null,
                CandidMap.fromCandid(#Record([])),
            );

            let count = switch (QueryExecution.get_unique_record_ids_from_query_plan(collection, Map.new(), query_plan)) {
                case (#Empty) 0;
                case (#BitMap(bitmap)) bitmap.size();
                case (#Ids(iter)) Iter.size(iter);
                case (#Interval(_index_name, intervals, _sorted_in_reverse)) {
                    Debug.print("count intervals: " # debug_show intervals);

                    var i = 0;
                    var sum = 0;
                    while (i < intervals.size()) {
                        sum += intervals.get(i).1 - intervals.get(i).0;
                        i := i + 1;
                    };

                    sum;
                };
            };

            #ok(count);
        };

        func async_count_iter_helper(iter : Iter<Nat>) : async (Nat) {
            let performance_start = InternetComputer.performanceCounter(0);
            func instructions() : Nat64 {
                InternetComputer.performanceCounter(0) - performance_start;
            };

            var count = 0;

            label counting_iter while (instructions() + 10_000_000 < MAX_QUERY_INSTRUCTIONS) {
                switch (iter.next()) {
                    case (?id) count += 1;
                    case (null) break counting_iter;
                };
            };

            count;

        };

        func async_count_iter(iter : Iter<Nat>) : async* (Nat) {
            let peekable_iter = Itertools.peekable(iter);

            var count = 0;
            while (Option.isSome(peekable_iter.peek())) {
                count += await async_count_iter_helper(peekable_iter);
            };

            count

        };

        public func async_count(query_builder : QueryBuilder) : async* Result<Nat, Text> {
            count(query_builder);
        };

        public func updateById(id : Nat, update_fn : (Record) -> Record) : Result<(), Text> {

            let ?prev_record_details = MemoryBTree.lookupVal(collection.main, main_btree_utils, id);
            let prev_record = blobify.from_blob(prev_record_details.0);
            // let prev_record = CollectionUtils.lookup_record<Record>(collection, blobify, id);

            let new_record = update_fn(prev_record);

            let new_candid_blob = blobify.to_blob(new_record);
            let new_candid = CollectionUtils.decode_candid_blob(collection, new_candid_blob);

            let candid_map = CandidMap.fromCandid(new_candid);
            let record_details = (new_candid_blob, candid_map.encoded_bytes());

            // not needed since it uses the same record type
            Utils.assert_result(Schema.validate_record(collection.schema, new_candid));

            assert ?prev_record_details == MemoryBTree.insert(collection.main, main_btree_utils, id, record_details);
            let prev_candid = CollectionUtils.decode_candid_blob(collection, prev_record_details.0);

            let #Record(prev_records) = prev_candid else return #err("Couldn't get records");
            let #Record(new_records) = new_candid else return #err("Couldn't get records");

            for (index in Map.vals(collection.indexes)) {

                let prev_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, prev_records);
                let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);

                assert ?id == MemoryBTree.remove(index.data, index_data_utils, prev_index_key_values);

                let new_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, new_records);
                ignore MemoryBTree.insert(index.data, index_data_utils, new_index_key_values, id);
            };

            #ok;
        };

        public func update(query_builder : QueryBuilder, update_fn : (Record) -> Record) : Result<(), Text> {

            let records_iter = switch (StableCollection.internal_find(collection, query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            for ((id) in records_iter) {
                let #ok(_) = updateById(id, update_fn);
            };

            #ok;
        };

        public func deleteById(id : Nat) : Result<Record, Text> {

            let ?prev_record_details = MemoryBTree.remove(collection.main, main_btree_utils, id);
            let prev_candid = CollectionUtils.decode_candid_blob(collection, prev_record_details.0);

            let #Record(prev_records) = prev_candid else return #err("Couldn't get records");
            // Debug.print("prev_records: " # debug_show prev_records);
            for (index in Map.vals(collection.indexes)) {

                let prev_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, prev_records);
                let index_data_utils : BTreeUtils<[Candid], RecordPointer> = CollectionUtils.get_index_data_utils(collection, index.key_details);

                assert ?id == MemoryBTree.remove<[Candid], RecordPointer>(index.data, index_data_utils, prev_index_key_values);
            };

            let prev_record = blobify.from_blob(prev_record_details.0);
            #ok(prev_record);
        };

        public func delete(query_builder : QueryBuilder) : Result<[Record], Text> {

            // let db_query = query_builder.build();
            let results_iter = switch (StableCollection.internal_find(collection, query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            let buffer = Buffer.Buffer<Record>(8);
            for ((id) in results_iter) {
                // Debug.print("deleting record: " # debug_show (id));
                let #ok(record) = deleteById(id);
                buffer.add(record);
            };

            #ok(Buffer.toArray(buffer));
        };

        // public func find()
    };

};
