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
import BitMap "mo:bit-map";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import ZT "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import ByteUtils "../ByteUtils";
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

    public type Index = ZT.Index;
    public type Candid = ZT.Candid;
    public type SortDirection = ZT.SortDirection;
    public type State<R> = ZT.State<R>;
    public type ZenQueryLang = ZT.ZenQueryLang;

    public type Candify<A> = ZT.Candify<A>;

    public type StableCollection = ZT.StableCollection;

    public type IndexKeyFields = ZT.IndexKeyFields;

    let DEFAULT_BTREE_ORDER = 256;
    let MAX_QUERY_INSTRUCTIONS : Nat64 = 5_000_000_000;
    let MAX_UPDATE_INSTRUCTIONS : Nat64 = 40_000_000_000;

    public class Collection<Record>(collection_name : Text, collection : StableCollection, blobify : ZT.Candify<Record>) = self {

        public func name() : Text = collection_name;
        public func size() : Nat = MemoryBTree.size(collection.main);

        let main_btree_utils : MemoryBTree.BTreeUtils<Nat, Blob> = CollectionUtils.get_main_btree_utils();

        public func keys() : Iter<Nat> {
            MemoryBTree.keys(collection.main, main_btree_utils);
        };

        public func filter_iter(condition : (Record) -> Bool) : Iter<Record> {

            let iter = MemoryBTree.vals(collection.main, main_btree_utils);
            let records = Iter.map<Blob, Record>(
                iter,
                func(candid_blob : Blob) {
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
            StableCollection.update_schema(collection, schema);
        };

        public func create_index(name : Text, index_key_details : [(Text, SortDirection)]) : Result<(), Text> {
            StableCollection.create_index(collection, main_btree_utils, name, index_key_details);
        };

        public func delete_index(name : Text) : Result<(), Text> {
            StableCollection.delete_index(collection, main_btree_utils, name);
        };

        public func clear_index(name : Text) : Result<(), Text> {
            StableCollection.clear_index(collection, main_btree_utils, name);
        };

        public func create_and_populate_index(name : Text, index_key_details : [(Text, SortDirection)]) : Result<(), Text> {
            StableCollection.create_and_populate_index(collection, main_btree_utils, name, index_key_details);
        };

        public func populate_index(name : Text) : Result<(), Text> {
            StableCollection.populate_index(collection, main_btree_utils, name);
        };

        public func populate_indexes(names : [Text]) : Result<(), Text> {
            StableCollection.populate_indexes(collection, main_btree_utils, names);
        };

        public func insert_with_id(id : Nat, record : Record) : Result<(), Text> {
            put_with_id(id, record);
        };

        public func put_with_id(id : Nat, record : Record) : Result<(), Text> {

            let candid_blob = blobify.to_blob(record);
            StableCollection.put_with_id(collection, main_btree_utils, id, candid_blob);

        };

        public func insert(record : Record) : Result<(Nat), Text> {
            put(record);
        };

        public func put(record : Record) : Result<(Nat), Text> {
            let candid_blob = blobify.to_blob(record);
            StableCollection.put(collection, main_btree_utils, candid_blob);
        };

        public func get(id : Nat) : Result<Record, Text> {
            switch (StableCollection.get(collection, main_btree_utils, id)) {
                case (#err(err)) return #err(err);
                case (#ok(record_details)) {
                    let record = blobify.from_blob(record_details);
                    #ok(record);
                };
            };
        };

        type RecordLimits = [(Text, ?State<ZT.CandidQuery>)];
        type FieldLimit = (Text, ?State<ZT.CandidQuery>);

        type Bounds = (RecordLimits, RecordLimits);

        type IndexDetails = {
            var sorted_in_reverse : ?Bool;
            intervals : Buffer.Buffer<(Nat, Nat)>;
        };

        type Iter<A> = Iter.Iter<A>;

        public func search_iter(query_builder : QueryBuilder) : Result<Iter<ZT.WrapId<Record>>, Text> {
            switch (StableCollection.internal_search(collection, query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = StableCollection.id_to_record_iter(collection, blobify, record_ids_iter);
                    #ok(record_iter);
                };
            };
        };

        public func search(query_builder : QueryBuilder) : Result<[ZT.WrapId<Record>], Text> {
            switch (StableCollection.internal_search(collection, query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = StableCollection.id_to_record_iter(collection, blobify, record_ids_iter);
                    let records = Iter.toArray(record_iter);
                    #ok(records);
                };
            };
        };

        // func async_skip_helper(iter : Iter<Nat>, skip : Nat) : async (Nat) {
        //     let performance_start = InternetComputer.performanceCounter(0);

        //     func instructions() : Nat64 {
        //         InternetComputer.performanceCounter(0) - performance_start;
        //     };

        //     var i = 0;
        //     while ((instructions() + 10_000_000 < MAX_UPDATE_INSTRUCTIONS) and i < skip) {
        //         ignore iter.next();
        //         i += 1;
        //     };

        //     i;
        // };

        // func async_skip(iter : Iter<Nat>, skip : Nat, performance_start : Nat64) : async* () {

        //     var skipped = 0;
        //     // Debug.print("starting async_skip: " # debug_show skip);
        //     while (skipped < skip) {
        //         skipped += await async_skip_helper(iter, skip - skipped);
        //         // Debug.print("skipped: " # debug_show skipped);
        //     };

        // };

        // public func async_find(query_builder : QueryBuilder, buffer : Buffer<ZT.WrapId<Record>>) : async* Result<(), Text> {
        //     switch (search_iter(query_builder)) {
        //         case (#err(err)) #err(err);
        //         case (#ok(records)) {
        //             for (record in records) {
        //                 buffer.add(record);
        //             };
        //             #ok(());
        //         };
        //     };
        // };

        public func stats() : ZT.CollectionStats {
            StableCollection.stats(collection);
        };

        // public func getBestIndex(db_query : QueryBuilder) : ?Index { };

        /// Returns the total number of records that match the query.
        /// This ignores the limit and skip parameters.
        public func count(query_builder : QueryBuilder) : Result<Nat, Text> {
            StableCollection.count(collection, query_builder);
        };

        // func async_count_iter_helper(iter : Iter<Nat>) : async (Nat) {
        //     let performance_start = InternetComputer.performanceCounter(0);
        //     func instructions() : Nat64 {
        //         InternetComputer.performanceCounter(0) - performance_start;
        //     };

        //     var count = 0;

        //     label counting_iter while (instructions() + 10_000_000 < MAX_QUERY_INSTRUCTIONS) {
        //         switch (iter.next()) {
        //             case (?id) count += 1;
        //             case (null) break counting_iter;
        //         };
        //     };

        //     count;

        // };

        // func async_count_iter(iter : Iter<Nat>) : async* (Nat) {
        //     let peekable_iter = Itertools.peekable(iter);

        //     var count = 0;
        //     while (Option.isSome(peekable_iter.peek())) {
        //         count += await async_count_iter_helper(peekable_iter);
        //     };

        //     count

        // };

        // public func async_count(query_builder : QueryBuilder) : async* Result<Nat, Text> {
        //     count(query_builder);
        // };

        public func updateById(id : Nat, update_operations : ZT.UpdateOperations<Record>) : Result<(), Text> {

            let internal_update_opertions = switch (update_operations) {
                case (#doc(record)) #doc(blobify.to_blob(record));
                case (#ops(field_ops)) #ops(field_ops);
            };

            StableCollection.update_by_id(collection, main_btree_utils, id, internal_update_opertions);

        };

        public func update(query_builder : QueryBuilder, update_operations : ZT.UpdateOperations<Record>) : Result<(), Text> {

            let records_iter = switch (StableCollection.internal_search(collection, query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            let internal_update_opertions = switch (update_operations) {
                case (#doc(record)) #doc(blobify.to_blob(record));
                case (#ops(field_ops)) #ops(field_ops);
            };

            for (id in records_iter) {
                let #ok(_) = StableCollection.update_by_id(collection, main_btree_utils, id, internal_update_opertions) else return #err("failed to update record");
            };

            #ok;
        };

        public func deleteById(id : Nat) : Result<Record, Text> {
            switch (StableCollection.delete_by_id(collection, main_btree_utils, id)) {
                case (#err(err)) return #err(err);
                case (#ok(record_details)) {
                    let record = blobify.from_blob(record_details);
                    #ok(record);
                };
            };
        };

        public func delete(query_builder : QueryBuilder) : Result<[Record], Text> {

            // let db_query = query_builder.build();
            let results_iter = switch (StableCollection.internal_search(collection, query_builder)) {
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

        // public func search()
    };

};
