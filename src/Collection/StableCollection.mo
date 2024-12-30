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
import Vector "mo:vector";
import Ids "mo:incremental-ids";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import ZT "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import ByteUtils "../ByteUtils";

import Index "Index";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "Utils";
import QueryPlan "QueryPlan";
import C "../Constants";
import QueryExecution "QueryExecution";
import Intervals "Intervals";

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

    public type RecordId = ZT.RecordId;
    public type Index = ZT.Index;
    public type Candid = ZT.Candid;
    public type SortDirection = ZT.SortDirection;
    public type State<R> = ZT.State<R>;
    public type ZenQueryLang = ZT.ZenQueryLang;

    public type Candify<A> = ZT.Candify<A>;

    public type StableCollection = ZT.StableCollection;

    public type IndexKeyFields = ZT.IndexKeyFields;
    type EvalResult = ZT.EvalResult;

    public func size(collection : StableCollection) : Nat {
        MemoryBTree.size(collection.main);
    };

    public func update_schema(collection : StableCollection, schema : ZT.Schema) : Result<(), Text> {

        let is_compatible = Schema.is_schema_backward_compatible(collection.schema, schema);
        if (not is_compatible) return #err("Schema is not backward compatible");

        collection.schema := schema;
        // Debug.print("Schema Updated: Ensure to update your Record type as well.");
        #ok;
    };

    public func create_index(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        _index_key_details : [(Text)],
    ) : Result<(), Text> {

        let index_key_details : [(Text, SortDirection)] = Array.append(
            Array.map<Text, (Text, SortDirection)>(
                _index_key_details,
                func(key : Text) : (Text, SortDirection) {
                    (key, #Ascending);
                },
            ),
            [(C.RECORD_ID_FIELD, #Ascending)],
        );

        // let sorted_index_key_details = Array.sort(index_key_details, func(a : (Text, SortDirection), b : (Text, SortDirection)) : Order { Text.compare(a.0, b.0) });

        let index_name = Text.join(
            "_",
            Iter.map<(Text, SortDirection), Text>(
                index_key_details.vals(),
                func((name, dir) : (Text, SortDirection)) : Text {
                    name # (debug_show dir);
                },
            ),
        );

        switch (Map.get(collection.indexes, thash, index_name)) {
            // doesn't fail if index already exists, just returns ok, because this is likely to be called in the top level actor block and will be executed multiple times during upgrades
            case (?_) return #ok();
            case (null) {};
        };

        let index_data : MemoryBTree.StableMemoryBTree = switch (Vector.removeLast(collection.freed_btrees)) {
            case (?btree) btree;
            case (null) MemoryBTree.new(?C.DEFAULT_BTREE_ORDER);
        };

        let index_data_utils = CollectionUtils.get_index_data_utils(collection, index_key_details);

        let candid_map = CandidMap.CandidMap(collection.schema, #Record([]));

        for ((id, candid_blob) in MemoryBTree.entries(collection.main, main_btree_utils)) {
            let candid = CollectionUtils.decode_candid_blob(collection, candid_blob);
            candid_map.reload(candid);

            let buffer = Buffer.Buffer<(Candid)>(8);

            switch (candid) {
                case (#Record(records)) {};
                case (_) return #err("Couldn't get records");
            };

            for ((index_key, dir) in index_key_details.vals()) {

                if (index_key == C.RECORD_ID_FIELD) {
                    buffer.add(#Nat(id));
                } else {
                    let ?value = candid_map.get(index_key) else return #err("Couldn't get value for index key: " # debug_show index_key);

                    buffer.add(value);
                };

            };

            let index_key_values = Buffer.toArray(buffer);
            ignore MemoryBTree.insert(index_data, index_data_utils, index_key_values, id);
        };

        let index : Index = {
            name = index_name;
            key_details = index_key_details;
            data = index_data;
        };

        ignore Map.put<Text, Index>(collection.indexes, thash, index_name, index);

        #ok();
    };

    public func delete_index(
        collection : StableCollection,
        _main_btree_utils : BTreeUtils<Nat, Blob>,
        index_key_details : [Text],
    ) : Result<(), Text> {

        let index_name = Text.join(
            "_",
            Iter.map<Text, Text>(
                index_key_details.vals(),
                func(key : Text) : Text {
                    key;
                },
            ),
        );

        let opt_index = Map.remove(collection.indexes, thash, index_name);

        switch (opt_index) {
            case (?index) {
                MemoryBTree.clear(index.data);
                Vector.add(collection.freed_btrees, index.data);

                #ok();
            };
            case (null) #err("Index not found");
        };

    };

    let MAX_QUERY_INSTRUCTIONS : Nat64 = 5_000_000_000;
    let MAX_UPDATE_INSTRUCTIONS : Nat64 = 40_000_000_000;

    func paginate(collection : StableCollection, eval : EvalResult, skip : Nat, opt_limit : ?Nat) : Iter<Nat> {

        let iter = switch (eval) {
            case (#Empty) {
                // Debug.print("#Empty");
                return Itertools.empty<Nat>();
            };
            case (#BitMap(bitmap)) {
                // Debug.print("#BitMap");
                bitmap.vals();
            };
            case (#Ids(iter)) {
                // Debug.print("#Ids");
                iter;
            };
            case (#Interval(index_name, _intervals, sorted_in_reverse)) {
                // Debug.print("#Interval");

                if (sorted_in_reverse) {
                    return Intervals.extract_intervals_in_pagination_range_for_reversed_intervals(collection, skip, opt_limit, index_name, _intervals, sorted_in_reverse);
                } else {
                    return Intervals.extract_intervals_in_pagination_range(collection, skip, opt_limit, index_name, _intervals, sorted_in_reverse);
                };

            };

        };

        let iter_with_offset = Itertools.skip(iter, skip);
        // Debug.print("skip: " # debug_show skip);

        var paginated_iter = switch (opt_limit) {
            case (?limit) {
                let iter_with_limit = Itertools.take(iter_with_offset, limit);
                (iter_with_limit);
            };
            case (null) (iter_with_offset);
        };

        paginated_iter;

    };

    public func insert_with_id(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, id : Nat, candid_blob : ZT.CandidBlob) : Result<(), Text> {
        put_with_id(collection, main_btree_utils, id, candid_blob);
    };

    public func put_with_id(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        id : Nat,
        candid_blob : ZT.CandidBlob,
    ) : Result<(), Text> {

        let candid = CollectionUtils.decode_candid_blob(collection, candid_blob);

        switch (candid) {
            case (#Record(_)) {};
            case (_) return #err("Values inserted into the collection must be #Records");
        };

        // Debug.print("validate: " # debug_show (collection.schema) #debug_show (candid));
        Utils.assert_result(Schema.validate_record(collection.schema, candid));

        // if this fails, it means the id already exists
        // insert() - should to used to update existing records
        //
        // also note that although we have already inserted the value into the main btree
        // the inserted value will be discarded because the call fails
        // meaning the canister state will not be updated
        // at least that's what I think - need to confirm
        let opt_prev = MemoryBTree.insert(collection.main, main_btree_utils, id, candid_blob);

        switch (opt_prev) {
            case (null) {};
            case (?prev) {
                ignore MemoryBTree.insert(collection.main, main_btree_utils, id, prev);
                return #err("Record with id (" # debug_show id # ") already exists");
            };
        };

        // should change getId to getPointer
        // let ?ref_pointer = MemoryBTree.getId(collection.main, main_btree_utils, id);
        // assert MemoryBTree.getId(collection.main, main_btree_utils, id) == ?id;

        if (Map.size(collection.indexes) == 0) return #ok();

        Debug.print("adding to indexes");

        let candid_map = CandidMap.CandidMap(collection.schema, candid);

        for (index in Map.vals(collection.indexes)) {

            let buffer = Buffer.Buffer<Candid>(8);

            for ((index_key, dir) in index.key_details.vals()) {

                Debug.print("index_key: " # debug_show index_key);
                if (index_key == C.RECORD_ID_FIELD) {
                    buffer.add(#Nat(id));
                } else {
                    let ?value = candid_map.get(index_key) else return #err("Couldn't get value for index key: " # debug_show index_key);

                    buffer.add(value);
                };

                Debug.print("buffer contents: " # debug_show Buffer.toArray(buffer));

            };

            let index_key_values = Buffer.toArray(buffer);

            let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);
            ignore MemoryBTree.insert(index.data, index_data_utils, index_key_values, id);
        };

        Debug.print("finished adding to indexes");

        #ok();
    };

    public func insert(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, candid_blob : ZT.CandidBlob) : Result<Nat, Text> {
        put(collection, main_btree_utils, candid_blob);
    };

    public func put(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, candid_blob : ZT.CandidBlob) : Result<Nat, Text> {
        let id = Ids.Gen.next(collection.ids);

        switch (put_with_id(collection, main_btree_utils, id, candid_blob)) {
            case (#err(msg)) return #err(msg);
            case (#ok(_)) {};
        };

        #ok(id);
    };

    public func get(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        id : Nat,
    ) : Result<ZT.CandidBlob, Text> {
        let ?record_details = MemoryBTree.get(collection.main, main_btree_utils, id) else return #err("Record not found");
        #ok(record_details);
    };

    public func search(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        query_builder : QueryBuilder,
    ) : Result<[(ZT.WrapId<ZT.CandidBlob>)], Text> {
        switch (internal_search(collection, query_builder)) {
            case (#err(err)) return #err(err);
            case (#ok(record_ids_iter)) {
                let candid_blob_iter = id_to_candid_blob_iter(collection, record_ids_iter);
                let candid_blobs = Iter.toArray(candid_blob_iter);
                #ok(candid_blobs);
            };
        };
    };

    /// Evaluates a query and returns an iterator of record ids.
    public func evaluate_query(collection : StableCollection, stable_query : ZT.StableQuery) : Result<Iter<Nat>, Text> {

        let query_operations = stable_query.query_operations;
        let sort_by = stable_query.sort_by;
        let pagination = stable_query.pagination;

        let (opt_cursor, cursor_map) = switch (pagination.cursor) {
            case (?(id, pagination_direction)) switch (CollectionUtils.lookup_candid_record(collection, id)) {
                case (?record) {
                    (?(id, record), CandidMap.CandidMap(collection.schema, record));
                };
                case (null) (null, CandidMap.CandidMap(collection.schema, #Record([])));
            };
            case (null) (null, CandidMap.CandidMap(collection.schema, #Record([])));
        };

        switch (Query.validate_query(collection, stable_query.query_operations)) {
            case (#err(err)) return #err("Invalid Query: " # err);
            case (#ok(_)) ();
        };

        // Debug.print("stable_query: " # debug_show stable_query);
        // Debug.print("pagination: " # debug_show pagination);
        // Debug.print("cursor_record: " # debug_show (opt_cursor));

        let query_plan : ZT.QueryPlan = QueryPlan.create_query_plan(
            collection,
            query_operations,
            sort_by,
            opt_cursor,
            cursor_map,
        );

        let sort_records_by_field_cmp = switch (sort_by) {
            case (?sort_by) get_sort_records_by_field_cmp(collection, sort_by);
            case (null) func(_ : Nat, _ : Nat) : Order = #equal;
        };

        let eval = QueryExecution.generate_record_ids_for_query_plan(collection, query_plan, sort_by, sort_records_by_field_cmp);

        let iter = paginate(collection, eval, Option.get(pagination.skip, 0), pagination.limit);

        return #ok((iter));

    };

    public func internal_search(collection : StableCollection, query_builder : QueryBuilder) : Result<Iter<Nat>, Text> {
        let stable_query = query_builder.build();
        switch (evaluate_query(collection, stable_query)) {
            case (#err(err)) #err(err);
            case (#ok(eval_result)) #ok(eval_result);
        };
    };

    public func id_to_record_iter<Record>(collection : StableCollection, blobify : Candify<Record>, iter : Iter<Nat>) : Iter<(Nat, Record)> {
        Iter.map<Nat, (Nat, Record)>(
            iter,
            func(id : Nat) : (Nat, Record) {
                let record = CollectionUtils.lookup_record<Record>(collection, blobify, id);
                (id, record);
            },
        );
    };

    public func id_to_candid_blob_iter<Record>(collection : StableCollection, iter : Iter<Nat>) : Iter<(Nat, ZT.CandidBlob)> {
        Iter.map<Nat, (Nat, ZT.CandidBlob)>(
            iter,
            func(id : Nat) : (Nat, ZT.CandidBlob) {
                let candid_blob = CollectionUtils.lookup_candid_blob(collection, id);
                (id, candid_blob);
            },
        );
    };

    public func search_iter(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        query_builder : QueryBuilder,
    ) : Result<Iter<ZT.WrapId<ZT.CandidBlob>>, Text> {
        switch (internal_search(collection, query_builder)) {
            case (#err(err)) return #err(err);
            case (#ok(record_ids_iter)) {
                let record_iter = id_to_candid_blob_iter(collection, record_ids_iter);
                #ok(record_iter);
            };
        };
    };

    public func get_sort_records_by_field_cmp(
        collection : StableCollection,
        sort_field : (Text, ZT.SortDirection),
    ) : (Nat, Nat) -> Order {

        let deserialized_records_map = Map.new<Nat, Candid.Candid>();

        func get_candid_map_bytes(id : Nat) : Candid.Candid {
            switch (Map.get(deserialized_records_map, nhash, id)) {
                case (?candid) candid;
                case (null) {
                    let ?candid = CollectionUtils.lookup_candid_record(collection, id) else Debug.trap("Couldn't find record with id: " # debug_show id);
                    // ignore Map.put(deserialized_records_map, nhash, id, record);
                    candid;
                };
            };
        };

        let opt_candid_map_a : ?CandidMap.CandidMap = null;
        let opt_candid_map_b : ?CandidMap.CandidMap = null;

        func sort_records_by_field_cmp(a : Nat, b : Nat) : Order {

            let record_a = get_candid_map_bytes(a);
            let record_b = get_candid_map_bytes(b);

            let candid_map_a : CandidMap.CandidMap = switch (opt_candid_map_a) {
                case (?candid_map) {
                    candid_map.reload(record_a);
                    candid_map;
                };
                case (null) {
                    let candid_map = CandidMap.CandidMap(collection.schema, record_a);
                    candid_map;
                };
            };

            let candid_map_b : CandidMap.CandidMap = switch (opt_candid_map_b) {
                case (?candid_map) {
                    candid_map.reload(record_b);
                    candid_map;
                };
                case (null) {
                    let candid_map = CandidMap.CandidMap(collection.schema, record_b);
                    candid_map;
                };
            };

            let ?value_a = candid_map_a.get(sort_field.0) else Debug.trap("Couldn't get value from CandidMap for key: " # sort_field.0);
            let ?value_b = candid_map_b.get(sort_field.0) else Debug.trap("Couldn't get value from CandidMap for key: " # sort_field.0);

            let order_num = Schema.cmp_candid(#Empty, value_a, value_b);

            let order_variant = if (sort_field.1 == #Ascending) {
                if (order_num == 0) #equal else if (order_num == 1) #greater else #less;
            } else {
                if (order_num == 0) #equal else if (order_num == 1) #less else #greater;
            };

            order_variant;
        };
        sort_records_by_field_cmp;
    };

    public func stats(collection : StableCollection) : ZT.CollectionStats {
        let main_btree_index = {
            stable_memory = {
                metadata_bytes = MemoryBTree.metadataBytes(collection.main);
                actual_data_bytes = MemoryBTree.bytes(collection.main);
            };
        };

        let indexes : [ZT.IndexStats] = Iter.toArray(
            Iter.map<(Text, Index), ZT.IndexStats>(
                Map.entries(collection.indexes),
                func((index_name, index) : (Text, Index)) : ZT.IndexStats {
                    let columns : [Text] = Array.map<(Text, Any), Text>(
                        index.key_details,
                        func((key, direction) : (Text, Any)) : Text = key,
                    );

                    let stable_memory : ZT.MemoryStats = {
                        metadata_bytes = MemoryBTree.metadataBytes(index.data);
                        actual_data_bytes = MemoryBTree.bytes(index.data);
                    };

                    { columns; stable_memory };

                },
            )
        );

        { indexes; main_btree_index; records = size(collection) };
    };

    public func count(collection : StableCollection, query_builder : QueryBuilder) : Result<Nat, Text> {
        let stable_query = query_builder.build();

        let query_plan = QueryPlan.create_query_plan(
            collection,
            stable_query.query_operations,
            null,
            null,
            CandidMap.CandidMap(collection.schema, #Record([])),
        );

        let count = switch (QueryExecution.get_unique_record_ids_from_query_plan(collection, Map.new(), query_plan)) {
            case (#Empty) 0;
            case (#BitMap(bitmap)) bitmap.size();
            case (#Ids(iter)) Iter.size(iter);
            case (#Interval(_index_name, intervals, _sorted_in_reverse)) {
                // Debug.print("count intervals: " # debug_show intervals);

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

    public func exists(collection : StableCollection, query_builder : QueryBuilder) : Result<Bool, Text> {
        let stable_query = query_builder.Limit(1).build();

        let query_plan = QueryPlan.create_query_plan(
            collection,
            stable_query.query_operations,
            null,
            null,
            CandidMap.CandidMap(collection.schema, #Record([])),
        );

        let sort_records_by_field_cmp = func(_ : Nat, _ : Nat) : Order = #equal;

        let eval = QueryExecution.generate_record_ids_for_query_plan(collection, query_plan, null, sort_records_by_field_cmp);

        let greater_than_0 = switch (eval) {
            case (#Empty) false;
            case (#BitMap(bitmap)) bitmap.size() > 0;
            case (#Ids(iter)) switch (iter.next()) {
                case (?_) true;
                case (null) false;
            };
            case (#Interval(index_name, _intervals, sorted_in_reverse)) {
                for (interval in _intervals.vals()) {
                    if (interval.1 - interval.0 > 0) return #ok(true);
                };

                false;
            };

        };

        #ok(greater_than_0);

    };

    public func delete_by_id(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, id : Nat) : Result<(ZT.CandidBlob), Text> {

        let ?prev_record_details = MemoryBTree.remove(collection.main, main_btree_utils, id);
        let prev_candid = CollectionUtils.decode_candid_blob(collection, prev_record_details);

        let #Record(prev_records) = prev_candid else return #err("Couldn't get records");
        // Debug.print("prev_records: " # debug_show prev_records);
        for (index in Map.vals(collection.indexes)) {

            let prev_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, prev_records);
            let index_data_utils : BTreeUtils<[Candid], RecordId> = CollectionUtils.get_index_data_utils(collection, index.key_details);

            assert ?id == MemoryBTree.remove<[Candid], RecordId>(index.data, index_data_utils, prev_index_key_values);
        };

        let candid_blob = prev_record_details;

        Ids.Gen.release(collection.ids, id);

        #ok(candid_blob);
    };

};
