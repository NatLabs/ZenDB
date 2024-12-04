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

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import ByteUtils "../ByteUtils";
import LegacyCandidMap "../LegacyCandidMap";

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

    public type RecordPointer = Nat;
    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type Candify<A> = T.Candify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;
    type EvalResult = T.EvalResult;

    let DEFAULT_BTREE_ORDER = 256;

    public func create_index(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, (Blob, [Nat8])>,
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
            case (null) MemoryBTree.new(?DEFAULT_BTREE_ORDER);
        };

        let index_data_utils = CollectionUtils.get_index_data_utils(collection, index_key_details);

        let candid_map = CandidMap.fromCandid(#Record([]));

        for ((id, (candid_blob, candid_map_bytes)) in MemoryBTree.entries(collection.main, main_btree_utils)) {
            let candid = CollectionUtils.decode_candid_blob(collection, candid_blob);
            candid_map.reload(candid_map_bytes);

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
        _main_btree_utils : BTreeUtils<Nat, (Blob, [Nat8])>,
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
                Debug.print("#Empty");
                return Itertools.empty<Nat>();
            };
            case (#BitMap(bitmap)) {
                Debug.print("#BitMap");
                bitmap.vals();
            };
            case (#Ids(iter)) {
                Debug.print("#Ids");
                iter;
            };
            case (#Interval(index_name, _intervals, sorted_in_reverse)) {
                Debug.print("#Interval");

                if (sorted_in_reverse) {
                    return Intervals.extract_intervals_in_pagination_range_for_reversed_intervals(collection, skip, opt_limit, index_name, _intervals, sorted_in_reverse);
                } else {
                    return Intervals.extract_intervals_in_pagination_range(collection, skip, opt_limit, index_name, _intervals, sorted_in_reverse);
                };

            };

        };

        let iter_with_offset = Itertools.skip(iter, skip);
        Debug.print("skip: " # debug_show skip);

        var paginated_iter = switch (opt_limit) {
            case (?limit) {
                let iter_with_limit = Itertools.take(iter_with_offset, limit);
                (iter_with_limit);
            };
            case (null) (iter_with_offset);
        };

        ((paginated_iter));

    };

    /// Evaluates a query and returns an iterator of record ids.
    public func evaluate_query(collection : StableCollection, stable_query : T.StableQuery) : Result<Iter<Nat>, Text> {

        let query_operations = stable_query.query_operations;
        let sort_by = stable_query.sort_by;
        let pagination = stable_query.pagination;

        let (opt_cursor, cursor_map) = switch (pagination.cursor) {
            case (?(id, pagination_direction)) switch (CollectionUtils.lookup_candid_record(collection, id)) {
                case (?record) {
                    (?(id, record), CandidMap.fromCandid(record));
                };
                case (null) (null, CandidMap.fromCandid(#Record([])));
            };
            case (null) (null, CandidMap.fromCandid(#Record([])));
        };

        switch (Query.validate_query(collection, stable_query.query_operations)) {
            case (#err(err)) return #err("Invalid Query: " # err);
            case (#ok(_)) ();
        };

        Debug.print("stable_query: " # debug_show stable_query);
        Debug.print("pagination: " # debug_show pagination);
        Debug.print("cursor_record: " # debug_show (opt_cursor));

        let query_plan : T.QueryPlan = QueryPlan.create_query_plan(
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

    public func internal_find(collection : StableCollection, query_builder : QueryBuilder) : Result<Iter<Nat>, Text> {
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

    public func find_iter<Record>(
        collection : StableCollection,
        blobify : Candify<Record>,
        main_btree_utils : BTreeUtils<Nat, (Blob, [Nat8])>,
        query_builder : QueryBuilder,
    ) : Result<Iter<T.WrapId<Record>>, Text> {
        switch (internal_find(collection, query_builder)) {
            case (#err(err)) return #err(err);
            case (#ok(record_ids_iter)) {
                let record_iter = id_to_record_iter(collection, blobify, record_ids_iter);
                #ok(record_iter);
            };
        };
    };

    public func get_sort_records_by_field_cmp(
        collection : StableCollection,
        sort_field : (Text, T.SortDirection),
    ) : (Nat, Nat) -> Order {

        let deserialized_records_map = Map.new<Nat, [Nat8]>();

        func get_candid_map_bytes(id : Nat) : [Nat8] {
            switch (Map.get(deserialized_records_map, nhash, id)) {
                case (?candid_map_bytes) candid_map_bytes;
                case (null) {
                    let ?candid_map_bytes = CollectionUtils.lookup_candid_map_bytes(collection, id) else Debug.trap("Couldn't find record with id: " # debug_show id);
                    // ignore Map.put(deserialized_records_map, nhash, id, record);
                    candid_map_bytes;
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
                    let candid_map = CandidMap.CandidMap(record_a);
                    candid_map;
                };
            };

            let candid_map_b : CandidMap.CandidMap = switch (opt_candid_map_b) {
                case (?candid_map) {
                    candid_map.reload(record_b);
                    candid_map;
                };
                case (null) {
                    let candid_map = CandidMap.CandidMap(record_b);
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

};
