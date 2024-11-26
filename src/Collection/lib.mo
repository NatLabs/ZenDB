/// A collection is a set of records of the same type.
///
/// ```motoko
/// type User = { name: Text, age: Nat };
/// let hydra_db = ZenDB();
/// let db = hydra_db.getDB("my_db");
///
/// let candify_users = {
///     to_blob = func(user: User) : Blob { to_candid(user) };
///     from_blob = func(blob: Blob) : User { let ?user : ?User = from_candid(blob); user; };
/// };
///
/// let users = db.getCollection<User>("users", candify_users);
///
/// let alice = { name = "Alice", age = 30 };
/// let bob = { name = "Bob", age = 25 };
///
/// let alice_id = users.put(alice);
/// let bob_id = users.put(bob);
///
/// ```
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

import Index "Index";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "Utils";
import QueryPlan "QueryPlan";
import C "Constants";
import QueryExecution "QueryExecution";

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

    func get_nested_candid_field(_candid_record : Candid, key : Text) : ?Candid {
        let nested_field_keys = Text.split(key, #text("."));

        var candid_record = _candid_record;

        for (key in nested_field_keys) {
            let #Record(record_fields) or #Option(#Record(record_fields)) = candid_record else return null;

            let ?found_field = Array.find<(Text, Candid)>(
                record_fields,
                func((variant_name, _) : (Text, Candid)) : Bool {
                    variant_name == key;
                },
            ) else return null;

            candid_record := found_field.1;

            // return #Null if the nested field was terminated early
            if (candid_record == #Null) return ? #Null;
        };

        return ?candid_record;
    };

    func get_nested_candid_type(_schema : Schema, key : Text) : ?Schema {
        let nested_field_keys = Text.split(key, #text("."));

        var schema = _schema;

        for (key in nested_field_keys) {
            let #Record(record_fields) or #Option(#Record(record_fields)) = schema else return null;

            let ?found_field = Array.find<(Text, Schema)>(
                record_fields,
                func((variant_name, _) : (Text, Schema)) : Bool {
                    variant_name == key;
                },
            ) else return null;

            schema := found_field.1;
        };

        return ?schema;
    };

    func id_to_record_iter<Record>(collection : StableCollection, blobify : Candify<Record>, iter : Iter<Nat>) : Iter<(Nat, Record)> {
        Iter.map<Nat, (Nat, Record)>(
            iter,
            func(id : Nat) : (Nat, Record) {
                let record = CollectionUtils.lookup_record<Record>(collection, blobify, id);
                (id, record);
            },
        );
    };

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

        public func create_index(_index_key_details : [(Text)]) : Result<(), Text> {

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

            let index_data = MemoryBTree.new(?DEFAULT_BTREE_ORDER);

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

        // func internal_find_best_index(query_builder) : Result<Iter<T.WrapId<Record>>, Text> {
        // };
        func interval_union(a : (Nat, Nat), b : (Nat, Nat)) : (Nat, Nat) {

            let start = Nat.min(a.0, b.0);
            let end = Nat.max(a.1, b.1);

            (start, end);

        };

        func interval_intersect(a : (Nat, Nat), b : (Nat, Nat)) : (Nat, Nat) {

            let start = Nat.max(a.0, a.1);
            let end = Nat.min(a.1, b.1);

            (start, end);

        };

        type RecordLimits = [(Text, ?State<Candid>)];
        type FieldLimit = (Text, ?State<Candid>);

        type Bounds = (RecordLimits, RecordLimits);

        func get_sort_records_by_field_cmp(
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

        type EvalResult = T.EvalResult;

        type IndexDetails = {
            var sorted_in_reverse : ?Bool;
            intervals : Buffer.Buffer<(Nat, Nat)>;
        };

        type Iter<A> = Iter.Iter<A>;

        let MAX_QUERY_INSTRUCTIONS : Nat64 = 5_000_000_000;
        let MAX_UPDATE_INSTRUCTIONS : Nat64 = 40_000_000_000;

        // tries to skip the number of records requested within the instruction limit
        // returns the number of records skipped

        func intervals_to_iter(collection : StableCollection, index_name : Text, _intervals : [(Nat, Nat)], sorted_in_reverse : Bool) : Iter<Nat> {

            let intervals = if (sorted_in_reverse) {
                Array.reverse(_intervals);
            } else {
                _intervals;
            };

            if (index_name == C.RECORD_ID_FIELD) {
                let record_ids = Itertools.flatten(
                    Iter.map(
                        intervals.vals(),
                        func(interval : (Nat, Nat)) : Iter<(Nat)> {
                            let record_ids = MemoryBTree.rangeKeys(collection.main, main_btree_utils, interval.0, interval.1);

                            if (sorted_in_reverse) {
                                return record_ids.rev();
                            };
                            record_ids;
                        },
                    )
                );

                return record_ids;
            };

            let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);

            let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);

            Itertools.flatten(
                Iter.map(
                    intervals.vals(),
                    func(interval : (Nat, Nat)) : Iter<(Nat)> {
                        let record_ids = MemoryBTree.rangeVals(index.data, index_data_utils, interval.0, interval.1);

                        if (sorted_in_reverse) {
                            return record_ids.rev();
                        };
                        record_ids;
                    },
                )
            );

        };

        func extract_intervals_in_pagination_range(skip : Nat, opt_limit : ?Nat, index_name : Text, _intervals : [(Nat, Nat)], sorted_in_reverse : Bool) : Iter<Nat> {

            var skipped = 0;
            var prev_acc_interval_size = 0;
            var i = 0;

            while (skipped > skip and i < _intervals.size()) {
                prev_acc_interval_size := skipped;

                let interval = _intervals.get(i);
                let size = interval.1 - interval.0;
                skipped += size;

                i += 1;
            };

            let remaining_skip = skip - prev_acc_interval_size;

            let new_intervals_first_start = _intervals.get(i).0 + remaining_skip;

            Debug.print("old_intervals: " # debug_show _intervals);

            let new_intervals = Array.tabulate<(Nat, Nat)>(
                _intervals.size() - i,
                func(j : Nat) : (Nat, Nat) {
                    if (j == 0) {
                        (new_intervals_first_start, _intervals.get(i).1);
                    } else {
                        _intervals.get(i + j);
                    };
                },
            );

            if (new_intervals.size() == 1 and new_intervals.get(0).0 == new_intervals.get(0).1) {
                return Itertools.empty<(Nat)>();
            };

            Debug.print("new_intervals: " # debug_show new_intervals);

            let limit = switch (opt_limit) {
                case (null) {
                    return intervals_to_iter(collection, index_name, new_intervals, sorted_in_reverse);
                };
                case (?limit) limit;
            };

            i := 0;
            var prev = 0;
            var add = 0;

            while (add < limit and i < new_intervals.size()) {
                prev := add;
                let interval = new_intervals.get(i);
                let size = interval.1 - interval.0;

                add += size;
                i += 1;
            };

            let remaining_limit = limit - prev;

            Debug.print("remaining_limit: " # debug_show remaining_limit);

            if (i == _intervals.size() and remaining_limit == 0) {
                return intervals_to_iter(collection, index_name, new_intervals, sorted_in_reverse);
            };

            let even_newer_intervals = Array.tabulate<(Nat, Nat)>(
                i,
                func(j : Nat) : (Nat, Nat) {
                    if (j == i - 1) {
                        (
                            new_intervals.get(j).0,
                            Nat.min(new_intervals.get(j).0 + remaining_limit, new_intervals.get(j).1),
                        );
                    } else {
                        new_intervals.get(j);
                    };
                },
            );

            Debug.print("even_newer_intervals: " # debug_show even_newer_intervals);

            return intervals_to_iter(collection, index_name, even_newer_intervals, sorted_in_reverse);

        };

        func extract_intervals_in_pagination_range_for_reversed_intervals(
            skip : Nat,
            opt_limit : ?Nat,
            index_name : Text,
            _intervals : [(Nat, Nat)],
            sorted_in_reverse : Bool,
        ) : Iter<Nat> {
            Debug.print("extract_intervals_in_pagination_range_for_reversed_intervals");

            var skipped = 0;
            var prev = 0;

            var i = _intervals.size();

            Debug.print("old_intervals: " # debug_show _intervals);

            //! calculate the total size and use it as the max bound when calculating the remaining skip
            while (skipped <= skip and i > 0) {

                i -= 1;
                prev := skipped;

                let interval = _intervals.get(i);
                let size = interval.1 - interval.0;
                skipped += size;

            };

            Debug.print("skipped: " # debug_show skipped);
            Debug.print("prev: " # debug_show prev);
            Debug.print("i: " # debug_show i);

            let remaining_skip = skip - prev;

            let new_intervals_last_end = _intervals.get(i).1 - remaining_skip;

            let new_intervals = Array.tabulate<(Nat, Nat)>(
                i + 1,
                func(j : Nat) : (Nat, Nat) {
                    if (j == (_intervals.size() - i - 1)) {
                        (_intervals.get(i).0, new_intervals_last_end);
                    } else {
                        _intervals.get(i + j);
                    };
                },
            );

            if (new_intervals.size() == 1 and new_intervals.get(0).0 == new_intervals.get(0).1) {
                return Itertools.empty<Nat>();
            };

            Debug.print("new_intervals: " # debug_show new_intervals);

            let limit = switch (opt_limit) {
                case (null) {
                    return intervals_to_iter(collection, index_name, new_intervals, sorted_in_reverse);
                };
                case (?limit) limit;
            };

            i := new_intervals.size();

            prev := 0;
            var add = 0;

            while (add <= limit and i > 0) {
                i -= 1;
                prev := add;
                let interval = new_intervals.get(i);
                let size = interval.1 - interval.0;

                add += size;
            };

            let remaining_limit = limit - prev;

            Debug.print("remaining_limit: " # debug_show remaining_limit);

            if (i == 0 and remaining_limit == 0) {
                return intervals_to_iter(collection, index_name, new_intervals, sorted_in_reverse);
            };

            let even_newer_intervals = Array.tabulate<(Nat, Nat)>(
                new_intervals.size() - i,
                func(j : Nat) : (Nat, Nat) {
                    if (j == (new_intervals.size() - i - 1)) {

                        (
                            Nat.max(
                                new_intervals.get(j).0,
                                new_intervals.get(j).1 - remaining_limit,
                            ),
                            new_intervals.get(j).1,
                        );
                    } else {
                        new_intervals.get(j);
                    };
                },
            );

            Debug.print("even_newer_intervals: " # debug_show even_newer_intervals);

            return intervals_to_iter(collection, index_name, even_newer_intervals, sorted_in_reverse);

        };

        func paginate(eval : EvalResult, skip : Nat, opt_limit : ?Nat) : Iter<Nat> {

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
                        return extract_intervals_in_pagination_range_for_reversed_intervals(skip, opt_limit, index_name, _intervals, sorted_in_reverse);
                    } else {
                        return extract_intervals_in_pagination_range(skip, opt_limit, index_name, _intervals, sorted_in_reverse);
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

        func evaluate_query(stable_query : T.StableQuery) : Result<Iter<Nat>, Text> {

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

            let iter = paginate(eval, Option.get(pagination.skip, 0), pagination.limit);

            return #ok((iter));

        };

        func internal_find(query_builder : QueryBuilder) : Result<Iter<Nat>, Text> {
            let stable_query = query_builder.build();
            switch (evaluate_query(stable_query)) {
                case (#err(err)) #err(err);
                case (#ok(eval_result)) #ok(eval_result);
            };
        };

        public func find_iter(query_builder : QueryBuilder) : Result<Iter<T.WrapId<Record>>, Text> {
            switch (internal_find(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = id_to_record_iter(collection, blobify, record_ids_iter);
                    #ok(record_iter);
                };
            };
        };

        public func find(query_builder : QueryBuilder) : Result<[T.WrapId<Record>], Text> {
            switch (internal_find(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = id_to_record_iter(collection, blobify, record_ids_iter);
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

                        let stable_memory : T.MemoryCollectionStats = {
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

            let count = switch (QueryExecution.get_unique_record_ids_from_query_plan(collection, query_plan)) {
                case (#Empty) 0;
                case (#BitMap(bitmap)) bitmap.size();
                case (#Ids(iter)) Iter.size(iter);
                case (#Interval(index_name, intervals, sorted_in_reverse)) {

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

            let records_iter = switch (internal_find(query_builder)) {
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
            let results_iter = switch (internal_find(query_builder)) {
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
