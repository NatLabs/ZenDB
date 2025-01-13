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
import CandidMod "../CandidMod";

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
        collection.formatted_schema := Candid.formatCandidType([schema], null)[0];
        // Debug.print("Schema Updated: Ensure to update your Record type as well.");
        #ok;
    };

    func insert_into_index(
        collection : StableCollection,
        index : Index,
        id : Nat,
        candid_map : CandidMap.CandidMap,
    ) : Result<(), Text> {

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

        #ok();
    };

    public func create_index(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        index_name : Text,
        index_key_details : [(Text, SortDirection)],
    ) : Result<(), Text> {

        let index_key_details : [(Text, SortDirection)] = Array.append(
            index_key_details,
            [(C.RECORD_ID_FIELD, #Ascending)],
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

        let index : Index = {
            name = index_name;
            key_details = index_key_details;
            data = index_data;
        };

        ignore Map.put<Text, Index>(collection.indexes, thash, index_name, index);

        #ok();
    };

    public func create_and_populate_index(
        collection : StableCollection,
        _main_btree_utils : BTreeUtils<Nat, Blob>,
        index_name : Text,
        index_key_details : [(Text, SortDirection)],
    ) : Result<(), Text> {

        switch (create_index(collection, _main_btree_utils, index_name, index_key_details)) {
            case (#err(err)) return #err(err);
            case (#ok(_)) {};
        };

        switch (populate_index(collection, _main_btree_utils, index_name, opt_batch_size)) {
            case (#err(err)) return #err(err);
            case (#ok(_)) {};
        };

        #ok();

    };

    public func clear_index(
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

        switch (Map.get(collection.indexes, thash, index_name)) {
            case (?index) MemoryBTree.clear(index.data);
            case (null) return #err("Index not found");
        };

        #ok()

    };

    func internal_populate_indexes(
        collection : StableCollection,
        indexes : Buffer.Buffer<Index>,
        entries : Iter<(Nat, Blob)>,
    ) : Result<(), Text> {
        Debug.print("internally populating indexes");

        for ((id, candid_blob) in entries) {
            let candid = CollectionUtils.decode_candid_blob(collection, candid_blob);
            let candid_map = CandidMap.CandidMap(collection.schema, candid);

            for (index in indexes.vals()) {
                switch (insert_into_index(collection, index, id, candid_map)) {
                    case (#err(err)) return #err(err);
                    case (#ok(_)) {};
                };
            };

        };

        #ok();

    };

    func recommended_entries_to_populate_based_on_benchmarks(
        num_indexes : Nat
    ) : Nat {
        let TRILLION = 1_000_000_000_000;
        let MILLION = 1_000_000;

        let max_instructions = 30 * TRILLION; // allows for 10T buffer
        let decode_cost = 300 * MILLION; // per entry
        let insert_cost = 150 * MILLION; // per entry per index

        // Calculate maximum number of entries
        let max_entries = max_instructions / (decode_cost + insert_cost * num_indexes);

        max_entries;
    };

    public func populate_index(
        collection : StableCollection,
        _main_btree_utils : BTreeUtils<Nat, Blob>,
        index_name : Text,
    ) : Result<(), Text> {
        populate_indexes(collection, _main_btree_utils, [index_name]);
    };

    public func populate_indexes(
        collection : StableCollection,
        _main_btree_utils : BTreeUtils<Nat, Blob>,
        index_names : [[Text]],
    ) : Result<(), Text> {

        let indexes = Buffer.Buffer<Index>(index_names.size());

        for (index_name in index_names.vals()) {
            let ?index = Map.get(collection.indexes, thash, index_name) else return #err("Index '" # index_name # "' does not exist");

            indexes.add(index);
        };

        Debug.print("collected indexes`");

        internal_populate_indexes(
            collection,
            indexes,
            MemoryBTree.entries(collection.main, _main_btree_utils),
        );

    };

    // public func async_populate_indexes(
    //     collection : StableCollection,
    //     _main_btree_utils : BTreeUtils<Nat, Blob>,
    //     indexes_key_details : [[Text]],
    //     opt_batch_size : ?Nat,
    // ) : async* Result<(), Text> {

    //     let recommended_batch_size = recommended_entries_to_populate_based_on_benchmarks(indexes_key_details.size());

    //     let BATCH_SIZE = Option.get(opt_batch_size, recommended_batch_size);

    //     let indexes = Buffer.Buffer<Index>(indexes_key_details.size());

    //     for (index_key_details in indexes_key_details.vals()) {
    //         let index_name = Text.join(
    //             "_",
    //             Iter.map<Text, Text>(
    //                 index_key_details.vals(),
    //                 func(key : Text) : Text {
    //                     key;
    //                 },
    //             ),
    //         );

    //         let ?index = Map.get(collection.indexes, thash, index_name) else return #err("Index with key_details '" # debug_show index_key_details # "' does not exist");

    //         indexes.add(index);
    //     };

    //     var size = 0;

    //     while (size < MemoryBTree.size(collection.main)) {

    //         let start = size;
    //         let end = Nat.min(size + BATCH_SIZE, MemoryBTree.size(collection.main));

    //         let res = await internal_populate_indexes(
    //             collection,
    //             indexes,
    //             MemoryBTree.range(collection.main, _main_btree_utils, start, end),
    //         );

    //         switch (res) {
    //             case (#err(err)) return #err(err);
    //             case (#ok(_)) {};
    //         };

    //         size += BATCH_SIZE;

    //     };

    //     #ok()

    // };

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

        //  Debug.print("adding to indexes");

        let candid_map = CandidMap.CandidMap(collection.schema, candid);

        for (index in Map.vals(collection.indexes)) {

            let buffer = Buffer.Buffer<Candid>(8);

            let index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, candid_map);

            let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);
            ignore MemoryBTree.insert(index.data, index_data_utils, index_key_values, id);
        };

        //  Debug.print("finished adding to indexes");

        #ok();
    };

    func handle_update_field_set_operation(
        candid_map : CandidMap.CandidMap,
        op : ZT.UpdateFieldSetOperations,
    ) : Result<Candid, Text> {

        func handle_nested_operations(
            nested_operations : [ZT.UpdateFieldSetOperations],
            operation_handler : (Iter<Candid>) -> Result<Candid, Text>,
        ) : Result<Candid, Text> {
            let candid_values = Array.init<Candid>(nested_operations.size(), #Null);

            for ((i, nested_op) in Itertools.enumerate(nested_operations.vals())) {

                switch (handle_update_field_set_operation(candid_map, nested_op)) {
                    case (#ok(candid_value)) candid_values[i] := candid_value;
                    case (#err(msg)) return #err(debug_show (nested_operations) # " failed: " # msg);
                };

            };

            operation_handler(candid_values.vals());
        };

        let new_value = switch (op) {
            case (#get(field_name)) {
                let ?value = candid_map.get(field_name) else return #err("Field '" # field_name # "' not found in record");
                #ok(value);
            };
            case (#add(nested_operations)) {
                handle_nested_operations(nested_operations, CandidMod.Multi.add);
            };
            case (#sub(nested_operations)) {
                handle_nested_operations(nested_operations, CandidMod.Multi.sub);
            };
            case (#mul(nested_operations)) {
                handle_nested_operations(nested_operations, CandidMod.Multi.mul);

            };
            case (#div(nested_operations)) {
                handle_nested_operations(nested_operations, CandidMod.Multi.div);

            };
            case (#val(candid)) { #ok(candid) };
        };

        new_value;
    };

    func partially_update_doc(candid_map : CandidMap.CandidMap, update_operations : [(Text, ZT.UpdateFieldOperations)]) : Result<Candid, Text> {

        for ((field_name, op) in update_operations.vals()) {
            //    Debug.print("field_name: " # field_name);

            let ?candid_type = candid_map.get_type(field_name) else Debug.trap("Field type'" # field_name # "' not found in record");
            let ?prev_candid = candid_map.get(field_name) else Debug.trap("Field '" # field_name # "' not found in record");

            let res = switch (op) {
                case (#set(nested_operation)) {
                    switch (nested_operation) {
                        case (#val(candid)) { #ok(candid) };
                        case (nested_operation) {
                            let resolved_value = switch (handle_update_field_set_operation(candid_map, nested_operation)) {
                                case (#ok(val)) val;
                                case (#err(msg)) return #err(msg);
                            };

                            //    Debug.print("cast: " # debug_show (candid_type) # " -> " # debug_show (resolved_value));
                            CandidMod.cast(candid_type, resolved_value);
                        };
                    };
                };
                case (#add(candid_value)) {
                    CandidMod.add(prev_candid, candid_value);
                };
                case (#sub(candid_value)) {
                    CandidMod.sub(prev_candid, candid_value);
                };
                case (#mul(candid_value)) {
                    CandidMod.mul(prev_candid, candid_value);
                };
                case (#div(candid_value)) {
                    CandidMod.div(prev_candid, candid_value);
                };
            };

            let new_value : Candid = switch (res) {
                case (#ok(new_value)) new_value;
                case (#err(msg)) return #err(msg);
            };

            //    Debug.print("new_value for field: " # field_name # " -> " # debug_show new_value);

            switch (candid_map.set(field_name, new_value)) {
                case (#err(err)) return #err("Failed to update field '" # field_name # "': " # err);
                case (#ok(_)) {};
            };

            //    Debug.print("updated field: " # field_name # " -> " # debug_show candid_map.get(field_name));

        };

        let candid = candid_map.extract_candid();

        #ok(candid);
    };

    func update_indexed_doc_data(collection : StableCollection, index : Index, id : Nat, prev_record_candid_map : CandidMap.CandidMap, new_record_candid_map : CandidMap.CandidMap) : Result<(), Text> {

        let prev_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, prev_record_candid_map);
        let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);

        ignore MemoryBTree.remove(index.data, index_data_utils, prev_index_key_values);

        let new_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, new_record_candid_map);
        ignore MemoryBTree.insert(index.data, index_data_utils, new_index_key_values, id);

        #ok;
    };

    func update_indexed_data_on_updated_fields(collection : StableCollection, id : Nat, prev_record_candid_map : CandidMap.CandidMap, new_record_candid_map : CandidMap.CandidMap, updated_fields : [Text]) : Result<(), Text> {

        let updated_fields_set = Set.fromIter(updated_fields.vals(), thash);

        for (index in Map.vals(collection.indexes)) {
            for ((index_key, _) in index.key_details.vals()) {
                if (Set.has(updated_fields_set, thash, index_key)) {
                    let #ok(_) = update_indexed_doc_data(collection, index, id, prev_record_candid_map, new_record_candid_map) else return #err("Failed to update index data");
                };
            };
        };

        #ok;
    };

    public func update_by_id<Record>(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, id : Nat, update_operations : ZT.InternalUpdateOperations) : Result<(), Text> {
        //    Debug.print("retrieving record for id: " # debug_show id);

        let ?prev_candid_blob = MemoryBTree.get(collection.main, main_btree_utils, id) else return #err("Record for id '" # debug_show (id) # "' not found");
        //    Debug.print("retrieved prev_candid_blob");
        let prev_candid = CollectionUtils.decode_candid_blob(collection, prev_candid_blob);
        //    Debug.print("decoded prev_candid_blob");
        let prev_candid_map = CandidMap.CandidMap(collection.schema, prev_candid);
        //    Debug.print("created prev_candid_map");

        //    Debug.print(debug_show ({ prev_candid_blob; prev_candid }));

        switch (update_operations) {
            case (#doc(new_candid_blob)) {
                let new_candid = CollectionUtils.decode_candid_blob(collection, new_candid_blob);
                let new_candid_map = CandidMap.CandidMap(collection.schema, new_candid);

                //    Debug.print(debug_show ({ new_candid_blob; new_candid }));

                Utils.assert_result(Schema.validate_record(collection.schema, new_candid));

                assert ?prev_candid_blob == MemoryBTree.insert(collection.main, main_btree_utils, id, new_candid_blob);

                for (index in Map.vals(collection.indexes)) {
                    let #ok(_) = update_indexed_doc_data(collection, index, id, prev_candid_map, new_candid_map) else return #err("Failed to update index data");
                };

            };
            case (#ops(field_updates)) {
                let new_candid_map = prev_candid_map.clone();

                let new_candid_record = switch (partially_update_doc(new_candid_map, field_updates)) {
                    case (#ok(new_candid_record)) new_candid_record;
                    case (#err(msg)) return #err(msg);
                };

                // should validated the updated fields instead of the entire record
                Utils.assert_result(Schema.validate_record(collection.schema, new_candid_record));

                let #ok(new_candid_blob) = Candid.encodeOne(
                    new_candid_record,
                    ?{
                        Candid.defaultOptions with types = ?[collection.formatted_schema];
                    },
                );

                assert ?prev_candid_blob == MemoryBTree.insert(collection.main, main_btree_utils, id, new_candid_blob);

                let updated_keys = Array.map<(Text, Any), Text>(
                    field_updates,
                    func(field_name : Text, _ : Any) : Text { field_name },
                );

                let #ok(_) = update_indexed_data_on_updated_fields(collection, id, prev_candid_map, new_candid_map, updated_keys) else return #err("Failed to update index data");

            };
        };

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

    public func keys(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>) : Iter<Nat> {
        MemoryBTree.keys(collection.main, main_btree_utils);
    };

    public func rangeKeys(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, start : Nat, end : Nat) : Iter<Nat> {
        MemoryBTree.rangeKeys(collection.main, main_btree_utils, start, end);
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

        let ?prev_candid_blob = MemoryBTree.remove(collection.main, main_btree_utils, id);
        let prev_candid = CollectionUtils.decode_candid_blob(collection, prev_candid_blob);
        let prev_candid_map = CandidMap.CandidMap(collection.schema, prev_candid);

        let #Record(prev_records) = prev_candid else return #err("Couldn't get records");
        // Debug.print("prev_records: " # debug_show prev_records);
        for (index in Map.vals(collection.indexes)) {

            let prev_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, prev_candid_map);
            let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);

            assert ?id == MemoryBTree.remove(index.data, index_data_utils, prev_index_key_values);
        };

        let candid_blob = prev_candid_blob;

        Ids.Gen.release(collection.ids, id);

        #ok(candid_blob);
    };

};
