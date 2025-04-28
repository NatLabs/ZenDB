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

import T "../Types";
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
import Logger "../Logger";
import UpdateOps "UpdateOps";

module StableCollection {

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

    public type RecordId = T.RecordId;
    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type InternalCandify<A> = T.InternalCandify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;
    type EvalResult = T.EvalResult;

    // public func new(db: ZenDB.StableDatabase) : StableCollection {

    //     let #Record(_) = processed_schema else return log_error_msg(db.logger, "Schema error: schema type is not a record");

    //     let schema_keys = Utils.extract_schema_keys(processed_schema);

    //     let stable_collection = {
    //         ids = Ids.create(db.id_store, name);
    //         var schema = processed_schema;
    //         schema_keys;
    //         schema_keys_set = Set.fromIter(schema_keys.vals(), thash);
    //         main = MemoryBTree.new(?C.DEFAULT_BTREE_ORDER);
    //         indexes = Map.new<Text, T.Index>();

    //         // db references
    //         freed_btrees = db.freed_btrees;
    //         logger = db.logger;
    //     };

    // };

    public func size(collection : StableCollection) : Nat {
        MemoryBTree.size(collection.main);
    };

    // BTree methods

    public func entries(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>) : Iter<(Nat, Blob)> {
        MemoryBTree.entries(collection.main, main_btree_utils);
    };

    public func keys(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>) : Iter<Nat> {
        MemoryBTree.keys(collection.main, main_btree_utils);
    };

    public func vals(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>) : Iter<Blob> {
        MemoryBTree.vals(collection.main, main_btree_utils);
    };

    public func range(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, start : Nat, end : Nat) : Iter<(Nat, Blob)> {
        MemoryBTree.range(collection.main, main_btree_utils, start, end);
    };

    public func rangeKeys(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, start : Nat, end : Nat) : Iter<Nat> {
        MemoryBTree.rangeKeys(collection.main, main_btree_utils, start, end);
    };

    public func rangeVals(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, start : Nat, end : Nat) : Iter<Blob> {
        MemoryBTree.rangeVals(collection.main, main_btree_utils, start, end);
    };

    // public func update_schema(collection : StableCollection, schema : T.Schema) : Result<(), Text> {

    //     let is_compatible = Schema.is_schema_backward_compatible(collection.schema, schema);
    //     if (not is_compatible) return Utils.log_error_msg(collection.logger, "Schema is not backward compatible");

    //     let formatted_schema = Candid.formatCandidType([schema], null)[0];

    //     collection.schema := formatted_schema;
    //     Logger.lazyInfo(
    //         collection.logger,
    //         func() = "Updating schema to: " # debug_show formatted_schema,
    //     );
    //     #ok;
    // };

    func insert_into_index(
        collection : StableCollection,
        index : Index,
        id : Nat,
        candid_map : CandidMap.CandidMap,
    ) : Result<(), Text> {

        let index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, candid_map);

        let index_data_utils = CollectionUtils.get_index_data_utils();
        ignore MemoryBTree.insert(index.data, index_data_utils, index_key_values, id);

        Logger.lazyLog(
            collection.logger,
            func() = "Storing record with id " # debug_show id # " in index " # index.name # ", originally "
            # debug_show (index_key_values) # ", now encoded as " # debug_show (index_data_utils.key.blobify.to_blob(index_key_values)),
        );

        #ok();
    };

    public func internal_create_index(
        collection : StableCollection,
        index_name : Text,
        index_key_details : [(Text, SortDirection)],
        is_unique : Bool,
        used_internally : Bool,
    ) : Result<T.Index, Text> {

        switch (Map.get(collection.indexes, thash, index_name)) {
            case (?index) {
                Logger.lazyInfo(
                    collection.logger,
                    func() = "Index '" # index_name # "' already exists",
                );
                return #ok(index);
            };
            case (null) {};
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "Creating index '" # index_name # "' with key details: " # debug_show index_key_details,
        );

        let opt_recycled_btree = Vector.removeLast(collection.freed_btrees);

        let index = Index.new(index_name, index_key_details, is_unique, used_internally, opt_recycled_btree);

        ignore Map.put<Text, Index>(collection.indexes, thash, index_name, index);
        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully created index: " # index_name,
        );

        #ok(index);
    };

    public func create_index(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        index_name : Text,
        _index_key_details : [(Text, SortDirection)],
        is_unique : Bool,
    ) : Result<(T.Index), Text> {

        StableCollection.internal_create_index(collection, index_name, _index_key_details, is_unique, false);
    };

    public func create_and_populate_index(
        collection : StableCollection,
        _main_btree_utils : BTreeUtils<Nat, Blob>,
        index_name : Text,
        index_key_details : [(Text, SortDirection)],
    ) : Result<(), Text> {

        //! todo: update the is_unique and used_internally flags
        switch (create_index(collection, _main_btree_utils, index_name, index_key_details, false)) {
            case (#err(err)) return #err(err);
            case (#ok(_)) {};
        };

        switch (populate_index(collection, _main_btree_utils, index_name)) {
            case (#err(err)) return #err(err);
            case (#ok(_)) {};
        };

        #ok();

    };

    public func clear_index(
        collection : StableCollection,
        _main_btree_utils : BTreeUtils<Nat, Blob>,
        index_name : Text,
    ) : Result<(), Text> {

        switch (Map.get(collection.indexes, thash, index_name)) {
            case (?index) MemoryBTree.clear(index.data);
            case (null) return Utils.log_error_msg(collection.logger, "Index not found");
        };

        #ok()

    };

    func internal_populate_indexes(
        collection : StableCollection,
        indexes : Buffer.Buffer<Index>,
        entries : Iter<(Nat, Blob)>,
    ) : Result<(), Text> {
        Logger.lazyInfo(
            collection.logger,
            func() = "Populating " # debug_show indexes.size() # " indexes",
        );

        var count = 0;
        for ((id, candid_blob) in entries) {
            let candid = CollectionUtils.decode_candid_blob(collection, candid_blob);
            let candid_map = CandidMap.CandidMap(collection.schema, candid);

            for (index in indexes.vals()) {
                switch (insert_into_index(collection, index, id, candid_map)) {
                    case (#err(err)) {
                        Logger.lazyError(
                            collection.logger,
                            func() = "Failed to insert into index '" # index.name # "': " # err,
                        );
                        return Utils.log_error_msg(collection.logger, err);
                    };
                    case (#ok(_)) {};
                };
            };
            count += 1;
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully populated indexes with " # debug_show count # " records",
        );
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
        index_names : [Text],
    ) : Result<(), Text> {

        Logger.lazyInfo(
            collection.logger,
            func() = "Starting to populate indexes: " # debug_show index_names,
        );
        let indexes = Buffer.Buffer<Index>(index_names.size());

        for (index_name in index_names.vals()) {
            let ?index = Map.get(collection.indexes, thash, index_name) else {
                Logger.lazyError(
                    collection.logger,
                    func() = "Index '" # index_name # "' does not exist",
                );
                return Utils.log_error_msg(collection.logger, "Index '" # index_name # "' does not exist");
            };

            indexes.add(index);
        };

        Logger.lazyLog(
            collection.logger,
            func() = "Collected " # debug_show indexes.size() # " indexes to populate",
        );

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

    //         let ?index = Map.get(collection.indexes, thash, index_name) else return Utils.log_error_msg(collection.logger, "Index with key_details '" # debug_show index_key_details # "' does not exist");

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
    //             case (#err(err)) return Utils.log_error_msg(collection.logger, err);
    //             case (#ok(_)) {};
    //         };

    //         size += BATCH_SIZE;

    //     };

    //     #ok()

    // };

    public func delete_index(
        collection : StableCollection,
        _main_btree_utils : BTreeUtils<Nat, Blob>,
        index_name : Text,
    ) : Result<(), Text> {
        Logger.info(collection.logger, "Deleting index: " # index_name);

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

        //         let ?index = Map.get(collection.indexes, thash, index_name) else return Utils.log_error_msg(collection.logger, "Index with key_details '" # debug_show index_key_details # "' does not exist");

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
        //             case (#err(err)) return Utils.log_error_msg(collection.logger, err);
        //             case (#ok(_)) {};
        //         };

        //         size += BATCH_SIZE;

        //     };

        //     #ok()

        // };
        let opt_index = Map.remove(collection.indexes, thash, index_name);

        switch (opt_index) {
            case (?index) {

                if (index.used_internally) {
                    return Utils.log_error_msg(collection.logger, "Index '" # index_name # "' cannot be deleted because it is used internally");
                };

                Logger.lazyLog(
                    collection.logger,
                    func() = "Clearing and recycling BTree for index: " # index_name,
                );
                MemoryBTree.clear(index.data);
                Vector.add(collection.freed_btrees, index.data);

                #ok();
            };
            case (null) {
                Logger.lazyError(
                    collection.logger,
                    func() = "Index not found: " # index_name,
                );
                return Utils.log_error_msg(collection.logger, "Index not found");
            };
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

    public func validate_schema_constraints_on_updated_fields(
        collection : StableCollection,
        record_id : Nat,
        candid_map : CandidMap.CandidMap,
        opt_updated_fields : ?[Text],
    ) : Result<(), Text> {

        let field_constraints_iter = switch (opt_updated_fields) {
            case (?updated_fields) {
                let buffer = Buffer.Buffer<(Text, [T.SchemaFieldConstraint])>(updated_fields.size());

                for (field_name in updated_fields.vals()) {

                    switch (Map.get(collection.field_constraints, thash, field_name)) {
                        case (?field_constraints) {
                            buffer.add((field_name, field_constraints));
                        };
                        case (null) {};
                    };

                };

                buffer.vals();
            };
            case (null) Map.entries(collection.field_constraints);
        };

        for ((field_name, field_constraints) in field_constraints_iter) {
            let ?field_value = candid_map.get(field_name) else return Utils.log_error_msg(collection.logger, "Field '" # field_name # "' not found in record");

            for (field_constraint in field_constraints.vals()) {
                switch (field_constraint) {
                    case (#Max(max_value)) {
                        switch (CandidMod.Ops.compare(field_value, #Float(max_value))) {
                            case (#greater) {
                                let error_msg = "Field '" # field_name # "' exceeds maximum value of " # debug_show max_value;
                                return Utils.log_error_msg(collection.logger, error_msg);
                            };
                            case (_) {};
                        };

                    };

                    case (#Min(min_value)) {
                        switch (CandidMod.Ops.compare(field_value, #Float(min_value))) {
                            case (#less) {
                                let error_msg = "Field '" # field_name # "' is less than minimum value of " # debug_show min_value;
                                return Utils.log_error_msg(collection.logger, error_msg);
                            };
                            case (_) {};
                        };

                    };

                    case (#MinSize(min_size)) {
                        let field_value_size = CandidMod.Ops.size(field_value);

                        switch (CandidMod.Ops.compare(#Nat(field_value_size), #Nat(min_size))) {
                            case (#less) {
                                let error_msg = "Field '" # field_name # "' is less than minimum size of " # debug_show min_size;
                                return Utils.log_error_msg(collection.logger, error_msg);
                            };
                            case (_) {};
                        };

                    };

                    case (#MaxSize(max_size)) {
                        let field_value_size = CandidMod.Ops.size(field_value);

                        switch (CandidMod.Ops.compare(#Nat(field_value_size), #Nat(max_size))) {
                            case (#greater) {
                                let error_msg = "Field '" # field_name # "' exceeds maximum size of " # debug_show max_size;
                                return Utils.log_error_msg(collection.logger, error_msg);
                            };
                            case (_) {};
                        };

                    };

                    case (#Size(min_size, max_size)) {
                        let field_value_size = CandidMod.Ops.size(field_value);

                        switch (CandidMod.Ops.compare(#Nat(field_value_size), #Nat(min_size))) {
                            case (#less) {
                                let error_msg = "Field '" # field_name # "' is less than minimum size of " # debug_show min_size;
                                return Utils.log_error_msg(collection.logger, error_msg);
                            };
                            case (_) {};
                        };

                        switch (CandidMod.Ops.compare(#Nat(field_value_size), #Nat(max_size))) {
                            case (#greater) {
                                let error_msg = "Field '" # field_name # "' exceeds maximum size of " # debug_show max_size;
                                return Utils.log_error_msg(collection.logger, error_msg);
                            };
                            case (_) {};
                        };

                    };

                };
            }

        };

        let unique_constraints_iter = switch (opt_updated_fields) {
            case (?updated_fields) {
                let new_unique_constraints_indexes = Set.new<Nat>();

                for (field_name in updated_fields.vals()) {
                    switch (Map.get(collection.fields_with_unique_constraints, thash, field_name)) {
                        case (?unique_constraints_indexes_set) {
                            for (unique_constraint_index in Set.keys(unique_constraints_indexes_set)) {
                                Set.add(new_unique_constraints_indexes, nhash, unique_constraint_index);
                            };
                        };
                        case (null) {};
                    };
                };

                Iter.map<Nat, ([Text], Index)>(
                    Set.keys(new_unique_constraints_indexes),
                    func(unique_constraint_index : Nat) : ([Text], Index) {
                        collection.unique_constraints[unique_constraint_index];
                    },
                );
            };
            case (null) collection.unique_constraints.vals();
        };

        for ((composite_field_keys, index) in unique_constraints_iter) {

            let compsite_field_values = CollectionUtils.get_index_columns(collection, index.key_details, record_id, candid_map);
            let index_data_utils = CollectionUtils.get_index_data_utils();

            let opt_prev_id = MemoryBTree.get(index.data, index_data_utils, compsite_field_values);

            switch (opt_prev_id) {
                case (null) {}; // no previous value, free to insert
                case (?prev_id) {

                    if (prev_id != record_id) {
                        let error_msg = "Unique constraint violation: Inserting new record failed because unique constraint on " # debug_show composite_field_keys # " is violated because record with id " # debug_show prev_id # " already has composite values " # debug_show compsite_field_values # " the same as the new record about to be inserted";
                        Logger.error(
                            collection.logger,
                            error_msg,
                        );
                        return Utils.log_error_msg(collection.logger, error_msg);
                    };

                }

            };

        };

        #ok()

    };

    public func insert_with_id(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, id : Nat, candid_blob : T.CandidBlob) : Result<(), Text> {
        put_with_id(collection, main_btree_utils, id, candid_blob);
    };

    public func put_with_id(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        id : Nat,
        candid_blob : T.CandidBlob,
    ) : Result<(), Text> {

        Logger.lazyInfo(
            collection.logger,
            func() = "ZenDB Collection.put_with_id(): Inserting record with id " # debug_show id,
        );

        let candid = CollectionUtils.decode_candid_blob(collection, candid_blob);

        switch (candid) {
            case (#Record(_)) {};
            case (_) return Utils.log_error_msg(collection.logger, "Values inserted into the collection must be #Records");
        };

        Logger.lazyLog(
            collection.logger,
            func() = "ZenDB Collection.put_with_id(): Inserting record with id " # debug_show id # " and candid " # debug_show candid,
        );

        switch (Schema.validate_record(collection.schema, candid)) {
            case (#ok(_)) {};
            case (#err(msg)) {
                let err_msg = "Schema validation failed: " # msg;

                Logger.lazyError(
                    collection.logger,
                    func() = err_msg,
                );
                return Utils.log_error_msg(collection.logger, err_msg);
            };
        };

        let candid_map = CandidMap.CandidMap(collection.schema, candid);

        switch (validate_schema_constraints_on_updated_fields(collection, id, candid_map, null)) {
            case (#ok(_)) {};
            case (#err(msg)) {
                let err_msg = "Schema Constraint validation failed: " # msg;
                return Utils.log_error_msg(collection.logger, err_msg);
            };
        };

        let opt_prev = MemoryBTree.insert(collection.main, main_btree_utils, id, candid_blob);

        switch (opt_prev) {
            case (null) {};
            case (?prev) {
                ignore MemoryBTree.insert(collection.main, main_btree_utils, id, prev);
                let error_msg = "Record with id " # debug_show id # " already exists";
                Logger.error(
                    collection.logger,
                    error_msg,
                );
                return Utils.log_error_msg(collection.logger, error_msg);
            };
        };

        if (Map.size(collection.indexes) == 0) return #ok();

        for (index in Map.vals(collection.indexes)) {
            let #ok(_) = update_indexed_record_fields(collection, index, id, candid_map, null);
        };

        #ok();
    };

    public func replace_record_by_id<Record>(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        id : Nat,
        new_candid_blob : T.CandidBlob,
    ) : Result<(), Text> {
        Logger.info(collection.logger, "Replacing record with id: " # debug_show id);

        let ?prev_candid_blob = MemoryBTree.get(collection.main, main_btree_utils, id) else return Utils.log_error_msg(collection.logger, "Record for id '" # debug_show (id) # "' not found");
        let prev_candid = CollectionUtils.decode_candid_blob(collection, prev_candid_blob);
        let prev_candid_map = CandidMap.CandidMap(collection.schema, prev_candid);

        let new_candid_value = CollectionUtils.decode_candid_blob(collection, new_candid_blob);
        let new_candid_map = CandidMap.CandidMap(collection.schema, new_candid_value);

        switch (Schema.validate_record(collection.schema, new_candid_value)) {
            case (#err(msg)) {
                return Utils.log_error_msg(collection.logger, "Schema validation failed: " # msg);
            };
            case (#ok(_)) {};
        };

        switch (validate_schema_constraints_on_updated_fields(collection, id, new_candid_map, null)) {
            case (#ok(_)) {};
            case (#err(msg)) {
                let err_msg = "Schema Constraint validation failed: " # msg;
                return Utils.log_error_msg(collection.logger, err_msg);
            };
        };

        assert ?prev_candid_blob == MemoryBTree.insert(collection.main, main_btree_utils, id, new_candid_blob);

        for (index in Map.vals(collection.indexes)) {
            let #ok(_) = update_indexed_record_fields(collection, index, id, new_candid_map, ?prev_candid_map) else {
                return Utils.log_error_msg(collection.logger, "Failed to update index data");
            };
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully replaced record with id: " # debug_show id,
        );
        #ok();
    };

    func partially_update_doc(collection : StableCollection, candid_map : CandidMap.CandidMap, update_operations : [(Text, T.FieldUpdateOperations)]) : Result<Candid, Text> {
        Debug.print("Partially updating doc with operations: " # debug_show update_operations);

        for ((field_name, op) in update_operations.vals()) {
            let ?field_type = candid_map.get_type(field_name) else return Utils.log_error_msg(collection.logger, "Field type '" # field_name # "' not found in record");
            let ?prev_candid = candid_map.get(field_name) else return Utils.log_error_msg(collection.logger, "Field '" # field_name # "' not found in record");

            let new_value = switch (UpdateOps.handle_field_update_operation(collection, candid_map, field_type, prev_candid, op)) {
                case (#ok(new_value)) new_value;
                case (#err(msg)) {
                    return Utils.log_error_msg(
                        collection.logger,
                        "Failed to update field '" # field_name # "' with operation '" # debug_show op # "': " # msg,
                    );
                };
            };

            switch (candid_map.set(field_name, new_value)) {
                case (#err(err)) return Utils.log_error_msg(collection.logger, "Failed to update field '" # field_name # "' with new value (" # debug_show new_value # "): " # err);
                case (#ok(_)) {};
            };
        };

        Debug.print("Updated candid map: ");
        Debug.print("About to extract candid");

        let candid = candid_map.extract_candid();

        #ok(candid);
    };

    func update_indexed_record_fields(collection : StableCollection, index : Index, id : Nat, new_record_candid_map : CandidMap.CandidMap, opt_prev_record_candid_map : ?CandidMap.CandidMap) : Result<(), Text> {

        let index_data_utils = CollectionUtils.get_index_data_utils();

        ignore do ? {
            let prev_record_candid_map = opt_prev_record_candid_map!;
            let prev_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, prev_record_candid_map);
            let ?prev_id = MemoryBTree.remove(index.data, index_data_utils, prev_index_key_values) else {
                return #err("Record with id " # debug_show id # " that is about to be updates does not exist in the index " # index.name);
            };

        };

        let new_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, new_record_candid_map);
        Debug.print("id: " # debug_show id);
        Debug.print("index.key_details: " # debug_show index.key_details);
        Debug.print("new_index_key_values: " # debug_show new_index_key_values);
        let opt_existing_id = MemoryBTree.insert(index.data, index_data_utils, new_index_key_values, id);
        switch (opt_existing_id) {
            case (null) {};
            case (?existing_id) {
                // if (existing_id != id) {
                return #err("New record with id " # debug_show id # " conflicts with existing id " # debug_show existing_id # " which already exists in the index " # index.name # " with the same index key values " # debug_show new_index_key_values);
                // };
            };
        };

        Logger.lazyLog(
            collection.logger,
            func() = "Storing record with id " # debug_show id # " in index " # index.name # ", originally "
            # debug_show (new_index_key_values) # ", now encoded as  as " # debug_show (index_data_utils.key.blobify.to_blob(new_index_key_values)),
        );

        #ok;
    };

    func update_indexed_data_on_updated_fields(collection : StableCollection, id : Nat, prev_record_candid_map : CandidMap.CandidMap, new_record_candid_map : CandidMap.CandidMap, updated_fields : [Text]) : Result<(), Text> {

        let updated_fields_set = Set.fromIter(updated_fields.vals(), thash);

        for (index in Map.vals(collection.indexes)) {
            for ((index_key, _) in index.key_details.vals()) {

                // only updates the fields that were changed
                if (Set.has(updated_fields_set, thash, index_key)) {
                    let #ok(_) = update_indexed_record_fields(collection, index, id, new_record_candid_map, ?prev_record_candid_map) else return Utils.log_error_msg(collection.logger, "Failed to update index data");
                };
            };
        };

        #ok;
    };

    public func update_by_id<Record>(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, id : Nat, field_updates : [(Text, T.FieldUpdateOperations)]) : Result<(), Text> {
        Logger.lazyInfo(
            collection.logger,
            func() = "Updating record with id: " # debug_show id,
        );

        let ?prev_candid_blob = MemoryBTree.get(collection.main, main_btree_utils, id) else return Utils.log_error_msg(collection.logger, "Record for id '" # debug_show (id) # "' not found");

        let prev_candid = CollectionUtils.decode_candid_blob(collection, prev_candid_blob);
        let prev_candid_map = CandidMap.CandidMap(collection.schema, prev_candid);

        let fields_with_updates = Array.map<(Text, T.FieldUpdateOperations), Text>(field_updates, func(k, _) = k);

        Logger.lazyLog(
            collection.logger,
            func() = "Performing partial update on fields: " # debug_show (fields_with_updates),
        );

        let new_candid_map = prev_candid_map.clone();

        let new_candid_record = switch (partially_update_doc(collection, new_candid_map, field_updates)) {
            case (#ok(new_candid_record)) new_candid_record;
            case (#err(msg)) {
                return Utils.log_error_msg(collection.logger, "Failed to update fields: " # msg);
            };
        };

        Debug.print("Updated candid map: " # debug_show new_candid_record);

        switch (Schema.validate_record(collection.schema, new_candid_record)) {
            case (#err(msg)) {
                return Utils.log_error_msg(collection.logger, "Schema validation failed: " # msg);
            };
            case (#ok(_)) {};
        };

        switch (validate_schema_constraints_on_updated_fields(collection, id, new_candid_map, ?fields_with_updates)) {
            case (#ok(_)) {};
            case (#err(msg)) {
                let err_msg = "Schema Constraint validation failed: " # msg;
                return Utils.log_error_msg(collection.logger, err_msg);
            };
        };

        let new_candid_blob = switch (Candid.encodeOne(new_candid_record, ?{ Candid.defaultOptions with types = ?[collection.schema] })) {
            case (#ok(new_candid_blob)) new_candid_blob;
            case (#err(msg)) {
                return Utils.log_error_msg(collection.logger, "Failed to encode new candid blob: " # msg);
            };
        };

        assert ?prev_candid_blob == MemoryBTree.insert(collection.main, main_btree_utils, id, new_candid_blob);

        let updated_keys = Array.map<(Text, Any), Text>(
            field_updates,
            func(field_name : Text, _ : Any) : Text { field_name },
        );

        let #ok(_) = update_indexed_data_on_updated_fields(collection, id, prev_candid_map, new_candid_map, updated_keys) else {
            return Utils.log_error_msg(collection.logger, "Failed to update index data");
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully updated record with id: " # debug_show id,
        );
        #ok();
    };

    public func insert(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, candid_blob : T.CandidBlob) : Result<Nat, Text> {
        put(collection, main_btree_utils, candid_blob);
    };

    public func put(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, candid_blob : T.CandidBlob) : Result<Nat, Text> {
        let id = Ids.Gen.next(collection.ids);

        switch (put_with_id(collection, main_btree_utils, id, candid_blob)) {
            case (#err(msg)) return Utils.log_error_msg(collection.logger, msg);
            case (#ok(_)) {};
        };

        #ok(id);
    };

    public func get(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        id : Nat,
    ) : Result<T.CandidBlob, Text> {
        let ?record_details = MemoryBTree.get(collection.main, main_btree_utils, id) else return Utils.log_error_msg(collection.logger, "Record not found");
        #ok(record_details);
    };

    public func search(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        query_builder : QueryBuilder,
    ) : Result<[(T.WrapId<T.CandidBlob>)], Text> {
        Logger.lazyLog(
            collection.logger,
            func() = "Executing search with query: " # debug_show (query_builder.build()),
        );

        switch (internal_search(collection, query_builder)) {
            case (#err(err)) {
                return Utils.log_error_msg(collection.logger, "Search failed: " # err);
            };
            case (#ok(record_ids_iter)) {
                let candid_blob_iter = id_to_candid_blob_iter(collection, record_ids_iter);
                let candid_blobs = Iter.toArray(candid_blob_iter);
                Logger.lazyLog(
                    collection.logger,
                    func() = "Search completed, found " # debug_show (candid_blobs.size()) # " results",
                );
                #ok(candid_blobs);
            };
        };
    };

    public func evaluate_query(collection : StableCollection, stable_query : T.StableQuery) : Result<Iter<Nat>, Text> {
        Logger.lazyLog(
            collection.logger,
            func() = "Evaluating query with operations: " # debug_show (stable_query.query_operations),
        );

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
            case (#err(err)) {
                return Utils.log_error_msg(collection.logger, "Invalid Query: " # err);
            };
            case (#ok(_)) ();
        };

        let #ok(formatted_query_operations) = Query.process_query(collection, query_operations) else {
            return Utils.log_error_msg(collection.logger, "Failed to process query operations");
        };

        let query_plan : T.QueryPlan = QueryPlan.create_query_plan(
            collection,
            formatted_query_operations,
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

        Logger.lazyLog(
            collection.logger,
            func() = "Query evaluation completed",
        );
        return #ok((iter));
    };

    public func internal_search(collection : StableCollection, query_builder : QueryBuilder) : Result<Iter<Nat>, Text> {
        let stable_query = query_builder.build();
        switch (evaluate_query(collection, stable_query)) {
            case (#err(err)) return Utils.log_error_msg(collection.logger, err);
            case (#ok(eval_result)) #ok(eval_result);
        };
    };

    public func id_to_record_iter<Record>(collection : StableCollection, blobify : InternalCandify<Record>, iter : Iter<Nat>) : Iter<(Nat, Record)> {
        Iter.map<Nat, (Nat, Record)>(
            iter,
            func(id : Nat) : (Nat, Record) {
                let record = CollectionUtils.lookup_record<Record>(collection, blobify, id);
                (id, record);
            },
        );
    };

    public func id_to_candid_blob_iter<Record>(collection : StableCollection, iter : Iter<Nat>) : Iter<(Nat, T.CandidBlob)> {
        Iter.map<Nat, (Nat, T.CandidBlob)>(
            iter,
            func(id : Nat) : (Nat, T.CandidBlob) {
                let candid_blob = CollectionUtils.lookup_candid_blob(collection, id);
                (id, candid_blob);
            },
        );
    };

    public func search_iter(
        collection : StableCollection,
        main_btree_utils : BTreeUtils<Nat, Blob>,
        query_builder : QueryBuilder,
    ) : Result<Iter<T.WrapId<T.CandidBlob>>, Text> {
        switch (internal_search(collection, query_builder)) {
            case (#err(err)) return Utils.log_error_msg(collection.logger, err);
            case (#ok(record_ids_iter)) {
                let record_iter = id_to_candid_blob_iter(collection, record_ids_iter);
                #ok(record_iter);
            };
        };
    };

    public func get_sort_records_by_field_cmp(
        collection : StableCollection,
        sort_field : (Text, T.SortDirection),
    ) : (Nat, Nat) -> Order {

        let deserialized_records_map = Map.new<Nat, Candid.Candid>();

        func get_candid_map_bytes(id : Nat) : Candid.Candid {
            switch (Map.get(deserialized_records_map, nhash, id)) {
                case (?candid) candid;
                case (null) {
                    let ?candid = CollectionUtils.lookup_candid_record(collection, id) else Debug.trap("Couldn't find record with id: " # debug_show id);
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

    public func stats(collection : StableCollection) : T.CollectionStats {
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

    public func delete_by_id(collection : StableCollection, main_btree_utils : BTreeUtils<Nat, Blob>, id : Nat) : Result<(T.CandidBlob), Text> {
        Logger.lazyInfo(
            collection.logger,
            func() = "Deleting record with id: " # debug_show id,
        );

        let ?prev_candid_blob = MemoryBTree.remove(collection.main, main_btree_utils, id) else {
            return Utils.log_error_msg(collection.logger, "Record not found");
        };

        let prev_candid = CollectionUtils.decode_candid_blob(collection, prev_candid_blob);
        let prev_candid_map = CandidMap.CandidMap(collection.schema, prev_candid);

        let #Record(prev_records) = prev_candid else {
            return Utils.log_error_msg(collection.logger, "Couldn't get records");
        };

        for (index in Map.vals(collection.indexes)) {
            let prev_index_key_values = CollectionUtils.get_index_columns(collection, index.key_details, id, prev_candid_map);
            let index_data_utils = CollectionUtils.get_index_data_utils();

            let removed_id = MemoryBTree.remove(index.data, index_data_utils, prev_index_key_values);
            if (removed_id != ?id) {
                Logger.lazyError(
                    collection.logger,
                    func() = "Failed to remove id " # debug_show id # " from index " # index.name,
                );
            };
        };

        let candid_blob = prev_candid_blob;
        Ids.Gen.release(collection.ids, id);

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully deleted record with id: " # debug_show id,
        );
        #ok(candid_blob);
    };

};
