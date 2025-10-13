import Prim "mo:prim";

import Principal "mo:base@0.16.0/Principal";
import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Nat32 "mo:base@0.16.0/Nat32";
import Result "mo:base@0.16.0/Result";
import Order "mo:base@0.16.0/Order";
import Iter "mo:base@0.16.0/Iter";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Option "mo:base@0.16.0/Option";
import Hash "mo:base@0.16.0/Hash";
import Float "mo:base@0.16.0/Float";
import Int "mo:base@0.16.0/Int";
import Int32 "mo:base@0.16.0/Int32";
import Blob "mo:base@0.16.0/Blob";
import Nat64 "mo:base@0.16.0/Nat64";
import Int16 "mo:base@0.16.0/Int16";
import Int64 "mo:base@0.16.0/Int64";
import Int8 "mo:base@0.16.0/Int8";
import Nat16 "mo:base@0.16.0/Nat16";
import Nat8 "mo:base@0.16.0/Nat8";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.3.3";
import Decoder "mo:serde@3.3.3/Candid/Blob/Decoder";
import Candid "mo:serde@3.3.3/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import BitMap "mo:bit-map@0.1.2";
import Vector "mo:vector@0.4.2";
import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import ByteUtils "mo:byte-utils@0.1.1";
import Ids "../Ids";

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import SchemaMap "SchemaMap";

import Index "Index";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "Utils";
import QueryPlan "QueryPlan";
import C "../Constants";
import QueryExecution "QueryExecution";
import Intervals "Intervals";
import CandidUtils "../CandidUtils";
import Logger "../Logger";
import UpdateOps "UpdateOps";
import BTree "../BTree";
import DocumentStore "DocumentStore";

module StableCollection {

    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; nhash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    type QueryBuilder = Query.QueryBuilder;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type DocumentId = T.DocumentId;
    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type InternalCandify<A> = T.InternalCandify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;
    type EvalResult = T.EvalResult;

    // public func new(
    //     db : ZenDB.StableDatabase,
    //     name : Text,
    //     memory_type : ZenDB.MemoryType,
    //     processed_schema : T.Schema,
    // ) : StableCollection {

    //     let schema_keys = Utils.get_schema_keys(processed_schema);

    //     var stable_collection : T.StableCollection = {
    //         ids = db.ids;
    //         name;
    //         schema = processed_schema;
    //         schema_map = SchemaMap.new(processed_schema);
    //         schema_keys;
    //         schema_keys_set = Set.fromIter(schema_keys.vals(), thash);
    //         documents = switch (db.memory_type) {
    //             case (#heap) { BTree.newHeap() };
    //             case (#stableMemory) {
    //                 switch (Vector.removeLast(db.freed_btrees)) {
    //                     case (?memory_btree) {
    //                         #stableMemory(memory_btree);
    //                     };
    //                     case (null) {
    //                         BTree.newStableMemory();
    //                     };
    //                 };
    //             };
    //         };

    //         indexes = Map.new<Text, T.Index>();

    //         field_constraints;
    //         unique_constraints = [];
    //         fields_with_unique_constraints = Map.new();

    //         // db references
    //         freed_btrees = db.freed_btrees;
    //         logger = db.logger;
    //         memory_type = db.memory_type;
    //     };

    // };

    public func size(collection : StableCollection) : Nat {
        DocumentStore.size(collection.documents);
    };

    /// Clear all the data in the collection.
    public func clear(collection : StableCollection) : () {
        DocumentStore.clear(collection.documents);

        for (index in Map.vals(collection.indexes)) {
            BTree.clear(index.data);
        };
    };

    // BTree methods

    public func entries(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>) : Iter<(T.DocumentId, Blob)> {
        DocumentStore.entries(collection.documents, main_btree_utils);
    };

    public func keys(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>) : Iter<T.DocumentId> {
        DocumentStore.keys(collection.documents, main_btree_utils);
    };

    public func vals(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>) : Iter<Blob> {
        DocumentStore.vals(collection.documents, main_btree_utils);
    };

    public func range(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : Iter<(T.DocumentId, Blob)> {
        DocumentStore.range(collection.documents, main_btree_utils, start, end);
    };

    public func range_keys(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : Iter<T.DocumentId> {
        DocumentStore.range_keys(collection.documents, main_btree_utils, start, end);
    };

    public func range_vals(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : Iter<Blob> {
        DocumentStore.range_vals(collection.documents, main_btree_utils, start, end);
    };

    // public func update_schema<NewRecord>(collection : StableCollection, schema : T.Schema) : Result<(), Text> {
    //     type PrevRecord = Record;

    //     let is_compatible = Schema.is_schema_backward_compatible(collection.schema, schema);
    //     if (not is_compatible) return Utils.log_error_msg(collection.logger, "Schema is not backward compatible");

    //     let processed_schema = Schema.process_schema(schema);
    //     let schema_keys = Utils.get_schema_keys(processed_schema);

    //     collection.schema := processed_schema;
    //     collection.schema_keys := schema_keys;

    //     let default_value_with_prev_schema = Schema.generate_default_value(collection.schema);

    //     Logger.lazyInfo(
    //         collection.logger,
    //         func() = "Updating schema to: " # debug_show processed_schema,
    //     );
    //     #ok;
    // };

    public func create_index_internal(
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

        let index = Index.new(collection, index_name, index_key_details, is_unique, used_internally);

        ignore Map.put<Text, Index>(collection.indexes, thash, index_name, index);
        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully created index: " # index_name,
        );

        #ok(index);
    };

    public func create_index(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_name : Text,
        _index_key_details : [(Text, SortDirection)],
        is_unique : Bool,
    ) : Result<(T.Index), Text> {

        let index_creation_response = StableCollection.create_index_internal(collection, index_name, _index_key_details, is_unique, false);

        let index = switch (index_creation_response) {
            case (#ok(index)) index;
            case (#err(err_msg)) return #err(err_msg);
        };

        switch (Index.populate_index(collection, index)) {
            case (#ok(_)) {};
            case (#err(err_msg)) return #err("Failed to create index '" # index_name # "': " # err_msg);
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully created and populated index: " # index_name,
        );

        #ok(index)

    };

    public func clear_index(
        collection : StableCollection,
        _main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_name : Text,
    ) : Result<(), Text> {

        switch (Map.get(collection.indexes, thash, index_name)) {
            case (?index) BTree.clear(index.data);
            case (null) return #err("Index not found");
        };

        #ok()

    };

    func internal_populate_indexes(
        collection : StableCollection,
        indexes : Buffer.Buffer<Index>,
        entries : Iter<(T.DocumentId, Blob)>,
    ) : Result<(), Text> {
        Logger.lazyInfo(
            collection.logger,
            func() = "Populating " # debug_show indexes.size() # " indexes",
        );

        var count = 0;
        for ((id, candid_blob) in entries) {
            let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);
            let candid_map = CandidMap.new(collection.schema_map, id, candid);

            for (index in indexes.vals()) {
                switch (Index.insert(collection, index, id, candid_map)) {
                    case (#err(err)) {
                        return #err("Failed to insert into index '" # index.name # "': " # err);
                    };
                    case (#ok(_)) {};
                };
            };
            count += 1;
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully populated indexes with " # debug_show count # " documents",
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

    public func repopulate_index(
        collection : StableCollection,
        _main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_name : Text,
    ) : Result<(), Text> {
        repopulate_indexes(collection, _main_btree_utils, [index_name]);
    };

    public func repopulate_indexes(
        collection : StableCollection,
        _main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_names : [Text],
    ) : Result<(), Text> {

        Logger.lazyInfo(
            collection.logger,
            func() = "Starting to populate indexes: " # debug_show index_names,
        );

        let indexes = Buffer.Buffer<Index>(index_names.size());

        for (index_name in index_names.vals()) {
            let ?index = Map.get(collection.indexes, thash, index_name) else {
                return #err("Index '" # index_name # "' does not exist");
            };

            indexes.add(index);
        };

        Logger.lazyDebug(
            collection.logger,
            func() = "Collected " # debug_show indexes.size() # " indexes to populate",
        );

        Index.repopulate_indexes(collection, Buffer.toArray(indexes));

    };

    public func delete_index(
        collection : StableCollection,
        _main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_name : Text,
    ) : Result<(), Text> {
        Logger.info(collection.logger, "Deleting index: " # index_name);

        let opt_index = Map.remove(collection.indexes, thash, index_name);

        switch (opt_index) {
            case (?index) {

                if (index.used_internally) {
                    return #err("Index '" # index_name # "' cannot be deleted because it is used internally");
                };

                Logger.lazyDebug(
                    collection.logger,
                    func() = "Clearing and recycling BTree for index: " # index_name,
                );
                BTree.clear(index.data);

                switch (index.data) {
                    case (#stableMemory(btree)) {
                        Vector.add(collection.freed_btrees, btree);
                    };
                    case (_) {};
                };

                #ok();
            };
            case (null) {
                return #err("Index not found");
            };
        };
    };

    let MAX_QUERY_INSTRUCTIONS : Nat64 = 5_000_000_000;
    let MAX_UPDATE_INSTRUCTIONS : Nat64 = 40_000_000_000;

    func paginate(collection : StableCollection, eval : EvalResult, skip : Nat, opt_limit : ?Nat) : Iter<T.DocumentId> {

        let iter = switch (eval) {
            case (#Empty) {
                Logger.lazyDebug(collection.logger, func() = "paginate(): Empty iterator");
                return Itertools.empty<T.DocumentId>();
            };
            case (#BitMap(bitmap)) {
                Logger.lazyDebug(collection.logger, func() = "paginate(): Bitmap iterator");
                let document_ids = Iter.map<Nat, T.DocumentId>(
                    bitmap.vals(),
                    func(n : Nat) : T.DocumentId {
                        CollectionUtils.convert_bitmap_8_byte_to_document_id(collection, n);
                    },
                );
            };
            case (#Ids(iter)) {
                Logger.lazyDebug(collection.logger, func() = "paginate(): Ids iterator");
                iter;
            };
            case (#Interval(index_name, _intervals, sorted_in_reverse)) {
                Logger.lazyDebug(collection.logger, func() = "paginate(): Interval iterator");

                if (sorted_in_reverse) {
                    return Intervals.extract_document_ids_in_pagination_range_for_reversed_intervals(collection, skip, opt_limit, index_name, _intervals, sorted_in_reverse);
                } else {
                    return Intervals.extract_document_ids_in_pagination_range(collection, skip, opt_limit, index_name, _intervals, sorted_in_reverse);
                };

            };

        };

        let iter_with_offset = Itertools.skip(iter, skip);

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
        document_id : Blob,
        candid_map : T.CandidMap,
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

        label validating_field_constraints for ((field_name, field_constraints) in field_constraints_iter) {
            let field_value = switch (CandidMap.get(candid_map, collection.schema_map, field_name)) {
                case (?field_value) field_value;
                case (null) {
                    if (
                        SchemaMap.is_nested_variant_field(collection.schema_map, field_name) or
                        SchemaMap.is_nested_option_field(collection.schema_map, field_name)
                    ) {
                        continue validating_field_constraints;
                    };

                    return #err("Schema Constraint Field '" # field_name # "' not found in document");

                };

            };

            for (field_constraint in field_constraints.vals()) {

                // move to CandidOps
                func unwrapOption(val : Candid) : Candid {
                    switch (val) {
                        case (#Option(inenr)) unwrapOption(inenr);
                        case (val) val;
                    };
                };

                switch (unwrapOption(field_value), field_constraint) {
                    case (#Null, _) {}; // ignore validation for null values
                    case (_, #Max(max_value)) {
                        switch (CandidUtils.Ops.compare(field_value, #Float(max_value))) {
                            case (#greater) {
                                let error_msg = "Field '" # field_name # "' exceeds maximum value of " # debug_show max_value;
                                return #err(error_msg);
                            };
                            case (_) {};
                        };

                    };

                    case (_, #Min(min_value)) {
                        switch (CandidUtils.Ops.compare(field_value, #Float(min_value))) {
                            case (#less) {
                                let error_msg = "Field '" # field_name # "' is less than minimum value of " # debug_show min_value;
                                return #err(error_msg);
                            };
                            case (_) {};
                        };

                    };

                    case (_, #MinSize(min_size)) {
                        let field_value_size = CandidUtils.Ops.size(field_value);

                        switch (CandidUtils.Ops.compare(#Nat(field_value_size), #Nat(min_size))) {
                            case (#less) {
                                let error_msg = "Field '" # field_name # "' is less than minimum size of " # debug_show min_size;
                                return #err(error_msg);
                            };
                            case (_) {};
                        };

                    };

                    case (_, #MaxSize(max_size)) {
                        let field_value_size = CandidUtils.Ops.size(field_value);

                        switch (CandidUtils.Ops.compare(#Nat(field_value_size), #Nat(max_size))) {
                            case (#greater) {
                                let error_msg = "Field '" # field_name # "' exceeds maximum size of " # debug_show max_size;
                                return #err(error_msg);
                            };
                            case (_) {};
                        };

                    };

                    case (_, #Size(min_size, max_size)) {
                        let field_value_size = CandidUtils.Ops.size(field_value);

                        switch (CandidUtils.Ops.compare(#Nat(field_value_size), #Nat(min_size))) {
                            case (#less) {
                                let error_msg = "Field '" # field_name # "' is less than minimum size of " # debug_show min_size;
                                return #err(error_msg);
                            };
                            case (_) {};
                        };

                        switch (CandidUtils.Ops.compare(#Nat(field_value_size), #Nat(max_size))) {
                            case (#greater) {
                                let error_msg = "Field '" # field_name # "' exceeds maximum size of " # debug_show max_size;
                                return #err(error_msg);
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

        label validating_unique_constraints for ((composite_field_keys, index) in unique_constraints_iter) {

            let ?compsite_field_values = CollectionUtils.getIndexColumns(collection, index.key_details, document_id, candid_map) else continue validating_unique_constraints;
            let index_data_utils = Index.get_index_data_utils(collection);
            // Debug.print("compsite_field_values: " # debug_show compsite_field_values);

            let opt_prev_document_id = BTree.get(index.data, index_data_utils, compsite_field_values);

            switch (opt_prev_document_id) {
                case (null) {}; // no previous value, free to insert
                case (?prev_document_id) {

                    if (prev_document_id != document_id) {
                        let error_msg = "Unique constraint violation: Inserting new document failed because unique constraint on " # debug_show composite_field_keys # " is violated because document with id " # debug_show prev_document_id # " already has composite values " # debug_show compsite_field_values # " the same as the new document about to be inserted";
                        return #err(error_msg);
                    };

                }

            };

        };

        #ok()

    };

    public func put(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        candid_blob : T.CandidBlob,
    ) : Result<T.DocumentId, Text> {

        let next_id = Ids.next(collection.ids);
        let next_id_as_blob = Blob.fromArray(ByteUtils.BigEndian.fromNat64(Nat64.fromNat(next_id)));
        let document_id = Utils.concat_blob(collection.instance_id, next_id_as_blob);

        Logger.lazyInfo(
            collection.logger,
            func() = "ZenDB Collection.put(): Inserting document with id " # debug_show document_id,
        );

        let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);

        Logger.lazyDebug(
            collection.logger,
            func() = "ZenDB Collection.put(): Inserting document with id " # debug_show document_id # " and candid " # debug_show candid,
        );

        switch (Schema.validate(collection.schema, candid)) {
            case (#ok(_)) {};
            case (#err(msg)) {
                let err_msg = "Schema validation failed: " # msg;
                return #err(err_msg);
            };
        };

        let candid_map = CandidMap.new(collection.schema_map, document_id, candid);

        switch (validate_schema_constraints_on_updated_fields(collection, document_id, candid_map, null)) {
            case (#ok(_)) {};
            case (#err(msg)) {
                let err_msg = "Schema Constraint validation failed: " # msg;
                return #err(err_msg);
            };
        };

        let opt_prev = DocumentStore.put(collection.documents, main_btree_utils, document_id, candid_blob);

        switch (opt_prev) {
            case (null) {};
            case (?prev) {
                Debug.trap("put(): Record with id " # debug_show document_id # " already exists. Internal error found, report this to the developers");
            };
        };

        if (Map.size(collection.indexes) == 0) return #ok(document_id);

        let updated_indexes = Buffer.Buffer<Index>(Map.size(collection.indexes));

        label updating_indexes for (index in Map.vals(collection.indexes)) {
            let res = update_indexed_document_fields(collection, index, document_id, candid_map, null);

            switch (res) {
                case (#err(err)) {
                    for (index in updated_indexes.vals()) {
                        ignore Index.remove(collection, index, document_id, candid_map);
                    };

                    ignore DocumentStore.remove(collection.documents, main_btree_utils, document_id);

                    return #err(err);
                };
                case (#ok(_)) {};
            };

            updated_indexes.add(index);
        };

        #ok(document_id);
    };

    public func replace_by_id<Record>(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        document_id : T.DocumentId,
        new_candid_blob : T.CandidBlob,
    ) : Result<(), Text> {
        Logger.info(collection.logger, "Replacing document with id: " # debug_show document_id);

        let ?prev_candid_blob = DocumentStore.get(collection.documents, main_btree_utils, document_id) else return #err("Record for id '" # debug_show (document_id) # "' not found");
        let prev_candid = CollectionUtils.decodeCandidBlob(collection, prev_candid_blob);
        let prev_candid_map = CandidMap.new(collection.schema_map, document_id, prev_candid);

        let new_candid_value = CollectionUtils.decodeCandidBlob(collection, new_candid_blob);
        let new_candid_map = CandidMap.new(collection.schema_map, document_id, new_candid_value);

        switch (Schema.validate(collection.schema, new_candid_value)) {
            case (#err(msg)) {
                return #err("Schema validation failed: " # msg);
            };
            case (#ok(_)) {};
        };

        switch (validate_schema_constraints_on_updated_fields(collection, document_id, new_candid_map, null)) {
            case (#ok(_)) {};
            case (#err(msg)) {
                let err_msg = "Schema Constraint validation failed: " # msg;
                return #err(err_msg);
            };
        };

        assert ?prev_candid_blob == DocumentStore.put(collection.documents, main_btree_utils, document_id, new_candid_blob);

        for (index in Map.vals(collection.indexes)) {
            let #ok(_) = update_indexed_document_fields(collection, index, document_id, new_candid_map, ?prev_candid_map) else {
                return #err("Failed to update index data");
            };
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully replaced document with id: " # debug_show document_id,
        );
        #ok();
    };

    func partially_update_doc(collection : StableCollection, candid_map : T.CandidMap, update_operations : [(Text, T.FieldUpdateOperations)]) : Result<Candid, Text> {
        Logger.lazyInfo(collection.logger, func() = "Partially updating document with operations: " # debug_show update_operations);

        for ((field_name, op) in update_operations.vals()) {
            let ?field_type = SchemaMap.get(collection.schema_map, field_name) else return #err("Field type '" # field_name # "' not found in document");
            let ?prev_candid = CandidMap.get(candid_map, collection.schema_map, field_name) else return #err("Field '" # field_name # "' not found in document");

            let new_value = switch (UpdateOps.handleFieldUpdateOperation(collection, candid_map, field_type, prev_candid, op)) {
                case (#ok(new_value)) new_value;
                case (#err(msg)) {
                    return #err("Failed to update field '" # field_name # "' with operation '" # debug_show op # "': " # msg);
                };
            };

            switch (CandidMap.set(candid_map, collection.schema_map, field_name, new_value)) {
                case (#err(err)) return #err("Failed to update field '" # field_name # "' with new value (" # debug_show new_value # "): " # err);
                case (#ok(_)) {};
            };
        };

        Logger.lazyDebug(
            collection.logger,
            func() = "Updated candid map, about to extract candid",
        );

        let candid = CandidMap.extract_candid(candid_map);

        #ok(candid);
    };

    func update_indexed_document_fields(
        collection : StableCollection,
        index : Index,
        document_id : Blob,
        new_document_candid_map : T.CandidMap,
        opt_prev_document_candid_map : ?T.CandidMap,
    ) : Result<(), Text> {

        let index_data_utils = Index.get_index_data_utils(collection);

        ignore do ? {
            let prev_document_candid_map = opt_prev_document_candid_map!;

            switch (Index.remove(collection, index, document_id, prev_document_candid_map)) {
                case (#err(err)) {
                    return #err(err);
                };
                case (#ok(_)) {};
            };

        };

        Logger.lazyDebug(
            collection.logger,
            func() = "Updating index for id: " # debug_show document_id,
        );
        Logger.lazyDebug(
            collection.logger,
            func() = "Index key details: " # debug_show index.key_details,
        );

        switch (Index.insert(collection, index, document_id, new_document_candid_map)) {
            case (#err(err)) {
                return #err("Failed to insert into index '" # index.name # "': " # err);
            };
            case (#ok(_)) {};
        };

        #ok;
    };

    func update_indexed_data_on_updated_fields(collection : StableCollection, id : T.DocumentId, prev_document_candid_map : T.CandidMap, new_document_candid_map : T.CandidMap, updated_fields : [Text]) : Result<(), Text> {

        let updated_fields_set = Set.fromIter(updated_fields.vals(), thash);

        for (index in Map.vals(collection.indexes)) {
            for ((index_key, _) in index.key_details.vals()) {

                // only updates the fields that were changed
                if (Set.has(updated_fields_set, thash, index_key)) {
                    let #ok(_) = update_indexed_document_fields(collection, index, id, new_document_candid_map, ?prev_document_candid_map) else return #err("Failed to update index data");
                };
            };
        };

        #ok;
    };

    public func update_by_id<Record>(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, id : T.DocumentId, field_updates : [(Text, T.FieldUpdateOperations)]) : Result<(), Text> {
        Logger.lazyInfo(
            collection.logger,
            func() = "Updating document with id: " # debug_show id,
        );

        let ?prev_candid_blob = DocumentStore.get(collection.documents, main_btree_utils, id) else return #err("Record for id '" # debug_show (id) # "' not found");

        let prev_candid = CollectionUtils.decodeCandidBlob(collection, prev_candid_blob);
        let prev_candid_map = CandidMap.new(collection.schema_map, id, prev_candid);

        let fields_with_updates = Array.map<(Text, T.FieldUpdateOperations), Text>(field_updates, func(k, _) = k);

        Logger.lazyDebug(
            collection.logger,
            func() = "Performing partial update on fields: " # debug_show (fields_with_updates),
        );

        let new_candid_map = CandidMap.clone(prev_candid_map, collection.schema_map);

        let new_candid_document = switch (partially_update_doc(collection, new_candid_map, field_updates)) {
            case (#ok(new_candid_document)) new_candid_document;
            case (#err(msg)) {
                return #err("Failed to update fields: " # msg);
            };
        };

        Logger.lazyDebug(
            collection.logger,
            func() = "Updated candid map: " # debug_show new_candid_document,
        );

        switch (Schema.validate(collection.schema, new_candid_document)) {
            case (#err(msg)) {
                return #err("Schema validation failed: " # msg);
            };
            case (#ok(_)) {};
        };

        switch (validate_schema_constraints_on_updated_fields(collection, id, new_candid_map, ?fields_with_updates)) {
            case (#ok(_)) {};
            case (#err(msg)) {
                let err_msg = "Schema Constraint validation failed: " # msg;
                return #err(err_msg);
            };
        };

        let new_candid_blob = switch (Candid.encodeOne(new_candid_document, ?{ Candid.defaultOptions with types = ?[collection.schema] })) {
            case (#ok(new_candid_blob)) new_candid_blob;
            case (#err(msg)) {
                return #err("Failed to encode new candid blob: " # msg);
            };
        };

        assert ?prev_candid_blob == DocumentStore.put(collection.documents, main_btree_utils, id, new_candid_blob);

        let updated_keys = Array.map<(Text, Any), Text>(
            field_updates,
            func(field_name : Text, _ : Any) : Text { field_name },
        );

        let #ok(_) = update_indexed_data_on_updated_fields(collection, id, prev_candid_map, new_candid_map, updated_keys) else {
            return #err("Failed to update index data");
        };

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully updated document with id: " # debug_show id,
        );
        #ok();
    };

    public func insert(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, candid_blob : T.CandidBlob) : Result<T.DocumentId, Text> {
        put(collection, main_btree_utils, candid_blob);
    };

    public func insert_docs(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        documents : [T.CandidBlob],
    ) : Result<[T.DocumentId], Text> {
        let ids = Buffer.Buffer<T.DocumentId>(documents.size());

        for (document in documents.vals()) {
            switch (insert(collection, main_btree_utils, document)) {
                case (#ok(id)) ids.add(id);
                case (#err(err)) {
                    for (id in ids.vals()) {
                        // rollback previously inserted documents
                        switch (delete_by_id(collection, main_btree_utils, id)) {
                            case (#ok(_)) {};
                            case (#err(err)) {
                                Logger.lazyError(collection.logger, func() = "Failed to rollback document with id: " # debug_show (id) # ": " # err);
                            };
                        };
                    };
                    return #err(err);
                };
            };
        };

        #ok(Buffer.toArray(ids));
    };

    public func get(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        id : T.DocumentId,
    ) : ?T.CandidBlob {
        DocumentStore.get(collection.documents, main_btree_utils, id);
    };

    public type SearchResult = {
        results : [(T.DocumentId, T.CandidBlob)];
        next : () -> SearchResult;
    };

    public func search(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        stable_query : T.StableQuery,
    ) : Result<[(T.WrapId<T.CandidBlob>)], Text> {
        Logger.lazyDebug(
            collection.logger,
            func() = "Executing search with query: " # debug_show (stable_query),
        );

        switch (internal_search(collection, stable_query)) {
            case (#err(err)) {
                return #err("Search failed: " # err);
            };
            case (#ok(document_ids_iter)) {
                let candid_blob_iter = ids_to_candid_blobs(collection, document_ids_iter);
                let candid_blobs = Iter.toArray(candid_blob_iter);
                Logger.lazyDebug(
                    collection.logger,
                    func() = "Search completed, found " # debug_show (candid_blobs.size()) # " results",
                );
                #ok(candid_blobs);
            };
        };
    };

    public func evaluate_query(collection : StableCollection, stable_query : T.StableQuery) : Result<Iter<T.DocumentId>, Text> {
        Logger.lazyDebug(
            collection.logger,
            func() = "Evaluating query with operations: " # debug_show (stable_query.query_operations),
        );

        let query_operations = stable_query.query_operations;
        let sort_by = stable_query.sort_by;
        let pagination = stable_query.pagination;

        let (opt_cursor, cursor_map) = switch (pagination.cursor) {
            case (?(id, pagination_direction)) switch (CollectionUtils.lookupCandidDocument(collection, id)) {
                case (?document) {
                    (?(id, document), CandidMap.new(collection.schema_map, id, document));
                };
                case (null) {
                    let #ok(default_value) = Schema.generate_default_value(collection.schema) else Debug.trap("Couldn't generate default value for schema: " # debug_show collection.schema);

                    (null, CandidMap.new(collection.schema_map, ("" : Blob), default_value));
                };

            };
            case (null) {
                let #ok(default_value) = Schema.generate_default_value(collection.schema) else Debug.trap("Couldn't generate default value for schema: " # debug_show collection.schema);
                (null, CandidMap.new(collection.schema_map, ("" : Blob), default_value));
            };
        };

        switch (Query.validateQuery(collection, stable_query.query_operations)) {
            case (#err(err)) {
                return #err("Invalid Query: " # err);
            };
            case (#ok(_)) ();
        };

        let formatted_query_operations = switch (Query.processQuery(collection, query_operations)) {
            case (#ok(formatted_query_operations)) formatted_query_operations;
            case (#err(err)) {
                return #err("Failed to process query operations: " # err);
            };
        };

        // Debug.print("Formatted query operations: " # debug_show formatted_query_operations);

        let query_plan : T.QueryPlan = QueryPlan.create_query_plan(
            collection,
            formatted_query_operations,
            sort_by,
            opt_cursor,
            // cursor_map,
        );

        let sort_documents_by_field_cmp = switch (sort_by) {
            case (?sort_by) get_document_field_cmp(collection, sort_by);
            case (null) func(_ : T.DocumentId, _ : T.DocumentId) : Order = #equal;
        };

        let eval = QueryExecution.generate_document_ids_for_query_plan(collection, query_plan, sort_by, sort_documents_by_field_cmp);
        let iter = paginate(collection, eval, Option.get(pagination.skip, 0), pagination.limit);

        Logger.lazyDebug(
            collection.logger,
            func() = "Query evaluation completed",
        );
        return #ok((iter));
    };

    public func internal_search(collection : StableCollection, stable_query : T.StableQuery) : Result<Iter<T.DocumentId>, Text> {
        // let stable_query = query_builder.build();
        switch (evaluate_query(collection, stable_query)) {
            case (#err(err)) return #err(err);
            case (#ok(eval_result)) #ok(eval_result);
        };
    };

    public func ids_to_documents<Record>(collection : StableCollection, blobify : InternalCandify<Record>, iter : Iter<T.DocumentId>) : Iter<(T.DocumentId, Record)> {
        Iter.map<T.DocumentId, (T.DocumentId, Record)>(
            iter,
            func(id : T.DocumentId) : (T.DocumentId, Record) {
                let document = CollectionUtils.lookupDocument<Record>(collection, blobify, id);
                (id, document);
            },
        );
    };

    public func ids_to_candid_blobs<Record>(collection : StableCollection, iter : Iter<T.DocumentId>) : Iter<(T.DocumentId, T.CandidBlob)> {
        Iter.map<T.DocumentId, (T.DocumentId, T.CandidBlob)>(
            iter,
            func(id : T.DocumentId) : (T.DocumentId, T.CandidBlob) {
                let candid_blob = CollectionUtils.lookupCandidBlob(collection, id);
                (id, candid_blob);
            },
        );
    };

    public func search_iter(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        query_builder : QueryBuilder,
    ) : Result<Iter<T.WrapId<T.CandidBlob>>, Text> {
        switch (internal_search(collection, query_builder.build())) {
            case (#err(err)) return #err(err);
            case (#ok(document_ids_iter)) {
                let document_iter = ids_to_candid_blobs(collection, document_ids_iter);
                #ok(document_iter);
            };
        };
    };

    public func get_document_field_cmp(
        collection : StableCollection,
        sort_field : (Text, T.SortDirection),
    ) : (T.DocumentId, T.DocumentId) -> Order {

        let deserialized_documents_map = Map.new<T.DocumentId, Candid.Candid>();

        func get_candid_map_bytes(id : T.DocumentId) : Candid.Candid {
            switch (Map.get(deserialized_documents_map, bhash, id)) {
                case (?candid) candid;
                case (null) {
                    let ?candid = CollectionUtils.lookupCandidDocument(collection, id) else Debug.trap("Couldn't find document with id: " # debug_show id);
                    candid;
                };
            };
        };

        let opt_candid_map_a : ?T.CandidMap = null;
        let opt_candid_map_b : ?T.CandidMap = null;

        func sort_documents_by_field_cmp(a : T.DocumentId, b : T.DocumentId) : Order {

            let document_a = get_candid_map_bytes(a);
            let document_b = get_candid_map_bytes(b);

            let candid_map_a : T.CandidMap = switch (opt_candid_map_a) {
                case (?candid_map) {
                    CandidMap.reload(candid_map, collection.schema_map, a, document_a);
                    candid_map;
                };
                case (null) {
                    let candid_map = CandidMap.new(collection.schema_map, a, document_a);
                    candid_map;
                };
            };

            let candid_map_b : T.CandidMap = switch (opt_candid_map_b) {
                case (?candid_map) {
                    CandidMap.reload(candid_map, collection.schema_map, b, document_b);
                    candid_map;
                };
                case (null) {
                    let candid_map = CandidMap.new(collection.schema_map, b, document_b);
                    candid_map;
                };
            };

            let ?value_a = CandidMap.get(candid_map_a, collection.schema_map, sort_field.0) else Debug.trap("Couldn't get value from CandidMap for key: " # sort_field.0);
            let ?value_b = CandidMap.get(candid_map_b, collection.schema_map, sort_field.0) else Debug.trap("Couldn't get value from CandidMap for key: " # sort_field.0);

            let order_num = Schema.cmp_candid(#Empty, value_a, value_b);

            let order_variant = if (sort_field.1 == #Ascending) {
                if (order_num == 0) #equal else if (order_num == 1) #greater else #less;
            } else {
                if (order_num == 0) #equal else if (order_num == 1) #less else #greater;
            };

            order_variant;
        };
        sort_documents_by_field_cmp;
    };

    public func stats(collection : StableCollection) : T.CollectionStats {

        let main_collection_memory : T.MemoryBTreeStats = BTree.getMemoryStats(collection.documents);

        let total_documents = StableCollection.size(collection);

        let indexes : [T.IndexStats] = Iter.toArray(
            Iter.map<(Text, Index), T.IndexStats>(
                Map.entries(collection.indexes),
                func((index_name, index) : (Text, Index)) : T.IndexStats {
                    Index.stats(index, total_documents);
                },
            )
        );

        var total_allocated_bytes : Nat = main_collection_memory.allocatedBytes;
        var total_free_bytes : Nat = main_collection_memory.freeBytes;
        var total_used_bytes : Nat = main_collection_memory.usedBytes;
        var total_data_bytes : Nat = main_collection_memory.dataBytes;
        var total_metadata_bytes : Nat = main_collection_memory.metadataBytes;

        var total_index_store_bytes : Nat = 0;

        for (index_stats in indexes.vals()) {
            total_allocated_bytes += index_stats.memory.allocatedBytes;
            total_free_bytes += index_stats.memory.freeBytes;
            total_used_bytes += index_stats.memory.usedBytes;
            total_data_bytes += index_stats.memory.dataBytes;
            total_metadata_bytes += index_stats.memory.metadataBytes;

            total_index_store_bytes += index_stats.memory.allocatedBytes;
        };

        let collection_stats : T.CollectionStats = {
            name = collection.name;
            schema = collection.schema;
            entries = total_documents;
            memory = main_collection_memory;
            memoryType = collection.memory_type;

            // Each entry is a document stored as an 8 byte key and the candid blob as the value
            avg_document_size = if (total_documents == 0) 0 else (main_collection_memory.dataBytes / total_documents);
            total_document_size = main_collection_memory.dataBytes;
            indexes;

            total_allocated_bytes = total_allocated_bytes;
            total_free_bytes = total_free_bytes;
            total_used_bytes = total_used_bytes;
            total_data_bytes = total_data_bytes;
            total_metadata_bytes = total_metadata_bytes;

            total_document_store_bytes = main_collection_memory.allocatedBytes;
            total_index_store_bytes = total_index_store_bytes;
        };

        collection_stats;
    };

    public func count(collection : StableCollection, query_builder : QueryBuilder) : Result<Nat, Text> {
        let stable_query = query_builder.build();

        let query_plan = QueryPlan.create_query_plan(
            collection,
            stable_query.query_operations,
            null,
            null,
        );

        let count = switch (QueryExecution.get_unique_document_ids_from_query_plan(collection, Map.new(), query_plan)) {
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
        );

        let sort_documents_by_field_cmp = func(_ : T.DocumentId, _ : T.DocumentId) : Order = #equal;

        let eval = QueryExecution.generate_document_ids_for_query_plan(collection, query_plan, null, sort_documents_by_field_cmp);

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

    public func delete_by_id(collection : StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, id : T.DocumentId) : Result<(T.CandidBlob), Text> {
        Logger.lazyInfo(
            collection.logger,
            func() = "Deleting document with id: " # debug_show id,
        );

        let ?prev_candid_blob = DocumentStore.remove(collection.documents, main_btree_utils, id) else {
            return #err("Record not found");
        };

        let prev_candid = CollectionUtils.decodeCandidBlob(collection, prev_candid_blob);
        let prev_candid_map = CandidMap.new(collection.schema_map, id, prev_candid);

        for (index in Map.vals(collection.indexes)) {

            switch (Index.remove(collection, index, id, prev_candid_map)) {
                case (#err(err)) {
                    return #err("Failed to remove from index '" # index.name # "': " # err);
                };
                case (#ok(_)) {};
            };
        };

        let candid_blob = prev_candid_blob;

        Logger.lazyInfo(
            collection.logger,
            func() = "Successfully deleted document with id: " # debug_show id,
        );
        #ok(candid_blob);
    };

};
