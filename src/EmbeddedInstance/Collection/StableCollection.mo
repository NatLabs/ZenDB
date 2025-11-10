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
import ExperimentalInternetComputer "mo:base@0.16.0/ExperimentalInternetComputer";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.4.0";
import Decoder "mo:serde@3.4.0/Candid/Blob/Decoder";
import Candid "mo:serde@3.4.0/Candid";
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

import CompositeIndex "Index/CompositeIndex";
import CommonIndexFns "Index/CommonIndexFns";
import TextIndex "Index/TextIndex";
import DocumentStore "DocumentStore";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "CollectionUtils";
import QueryPlan "QueryPlan";
import QueryExecution "QueryExecution";
import Intervals "Intervals";

import C "../Constants";
import CandidUtils "../CandidUtils";
import Logger "../Logger";
import UpdateOps "UpdateOps";
import BTree "../BTree";

module StableCollection {
    let LOGGER_NAMESPACE = "StableCollection";

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
    public type CompositeIndex = T.CompositeIndex;
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
    // ) : T.StableCollection {

    //     let schema_keys = Utils.get_schema_keys(processed_schema);

    //     var stable_collection : T.StableCollection = {
    //         ids = db.ids;
    //         name;
    //         schema = processed_schema;
    //         schema_map = SchemaMap.new(processed_schema);
    //         schema_keys;
    //         schema_keys_set = Set.fromIter(schema_keys.vals(), Map.thash);
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

    //         indexes = Map.new<Text, T.CompositeIndex>();

    //         field_constraints;
    //         unique_constraints = [];
    //         fields_with_unique_constraints = Map.new();

    //         // db references
    //         freed_btrees = db.freed_btrees;
    //         logger = db.logger;
    //         memory_type = db.memory_type;
    //     };

    // };

    public func size(collection : T.StableCollection) : Nat {
        DocumentStore.size(collection.documents);
    };

    /// Clear all the data in the collection.
    public func clear(collection : T.StableCollection) : () {
        DocumentStore.clear(collection.documents);

        for (index in Map.vals(collection.indexes)) {
            CommonIndexFns.clear(collection, index);
        };
    };

    // BTree methods

    public func entries(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>) : Iter<(T.DocumentId, Blob)> {
        DocumentStore.entries(collection.documents, main_btree_utils);
    };

    public func keys(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>) : Iter<T.DocumentId> {
        DocumentStore.keys(collection.documents, main_btree_utils);
    };

    public func vals(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>) : Iter<Blob> {
        DocumentStore.vals(collection.documents, main_btree_utils);
    };

    public func range(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : Iter<(T.DocumentId, Blob)> {
        DocumentStore.range(collection.documents, main_btree_utils, start, end);
    };

    public func range_keys(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : Iter<T.DocumentId> {
        DocumentStore.range_keys(collection.documents, main_btree_utils, start, end);
    };

    public func range_vals(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, start : Nat, end : Nat) : Iter<Blob> {
        DocumentStore.range_vals(collection.documents, main_btree_utils, start, end);
    };

    // public func update_schema<NewRecord>(collection : T.StableCollection, schema : T.Schema) : T.Result<(), Text> {
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
        collection : T.StableCollection,
        index_name : Text,
        index_key_details : [(Text, SortDirection)],
        is_unique : Bool,
        used_internally : Bool,
    ) : T.Result<T.CompositeIndex, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("create_index_internal");

        switch (Map.get(collection.indexes, Map.thash, index_name)) {
            case (?index) {
                log.lazyInfo(func() = "CompositeIndex '" # index_name # "' already exists");
                let internal_index = CommonIndexFns.get_internal_index(index);
                return #ok(internal_index);
            };
            case (null) {};
        };

        log.lazyInfo(func() = "Creating index '" # index_name # "' with key details: " # debug_show index_key_details);
        log.lazyDebug(func() = "index_key_details: " # debug_show (index_key_details));

        let index = CompositeIndex.new(collection, index_name, index_key_details, is_unique, used_internally);

        ignore Map.put(collection.indexes, Map.thash, index_name, #composite_index(index));

        log.lazyInfo(func() = "Successfully created index: " # index_name);

        #ok(index);
    };

    public func create_composite_index(
        collection : T.StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_name : Text,
        _index_key_details : [(Text, SortDirection)],
        is_unique : Bool,
    ) : T.Result<(T.CompositeIndex), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("create_composite_index");

        let index_creation_response = StableCollection.create_index_internal(collection, index_name, _index_key_details, is_unique, false);

        let index = switch (index_creation_response) {
            case (#ok(index)) index;
            case (#err(err_msg)) return #err(err_msg);
        };

        switch (CompositeIndex.populate_index(collection, index)) {
            case (#ok(_)) {};
            case (#err(err_msg)) return #err("Failed to create index '" # index_name # "': " # err_msg);
        };

        log.lazyInfo(func() = "Successfully created and populated index: " # index_name);

        #ok(index)

    };

    func get_existing_keys<V>(
        map : Map<Text, V>,
        index_names : [Text],
    ) : [Text] {

        let indexes = Buffer.Buffer<Text>(index_names.size());

        for (index_name in index_names.vals()) {
            switch (Map.get(map, Map.thash, index_name)) {
                case (?index) indexes.add(index_name);
                case (null) {};
            };
        };

        Buffer.toArray(indexes)

    };

    public func create_populate_indexes_batch(
        collection : StableCollection,
        index_configs : [T.CreateInternalIndexParams],
        opt_performance_init : ?Nat,
    ) : T.Result<(batch_id : Nat), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("create_populate_indexes_batch");

        let performance = Performance(collection.is_running_locally, opt_performance_init);
        let index_names = Array.map(index_configs, func(config : T.CreateInternalIndexParams) : Text = config.0);

        // then check if there any of those indexes are currently being populated or created
        let existing_indexes_in_batch_operations = get_existing_keys(collection.indexes_in_batch_operations, index_names);
        if (existing_indexes_in_batch_operations.size() > 0) {
            return #err(
                "Failed to create indexes because the following indexes are currently being created or populated: " # debug_show (existing_indexes_in_batch_operations)
            );
        };

        let newly_created_indexes = Buffer.Buffer<T.Index>(index_configs.size());

        for ((index_name, index_key_details, options) in index_configs.vals()) {
            log.lazyInfo(func() = "Creating indexes: " # debug_show (index_name, index_key_details));
            let composite_index = CompositeIndex.new(collection, index_name, index_key_details, options.is_unique, options.used_internally);
            let index = #composite_index(composite_index);
            ignore Map.put(collection.indexes_in_batch_operations, Map.thash, index_name, index);
            newly_created_indexes.add(index);
        };

        let main_btree_utils = DocumentStore.getBtreeUtils(collection.documents);

        /// create batch population job
        let first_document_id = if (DocumentStore.size(collection.documents) != 0) {
            switch (DocumentStore.getMin(collection.documents, main_btree_utils)) {
                case (?(doc_id, _)) doc_id;
                case (null) ("" : Blob);
            };
        } else ("" : Blob);

        let batch : T.BatchPopulateIndex = {
            id = Ids.next(collection.ids);
            indexes = Buffer.toArray(newly_created_indexes);
            var indexed_documents = 0;
            total_documents = StableCollection.size(collection);

            var avg_instructions_per_document = 0;
            var total_instructions_used = performance.total_instructions_used();

            var next_document_to_process = first_document_id;

            var num_documents_to_process_per_batch = null;
            var done_processing = false;

        };

        // store the batch in the stable store
        ignore Map.put<Nat, T.BatchPopulateIndex>(collection.populate_index_batches, Map.nhash, batch.id, batch);

        log.lazyInfo(func() = "Created Batch with id: " # debug_show (batch.id));

        #ok(batch.id)

    };

    public func batch_create_indexes(
        collection : StableCollection,
        index_configs : [T.CreateInternalIndexParams],
    ) : Result<(batch_id : Nat), Text> {
        if (index_configs.size() == 0) {
            return #err("No index configurations provided");
        };

        let performance = Performance(collection.is_running_locally, null);

        // first check if any of the indexes already exist, fail if they do
        let index_names = Array.map(index_configs, func(config : T.CreateInternalIndexParams) : Text = config.0);
        let existing_indexes = get_existing_keys(collection.indexes, index_names);
        if (existing_indexes.size() > 0) {
            return #err(
                "Failed to create indexes because the following indexes already exist: " # debug_show (existing_indexes)
            );
        };

        create_populate_indexes_batch(collection, index_configs, ?(performance.total_instructions_used()));

    };

    func process_index_population_batch(
        collection : StableCollection,
        batch : T.BatchPopulateIndex,
        starting_document_id : T.DocumentId,
        num_documents_to_process : Nat,
    ) : T.Result<(Nat), Text> {

        if (batch.total_documents == 0 or batch.done_processing) {
            batch.done_processing := true;
            let res = commit_batch_populate_indexes(collection, batch.id);
            return Result.mapOk<(), Nat, Text>(res, func(_ : ()) : Nat = 0);
        };

        let main_btree_utils = DocumentStore.getBtreeUtils(collection.documents);
        let document_scan_iter = DocumentStore.scan(collection.documents, main_btree_utils, ?starting_document_id, null);

        // Convert to an array to avoid sliding iterator issues
        let entries = Iter.toArray(Itertools.take(document_scan_iter, num_documents_to_process + 1)); // +1 to peek next
        let entries_iter = Itertools.peekable(entries.vals());

        let errors = Buffer.Buffer<Text>(collection.indexes.size());
        var processed_documents = 0;

        for ((document_id, candid_blob) in Itertools.take(entries_iter, num_documents_to_process)) {
            let candid_map = CollectionUtils.get_candid_map_no_cache(collection, document_id, ?candid_blob);

            let processed_indexes = Buffer.Buffer<T.Index>(collection.indexes.size());

            for (index in batch.indexes.vals()) {
                switch (CommonIndexFns.insertWithCandidMap(collection, index, document_id, candid_map)) {
                    case (#err(err)) { errors.add(err) };
                    case (#ok(_)) { processed_indexes.add(index) };
                };
            };

            if (errors.size() > 0) {

                for (processed_index in processed_indexes.vals()) {
                    ignore CommonIndexFns.removeWithCandidMap(collection, processed_index, document_id, candid_map);
                };

                return #err(
                    "Failed to populate indexes for document id " # debug_show (document_id) # " due to the following errors: " # debug_show (Buffer.toArray(errors))
                );

            };

            batch.indexed_documents += 1;
            processed_documents += 1;

            switch (entries_iter.peek()) {
                case (?(document_id, _)) batch.next_document_to_process := document_id;
                case (null) {};
            };

        };

        switch (document_scan_iter.next()) {
            case (?_) return #ok(processed_documents); // still more to process
            case (null) {};
        };

        // done processing all documents in the collection
        batch.done_processing := true;

        switch (commit_batch_populate_indexes(collection, batch.id)) {
            case (#ok(())) {};
            case (#err(err_msg)) return #err(err_msg);
        };

        #ok(processed_documents);

    };

    func commit_batch_populate_indexes(
        collection : StableCollection,
        batch_id : Nat,
    ) : Result<(), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("commit_batch_populate_indexes");

        log.lazyInfo(func() = "Committing batch with id " # debug_show batch_id);
        let ?batch = Map.get(collection.populate_index_batches, Map.nhash, batch_id) else {
            return #err("Batch with id " # debug_show batch_id # " not found");
        };

        if (not batch.done_processing) {
            return #err("Cannot commit batch with id " # debug_show batch_id # " because it is not done processing");
        };

        // commit batch
        let replaced_indexes = Buffer.Buffer<T.Index>(batch.indexes.size());

        for (index in batch.indexes.vals()) {
            let name = CommonIndexFns.name(index);

            // - Replace any existing index in the main indexes map and store the old one
            switch (Map.put(collection.indexes, Map.thash, name, index)) {
                case (?old_index) replaced_indexes.add(old_index);
                case (null) {};
            };

            // - Remove the index from the indexes_in_batch_operations map
            ignore Map.remove(collection.indexes_in_batch_operations, Map.thash, name);
        };

        // - Now safely deallocate all the old/replaced indexes
        for (old_index in replaced_indexes.vals()) {
            CommonIndexFns.deallocate(collection, old_index);
        };

        ignore Map.remove<Nat, T.BatchPopulateIndex>(collection.populate_index_batches, Map.nhash, batch.id);

        #ok(());

    };

    class Performance(is_running_locally : Bool, opt_init_instructions : ?Nat) {
        let init_instructions = Option.get(opt_init_instructions, 0);
        var instructions_at_last_call = init_instructions;

        func get_instructions() : Nat {
            if (is_running_locally) {
                instructions_at_last_call += 100;
                instructions_at_last_call;
            } else {
                Nat64.toNat(ExperimentalInternetComputer.performanceCounter(0));
            };
        };

        public func total_instructions_used() : Nat {
            get_instructions();
        };

        public func instructions_used_since_init() : Nat {
            get_instructions() - init_instructions;
        };

        public func instructions_used_since_last_call() : Nat {
            let current_instructions = get_instructions();
            let used = current_instructions - instructions_at_last_call;
            instructions_at_last_call := current_instructions;
            used

        };

    };

    class Cycles(is_running_locally : Bool) {

    };

    public func populate_indexes_in_batch(
        collection : StableCollection,
        batch_id : Nat,
        opt_performance_init : ?Nat,
    ) : Result<(Bool), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("populate_indexes_in_batch");

        let MAX_INSTRUCTIONS = Nat64.toNat(C.MAX_UPDATE_INSTRUCTIONS * 80 / 100);

        let performance = Performance(collection.is_running_locally, opt_performance_init);
        let main_btree_utils = DocumentStore.getBtreeUtils(collection.documents);

        let batch = switch (Map.get(collection.populate_index_batches, Map.nhash, batch_id)) {
            case (?batch) batch;
            case (null) return #err("Batch with id " # debug_show batch_id # " not found");
        };

        var error : ?Text = null;

        log.lazyDebug(func() = "total expected documents in batch: " # debug_show (batch.total_documents));
        log.lazyDebug(func() = "total documents in collection: " # debug_show (StableCollection.size(collection)));
        var documents_to_process = 100; // Start with initial calibration batch size
        var multiplier : Float = 2.0;

        while (multiplier > 1.0) {

            while (
                (performance.total_instructions_used() + (batch.avg_instructions_per_document * documents_to_process)) < MAX_INSTRUCTIONS and
                not batch.done_processing
            ) {
                log.lazyDebug(func() = "Calibrating batch size, trying: " # debug_show documents_to_process);

                log.lazyDebug(func() = "Starting document id: " # debug_show (batch.next_document_to_process));

                switch (process_index_population_batch(collection, batch, batch.next_document_to_process, documents_to_process)) {
                    case (#err(err)) error := ?err;
                    case (#ok(_)) {};
                };

                batch.total_instructions_used += performance.instructions_used_since_last_call();
                log.lazyDebug(func() = " batch.total_instructions_used: " # debug_show (batch.total_instructions_used));

                log.lazyDebug(func() = " batch.indexed_documents: " # debug_show (batch.indexed_documents));

                batch.avg_instructions_per_document := batch.total_instructions_used / (if (batch.indexed_documents == 0) 1 else batch.indexed_documents);
                log.lazyDebug(func() = " batch.avg_instructions_per_document: " # debug_show (batch.avg_instructions_per_document));

                switch (error) {
                    case (?err) {
                        return #err("Failed to populate indexes in batch " # debug_show batch.id # ": " # err);
                    };
                    case (null) {};
                };

                log.lazyDebug(func() = "was there an error? " # debug_show (error));

                log.lazyDebug(func() = "done processing? " # debug_show (batch.done_processing));

                if (batch.done_processing) {
                    batch.num_documents_to_process_per_batch := ?batch.indexed_documents;
                    return #ok(false);
                };

                documents_to_process := Int.abs(Float.toInt(Float.fromInt(documents_to_process) * multiplier));
            };

            multiplier -= 0.5;

        };

        batch.num_documents_to_process_per_batch := ?batch.indexed_documents;
        return #ok(not batch.done_processing);

    };

    func rollback_and_delete_batch_populate_indexes(
        collection : StableCollection,
        batch_id : Nat,
    ) : Result<(), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("rollback_and_delete_batch_populate_indexes");
        log.logInfo(
            "Rolling back batch populate indexes with id: " # debug_show batch_id
        );

        // Get the batch from storage
        let ?batch = Map.get(collection.populate_index_batches, Map.nhash, batch_id) else {
            return #err("Batch with id " # debug_show batch_id # " not found");
        };

        // Remove all indexes from the batch operations map and deallocate them
        for (index in batch.indexes.vals()) {
            let index_name = CommonIndexFns.name(index);

            switch (Map.remove(collection.indexes_in_batch_operations, Map.thash, index_name)) {
                case (?removed_index) {
                    log.lazyDebug(func() = "Deallocating index: " # index_name);
                    CommonIndexFns.deallocate(collection, removed_index);
                };
                case (null) {

                };
            };
        };

        // Remove the batch from storage
        ignore Map.remove<Nat, T.BatchPopulateIndex>(collection.populate_index_batches, Map.nhash, batch_id);

        log.lazyInfo(func() = "Successfully rolled back batch populate indexes with id: " # debug_show batch_id);

        #ok();
    };

    public func create_and_populate_index_in_one_call(
        collection : T.StableCollection,
        index_name : Text,
        index_key_details : [(Text, T.SortDirection)],
        options : T.CreateIndexInternalOptions,
    ) : T.Result<(), Text> {

        let performance = Performance(collection.is_running_locally, null);
        let index_config = [(index_name, index_key_details, options)];

        let batch_id = switch (batch_create_indexes(collection, index_config)) {
            case (#ok(batch_id)) batch_id;
            case (#err(err)) return #err(err);
        };

        label batch_processing loop switch (populate_indexes_in_batch(collection, batch_id, ?(performance.total_instructions_used()))) {
            case (#ok(continue_processing)) {
                if (not continue_processing) break batch_processing;
            };
            case (#err(err)) {
                ignore rollback_and_delete_batch_populate_indexes(collection, batch_id);
                return #err(err);
            };
        };

        #ok();

    };

    public func create_text_index(
        collection : T.StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_name : Text,
        field : Text,
        tokenizer : T.Tokenizer,
    ) : T.Result<T.TextIndex, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("create_text_index");

        let text_index = TextIndex.new(
            collection,
            index_name,
            field,
            tokenizer,
        );

        // todo: wip
        ignore Map.put<Text, T.Index>(collection.indexes, Map.thash, index_name, #text_index(text_index));

        log.lazyInfo(func() = "Successfully created text index: " # index_name);

        #ok(text_index);
    };

    public func clear_index(
        collection : T.StableCollection,
        _main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_name : Text,
    ) : T.Result<(), Text> {

        switch (Map.get(collection.indexes, Map.thash, index_name)) {
            case (?index) switch (index) {
                case (#composite_index(composite_index)) {
                    CompositeIndex.clear(collection, composite_index);
                };
                case (#text_index(text_index)) {
                    TextIndex.clear(collection, text_index);
                };
            };
            case (null) return #err("CompositeIndex not found");
        };

        #ok()

    };

    func internal_populate_indexes(
        collection : T.StableCollection,
        indexes : Buffer.Buffer<T.Index>,
        entries : Iter<(T.DocumentId, Blob)>,
    ) : T.Result<(), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("internal_populate_indexes");
        log.lazyInfo(func() = "Populating " # debug_show indexes.size() # " indexes");

        var count = 0;
        for ((id, candid_blob) in entries) {
            let candid_map = CollectionUtils.get_candid_map_no_cache(collection, id, ?candid_blob);

            for (index in indexes.vals()) {
                switch (CommonIndexFns.insertWithCandidMap(collection, index, id, candid_map)) {
                    case (#err(err)) {
                        return #err("Failed to insert into index '" # CommonIndexFns.name(index) # "': " # err);
                    };
                    case (#ok(_)) {};
                };
            };
            count += 1;
        };

        log.lazyInfo(func() = "Successfully populated indexes with " # debug_show count # " documents");
        #ok();
    };

    func recommended_entries_to_populate_based_on_benchmarks(
        num_indexes : Nat
    ) : Nat {

        let max_instructions = 30 * C.TRILLION; // allows for 10T buffer
        let decode_cost = 300 * C.MILLION; // per entry
        let insert_cost = 150 * C.MILLION; // per entry per index

        // Calculate maximum number of entries
        let max_entries = max_instructions / (decode_cost + insert_cost * num_indexes);

        max_entries;
    };

    public func repopulate_index(
        collection : T.StableCollection,
        _main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_name : Text,
    ) : T.Result<(), Text> {
        repopulate_indexes(collection, _main_btree_utils, [index_name]);
    };

    public func repopulate_indexes(
        collection : T.StableCollection,
        _main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        index_names : [Text],
    ) : T.Result<(), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("repopulate_indexes");

        log.lazyInfo(func() = "Starting to populate indexes: " # debug_show index_names);

        let indexes = Buffer.Buffer<T.Index>(index_names.size());

        for (index_name in index_names.vals()) {
            let ?index = Map.get(collection.indexes, Map.thash, index_name) else {
                return #err("CompositeIndex '" # index_name # "' does not exist");
            };

            indexes.add(index);
        };

        log.lazyDebug(func() = "Collected " # debug_show indexes.size() # " indexes to populate");

        CommonIndexFns.populate_indexes(collection, Buffer.toArray(indexes));

    };

    public func delete_index(
        collection : T.StableCollection,
        index_name : Text,
    ) : T.Result<(), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("delete_index");
        log.logInfo("Deleting index: " # index_name);

        let opt_index = Map.get(collection.indexes, Map.thash, index_name);

        let index = switch (opt_index) {
            case (?index) { index };
            case (null) {
                return #err("CompositeIndex not found");
            };
        };

        let composite_index = switch (index) {
            case (#text_index(text_index)) {
                text_index.internal_index;
            };
            case (#composite_index(composite_index)) {
                composite_index;
            };
        };

        if (composite_index.used_internally) {
            return #err("CompositeIndex '" # index_name # "' cannot be deleted because it is used internally");
        };

        log.lazyDebug(func() = "Clearing and recycling BTree for index: " # index_name);

        BTree.clear(composite_index.data);

        switch (composite_index.data) {
            case (#stableMemory(btree)) {
                Vector.add(collection.freed_btrees, btree);
            };
            case (_) {};
        };

        ignore Map.remove(collection.indexes, Map.thash, index_name);

        #ok();
    };

    let MAX_QUERY_INSTRUCTIONS : Nat64 = 5_000_000_000;
    let MAX_UPDATE_INSTRUCTIONS : Nat64 = 40_000_000_000;

    func paginate(collection : T.StableCollection, eval : EvalResult, skip : Nat, opt_limit : ?Nat) : Iter<T.DocumentId> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("paginate");

        let iter = switch (eval) {
            case (#Empty) {
                log.lazyDebug(func() = "Empty iterator");
                return Itertools.empty<T.DocumentId>();
            };
            case (#BitMap(bitmap)) {
                log.lazyDebug(func() = "Bitmap iterator");
                let document_ids = Iter.map<Nat, T.DocumentId>(
                    bitmap.vals(),
                    func(n : Nat) : T.DocumentId {
                        CollectionUtils.convert_bitmap_8_byte_to_document_id(collection, n);
                    },
                );
            };
            case (#Ids(iter)) {
                log.lazyDebug(func() = "Ids iterator");
                Iter.map<(T.DocumentId, ?[(Text, T.Candid)]), T.DocumentId>(iter, func((id, _)) : T.DocumentId { id });
            };
            case (#Interval(index_name, _intervals, sorted_in_reverse)) {
                log.lazyDebug(func() = "Interval iterator");

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
        collection : T.StableCollection,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
        opt_updated_fields : ?[Text],
    ) : T.Result<(), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("validate_schema_constraints_on_updated_fields");

        let field_constraints_iter = switch (opt_updated_fields) {
            case (?updated_fields) {
                let buffer = Buffer.Buffer<(Text, [T.SchemaFieldConstraint])>(updated_fields.size());

                for (field_name in updated_fields.vals()) {

                    switch (Map.get(collection.field_constraints, Map.thash, field_name)) {
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
                    switch (Map.get(collection.fields_with_unique_constraints, Map.thash, field_name)) {
                        case (?unique_constraints_indexes_set) {
                            for (unique_constraint_index in Set.keys(unique_constraints_indexes_set)) {
                                Set.add(new_unique_constraints_indexes, Map.nhash, unique_constraint_index);
                            };
                        };
                        case (null) {};
                    };
                };

                Iter.map<Nat, ([Text], T.CompositeIndex)>(
                    Set.keys(new_unique_constraints_indexes),
                    func(unique_constraint_index : Nat) : ([Text], T.CompositeIndex) {
                        collection.unique_constraints[unique_constraint_index];
                    },
                );
            };
            case (null) collection.unique_constraints.vals();
        };

        label validating_unique_constraints for ((composite_field_keys, index) in unique_constraints_iter) {

            let ?compsite_field_values = CollectionUtils.getIndexColumns(collection, index.key_details, document_id, candid_map) else continue validating_unique_constraints;
            let index_data_utils = CompositeIndex.get_index_data_utils(collection);
            log.lazyDebug(func() = "compsite_field_values: " # debug_show compsite_field_values);

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

    public func replace_by_id<Record>(
        collection : T.StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        document_id : T.DocumentId,
        new_candid_blob : T.CandidBlob,
    ) : T.Result<T.ReplaceByIdResult, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("replace_by_id");
        let performance = Performance(collection.is_running_locally, null);

        log.logInfo("Replacing document with id: " # debug_show document_id);

        let prev_candid_map = CollectionUtils.get_candid_map_no_cache(collection, document_id, null);

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

        assert Option.isSome(DocumentStore.put(collection.documents, main_btree_utils, document_id, new_candid_blob));

        for (index in Map.vals(collection.indexes)) {
            let #ok(_) = update_indexed_document_fields(collection, index, document_id, new_candid_map, ?prev_candid_map) else {
                return #err("Failed to update index data");
            };
        };

        ignore CollectionUtils.remove_candid_map_from_cache(collection, document_id);

        log.lazyInfo(func() = "Successfully replaced document with id: " # debug_show document_id);

        let instructions = performance.total_instructions_used();

        #ok({
            instructions = instructions;
        });
    };

    func partially_update_doc(collection : T.StableCollection, candid_map : T.CandidMap, update_operations : [(Text, T.FieldUpdateOperations)]) : T.Result<Candid, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("partially_update_doc");
        log.lazyInfo(func() = "Partially updating document with operations: " # debug_show update_operations);

        for ((field_name, op) in update_operations.vals()) {
            let ?field_type = SchemaMap.get(collection.schema_map, field_name) else return #err("Field type '" # field_name # "' not found in document");
            let ?prev_candid_value = CandidMap.get(candid_map, collection.schema_map, field_name) else return #err("Field '" # field_name # "' not found in document");

            let new_value = switch (UpdateOps.handleFieldUpdateOperation(collection, candid_map, field_type, prev_candid_value, op)) {
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

        log.lazyDebug(func() = "Updated candid map, about to extract candid");

        let candid = CandidMap.extract_candid(candid_map);

        #ok(candid);
    };

    func update_indexed_document_fields(
        collection : T.StableCollection,
        index : T.Index,
        document_id : T.DocumentId,
        new_document_candid_map : T.CandidMap,
        opt_prev_document_candid_map : ?T.CandidMap,
    ) : T.Result<(), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("update_indexed_document_fields");

        ignore do ? {
            let prev_document_candid_map = opt_prev_document_candid_map!;

            switch (CommonIndexFns.removeWithCandidMap(collection, index, document_id, prev_document_candid_map)) {
                case (#err(err)) {
                    return #err(err);
                };
                case (#ok(_)) {};
            };

        };

        log.lazyDebug(func() = "Updating index for id: " # debug_show document_id);

        switch (CommonIndexFns.insertWithCandidMap(collection, index, document_id, new_document_candid_map)) {
            case (#err(err)) {
                return #err("Failed to insert into index '" # CommonIndexFns.name(index) # "': " # err);
            };
            case (#ok(_)) {};
        };

        #ok;
    };

    func update_indexed_data_on_updated_fields(collection : T.StableCollection, document_id : T.DocumentId, prev_document_candid_map : T.CandidMap, new_document_candid_map : T.CandidMap, updated_fields : [Text]) : T.Result<(), Text> {

        let updated_fields_set = Set.fromIter(updated_fields.vals(), Map.thash);

        for (index in Map.vals(collection.indexes)) {
            let internal_index = CommonIndexFns.get_internal_index(index);

            for ((index_key, _) in internal_index.key_details.vals()) {

                // only updates the fields that were changed
                if (Set.has(updated_fields_set, Map.thash, index_key)) {
                    let #ok(_) = update_indexed_document_fields(collection, index, document_id, new_document_candid_map, ?prev_document_candid_map) else return #err("Failed to update index data");
                };
            };
        };

        #ok;
    };

    public func update_by_id<Record>(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, id : T.DocumentId, field_updates : [(Text, T.FieldUpdateOperations)]) : T.Result<T.UpdateByIdResult, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("update_by_id");
        let performance = Performance(collection.is_running_locally, null);

        log.lazyInfo(func() = "Updating document with id: " # debug_show id);

        let prev_candid_map = CollectionUtils.get_candid_map_no_cache(collection, id, null);

        let fields_with_updates = Array.map<(Text, T.FieldUpdateOperations), Text>(field_updates, func(k, _) = k);

        log.lazyDebug(func() = "Performing partial update on fields: " # debug_show (fields_with_updates));

        let new_candid_map = CandidMap.clone(prev_candid_map, collection.schema_map);

        let new_candid_document = switch (partially_update_doc(collection, new_candid_map, field_updates)) {
            case (#ok(new_candid_document)) new_candid_document;
            case (#err(msg)) {
                return #err("Failed to update fields: " # msg);
            };
        };

        log.lazyDebug(func() = "Updated candid map: " # debug_show new_candid_document);

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

        let new_candid_blob = switch (Candid.TypedSerializer.encode(collection.candid_serializer, [new_candid_document])) {
            case (#ok(new_candid_blob)) new_candid_blob;
            case (#err(msg)) {
                return #err("Failed to encode new candid blob: " # msg);
            };
        };

        assert Option.isSome(DocumentStore.put(collection.documents, main_btree_utils, id, new_candid_blob));

        let updated_keys = Array.map<(Text, Any), Text>(
            field_updates,
            func(field_name : Text, _ : Any) : Text { field_name },
        );

        let #ok(_) = update_indexed_data_on_updated_fields(collection, id, prev_candid_map, new_candid_map, updated_keys) else {
            return #err("Failed to update index data");
        };

        ignore CollectionUtils.remove_candid_map_from_cache(collection, id);

        log.lazyInfo(
            func() = "Successfully updated document with id: " # debug_show id
        );

        let instructions = performance.total_instructions_used();

        #ok({
            instructions = instructions;
        });
    };

    public func insert(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, candid_blob : T.CandidBlob) : T.Result<T.DocumentId, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("insert");

        let next_id = Ids.next(collection.ids);
        let next_id_as_blob = Blob.fromArray(ByteUtils.BigEndian.fromNat64(Nat64.fromNat(next_id)));
        let document_id = Utils.concat_blob(collection.instance_id, next_id_as_blob);

        log.lazyInfo(func() = "ZenDB Collection.put(): Inserting document with id " # debug_show document_id);

        let candid = CollectionUtils.decodeCandidBlob(collection, candid_blob);

        log.lazyDebug(func() = "ZenDB Collection.put(): Inserting document with id " # debug_show document_id # " and candid " # debug_show candid);

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

        log.lazyDebug(
            func() = "Total indexes: " # debug_show (Map.size(collection.indexes))
        );

        if (Map.size(collection.indexes) == 0) return #ok(document_id);

        let updated_indexes = Buffer.Buffer<T.Index>(Map.size(collection.indexes));

        label updating_indexes for (index in Map.vals(collection.indexes)) {
            let res = update_indexed_document_fields(collection, index, document_id, candid_map, null);

            switch (res) {
                case (#err(err)) {
                    for (index in updated_indexes.vals()) {
                        ignore CommonIndexFns.removeWithCandidMap(collection, index, document_id, candid_map);
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

    public func insert_docs(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        documents : [T.CandidBlob],
    ) : Result<[T.DocumentId], Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("insert_docs");
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
                                log.lazyError(func() = "Failed to rollback document with id: " # debug_show (id) # ": " # err);
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
        collection : T.StableCollection,
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
        collection : T.StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        stable_query : T.StableQuery,
    ) : T.Result<T.SearchResult<T.CandidBlob>, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("search");
        let performance = Performance(collection.is_running_locally, null);

        log.lazyDebug(func() = "Executing search with query: " # debug_show (stable_query));

        switch (internal_search(collection, stable_query)) {
            case (#err(err)) {
                return #err("Search failed: " # err);
            };
            case (#ok(document_ids_iter)) {
                let candid_blob_iter = ids_to_candid_blobs(collection, document_ids_iter);
                let candid_blobs = Iter.toArray(candid_blob_iter);
                log.lazyDebug(func() = "Search completed, found " # debug_show (candid_blobs.size()) # " results");

                let instructions = performance.total_instructions_used();

                #ok({
                    documents = candid_blobs;
                    instructions = instructions;
                });
            };
        };
    };

    public func evaluate_query(collection : T.StableCollection, stable_query : T.StableQuery) : T.Result<Iter<T.DocumentId>, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("evaluate_query");
        log.lazyDebug(func() = "Evaluating query with operations: " # debug_show (stable_query.query_operations));

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

        log.lazyDebug(
            func() = "Formatted query operations: " # debug_show formatted_query_operations
        );

        let query_plan : T.QueryPlan = QueryPlan.create_query_plan(
            collection,
            formatted_query_operations,
            sort_by,
            opt_cursor,
            // cursor_map,
        );

        let sort_documents_by_field_cmp = switch (sort_by) {
            case (?sort_by) get_document_field_cmp(collection, sort_by);
            case (null) func(_ : (T.DocumentId, ?[(Text, T.Candid)]), _ : (T.DocumentId, ?[(Text, T.Candid)])) : Order = #equal;
        };

        let eval = QueryExecution.generate_document_ids_for_query_plan(collection, query_plan, sort_by, sort_documents_by_field_cmp);
        let iter = paginate(collection, eval, Option.get(pagination.skip, 0), pagination.limit);

        log.lazyDebug(func() = "Query evaluation completed");
        return #ok((iter));
    };

    public func internal_search(collection : T.StableCollection, stable_query : T.StableQuery) : T.Result<Iter<T.DocumentId>, Text> {
        // let stable_query = query_builder.build();
        switch (evaluate_query(collection, stable_query)) {
            case (#err(err)) return #err(err);
            case (#ok(eval_result)) #ok(eval_result);
        };
    };

    public func ids_to_documents<Record>(collection : T.StableCollection, blobify : InternalCandify<Record>, iter : Iter<T.DocumentId>) : Iter<(T.DocumentId, Record)> {
        Iter.map<T.DocumentId, (T.DocumentId, Record)>(
            iter,
            func(id : T.DocumentId) : (T.DocumentId, Record) {
                let document = CollectionUtils.lookupDocument<Record>(collection, blobify, id);
                (id, document);
            },
        );
    };

    public func ids_to_candid_blobs<Record>(collection : T.StableCollection, iter : Iter<T.DocumentId>) : Iter<(T.DocumentId, T.CandidBlob)> {
        Iter.map<T.DocumentId, (T.DocumentId, T.CandidBlob)>(
            iter,
            func(id : T.DocumentId) : (T.DocumentId, T.CandidBlob) {
                let candid_blob = CollectionUtils.lookupCandidBlob(collection, id);
                (id, candid_blob);
            },
        );
    };

    public func search_iter(
        collection : T.StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        stable_query : T.StableQuery,
    ) : T.Result<Iter<T.WrapId<T.CandidBlob>>, Text> {
        switch (internal_search(collection, stable_query)) {
            case (#err(err)) return #err(err);
            case (#ok(document_ids_iter)) {
                let document_iter = ids_to_candid_blobs(collection, document_ids_iter);
                #ok(document_iter);
            };
        };
    };

    public func get_document_field_cmp(
        collection : T.StableCollection,
        sort_field : (Text, T.SortDirection),
    ) : ((T.DocumentId, ?[(Text, T.Candid)]), (T.DocumentId, ?[(Text, T.Candid)])) -> Order {

        func sort_documents_by_field_cmp(a : (T.DocumentId, ?[(Text, T.Candid)]), b : (T.DocumentId, ?[(Text, T.Candid)])) : Order {

            func get_value((id, opt_sort_value) : (T.DocumentId, ?[(Text, T.Candid)])) : T.Candid {
                switch (opt_sort_value) {
                    case (?fields) {
                        // Find the field matching sort_field.0
                        let opt_entry = Array.find<(Text, T.Candid)>(fields, func((key, _)) : Bool { key == sort_field.0 });
                        switch (opt_entry) {
                            case (?candid_value) return candid_value.1;
                            case (null) {};
                        };
                    };
                    case (null) {};
                };

                // Sort field not in indexed fields, fetch from document
                let ?candid_value = CandidMap.get(CollectionUtils.get_and_cache_candid_map(collection, id), collection.schema_map, sort_field.0) else {
                    Debug.trap("Couldn't get value from CandidMap for key: " # sort_field.0);
                };

                candid_value;
            };

            let value_a = get_value(a);
            let value_b = get_value(b);

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

    public func stats(collection : T.StableCollection) : T.CollectionStats {

        let main_collection_memory : T.MemoryBTreeStats = BTree.getMemoryStats(collection.documents);

        let total_documents = StableCollection.size(collection);

        let indexes : [T.IndexStats] = Iter.toArray(
            Iter.map<(Text, T.Index), T.IndexStats>(
                Map.entries(collection.indexes),
                func((index_name, index) : (Text, T.Index)) : T.IndexStats {
                    let internal_index = CommonIndexFns.get_internal_index(index);
                    CompositeIndex.stats(internal_index, total_documents);
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

    public func count(collection : T.StableCollection, stable_query : T.StableQuery) : T.Result<T.CountResult, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("count");
        let performance = Performance(collection.is_running_locally, null);

        log.lazyDebug(func() = "Counting documents with query: " # debug_show (stable_query));

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

        let instructions = performance.total_instructions_used();

        #ok({
            count = count;
            instructions = instructions;
        });
    };

    public func exists(collection : T.StableCollection, query_builder : QueryBuilder) : T.Result<Bool, Text> {
        let stable_query = query_builder.Limit(1).build();

        let query_plan = QueryPlan.create_query_plan(
            collection,
            stable_query.query_operations,
            null,
            null,
        );

        let sort_documents_by_field_cmp = func(_ : (T.DocumentId, ?[(Text, T.Candid)]), _ : (T.DocumentId, ?[(Text, T.Candid)])) : Order = #equal;

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

    public func delete_by_id(collection : T.StableCollection, main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>, id : T.DocumentId) : T.Result<T.DeleteByIdResult<T.CandidBlob>, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("delete_by_id");
        let performance = Performance(collection.is_running_locally, null);

        log.lazyInfo(func() = "Deleting document with id: " # debug_show id);

        let ?prev_candid_blob = DocumentStore.remove(collection.documents, main_btree_utils, id) else {
            return #err("Record not found");
        };

        let prev_candid_map = CollectionUtils.get_candid_map_no_cache(collection, id, ?prev_candid_blob);

        for ((index_name, index) in Map.entries(collection.indexes)) {

            switch (CommonIndexFns.removeWithCandidMap(collection, index, id, prev_candid_map)) {
                case (#err(err)) {
                    return #err("Failed to remove from index '" # index_name # "': " # err);
                };
                case (#ok(_)) {};
            };
        };

        ignore CollectionUtils.remove_candid_map_from_cache(collection, id);

        log.lazyInfo(func() = "Successfully deleted document with id: " # debug_show id);

        let instructions = performance.total_instructions_used();

        #ok({
            deleted_document = prev_candid_blob;
            instructions = instructions;
        });
    };

    public func deallocate(collection : T.StableCollection) : () {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("deallocate");

        log.logInfo("Deallocating collection: " # collection.name);

        for (index in Itertools.chain(Map.vals(collection.indexes), Map.vals(collection.indexes_in_batch_operations))) {
            CommonIndexFns.deallocate(collection, index);
        };

        BTree.clear(collection.documents);

        switch (collection.documents) {
            case (#stableMemory(memory_btree)) {
                Vector.add(collection.freed_btrees, memory_btree);
            };
            case (#heap(_)) {};
        };

        log.logInfo("Successfully deallocated collection: " # collection.name);

    };

    /// Updates multiple documents matching a query with the given update operations
    public func update_documents(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        stable_query : T.StableQuery,
        update_operations : [(Text, T.FieldUpdateOperations)],
    ) : Result<T.UpdateResult, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("update_documents");
        let performance = Performance(collection.is_running_locally, null);

        let documents_iter = switch (internal_search(collection, stable_query)) {
            case (#err(err)) {
                log.lazyError(func() = "Failed to find documents to update: " # err);
                return #err("Failed to find documents to update: " # err);
            };
            case (#ok(documents_iter)) documents_iter;
        };

        var total_updated = 0;

        for (id in documents_iter) {
            switch (update_by_id(collection, main_btree_utils, id, update_operations)) {
                case (#ok(_)) total_updated += 1;
                case (#err(err)) {
                    log.lazyError(func() = "Failed to update document with id: " # debug_show (id) # ": " # err);
                    return #err("Failed to update document with id: " # debug_show (id) # ": " # err);
                };
            };
        };

        let instructions = performance.total_instructions_used();

        #ok({
            updated_count = total_updated;
            instructions = instructions;
        });
    };

    /// Replaces multiple documents by their ids
    public func replace_docs(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        documents : [(T.DocumentId, Blob)],
    ) : Result<T.ReplaceDocsResult, Text> {
        let performance = Performance(collection.is_running_locally, null);

        for ((id, candid_blob) in documents.vals()) {
            switch (replace_by_id(collection, main_btree_utils, id, candid_blob)) {
                case (#ok(_)) {};
                case (#err(err)) return #err(err);
            };
        };

        let instructions = performance.total_instructions_used();

        #ok({
            instructions = instructions;
        });
    };

    /// Deletes multiple documents matching a query
    public func delete_documents<Record>(
        collection : StableCollection,
        main_btree_utils : T.BTreeUtils<T.DocumentId, T.Document>,
        blobify : T.InternalCandify<Record>,
        stable_query : T.StableQuery,
    ) : Result<T.DeleteResult<Record>, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("delete_documents");
        let performance = Performance(collection.is_running_locally, null);

        let results_iter = switch (internal_search(collection, stable_query)) {
            case (#err(err)) {
                log.lazyError(func() = "Failed to find documents to delete: " # err);
                return #err("Failed to find documents to delete: " # err);
            };
            case (#ok(documents_iter)) documents_iter;
        };

        // need to convert the iterator to an array before deleting
        // to avoid invalidating the iterator as its reference in the btree
        // might slide when elements are deleted.
        let results = Iter.toArray(results_iter);

        let buffer = Buffer.Buffer<(T.DocumentId, Record)>(8);
        for (id in results.vals()) {
            switch (delete_by_id(collection, main_btree_utils, id)) {
                case (#ok(result)) {
                    let document = blobify.from_blob(result.deleted_document);
                    buffer.add(id, document);
                };
                case (#err(err)) return #err(err);
            };
        };

        let instructions = performance.total_instructions_used();

        #ok({
            deleted_documents = Buffer.toArray(buffer);
            instructions = instructions;
        });
    };

    /// Creates a batch populate operation from index names
    public func batch_populate_indexes_from_names(
        collection : StableCollection,
        index_names : [Text],
    ) : Result<(batch_id : Nat), Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("batch_populate_indexes_from_names");
        var error : ?Text = null;

        let index_configs = Array.map<Text, T.CreateInternalIndexParams>(
            index_names,
            func(name : Text) : T.CreateInternalIndexParams {
                switch (Map.get(collection.indexes, Map.thash, name)) {
                    case (?index) switch (index) {
                        case (#composite_index(composite_index)) {
                            (
                                name,
                                composite_index.key_details,
                                {
                                    is_unique = composite_index.is_unique;
                                    used_internally = composite_index.used_internally;
                                },
                            );
                        };
                        case (#text_index(text_index)) {
                            (
                                text_index.internal_index.name,
                                text_index.internal_index.key_details,
                                {
                                    is_unique = text_index.internal_index.is_unique;
                                    used_internally = text_index.internal_index.used_internally;
                                },
                            );
                        };
                    };
                    case (null) {
                        error := ?("Could not find index with name: " # name);
                        ("", [], T.CreateIndexOptions.internal_default()); // dummy return to satisfy the type checker
                    };
                };
            },
        );

        switch (error) {
            case (?err) return #err(err);
            case (null) {};
        };

        switch (create_populate_indexes_batch(collection, index_configs, null)) {
            case (#err(err)) {
                log.lazyError(func() = "Failed to create populate index batch: " # err);
                #err("Failed to create populate index batch: " # err);
            };
            case (#ok(batch_id)) #ok(batch_id);
        };
    };

};
