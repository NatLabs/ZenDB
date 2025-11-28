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

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.4.0";
import Candid "mo:serde@3.4.0/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import SparseBitMap64 "mo:bit-map@1.1.0/SparseBitMap64";
import Vector "mo:vector@0.4.2";

import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";
import Int8Cmp "mo:memory-collection@0.3.2/TypeUtils/Int8Cmp";
import BpTree "mo:augmented-btrees@0.7.1/BpTree";
import BpTreeTypes "mo:augmented-btrees@0.7.1/BpTree/Types";
import LruCache "mo:lru-cache@2.0.0";

import TypeMigrations "TypeMigrations";

module T {

    public type VersionedStableStore = TypeMigrations.VersionedStableStore;
    public type PrevVersionedStableStore = TypeMigrations.PrevVersionedStableStore;

    public type Vector<A> = Vector.Vector<A>;
    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    public let { thash; bhash; nhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    public type Order = Order.Order;

    public type BitMap = SparseBitMap64.SparseBitMap64;
    public type SparseBitMap64 = SparseBitMap64.SparseBitMap64;

    public type Candid = Serde.Candid;

    public type CandidQuery = Serde.Candid or {
        #Minimum;
        #Maximum;
    };

    public type CandidType = Serde.CandidType;

    public type Interval = (Nat, Nat);

    public type Candify<A> = {
        from_blob : Blob -> ?A;
        to_blob : A -> Blob;
    };

    public type InternalCandify<A> = {
        from_blob : Blob -> A;
        to_blob : A -> Blob;
    };

    public type MemoryBTree = MemoryBTree.StableMemoryBTree;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;
    public type Blobify<A> = TypeUtils.Blobify<A>;
    public type MemoryBTreeUtils<K, V> = MemoryBTree.BTreeUtils<K, V>;

    public type ExpectedIndex = BpTreeTypes.ExpectedIndex;

    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type DocumentId = Blob;
    public type CandidDocument = [(Text, Candid)];
    public type CandidBlob = Blob;

    public type Tuple<A, B> = { _0_ : A; _1_ : B };
    public func Tuple<A, B>(a : A, b : B) : Tuple<A, B> {
        { _0_ = a; _1_ = b };
    };

    public type Triple<A, B, C> = { _0_ : A; _1_ : B; _2_ : C };
    public func Triple<A, B, C>(a : A, b : B, c : C) : Triple<A, B, C> {
        { _0_ = a; _1_ = b; _2_ = c };
    };

    public type Quadruple<A, B, C, D> = { _0_ : A; _1_ : B; _2_ : C; _3_ : D };
    public func Quadruple<A, B, C, D>(a : A, b : B, c : C, d : D) : Quadruple<A, B, C, D> {
        { _0_ = a; _1_ = b; _2_ = c; _3_ = d };
    };

    public type Quintuple<A, B, C, D, E> = {
        _0_ : A;
        _1_ : B;
        _2_ : C;
        _3_ : D;
        _4_ : E;
    };
    public func Quintuple<A, B, C, D, E>(a : A, b : B, c : C, d : D, e : E) : Quintuple<A, B, C, D, E> {
        { _0_ = a; _1_ = b; _2_ = c; _3_ = d; _4_ = e };
    };

    public type SortDirection = {
        #Ascending;
        #Descending;
    };

    public type CreateIndexSortDirection = {
        #Ascending;
    };

    public type SchemaMap = {
        map : Map.Map<Text, T.Schema>;
        fields_with_array_type : [Text];
    };

    public type ResolvedConstraints = {

    };

    public type BTree<K, V> = {
        #stableMemory : T.MemoryBTree;
        #heap : BpTree.BpTree<Blob, V>;
    };

    public type BpTreeUtils<K> = {
        blobify : TypeUtils.Blobify<K>;
        cmp : BpTreeTypes.CmpFn<Blob>;
    };

    public type BTreeUtils<K, V> = {
        #stableMemory : MemoryBTree.BTreeUtils<K, V>;
        #heap : BpTreeUtils<K>;
    };

    public type Ids = [var Nat];

    public type Document = {
        #v0 : CandidBlob;
    };

    public type CompositeIndex = {
        name : Text;
        key_details : [(Text, SortDirection)];

        data : BTree<[CandidQuery], DocumentId>; // using CandidQuery here for comparison only, the actual data stored here is Candid
        used_internally : Bool; // if true, the index cannot be deleted by user if true
        is_unique : Bool; // if true, the index is unique and the document ids are not concatenated with the index key values to make duplicate values appear unique
    };

    public type TextIndex = {
        internal_index : T.CompositeIndex;
        field : Text; // the field this index is on
        tokenizer : Tokenizer; // the tokenizer used for this index
    };

    public type Index = {
        #text_index : T.TextIndex;
        #composite_index : T.CompositeIndex;
    };

    public type DocumentStore = BTree<DocumentId, Document>;

    public type IndexConfig = {
        name : Text;
        key_details : [(Text, SortDirection)];
        is_unique : Bool;
        used_internally : Bool;
    };

    public type CreateIndexParams = (
        name : Text,
        key_details : [(field : Text, CreateIndexSortDirection)],
        create_index_options : ?T.CreateIndexOptions,
    );

    public type CreateInternalIndexParams = (
        name : Text,
        key_details : [(field : Text, CreateIndexSortDirection)],
        create_index_options : T.CreateIndexInternalOptions,
    );

    public type BatchPopulateIndex = {
        id : Nat;
        indexes : [Index];
        var indexed_documents : Nat;
        total_documents : Nat;

        var avg_instructions_per_document : Nat;
        var total_instructions_used : Nat;

        var next_document_to_process : DocumentId;

        var num_documents_to_process_per_batch : ?Nat;
        var done_processing : Bool;

    };

    public type TwoQueueCache<K, V> = {
        var main_cache : LruCache.LruCache<K, V>; // stores frequently accessed items
        var ghost_cache : LruCache.LruCache<K, V>; // items that were recently evicted from the main cache
        var admission_cache : LruCache.LruCache<K, ()>; // items in line for the main cache (keys only)
    };

    public type StableCollection = {
        ids : Ids;
        instance_id : Blob;
        name : Text;
        schema : Schema;
        schema_map : SchemaMap;
        schema_keys : [Text];
        schema_keys_set : Set<Text>;

        documents : DocumentStore;
        indexes : Map<Text, Index>;
        indexes_in_batch_operations : Map<Text, Index>;
        populate_index_batches : Map<Nat, BatchPopulateIndex>;
        hidden_indexes : Set<Text>;

        candid_serializer : Candid.TypedSerializer;

        field_constraints : Map<Text, [SchemaFieldConstraint]>;
        unique_constraints : [([Text], CompositeIndex)];
        fields_with_unique_constraints : Map<Text, Set<Nat>>; // the value is the index of the unique constraint in the unique_constraints list

        // reference to the freed btrees to the same variable in
        // the ZenDB database document
        candid_map_cache : TwoQueueCache<T.DocumentId, T.CandidMap>;
        freed_btrees : Vector.Vector<MemoryBTree.StableMemoryBTree>;
        logger : Logger;
        memory_type : MemoryType;
        is_running_locally : Bool;
    };

    type NestedCandid = {
        #Candid : (T.Schema, Candid);
        #CandidMap : (Map.Map<Text, NestedCandid>);
    };

    public type CandidMap = {
        candid_map : Map.Map<Text, NestedCandid>;
    };

    public type MemoryType = {
        #heap;
        #stableMemory;
    };

    public type StableDatabase = {
        ids : Ids;
        instance_id : Blob;
        name : Text;
        collections : Map<Text, StableCollection>;
        memory_type : MemoryType;

        // reference to the freed btrees to the same variable in
        // the ZenDB database document
        candid_map_cache : TwoQueueCache<T.DocumentId, T.CandidMap>;
        freed_btrees : Vector.Vector<MemoryBTree.StableMemoryBTree>;
        logger : Logger;
        is_running_locally : Bool;
    };

    public type StableStore = {
        ids : Ids;
        canister_id : Principal;

        /// First 4 bytes of the canister id
        /// This id is concatenated with each document id to ensure uniqueness across canisters
        instance_id : Blob;
        databases : Map<Text, StableDatabase>;
        candid_map_cache : TwoQueueCache<T.DocumentId, T.CandidMap>;

        memory_type : MemoryType;
        freed_btrees : Vector.Vector<MemoryBTree.StableMemoryBTree>;
        logger : Logger;
        is_running_locally : Bool;
    };

    public type LogLevel = {
        #Debug;
        #Info;
        #Warn;
        #Error;
        #Trap;
    };

    public type Logger = {
        var log_level : LogLevel;
        var next_thread_id : Nat;
        var is_running_locally : Bool;
    };

    public type IndexKeyFields = [(Text, Candid)];

    public type FieldLimit = (Text, ?State<CandidQuery>);
    public type DocumentLimits = [(Text, ?State<CandidQuery>)];
    public type Bounds = (DocumentLimits, DocumentLimits);

    public type ZqlOperators = {
        #eq : Candid;
        #gte : Candid;
        #lte : Candid;
        #lt : Candid;
        #gt : Candid;

        #anyOf : [Candid];
        #not_ : ZqlOperators;

        #between : (Candid, Candid); // [min, max] - both inclusive
        #betweenExclusive : (Candid, Candid); // (min, max) - both exclusive
        #betweenLeftOpen : (Candid, Candid); // (min, max] - min exclusive, max inclusive
        #betweenRightOpen : (Candid, Candid); // [min, max) - min inclusive, max exclusive
        #exists;
        #startsWith : Candid;

    };

    public type ZenQueryLang = {

        #Operation : (Text, ZqlOperators);
        #And : [ZenQueryLang];
        #Or : [ZenQueryLang];

    };

    public type PaginationToken = { last_document_id : ?DocumentId };

    public type PaginationDirection = {
        #Forward;
        #Backward;
    };

    public type StableQueryPagination = {
        cursor : ?T.PaginationToken;
        limit : ?Nat;
        skip : ?Nat;
    };
    public type StableQuery = {
        query_operations : ZenQueryLang;
        pagination : StableQueryPagination;
        sort_by : ?(Text, SortDirection);
    };
    public type Operator = {
        #Eq;
        #Gt;
        #Lt;
    };

    public type WrapId<Document> = (DocumentId, Document);

    public type State<T> = {
        #Inclusive : T;
        #Exclusive : T;
    };

    public type CandidInclusivityQuery = State<CandidQuery>;

    public type FullScanDetails = {
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        scan_bounds : Bounds;
        filter_bounds : Bounds;
    };

    public type IndexScanDetails = {
        index_name : Text;
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        sorted_in_reverse : Bool;
        interval : Interval;
        scan_bounds : Bounds;
        filter_bounds : Bounds;
        simple_operations : [(Text, T.ZqlOperators)];
    };

    public type IndexIntersectionDetails = {

    };
    public type ScanDetails = {
        #IndexScan : IndexScanDetails;
        #FullScan : FullScanDetails;
    };
    public type QueryPlan = {
        is_and_operation : Bool;
        subplans : [QueryPlan]; // result of nested #And/#Or operations
        simple_operations : [(Text, T.ZqlOperators)];
        scans : [ScanDetails]; // scan results from simple #Operation
    };

    public type QueryPlanResult = {
        query_plan : QueryPlan;
        opt_last_pagination_document_id : ?T.DocumentId;
    };

    public type CreateIndexOptions = {
        is_unique : Bool;
    };

    public type CreateIndexInternalOptions = CreateIndexOptions and {
        used_internally : Bool;
    };

    public module CreateIndexOptions {
        public func default() : CreateIndexOptions {
            { is_unique = false };
        };

        public func internal_default() : CreateIndexInternalOptions {
            { default() with used_internally = false };
        };

        public func to_internal_default(options : CreateIndexOptions) : CreateIndexInternalOptions {
            { is_unique = options.is_unique; used_internally = false };
        };

        public func internal_from_opt(opt_options : ?CreateIndexOptions) : CreateIndexInternalOptions {
            switch (opt_options) {
                case (?options) {
                    to_internal_default(options);
                };
                case (null) {
                    internal_default();
                };
            };
        };
    };

    public type CreateCollectionOptions = {
        schema_constraints : [T.SchemaConstraint];
    };

    public module CreateCollectionOptions {
        public func default() : CreateCollectionOptions {
            { schema_constraints = [] };
        };
    };

    /// MemoryBTree Stats
    ///
    /// ## Memory BTree Statistics
    ///
    /// ### Memory Allocation
    /// - **allocatedPages**: Total pages allocated across all regions
    ///   - Data region pages
    ///   - Values region pages
    ///   - Leaves region pages
    ///   - Branches region pages
    /// - **bytesPerPage**: Size of each memory page
    /// - **allocatedBytes**: Total bytes available from allocated pages
    ///   - Sum of capacity across all regions (data + values + leaves + branches)
    /// - **usedBytes**: Bytes currently in use across all regions
    ///   - Sum of allocated bytes across all regions (data + values + leaves + branches)
    /// - **freeBytes**: Unused bytes (allocatedBytes - usedBytes)
    ///
    /// ### Data Storage
    /// - **dataBytes**: Bytes used for storing keys and values
    ///   - Data region (keys)
    ///   - Values region (values)
    /// - **keyBytes**: Bytes used specifically for storing keys (data region)
    /// - **valueBytes**: Bytes used specifically for storing values (values region)
    ///
    /// ### Metadata Storage
    /// - **metadataBytes**: Bytes used for internal nodes
    ///   - Leaves region
    ///   - Branches region
    /// - **leafBytes**: Bytes used specifically for leaf nodes
    /// - **branchBytes**: Bytes used specifically for branch nodes
    ///
    /// ### Node Counts
    /// - **leafCount**: Number of leaf nodes in the BTree
    /// - **branchCount**: Number of branch nodes in the BTree
    /// - **totalNodeCount**: Total number of nodes (leafCount + branchCount)

    public type MemoryBTreeStats = MemoryBTree.MemoryBTreeStats;

    public type IndexStats = {
        /// The name of the index
        name : Text;

        /// Composite index fields selected for the index
        fields : [(Text, SortDirection)];

        /// The number of documents in the index
        entries : Nat;

        /// The memory information for the index
        memory : MemoryBTreeStats;

        /// Flag indicating if the index is unique
        is_unique : Bool;

        /// Flag indicating if the index is used internally (these indexes cannot be deleted by user)
        used_internally : Bool;

        /// Flag indicating if the index is hidden from queries
        hidden : Bool;

        /// The average size in bytes of an index key
        avg_index_key_size : Nat;

        /// The total size in bytes of all index keys
        total_index_key_size : Nat;

        total_index_data_bytes : Nat;

    };

    public type CollectionStats = {
        /// The name of the collection
        name : Text;

        /// The schema of the collection
        schema : Schema;

        /// The collection's memory type
        memoryType : MemoryType;

        /// The number of documents in the collection
        entries : Nat;

        /// The btree's memory information for the collection
        memory : MemoryBTreeStats;

        /// The index information for the collection
        indexes : [IndexStats];

        /// The average size in bytes of a document in the collection
        avg_document_size : Nat;

        /// The total size in bytes of all documents in the collection
        total_document_size : Nat;

        total_allocated_bytes : Nat;

        total_used_bytes : Nat;

        total_free_bytes : Nat;

        total_data_bytes : Nat;

        total_metadata_bytes : Nat;

        total_document_store_bytes : Nat;

        /// The total size in bytes of all indexes in the collection
        total_index_store_bytes : Nat;

        // replicated_query_instructions : Nat;

        // schema_versions : [Schema]

    };

    public type DatabaseStats = {
        /// The name of the database
        name : Text;

        /// The database's memory type
        memoryType : MemoryType;

        /// The number of collections in the database
        collections : Nat;

        /// The collection statistics for each collection in the database
        collection_stats : [CollectionStats];

        /// The total memory allocation across all collections in the database
        total_allocated_bytes : Nat;

        /// The total memory currently used across all collections in the database
        total_used_bytes : Nat;

        /// The total memory available across all collections in the database
        total_free_bytes : Nat;

        total_data_bytes : Nat;
        total_metadata_bytes : Nat;

        total_document_store_bytes : Nat;
        total_index_store_bytes : Nat;

    };

    public type CacheStats = {
        capacity : Nat;
        size : Nat;
    };

    public type InstanceStats = {
        /// The memory type of the instance
        memory_type : MemoryType;

        /// The number of databases in the instance
        databases : Nat;

        /// The database statistics for each database in the instance
        database_stats : [DatabaseStats];

        cache_stats : CacheStats;

        /// The total memory allocation across all databases in the instance
        total_allocated_bytes : Nat;

        /// The total memory currently used across all databases in the instance
        total_used_bytes : Nat;

        /// The total memory available across all databases in the instance
        total_free_bytes : Nat;

        total_data_bytes : Nat;
        total_metadata_bytes : Nat;

        total_document_store_bytes : Nat;
        total_index_store_bytes : Nat;

    };

    public type EvalResult = {
        #Empty;
        #Ids : Iter<(DocumentId, ?[(Text, Candid)])>; // todo: returned the assumed size with the iterator, can help in choosing the smallest set of ids
        #BitMap : T.SparseBitMap64;
        #Interval : (index : Text, interval : [Interval], is_reversed : Bool);
    };

    public type BestIndexResult = {
        index : T.CompositeIndex;
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
        sorted_in_reverse : Bool;
        fully_covered_equality_and_range_fields : Set.Set<Text>;
        score : Float;

        fully_covered_equal_fields : Set.Set<Text>;
        fully_covered_sort_fields : Set.Set<Text>;
        fully_covered_range_fields : Set.Set<Text>;

        interval : T.Interval; // (start, end) range of matching entries in the index
    };

    public type FieldUpdateOperations = {
        #currValue : (); // refers to the current (prior to the update) of the field you are updating
        #get : (Text); // retrieves the value of the given field path (eg. #get("profile.age") return #Nat(28))

        // multi-value operations
        #addAll : [FieldUpdateOperations];
        #subAll : [FieldUpdateOperations];
        #mulAll : [FieldUpdateOperations];
        #divAll : [FieldUpdateOperations];

        // Number operations
        #add : (FieldUpdateOperations, FieldUpdateOperations);
        #sub : (FieldUpdateOperations, FieldUpdateOperations);
        #mul : (FieldUpdateOperations, FieldUpdateOperations);
        #div : (FieldUpdateOperations, FieldUpdateOperations);
        #abs : (FieldUpdateOperations);
        #neg : (FieldUpdateOperations);
        #floor : (FieldUpdateOperations);
        #ceil : (FieldUpdateOperations);
        #sqrt : (FieldUpdateOperations);
        #pow : (FieldUpdateOperations, FieldUpdateOperations);
        #min : (FieldUpdateOperations, FieldUpdateOperations);
        #max : (FieldUpdateOperations, FieldUpdateOperations);
        #mod : (FieldUpdateOperations, FieldUpdateOperations);

        // Text operations
        #trim : (FieldUpdateOperations, Text);
        #lowercase : (FieldUpdateOperations);
        #uppercase : (FieldUpdateOperations);
        #replaceSubText : (FieldUpdateOperations, Text, Text);
        #slice : (FieldUpdateOperations, Nat, Nat);
        #concat : (FieldUpdateOperations, FieldUpdateOperations);
        #concatAll : [FieldUpdateOperations];

    } or Candid;

    public type CrossCanisterRecordsCursor = {
        collection_name : Text;
        collection_query : T.StableQuery;
        results : T.Result<[(T.DocumentId, T.CandidBlob)], Text>;
    };

    public type SchemaFieldConstraint = {
        #Min : Float;
        #Max : Float;
        #Size : (min_size : Nat, max_size : Nat);
        #MinSize : Nat;
        #MaxSize : Nat;
    };

    public type SchemaConstraint = {
        #Unique : [Text];
        #Field : (Text, [SchemaFieldConstraint]);
    };

    // public type CandidHeapStorageType = {
    //     // stored by both memory types
    //     #candid_blob : CandidBlob;

    //     // heap only
    //     #candid_variant : Candid;
    //     #candid_map : { get : () -> () }; // placeholder for map type

    // };

    public type Token = (Text, [(start : Nat, end : Nat)]);

    public type Tokenizer = {
        #basic;
    };

    public type CompareFunc<K> = (K, K) -> Order;

    // Custom result types for operations that include instruction counts
    public type SearchResult<Record> = {
        documents : [WrapId<Record>];
        instructions : Nat;
        pagination_token : PaginationToken;
        has_more : Bool;
    };

    public type SearchOneResult<Record> = {
        document : ?WrapId<Record>;
        instructions : Nat;
    };
    public type CountResult = {
        count : Nat;
        instructions : Nat;
    };

    public type UpdateByIdResult = {
        instructions : Nat;
    };

    public type UpdateResult = {
        updated_count : Nat;
        instructions : Nat;
    };
    public type ReplaceByIdResult = {
        instructions : Nat;
    };

    public type ReplaceDocsResult = {
        instructions : Nat;
    };

    public type DeleteByIdResult<Record> = {
        deleted_document : Record;
        instructions : Nat;
    };

    public type DeleteResult<Record> = {
        deleted_documents : [(DocumentId, Record)];
        instructions : Nat;
    };

};
