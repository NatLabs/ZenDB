module {
    public let DOCUMENT_ID = ":id";

    // This field is used to allow multiple null values in a unique index by attaching the document's id to the null value.
    public let UNIQUE_INDEX_NULL_EXEMPT_ID = ":unique_index_null_exempt_id";

    public let HEAP_BTREE_ORDER = 32;
    public let STABLE_MEMORY_BTREE_ORDER = 512;

    public let MAX_QUERY_INSTRUCTIONS : Nat64 = 5_000_000_000;
    public let MAX_UPDATE_INSTRUCTIONS : Nat64 = 40_000_000_000;

    public let CURRENT_DOCUMENT_VERSION = 0;

    // public let MAX_DATABASES_PER_INSTANCE = 256; // 2^8
    // public let MAX_COLLECTIONS_PER_DATABASE = 65536; // 2^16
    // public let MAX_RECORDS_PER_COLLECTION = 1_099_511_627_776; // 2^40

    public let MILLION = 1_000_000;
    public let BILLION = 1_000_000_000;
    public let TRILLION = 1_000_000_000_000;

    // Safe operation limits based on current benchmarks and a 20B instruction limit
    // These thresholds are used by the index scoring algorithm to select optimal indexes
    // MAX_IN_MEMORY_FILTER_ENTRIES: Maximum entries that can be safely filtered in-memory
    // MAX_IN_MEMORY_SORT_ENTRIES: Maximum entries that can be safely sorted in-memory
    // Note: These are conservative "safe" limits, not absolute maximums
    public let MAX_IN_MEMORY_FILTER_ENTRIES = 70_000;
    public let MAX_IN_MEMORY_SORT_ENTRIES = 7_000;

};
