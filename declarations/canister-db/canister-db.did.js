export const idlFactory = ({ IDL }) => {
    const CandidType = IDL.Rec();
    const Candid__1 = IDL.Rec();
    const KeyValuePair = IDL.Rec();
    const ZenQueryLang = IDL.Rec();
    const ZqlOperators = IDL.Rec();
    const Result = IDL.Variant({ ok: IDL.Null, err: IDL.Text });
    const SortDirection = IDL.Variant({
        Descending: IDL.Null,
        Ascending: IDL.Null,
    });
    const PaginationDirection = IDL.Variant({
        Backward: IDL.Null,
        Forward: IDL.Null,
    });
    const StableQueryPagination = IDL.Record({
        cursor: IDL.Opt(IDL.Tuple(IDL.Nat, PaginationDirection)),
        skip: IDL.Opt(IDL.Nat),
        limit: IDL.Opt(IDL.Nat),
    });
    Candid__1.fill(
        IDL.Variant({
            Int: IDL.Int,
            Map: IDL.Vec(KeyValuePair),
            Nat: IDL.Nat,
            Empty: IDL.Null,
            Nat16: IDL.Nat16,
            Nat32: IDL.Nat32,
            Nat64: IDL.Nat64,
            Blob: IDL.Vec(IDL.Nat8),
            Bool: IDL.Bool,
            Int8: IDL.Int8,
            Record: IDL.Vec(KeyValuePair),
            Nat8: IDL.Nat8,
            Null: IDL.Null,
            Text: IDL.Text,
            Int16: IDL.Int16,
            Int32: IDL.Int32,
            Int64: IDL.Int64,
            Option: Candid__1,
            Float: IDL.Float64,
            Variant: KeyValuePair,
            Tuple: IDL.Vec(Candid__1),
            Principal: IDL.Principal,
            Array: IDL.Vec(Candid__1),
        })
    );
    KeyValuePair.fill(IDL.Tuple(IDL.Text, Candid__1));
    const Candid = IDL.Variant({
        Int: IDL.Int,
        Map: IDL.Vec(KeyValuePair),
        Nat: IDL.Nat,
        Empty: IDL.Null,
        Minimum: IDL.Null,
        Nat16: IDL.Nat16,
        Nat32: IDL.Nat32,
        Nat64: IDL.Nat64,
        Blob: IDL.Vec(IDL.Nat8),
        Bool: IDL.Bool,
        Int8: IDL.Int8,
        Record: IDL.Vec(KeyValuePair),
        Nat8: IDL.Nat8,
        Null: IDL.Null,
        Text: IDL.Text,
        Int16: IDL.Int16,
        Int32: IDL.Int32,
        Int64: IDL.Int64,
        Option: Candid__1,
        Float: IDL.Float64,
        Maximum: IDL.Null,
        Variant: KeyValuePair,
        Tuple: IDL.Vec(Candid__1),
        Principal: IDL.Principal,
        Array: IDL.Vec(Candid__1),
    });
    ZqlOperators.fill(
        IDL.Variant({
            In: IDL.Vec(Candid),
            eq: Candid,
            gt: Candid,
            lt: Candid,
            Not: ZqlOperators,
            gte: Candid,
            lte: Candid,
        })
    );
    ZenQueryLang.fill(
        IDL.Variant({
            Or: IDL.Vec(ZenQueryLang),
            And: IDL.Vec(ZenQueryLang),
            Operation: IDL.Tuple(IDL.Text, ZqlOperators),
        })
    );
    const StableQuery = IDL.Record({
        sort_by: IDL.Opt(IDL.Tuple(IDL.Text, SortDirection)),
        pagination: StableQueryPagination,
        query_operations: ZenQueryLang,
    });
    const Result_3 = IDL.Variant({ ok: IDL.Nat, err: IDL.Text });
    const DocumentId = IDL.Nat;
    const CandidBlob = IDL.Vec(IDL.Nat8);
    const Result_8 = IDL.Variant({
        ok: IDL.Vec(IDL.Tuple(DocumentId, CandidBlob)),
        err: IDL.Text,
    });
    const CrossCanisterRecordsCursor = IDL.Record({
        results: Result_8,
        collection_name: IDL.Text,
        collection_query: StableQuery,
    });
    const Result_7 = IDL.Variant({
        ok: CrossCanisterRecordsCursor,
        err: IDL.Text,
    });
    const Result_6 = IDL.Variant({ ok: CandidBlob, err: IDL.Text });
    const Result_5 = IDL.Variant({ ok: DocumentId, err: IDL.Text });
    CandidType.fill(
        IDL.Variant({
            Int: IDL.Null,
            Map: IDL.Vec(IDL.Tuple(IDL.Text, CandidType)),
            Nat: IDL.Null,
            Empty: IDL.Null,
            Nat16: IDL.Null,
            Nat32: IDL.Null,
            Nat64: IDL.Null,
            Blob: IDL.Null,
            Bool: IDL.Null,
            Int8: IDL.Null,
            Record: IDL.Vec(IDL.Tuple(IDL.Text, CandidType)),
            Nat8: IDL.Null,
            Null: IDL.Null,
            Text: IDL.Null,
            Int16: IDL.Null,
            Int32: IDL.Null,
            Int64: IDL.Null,
            Option: CandidType,
            Float: IDL.Null,
            Variant: IDL.Vec(IDL.Tuple(IDL.Text, CandidType)),
            Tuple: IDL.Vec(CandidType),
            Principal: IDL.Null,
            Array: CandidType,
            Recursive: IDL.Nat,
        })
    );
    const Schema = IDL.Variant({
        Int: IDL.Null,
        Map: IDL.Vec(IDL.Tuple(IDL.Text, CandidType)),
        Nat: IDL.Null,
        Empty: IDL.Null,
        Nat16: IDL.Null,
        Nat32: IDL.Null,
        Nat64: IDL.Null,
        Blob: IDL.Null,
        Bool: IDL.Null,
        Int8: IDL.Null,
        Record: IDL.Vec(IDL.Tuple(IDL.Text, CandidType)),
        Nat8: IDL.Null,
        Null: IDL.Null,
        Text: IDL.Null,
        Int16: IDL.Null,
        Int32: IDL.Null,
        Int64: IDL.Null,
        Option: CandidType,
        Float: IDL.Null,
        Variant: IDL.Vec(IDL.Tuple(IDL.Text, CandidType)),
        Tuple: IDL.Vec(CandidType),
        Principal: IDL.Null,
        Array: CandidType,
        Recursive: IDL.Nat,
    });
    const Result_4 = IDL.Variant({ ok: Schema, err: IDL.Text });
    const MemoryStats = IDL.Record({
        metadata_bytes: IDL.Nat,
        actual_data_bytes: IDL.Nat,
    });
    const IndexStats = IDL.Record({
        stable_memory: MemoryStats,
        columns: IDL.Vec(IDL.Text),
    });
    const CollectionStats = IDL.Record({
        records: IDL.Nat,
        main_btree_index: IDL.Record({ stable_memory: MemoryStats }),
        indexes: IDL.Vec(IndexStats),
    });
    const Result_2 = IDL.Variant({ ok: CollectionStats, err: IDL.Text });
    const Result_1 = IDL.Variant({ ok: IDL.Text, err: IDL.Text });
    const CanisterDB = IDL.Service({
        zendb_api_version: IDL.Func([], [IDL.Nat], ["query"]),
        zendb_collection_clear: IDL.Func([IDL.Text], [Result], []),
        zendb_collection_count_records: IDL.Func(
            [IDL.Text, StableQuery],
            [Result_3],
            ["query"]
        ),
        zendb_create_collection_index: IDL.Func(
            [IDL.Text, IDL.Vec(IDL.Text)],
            [Result],
            []
        ),
        zendb_collection_delete_index: IDL.Func(
            [IDL.Text, IDL.Vec(IDL.Text)],
            [Result],
            []
        ),
        zendb_collection_delete_record_by_id: IDL.Func(
            [IDL.Text, DocumentId],
            [Result],
            []
        ),
        zendb_collection_find_records: IDL.Func(
            [IDL.Text, StableQuery],
            [Result_7],
            ["query"]
        ),
        zendb_collection_get_record: IDL.Func(
            [IDL.Text, DocumentId],
            [Result_6],
            ["query"]
        ),
        zendb_collection_insert_all_records: IDL.Func(
            [IDL.Text, IDL.Vec(Candid)],
            [Result_5],
            []
        ),
        zendb_collection_insert_record: IDL.Func(
            [IDL.Text, Candid],
            [Result_5],
            []
        ),
        zendb_collection_insert_record_with_id: IDL.Func(
            [IDL.Text, DocumentId, Candid],
            [Result],
            []
        ),
        zendb_collection_schema: IDL.Func([IDL.Text], [Result_4], ["query"]),
        zendb_collection_size: IDL.Func([IDL.Text], [Result_3], ["query"]),
        zendb_collection_stats: IDL.Func([IDL.Text], [Result_2], ["query"]),
        zendb_create_collection: IDL.Func([IDL.Text, Schema], [Result_1], []),
        zendb_delete_collection: IDL.Func([IDL.Text], [Result], []),
        zendb_get_database_name: IDL.Func([], [IDL.Text], ["query"]),
    });
    return CanisterDB;
};
export const init = ({ IDL }) => {
    return [IDL.Text];
};
