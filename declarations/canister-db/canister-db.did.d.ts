import type { Principal } from '@dfinity/principal';
import type { ActorMethod } from '@dfinity/agent';
import type { IDL } from '@dfinity/candid';

export type Candid = { 'Int' : bigint } |
  { 'Map' : Array<KeyValuePair> } |
  { 'Nat' : bigint } |
  { 'Empty' : null } |
  { 'Minimum' : null } |
  { 'Nat16' : number } |
  { 'Nat32' : number } |
  { 'Nat64' : bigint } |
  { 'Blob' : Uint8Array | number[] } |
  { 'Bool' : boolean } |
  { 'Int8' : number } |
  { 'Record' : Array<KeyValuePair> } |
  { 'Nat8' : number } |
  { 'Null' : null } |
  { 'Text' : string } |
  { 'Int16' : number } |
  { 'Int32' : number } |
  { 'Int64' : bigint } |
  { 'Option' : Candid__1 } |
  { 'Float' : number } |
  { 'Maximum' : null } |
  { 'Variant' : KeyValuePair } |
  { 'Tuple' : Array<Candid__1> } |
  { 'Principal' : Principal } |
  { 'Array' : Array<Candid__1> };
export type CandidBlob = Uint8Array | number[];
export type CandidType = { 'Int' : null } |
  { 'Map' : Array<[string, CandidType]> } |
  { 'Nat' : null } |
  { 'Empty' : null } |
  { 'Nat16' : null } |
  { 'Nat32' : null } |
  { 'Nat64' : null } |
  { 'Blob' : null } |
  { 'Bool' : null } |
  { 'Int8' : null } |
  { 'Record' : Array<[string, CandidType]> } |
  { 'Nat8' : null } |
  { 'Null' : null } |
  { 'Text' : null } |
  { 'Int16' : null } |
  { 'Int32' : null } |
  { 'Int64' : null } |
  { 'Option' : CandidType } |
  { 'Float' : null } |
  { 'Variant' : Array<[string, CandidType]> } |
  { 'Tuple' : Array<CandidType> } |
  { 'Principal' : null } |
  { 'Array' : CandidType } |
  { 'Recursive' : bigint };
export type Candid__1 = { 'Int' : bigint } |
  { 'Map' : Array<KeyValuePair> } |
  { 'Nat' : bigint } |
  { 'Empty' : null } |
  { 'Nat16' : number } |
  { 'Nat32' : number } |
  { 'Nat64' : bigint } |
  { 'Blob' : Uint8Array | number[] } |
  { 'Bool' : boolean } |
  { 'Int8' : number } |
  { 'Record' : Array<KeyValuePair> } |
  { 'Nat8' : number } |
  { 'Null' : null } |
  { 'Text' : string } |
  { 'Int16' : number } |
  { 'Int32' : number } |
  { 'Int64' : bigint } |
  { 'Option' : Candid__1 } |
  { 'Float' : number } |
  { 'Variant' : KeyValuePair } |
  { 'Tuple' : Array<Candid__1> } |
  { 'Principal' : Principal } |
  { 'Array' : Array<Candid__1> };
export interface CanisterDB {
  'zendb_api_version' : ActorMethod<[], bigint>,
  'zendb_collection_clear' : ActorMethod<[string], Result>,
  'zendb_collection_count_records' : ActorMethod<
    [string, StableQuery],
    Result_3
  >,
  'zendb_create_collection_index' : ActorMethod<
    [string, Array<string>],
    Result
  >,
  'zendb_collection_delete_index' : ActorMethod<
    [string, Array<string>],
    Result
  >,
  'zendb_collection_delete_record_by_id' : ActorMethod<
    [string, DocumentId],
    Result
  >,
  'zendb_collection_find_records' : ActorMethod<
    [string, StableQuery],
    Result_7
  >,
  'zendb_collection_get_record' : ActorMethod<[string, DocumentId], Result_6>,
  'zendb_collection_insert_all_records' : ActorMethod<
    [string, Array<Candid>],
    Result_5
  >,
  'zendb_collection_insert_record' : ActorMethod<[string, Candid], Result_5>,
  'zendb_collection_insert_record_with_id' : ActorMethod<
    [string, DocumentId, Candid],
    Result
  >,
  'zendb_collection_schema' : ActorMethod<[string], Result_4>,
  'zendb_collection_size' : ActorMethod<[string], Result_3>,
  'zendb_collection_stats' : ActorMethod<[string], Result_2>,
  'zendb_create_collection' : ActorMethod<[string, Schema], Result_1>,
  'zendb_delete_collection' : ActorMethod<[string], Result>,
  'zendb_get_database_name' : ActorMethod<[], string>,
}
export interface CollectionStats {
  'records' : bigint,
  'main_btree_index' : { 'stable_memory' : MemoryStats },
  'indexes' : Array<IndexStats>,
}
export interface CrossCanisterRecordsCursor {
  'results' : Result_8,
  'collection_name' : string,
  'collection_query' : StableQuery,
}
export interface IndexStats {
  'stable_memory' : MemoryStats,
  'columns' : Array<string>,
}
export type KeyValuePair = [string, Candid__1];
export interface MemoryStats {
  'metadata_bytes' : bigint,
  'actual_data_bytes' : bigint,
}
export type PaginationDirection = { 'Backward' : null } |
  { 'Forward' : null };
export type DocumentId = bigint;
export type Result = { 'ok' : null } |
  { 'err' : string };
export type Result_1 = { 'ok' : string } |
  { 'err' : string };
export type Result_2 = { 'ok' : CollectionStats } |
  { 'err' : string };
export type Result_3 = { 'ok' : bigint } |
  { 'err' : string };
export type Result_4 = { 'ok' : Schema } |
  { 'err' : string };
export type Result_5 = { 'ok' : DocumentId } |
  { 'err' : string };
export type Result_6 = { 'ok' : CandidBlob } |
  { 'err' : string };
export type Result_7 = { 'ok' : CrossCanisterRecordsCursor } |
  { 'err' : string };
export type Result_8 = { 'ok' : Array<[DocumentId, CandidBlob]> } |
  { 'err' : string };
export type Schema = { 'Int' : null } |
  { 'Map' : Array<[string, CandidType]> } |
  { 'Nat' : null } |
  { 'Empty' : null } |
  { 'Nat16' : null } |
  { 'Nat32' : null } |
  { 'Nat64' : null } |
  { 'Blob' : null } |
  { 'Bool' : null } |
  { 'Int8' : null } |
  { 'Record' : Array<[string, CandidType]> } |
  { 'Nat8' : null } |
  { 'Null' : null } |
  { 'Text' : null } |
  { 'Int16' : null } |
  { 'Int32' : null } |
  { 'Int64' : null } |
  { 'Option' : CandidType } |
  { 'Float' : null } |
  { 'Variant' : Array<[string, CandidType]> } |
  { 'Tuple' : Array<CandidType> } |
  { 'Principal' : null } |
  { 'Array' : CandidType } |
  { 'Recursive' : bigint };
export type SortDirection = { 'Descending' : null } |
  { 'Ascending' : null };
export interface StableQuery {
  'sort_by' : [] | [[string, SortDirection]],
  'pagination' : StableQueryPagination,
  'query_operations' : ZenQueryLang,
}
export interface StableQueryPagination {
  'cursor' : [] | [[bigint, PaginationDirection]],
  'skip' : [] | [bigint],
  'limit' : [] | [bigint],
}
export type ZenQueryLang = { 'Or' : Array<ZenQueryLang> } |
  { 'And' : Array<ZenQueryLang> } |
  { 'Operation' : [string, ZqlOperators] };
export type ZqlOperators = { 'In' : Array<Candid> } |
  { 'eq' : Candid } |
  { 'gt' : Candid } |
  { 'lt' : Candid } |
  { 'Not' : ZqlOperators } |
  { 'gte' : Candid } |
  { 'lte' : Candid };
export interface _SERVICE extends CanisterDB {}
export declare const idlFactory: IDL.InterfaceFactory;
export declare const init: (args: { IDL: typeof IDL }) => IDL.Type[];
