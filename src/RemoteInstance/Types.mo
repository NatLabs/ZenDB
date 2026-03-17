import Map "mo:map@9.0.1/Map";
import Vector "mo:vector@0.4.2";
import CanisterRBAC "mo:canister-rbac@0.1.0";

import ZenDB_types "../EmbeddedInstance/Types";

module {
    let ZT = ZenDB_types;

    public module Roles {
        public let MANAGER = "manager";
        public let USER = "user";
        public let GUEST = "guest";
    };

    public module Permissions {
        public let MANAGE = "manage"; // create/delete collections, create/delete indexes
        public let READ = "read"; // read documents
        public let WRITE = "write"; // insert/update/delete documents
    };

    public type CanisterStats = ZenDB_types.InstanceStats;

    public type ShardingStrategy = {
        #fill_first : {
            max_canister_size_in_bytes : Nat; // Max storage size before creating new canister
            threshold_percent : Float; // 0.8 = create new canister at 80% capacity
        };
    };

    public let DefaultShardingStrategy : ShardingStrategy = #fill_first({
        max_canister_size_in_bytes = 429_496_729_600; // ~ 400 GiB
        threshold_percent = 0.9;
    });

    public type CollectionLayout = {
        name : Text;
        schema : ZT.Schema;
        // todo:should store the memory infor of the collection in each canister as well
        canisters : Vector.Vector<Principal>;
    };

    public type DatabaseLayout = {
        name : Text;
        collections : Map.Map<Text, CollectionLayout>;
    };

    public type CanisterInfo = {
        id : Principal;
        status : { #active; #low_memory; #full }; // Need to know if canister can accept new data
        total_allocated_bytes : Nat; // Total allocated bytes in the canister
        total_used_bytes : Nat; // Total used bytes in the canister
        total_free_bytes : Nat; // Total free bytes in the canister
        total_data_bytes : Nat; // Total bytes used for data storage
        total_metadata_bytes : Nat; // Total bytes used for metadata storage (indexes, etc
        total_index_data_bytes : Nat; // Total bytes used for index data storage
    };

    public type CanisterMemoryInfo = {
        total_allocated_bytes : Nat;
        total_used_bytes : Nat;
        total_free_bytes : Nat;
        total_data_bytes : Nat;
        total_metadata_bytes : Nat;
        total_index_data_bytes : Nat;
    };

    public type ClusterLayout = {
        dbs : [DatabaseLayout];
        canisters : [CanisterInfo];
        // Essential: How to decide where new documents go
        sharding_strategy : ShardingStrategy;
    };

    public type ClusterSettings = {
        canister_dbs : [Principal];
        sharding_strategy : ShardingStrategy;
    };

    public type CrossCanisterRecordsCursor = {
        collection_name : Text;
        collection_query : ZT.StableQuery;
        results : ZT.Result<[(ZT.DocumentId, ZT.CandidBlob)], Text>;
    };

    public type UserAccessDetails = [(resource_scope : CanisterRBAC.Types.ResourceScope, role : Text, permissions : [Text])];

};
