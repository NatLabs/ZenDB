import Principal "mo:base/Principal";
import Random "mo:base/Random";
import Int64 "mo:base/Int64";
import Blob "mo:base/Blob";
import Result "mo:base/Result";

import ZT "../Types";
import ZenDB "..";
import StableDatabase "../Database/StableDatabase";
import StableCollection "../Collection/StableCollection";
import RolesAuth "RolesAuth";

shared ({ caller = owner }) actor class CanisterDB(db_name : Text) = this_canister {

    let Permissions = {
        READ = "read";
        WRITE = "write";
        MANAGE = "manage";
    };

    let Roles = {
        MANAGER = "manager";
        USER = "user";
        GUEST = "guest";
    };

    let default_roles = [
        {
            name = Roles.MANAGER;
            permissions = [Permissions.MANAGE];
        },
        {
            name = Roles.USER;
            permissions = [Permissions.READ, Permissions.WRITE];
        },
        {
            name = Roles.GUEST;
            permissions = [];
        },
    ];

    stable let roles_sstore = RolesAuth.init_stable_store(default_roles);
    let auth = RolesAuth.RolesAuth(roles_sstore);
    ignore auth.assign_roles(owner, [Roles.MANAGER, Roles.USER]);

    let canister_id = Principal.fromActor(this_canister);
    ignore auth.assign_roles(canister_id, [Roles.USER]);

    auth.set_missing_permissions_error_message(
        func(caller : Principal, permission : Text) : Text {
            "ZenDB [" #db_name # "]: Caller " # debug_show caller # " does not have permission " #permission;
        }
    );

    stable let zendb_sstore_v0_0_1 = ZenDB.newStableStore();
    let zendb = ZenDB.launch(zendb_sstore_v0_0_1);

    public shared query func zendb_api_version() : async Nat {
        // "0.0.1";
        1;
    };

    public shared query func zendb_get_database_name() : async Text {
        db_name;
    };

    public shared ({ caller }) func zendb_create_collection(collection_name : Text, schema : ZT.Schema) : async (ZT.Result<Text, Text>) {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            func() : (ZT.Result<Text, Text>) {
                let collection = StableDatabase.create_collection(zendb_sstore_v0_0_1, collection_name, schema);

                Result.mapOk(collection, func(c : ZT.StableCollection) : Text { c.name });
            },
        );

    };

    public shared ({ caller }) func zendb_delete_collection(collection_name : Text) : async (ZT.Result<(), Text>) {
        // auth.allow_rs(
        //     caller,
        //     Permissions.MANAGE,
        //     func() : (ZT.Result<(), Text>) {
        //         StableDatabase.delete_collection(zendb, collection_name);
        //     },
        // );
        #err("");
    };

    public shared query func zendb_collection_size(collection_name : Text) : async ZT.Result<Nat, Text> {
        // auth.allows_rs(
        //     caller,
        //     Permissions.READ,
        //     func() : (ZT.Result<Nat, Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         #ok(StableCollection.size(collection));
        //     },
        // )
        #err("");

    };

    public shared query func zendb_collection_schema(collection_name : Text) : async (ZT.Result<ZT.Schema, Text>) {
        // auth.allows_rs(
        //     caller,
        //     Permissions.READ,
        //     func() : (ZT.Result<ZT.Schema, Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         #ok(StableCollection.get_schema(collection));
        //     },
        // );
        #err("");
    };

    public shared ({ caller }) func zendb_collection_clear(collection_name : Text) : async (ZT.Result<(), Text>) {
        // auth.allow_rs(
        //     caller,
        //     Permissions.MANAGE,
        //     func() : (ZT.Result<(), Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         StableCollection.clear(collection);
        //     },
        // );
        #err("");
    };

    public shared query func zendb_collection_stats(collection_name : Text) : async (ZT.Result<ZT.CollectionStats, Text>) {
        // auth.allows_rs(
        //     caller,
        //     Permissions.READ,
        //     func() : (ZT.Result<ZT.CollectionStats, Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         #ok(StableCollection.stats(collection));
        //     },
        // );
        #err("");
    };

    public shared ({ caller }) func zendb_collection_create_index(collection_name : Text, index_keys : [Text]) : async (ZT.Result<(), Text>) {
        // auth.allow_rs(
        //     caller,
        //     Permissions.MANAGE,
        //     func() : (ZT.Result<(), Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         StableCollection.create_index(collection, index_keys);
        //     },
        // );
        #err("");
    };

    public shared ({ caller }) func zendb_collection_delete_index(collection_name : Text, index_keys : [Text]) : async (ZT.Result<(), Text>) {
        // auth.allow_rs(
        //     caller,
        //     Permissions.MANAGE,
        //     func() : (ZT.Result<(), Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         StableCollection.delete_index(collection, index_keys);
        //     },
        // );
        #err("");
    };

    public shared ({ caller }) func zendb_collection_insert_record(collection_name : Text, record : ZT.Candid) : async (ZT.Result<ZT.RecordId, Text>) {
        // let rand_blob = await Random.blob();

        // auth.allow_rs(
        //     caller,
        //     Permissions.WRITE,
        //     func() : (ZT.Result<ZT.RecordId, Text>) {
        //         let rand_bytes = Blob.toArray(rand_blob);
        //         let rand = Random.Finite(rand_blob);

        //         let time = Int64.fromInt(Time.now());

        //         var unique_process_bytes : Nat = 0;

        //         for (i in Itertools.range(0, 4)) {
        //             unique_process_bytes := (unique_process_bytes * 256) + Nat8.toNat(rand_bytes[i]);
        //         };

        //         unique_process_bytes := unique_process_bytes * (2 ** 32);

        //         let num = rand.range(2 ** 32);

        //         let record_id = (unique_process_bytes) + num;

        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         StableCollection.insert_with_id(collection, record_id, record);
        //     },
        // );
        #err("");
    };

    public shared ({ caller }) func zendb_collection_insert_all_records(collection_name : Text, records : [ZT.Candid]) : async (ZT.Result<ZT.RecordId, Text>) {
        // let rand_blob = await Random.blob();

        // auth.allow_rs(
        //     caller,
        //     Permissions.WRITE,
        //     func() : (ZT.Result<ZT.RecordId, Text>) {
        //         let rand_bytes = Blob.toArray(rand_blob);
        //         let rand = Random.Finite(rand_blob);

        //         let time = Int64.fromInt(Time.now());

        //         var unique_process_bytes : Nat = 0;

        //         for (i in Itertools.range(0, 4)) {
        //             unique_process_bytes := (unique_process_bytes * 256) + Nat8.toNat(rand_bytes[i]);
        //         };

        //         unique_process_bytes := unique_process_bytes * (2 ** 32);

        //         let num = rand.range(2 ** 32);

        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         assert records.size() < 2 ** 32;

        //         for ((i, record) in Itertools.enumerate(records.vals())) {
        //             let record_id = (unique_process_bytes) + (num + 1);
        //             StableCollection.insert_with_id(collection, record_id, record);
        //         };

        //     },
        // );
        #err("");
    };

    public shared ({ caller }) func zendb_collection_insert_record_with_id(collection_name : Text, record_id : ZT.RecordId, record : ZT.Candid) : async (ZT.Result<(), Text>) {
        // auth.allow_rs(
        //     caller,
        //     Permissions.WRITE,
        //     func() : (ZT.Result<(), Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         StableCollection.insert_with_id(collection, record_id, record);
        //     },
        // );
        #err("");
    };

    public shared ({ caller }) func zendb_collection_delete_record_by_id(collection_name : Text, record_id : ZT.RecordId) : async (ZT.Result<(), Text>) {
        // auth.allow_rs(
        //     caller,
        //     Permissions.WRITE,
        //     func() : (ZT.Result<(), Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         StableCollection.delete_by_id(collection, record_id);
        //     },
        // );
        #err("");
    };

    // public shared ({ caller }) func zendb_collection_update_record_by_id(collection_name : Text, record_id : ZT.RecordId, record : ZT.Candid) : async (ZT.Result<(), Text>) {
    //     auth.allow_rs(
    //         caller,
    //         Permissions.WRITE,
    //         func() : (ZT.Result<(), Text>) {
    //             let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
    //                 case (#ok(collection)) collection;
    //                 case (#err(msg)) return #err(msg);
    //             };

    //             StableCollection.update_by_id(collection, record_id, record);
    //         },
    //     );
    // };

    public shared query func zendb_collection_get_record(collection_name : Text, record_id : ZT.RecordId) : async (ZT.Result<ZT.CandidBlob, Text>) {
        // auth.allows_rs(
        //     caller,
        //     Permissions.READ,
        //     func() : (ZT.Result<ZT.CandidBlob, Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         let record = switch (StableCollection.get(collection, record_id)) {
        //             case (#ok(record)) record;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         #ok(record);
        //     },
        // );
        #err("");
    };

    public shared query func zendb_collection_find_records(collection_name : Text, collection_query : ZT.StableQuery) : async (ZT.Result<ZT.CrossCanisterRecordsCursor, Text>) {
        // auth.allows_rs(
        //     caller,
        //     Permissions.READ,
        //     func() : (ZT.Result<ZT.CrossCanisterRecordsCursor, Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         let results = switch (StableCollection.find(collection, collection_query)) {
        //             case (#ok(cursor)) results;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         let cross_canister_cursor = {
        //             collection_name = collection_name;
        //             collection_query = collection_query.build();
        //             results;
        //         };

        //         #ok(cross_canister_cursor);
        //     },
        // );
        #err("");
    };

    public shared query func zendb_collection_count_records(collection_name : Text, collection_query : ZT.StableQuery) : async (ZT.Result<Nat, Text>) {
        // auth.allows_rs<ZT.Result<Nat, Text>>(
        //     caller,
        //     Permissions.READ,
        //     func() : (ZT.Result<Nat, Text>) {
        //         let collection = switch (StableDatabase.get_collection(zendb, collection_name)) {
        //             case (#ok(collection)) collection;
        //             case (#err(msg)) return #err(msg);
        //         };

        //         #ok(StableCollection.count(collection, collection_query));
        //     },
        // );
        #err("");
    };

};
