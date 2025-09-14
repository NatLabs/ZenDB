import Principal "mo:base@0.16.0/Principal";
import Random "mo:base@0.16.0/Random";
import Int64 "mo:base@0.16.0/Int64";
import Blob "mo:base@0.16.0/Blob";
import Result "mo:base@0.16.0/Result";

import Map "mo:map@9.0.1/Map";

import ClusterTypes "../Types";
import ZenDB "../..";
import ZT "../../Types";
import StableDatabase "../../Database/StableDatabase";
import StableCollection "../../Collection/StableCollection";
import RolesAuth "../RolesAuth";
import CollectionUtils "../../Collection/Utils";
import Utils "../../Utils";

shared ({ caller = owner }) persistent actor class CanisterDB() = this_canister {

    transient let Permissions = {
        READ = "read";
        WRITE = "write";
        MANAGE = "manage";
    };

    transient let Roles = {
        MANAGER = "manager";
        USER = "user";
        GUEST = "guest";
    };

    transient let default_roles = [
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
            permissions = [Permissions.READ];
        },
    ];

    stable let roles_sstore = RolesAuth.init_stable_store(default_roles);
    transient let auth = RolesAuth.RolesAuth(roles_sstore);

    ignore auth.assign_roles(owner, [Roles.MANAGER, Roles.USER]);

    let canister_id = Principal.fromActor(this_canister);
    let canister_id_as_blob = Principal.toBlob(canister_id);

    ignore auth.assign_roles(canister_id, [Roles.MANAGER, Roles.USER]);

    auth.set_missing_permissions_error_message(
        func(caller : Principal, permission : Text) : Text {
            "ZenDB [ db_name ]: Caller " # debug_show caller # " does not have permission " #permission;
        }
    );

    stable var zendb_instance = ZenDB.newStableStore(null);

    public shared query func zendb_api_version() : async Text {
        "0.0.1";
    };

    public shared ({ caller }) func grant_role(target : Principal, role : Text) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.MANAGE,
            func() : (ZT.Result<(), Text>) {
                auth.assign_roles(target, [role]);
            },
        );
    };

    public shared ({ caller }) func grant_roles(target : Principal, roles : [Text]) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.MANAGE,
            func() : (ZT.Result<(), Text>) {
                auth.assign_roles(target, roles);
            },
        );
    };

    // public shared ({ caller }) func revoke_role(target : Principal, role : Text) : async ZT.Result<(), Text> {
    //     auth.allow(
    //         caller,
    //         Permissions.MANAGE,
    //         func() : (ZT.Result<(), Text>) {
    //             auth.remove_roles(target, [role]);
    //             #ok(());
    //         },
    //     );
    // };

    // public shared ({ caller }) func revoke_roles(target : Principal, roles : [Text]) : async ZT.Result<(), Text> {
    //     auth.allow(
    //         caller,
    //         Permissions.MANAGE,
    //         func() : (ZT.Result<(), Text>) {
    //             for (role in roles.vals()) {
    //                 let #ok(_) = auth.unassign_role(target, role) else return #err("Failed to unassign role");
    //             };
    //             #ok(());
    //         },
    //     );
    // };

    // ! supports multiple databases
    // public shared query func zendb_get_database_name() : async Text {
    //     db_name;
    // };

    public shared ({ caller }) func zendb_create_database(db_name : Text) : async (ZT.Result<(), Text>) {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            func() : (ZT.Result<(), Text>) {

                Result.mapOk<Any, (), Text>(
                    ZenDB.createDB(zendb_instance, db_name),
                    func(_) { () },
                );

            },
        );
    };

    public shared ({ caller }) func zendb_create_collection(db_name : Text, collection_name : Text, schema : ZT.Schema) : async (ZT.Result<(), Text>) {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            func() : (ZT.Result<(), Text>) {
                let ?db = Map.get<Text, ZenDB.Types.StableDatabase>(zendb_instance.databases, Map.thash, db_name) else {
                    return #err("Database '" # db_name # "' does not exist");
                };

                let collection = StableDatabase.createCollection(db, collection_name, schema, null);

                Result.mapOk<ZT.StableCollection, (), Text>(collection, func(_) { () });
            },
        );

    };

    func get_collection(db_name : Text, collection_name : Text) : ZT.Result<ZT.StableCollection, Text> {
        let ?db = Map.get<Text, ZenDB.Types.StableDatabase>(zendb_instance.databases, Map.thash, db_name) else {
            return #err("Database '" # db_name # "' does not exist");
        };

        let #ok(collection) = StableDatabase.getCollection(db, collection_name) else {
            return #err("Collection '" # collection_name # "' does not exist in database '" # db_name # "'");
        };

        #ok(collection);
    };

    public shared ({ caller }) func zendb_collection_insert_document(db_name : Text, collection_name : Text, candid_document_blob : Blob) : async (ZT.Result<ZT.DocumentId, Text>) {
        auth.allow_rs(
            caller,
            Permissions.WRITE,
            func() : (ZT.Result<ZT.DocumentId, Text>) {

                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);
                StableCollection.insert(collection, main_btree_utils, candid_document_blob);
            },
        );
    };

    public shared query ({ caller }) func zendb_collection_get_document(db_name : Text, collection_name : Text, document_id : ZT.DocumentId) : async (ZT.Result<ZT.CandidBlob, Text>) {
        auth.allow_rs(
            caller,
            Permissions.READ,
            func() : (ZT.Result<ZT.CandidBlob, Text>) {

                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

                switch (StableCollection.get(collection, main_btree_utils, document_id)) {
                    case (?document_blob) #ok(document_blob);
                    case null #err("Document with id " # debug_show (document_id) # " does not exist in collection '" # collection_name # "' in database '" # db_name # "'");
                };
            },
        );
    };

    public shared query ({ caller }) func zendb_collection_search(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery) : async (ZT.Result<[(Nat, Blob)], Text>) {
        auth.allow_rs(
            caller,
            Permissions.READ,
            func() : (ZT.Result<[(Nat, Blob)], Text>) {

                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

                StableCollection.search(collection, main_btree_utils, stable_query);
            },
        );
    };

    public shared query ({ caller }) func zendb_stats() : async ZenDB.Types.InstanceStats {
        auth.allow(
            caller,
            Permissions.READ,
            func() : (ZenDB.Types.InstanceStats) {
                ZenDB.stats(zendb_instance);
            },
        );

    };

    public shared query ({ caller }) func zendb_collection_get_schema(db_name : Text, collection_name : Text) : async ZT.Result<ZT.Schema, Text> {
        auth.allow(
            caller,
            Permissions.READ,
            func() : (ZT.Result<ZT.Schema, Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                #ok(collection.schema);
            },
        );
    };

    public shared ({ caller }) func zendb_collection_replace_document(db_name : Text, collection_name : Text, id : Nat, document_blob : Blob) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.WRITE,
            func() : (ZT.Result<(), Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

                StableCollection.replaceById(collection, main_btree_utils, id, document_blob);
            },
        );

    };

    public shared ({ caller }) func zendb_collection_delete_document_by_id(db_name : Text, collection_name : Text, id : Nat) : async ZT.Result<Blob, Text> {
        auth.allow(
            caller,
            Permissions.WRITE,
            func() : (ZT.Result<(Blob), Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

                StableCollection.deleteById(collection, main_btree_utils, id);
            },
        );
    };

    // public shared ({ caller }) func zendb_collection_delete_document(db_name : Text, collection_name : Text, db_query : ZT.StableQuery) : async ZT.Result<[(Nat, Blob)], Text> {
    //     auth.allow(
    //         caller,
    //         Permissions.WRITE,
    //         func() : (ZT.Result<[(Nat, Blob)], Text>) {
    //             let collection_res = get_collection(db_name, collection_name);
    //             let #ok(collection) = collection_res else return Utils.send_error(collection_res);

    //             let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

    //             StableCollection.delete(collection, main_btree_utils, db_query);
    //         },
    //     );
    // };

    public shared ({ caller }) func zendb_collection_update_document_by_id(db_name : Text, collection_name : Text, id : Nat, update_operations : [(Text, ZT.FieldUpdateOperations)]) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.WRITE,
            func() : (ZT.Result<(), Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

                StableCollection.updateById(collection, main_btree_utils, id, update_operations);
            },
        );
    };

    // public shared ({ caller }) func zendb_collection_update_documents(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery, update_operations : [(Text, ZT.FieldUpdateOperations)]) : async ZT.Result<[Nat], Text> {
    //     auth.allow(
    //         caller,
    //         Permissions.WRITE,
    //         func() : (ZT.Result<[Nat], Text>) {
    //             let collection_res = get_collection(db_name, collection_name);
    //             let #ok(collection) = collection_res else return Utils.send_error(collection_res);

    //             let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

    //             StableCollection.update(collection, main_btree_utils, stable_query, update_operations);
    //         },
    //     );
    // };

    public shared ({ caller }) func zendb_collection_create_index(db_name : Text, collection_name : Text, index_name : Text, index_fields : [(Text, ZT.SortDirection)], options : ?ZT.CreateIndexOptions) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.MANAGE,
            func() : (ZT.Result<(), Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

                let is_unique = switch (options) {
                    case (?options) options.isUnique;
                    case (null) false;
                };

                let res = StableCollection.createIndex(collection, main_btree_utils, index_name, index_fields, is_unique);
                Result.mapOk<Any, (), Text>(res, func(_) { () });
            },
        );
    };

    public shared ({ caller }) func zendb_collection_delete_index(db_name : Text, collection_name : Text, index_name : Text) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.MANAGE,
            func() : (ZT.Result<(), Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

                StableCollection.deleteIndex(collection, main_btree_utils, index_name);
            },
        );
    };

    public shared query ({ caller }) func zendb_collection_repopulate_index(db_name : Text, collection_name : Text, index_name : Text) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.MANAGE,
            func() : (ZT.Result<(), Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                let main_btree_utils = CollectionUtils.getMainBtreeUtils(collection);

                StableCollection.repopulateIndex(collection, main_btree_utils, index_name);
            },
        );
    };
};
