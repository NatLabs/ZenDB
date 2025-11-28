import Array "mo:base@0.16.0/Array";
import Principal "mo:base@0.16.0/Principal";
import Random "mo:base@0.16.0/Random";
import Int64 "mo:base@0.16.0/Int64";
import Blob "mo:base@0.16.0/Blob";
import Result "mo:base@0.16.0/Result";
import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Iter "mo:base@0.16.0/Iter";

import Map "mo:map@9.0.1/Map";

import ClusterTypes "../Types";
import ZenDB "../../EmbeddedInstance";
import RolesAuth "../RolesAuth";
import StableRolesAuth "../RolesAuth/StableRolesAuth";

import ZT "../../EmbeddedInstance/Types";
import EmbeddedInstance "../../EmbeddedInstance";
import StableDatabase "../../EmbeddedInstance/Database/StableDatabase";
import StableCollection "../../EmbeddedInstance/Collection/StableCollection";
import CollectionUtils "../../EmbeddedInstance/Collection/CollectionUtils";
import Utils "../../EmbeddedInstance/Utils";
import TypeMigrations "../../EmbeddedInstance/TypeMigrations";
import Query "../../EmbeddedInstance/Query";

(
    with migration = func({
        zendb_instance : ZenDB.Types.PrevVersionedStableStore;
        roles_sstore : StableRolesAuth.PrevStableRolesAuth;
    }) : ({
        zendb_instance : ZenDB.Types.VersionedStableStore;
        roles_sstore : StableRolesAuth.StableRolesAuth;
    }) {
        return {
            zendb_instance = ZenDB.upgrade(zendb_instance);
            roles_sstore = StableRolesAuth.migrate(roles_sstore);
        };
    }
)
shared ({ caller = owner }) persistent actor class CanisterDB() = this_canister {

    transient let Permissions = {
        READ = "read";
        WRITE = "write";
        MANAGE = "manage";
    };

    transient let Roles = {
        ADMIN = "admin";
        EDITOR = "editor";
        VIEWER = "viewer";
    };

    transient let default_roles = [
        {
            name = Roles.ADMIN;
            permissions = [Permissions.MANAGE, Permissions.WRITE, Permissions.READ];
        },
        {
            name = Roles.EDITOR;
            permissions = [Permissions.WRITE, Permissions.READ];
        },
        {
            name = Roles.VIEWER;
            permissions = [Permissions.READ];
        },
    ];

    stable var roles_sstore = RolesAuth.init_stable_store(default_roles);
    transient let auth = RolesAuth.RolesAuth(roles_sstore);

    ignore auth.assign_roles(owner, [Roles.ADMIN]);

    stable let canister_id = Principal.fromActor(this_canister);
    stable let canister_id_as_blob = Principal.toBlob(canister_id);

    ignore auth.assign_roles(canister_id, [Roles.ADMIN]);

    stable var zendb_instance = ZenDB.newStableStore(canister_id, null);

    system func postupgrade<system>() {};

    ZenDB.setLogLevel(zendb_instance, #Debug);
    ZenDB.setIsRunLocally(zendb_instance, false);
    ZenDB.updateCacheCapacity(zendb_instance, 1_000_000);

    public shared query func zendb_v1_api_version() : async Text {
        "0.0.1";
    };

    public shared ({ caller }) func grant_role(target : Principal, role : Text) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.MANAGE,
            "role(" # role # ")",
            func() : (ZT.Result<(), Text>) {
                auth.assign_roles(target, [role]);
            },
        );
    };

    public shared ({ caller }) func grant_roles(target : Principal, roles : [Text]) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.MANAGE,
            "roles",
            func() : (ZT.Result<(), Text>) {
                auth.assign_roles(target, roles);
            },
        );
    };

    public shared ({ caller }) func revoke_role(target : Principal, role : Text) : async ZT.Result<(), Text> {
        auth.allow(
            caller,
            Permissions.MANAGE,
            "role(" # role # ")",
            func() : (ZT.Result<(), Text>) {
                auth.unassign_role(target, role);
            },
        );
    };

    public shared query ({ caller }) func list_roles() : async [Text] {
        auth.allow(
            caller,
            Permissions.READ,
            "roles",
            func() : [Text] {
                auth.list_roles();
            },
        );
    };

    public shared query ({ caller }) func get_role_permissions(role_name : Text) : async ZT.Result<[Text], Text> {
        auth.allow(
            caller,
            Permissions.READ,
            "role(" # role_name # ")",
            func() : ZT.Result<[Text], Text> {
                auth.get_role_permissions(role_name);
            },
        );
    };

    public shared query ({ caller }) func get_user_roles(user : Principal) : async [Text] {
        // Users can query their own roles, or must have MANAGE permission to query others
        if (caller == user) {
            return auth.get_user_roles(user);
        };

        auth.allow(
            caller,
            Permissions.MANAGE,
            "user(" # debug_show (user) # ")/roles",
            func() : [Text] {
                auth.get_user_roles(user);
            },
        );
    };

    public shared query ({ caller }) func get_my_roles() : async [Text] {
        auth.get_user_roles(caller);
    };

    public shared query ({ caller }) func get_all_users_roles() : async [(Principal, [Text])] {
        auth.allow(
            caller,
            Permissions.MANAGE,
            "users/roles",
            func() : [(Principal, [Text])] {
                auth.get_all_users_roles();
            },
        );
    };

    // public shared ({ caller }) func revoke_roles(target : Principal, roles : [Text]) : async ZT.Result<(), Text> {
    //     auth.allow(
    //         caller,
    //         Permissions.MANAGE,
    //         func() : (ZT.Result<(), Text>) {
    //             for (role in roles.vals()) {
    //                 let #ok(_) = auth.unassign_roles(target, role) else return #err("Failed to unassign role");
    //             };
    //             #ok(());
    //         },
    //     );
    // };

    // ! supports multiple databases
    // public shared query func zendb_get_database_name() : async Text {
    //     db_name;
    // };

    public shared ({ caller }) func zendb_v1_create_database(db_name : Text) : async (ZT.Result<(), Text>) {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")",
            func() : (ZT.Result<(), Text>) {

                Result.mapOk<Any, (), Text>(
                    ZenDB.createDB(zendb_instance, db_name),
                    func(_) { () },
                );

            },
        );
    };

    public shared query ({ caller }) func zendb_v1_list_database_names() : async [Text] {
        auth.allow(
            caller,
            Permissions.READ,
            "databases",
            func() : [Text] {
                ZenDB.listDatabaseNames(zendb_instance);
            },
        );
    };

    public shared composite query ({ caller }) func zendb_v1_list_database_names_composite_query() : async [Text] {
        auth.allow(
            caller,
            Permissions.READ,
            "databases",
            func() : [Text] {
                ZenDB.listDatabaseNames(zendb_instance);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_rename_database(old_name : Text, new_name : Text) : async (ZT.Result<(), Text>) {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # old_name # ")",
            func() : (ZT.Result<(), Text>) {
                ZenDB.renameDB(zendb_instance, old_name, new_name);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_create_collection(db_name : Text, collection_name : Text, schema : ZT.Schema, opt_options : ?ZT.CreateCollectionOptions) : async (ZT.Result<(), Text>) {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<(), Text>) {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    return #err("Database '" # db_name # "' does not exist");
                };

                let collection = StableDatabase.create_collection(db, collection_name, schema, opt_options);

                Result.mapOk<ZT.StableCollection, (), Text>(collection, func(_) { () });
            },
        );

    };

    public shared ({ caller }) func zendb_v1_delete_collection(db_name : Text, collection_name : Text) : async (ZT.Result<(), Text>) {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<(), Text>) {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    return #err("Database '" # db_name # "' does not exist");
                };

                StableDatabase.delete_collection(db, collection_name);
            },
        );
    };

    func get_collection(db_name : Text, collection_name : Text) : ZT.Result<ZT.StableCollection, Text> {
        let sstore = TypeMigrations.get_current_state(zendb_instance);
        let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
            return #err("Database '" # db_name # "' does not exist");
        };

        let #ok(collection) = StableDatabase.get_collection(db, collection_name) else {
            return #err("Collection '" # collection_name # "' does not exist in database '" # db_name # "'");
        };

        #ok(collection);
    };

    public shared ({ caller }) func zendb_v1_collection_insert_document(db_name : Text, collection_name : Text, candid_document_blob : Blob) : async (ZT.Result<ZT.DocumentId, Text>) {
        auth.allow_rs(
            caller,
            Permissions.WRITE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<ZT.DocumentId, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.insert(collection, candid_document_blob);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_insert_documents(db_name : Text, collection_name : Text, candid_document_blobs : [Blob]) : async (ZT.Result<[ZT.DocumentId], Text>) {
        auth.allow_rs(
            caller,
            Permissions.WRITE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<[ZT.DocumentId], Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.insert_docs(collection, candid_document_blobs);
            },
        );
    };

    func _zendb_collection_get_document(caller : Principal, db_name : Text, collection_name : Text, document_id : ZT.DocumentId) : (ZT.Result<ZT.CandidBlob, Text>) {
        auth.allow_rs(
            caller,
            Permissions.READ,
            "database:" # db_name # "/collection:" # collection_name # "/document:" # debug_show document_id,
            func() : (ZT.Result<ZT.CandidBlob, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                switch (StableCollection.get(collection, document_id)) {
                    case (?document_blob) #ok(document_blob);
                    case null #err("Document with id " # debug_show (document_id) # " does not exist in collection '" # collection_name # "' in database '" # db_name # "'");
                };
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_get_document(db_name : Text, collection_name : Text, document_id : ZT.DocumentId) : async (ZT.Result<ZT.CandidBlob, Text>) {
        _zendb_collection_get_document(caller, db_name, collection_name, document_id);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_get_document_composite_query(db_name : Text, collection_name : Text, document_id : ZT.DocumentId) : async (ZT.Result<ZT.CandidBlob, Text>) {
        _zendb_collection_get_document(caller, db_name, collection_name, document_id);
    };

    func _zendb_collection_search(caller : Principal, db_name : Text, collection_name : Text, query_builder : ZT.StableQuery) : (ZT.Result<ZT.SearchResult<Blob>, Text>) {
        auth.allow_rs(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<ZT.SearchResult<Blob>, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.search(collection, query_builder);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_search(
        db_name : Text,
        collection_name : Text,
        stable_query : ZT.StableQuery,
    ) : async (ZT.Result<ZT.SearchResult<Blob>, Text>) {
        _zendb_collection_search(caller, db_name, collection_name, stable_query);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_search_composite_query(
        db_name : Text,
        collection_name : Text,
        stable_query : ZT.StableQuery,
    ) : async (ZT.Result<ZT.SearchResult<Blob>, Text>) {
        _zendb_collection_search(caller, db_name, collection_name, stable_query);
    };

    func _zendb_collection_search_for_one(caller : Principal, db_name : Text, collection_name : Text, query_builder : ZT.StableQuery) : (ZT.Result<ZT.SearchOneResult<Blob>, Text>) {
        auth.allow_rs(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<ZT.SearchOneResult<Blob>, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.search_for_one(collection, query_builder);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_search_for_one(
        db_name : Text,
        collection_name : Text,
        stable_query : ZT.StableQuery,
    ) : async (ZT.Result<ZT.SearchOneResult<Blob>, Text>) {
        _zendb_collection_search_for_one(caller, db_name, collection_name, stable_query);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_search_for_one_composite_query(
        db_name : Text,
        collection_name : Text,
        stable_query : ZT.StableQuery,
    ) : async (ZT.Result<ZT.SearchOneResult<Blob>, Text>) {
        _zendb_collection_search_for_one(caller, db_name, collection_name, stable_query);
    };

    func _zendb_collection_size(caller : Principal, db_name : Text, collection_name : Text) : Nat {
        auth.allow(
            caller,
            Permissions.READ,
            "database:" # db_name # "/collection:" # collection_name,
            func() : (Nat) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Debug.trap("Collection not found");
                StableCollection.size(collection);

            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_size(db_name : Text, collection_name : Text) : async Nat {
        _zendb_collection_size(caller, db_name, collection_name);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_size_composite_query(db_name : Text, collection_name : Text) : async Nat {
        _zendb_collection_size(caller, db_name, collection_name);
    };

    func _zendb_collection_count(caller : Principal, db_name : Text, collection_name : Text, stable_query : ZT.StableQuery) : ZT.CountResult {
        auth.allow(
            caller,
            Permissions.READ,
            "database:" # db_name # "/collection:" # collection_name,
            func() : ZT.CountResult {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else Debug.trap("Collection not found");

                let count_result = StableCollection.count(collection, stable_query);
                let #ok(response) = count_result else {
                    let #err(err_msg) = count_result else Debug.trap("Unexpected error");
                    Debug.trap(err_msg);
                };

                response;
            },
        );
    };

    /// Returns the total number of documents that match the query.
    /// This ignores the limit and skip parameters.
    public shared query ({ caller }) func zendb_v1_collection_count(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery) : async ZT.CountResult {
        _zendb_collection_count(caller, db_name, collection_name, stable_query);
    };
    /// Returns the total number of documents that match the query using composite query.
    /// This ignores the limit and skip parameters.
    public shared composite query ({ caller }) func zendb_v1_collection_count_composite_query(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery) : async ZT.CountResult {
        _zendb_collection_count(caller, db_name, collection_name, stable_query);
    };

    func _zendb_stats(caller : Principal) : ZenDB.Types.InstanceStats {
        auth.allow(
            caller,
            Permissions.READ,
            "instance",
            func() : (ZenDB.Types.InstanceStats) {
                ZenDB.stats(zendb_instance);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_stats() : async ZenDB.Types.InstanceStats {
        _zendb_stats(caller);
    };

    public shared composite query ({ caller }) func zendb_v1_stats_composite_query() : async ZenDB.Types.InstanceStats {
        _zendb_stats(caller);
    };

    func _zendb_database_stats(caller : Principal, db_name : Text) : ZT.DatabaseStats {
        auth.allow(
            caller,
            Permissions.READ,
            "database:" # db_name,
            func() : ZT.DatabaseStats {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    Debug.trap("Database '" # db_name # "' does not exist");
                };

                StableDatabase.stats(db);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_database_stats(db_name : Text) : async ZT.DatabaseStats {
        _zendb_database_stats(caller, db_name);
    };

    public shared composite query ({ caller }) func zendb_v1_database_stats_composite_query(db_name : Text) : async ZT.DatabaseStats {
        _zendb_database_stats(caller, db_name);
    };

    func _zendb_database_list_collection_names(caller : Principal, db_name : Text) : [Text] {
        auth.allow(
            caller,
            Permissions.READ,
            "database:" # db_name # "/collections",
            func() : [Text] {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    Debug.trap("Database '" # db_name # "' does not exist");
                };

                StableDatabase.list_collection_names(db);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_database_list_collection_names(db_name : Text) : async [Text] {
        _zendb_database_list_collection_names(caller, db_name);
    };

    public shared composite query ({ caller }) func zendb_v1_database_list_collection_names_composite_query(db_name : Text) : async [Text] {
        _zendb_database_list_collection_names(caller, db_name);
    };

    func _zendb_database_get_collection_stats(caller : Principal, db_name : Text, collection_name : Text) : ?ZT.CollectionStats {
        auth.allow(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : ?ZT.CollectionStats {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    Debug.trap("Database '" # db_name # "' does not exist");
                };

                StableDatabase.get_collection_stats(db, collection_name);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_database_get_collection_stats(db_name : Text, collection_name : Text) : async ?ZT.CollectionStats {
        _zendb_database_get_collection_stats(caller, db_name, collection_name);
    };

    public shared composite query ({ caller }) func zendb_v1_database_get_collection_stats_composite_query(db_name : Text, collection_name : Text) : async ?ZT.CollectionStats {
        _zendb_database_get_collection_stats(caller, db_name, collection_name);
    };

    func _zendb_database_get_all_collections_stats(caller : Principal, db_name : Text) : [(Text, ZT.CollectionStats)] {
        auth.allow(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collections",
            func() : [(Text, ZT.CollectionStats)] {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    Debug.trap("Database '" # db_name # "' does not exist");
                };

                StableDatabase.get_all_collections_stats(db);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_database_get_all_collections_stats(db_name : Text) : async [(Text, ZT.CollectionStats)] {
        _zendb_database_get_all_collections_stats(caller, db_name);
    };

    public shared composite query ({ caller }) func zendb_v1_database_get_all_collections_stats_composite_query(db_name : Text) : async [(Text, ZT.CollectionStats)] {
        _zendb_database_get_all_collections_stats(caller, db_name);
    };

    func _zendb_collection_stats(caller : Principal, db_name : Text, collection_name : Text) : ZT.CollectionStats {
        auth.allow(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : ZT.CollectionStats {
                let collection_result = get_collection(db_name, collection_name);
                let #ok(collection) = collection_result else {
                    let #err(err_msg) = collection_result else Debug.trap("Unexpected error");
                    Debug.trap(err_msg);
                };

                StableCollection.stats(collection);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_stats(db_name : Text, collection_name : Text) : async ZT.CollectionStats {
        _zendb_collection_stats(caller, db_name, collection_name);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_stats_composite_query(db_name : Text, collection_name : Text) : async ZT.CollectionStats {
        _zendb_collection_stats(caller, db_name, collection_name);
    };

    func _zendb_collection_get_schema(caller : Principal, db_name : Text, collection_name : Text) : ZT.Result<ZT.Schema, Text> {
        auth.allow(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collection(" # collection_name # ")/schema",
            func() : (ZT.Result<ZT.Schema, Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                #ok(collection.schema);
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_get_schema(db_name : Text, collection_name : Text) : async ZT.Result<ZT.Schema, Text> {
        _zendb_collection_get_schema(caller, db_name, collection_name);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_get_schema_composite_query(db_name : Text, collection_name : Text) : async ZT.Result<ZT.Schema, Text> {
        _zendb_collection_get_schema(caller, db_name, collection_name);
    };

    func _zendb_collection_list_index_names(caller : Principal, db_name : Text, collection_name : Text) : ZT.Result<[Text], Text> {
        auth.allow(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collection(" # collection_name # ")/indexes",
            func() : ZT.Result<[Text], Text> {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                #ok(StableCollection.list_index_names(collection));
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_list_index_names(db_name : Text, collection_name : Text) : async ZT.Result<[Text], Text> {
        _zendb_collection_list_index_names(caller, db_name, collection_name);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_list_index_names_composite_query(db_name : Text, collection_name : Text) : async ZT.Result<[Text], Text> {
        _zendb_collection_list_index_names(caller, db_name, collection_name);
    };

    func _zendb_collection_get_indexes(caller : Principal, db_name : Text, collection_name : Text) : ZT.Result<[(Text, ZT.IndexStats)], Text> {
        auth.allow(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collection(" # collection_name # ")/indexes",
            func() : ZT.Result<[(Text, ZT.IndexStats)], Text> {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                #ok(StableCollection.get_indexes(collection));
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_get_indexes(db_name : Text, collection_name : Text) : async ZT.Result<[(Text, ZT.IndexStats)], Text> {
        _zendb_collection_get_indexes(caller, db_name, collection_name);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_get_indexes_composite_query(db_name : Text, collection_name : Text) : async ZT.Result<[(Text, ZT.IndexStats)], Text> {
        _zendb_collection_get_indexes(caller, db_name, collection_name);
    };

    func _zendb_collection_get_index(caller : Principal, db_name : Text, collection_name : Text, index_name : Text) : ZT.Result<?ZT.IndexStats, Text> {
        auth.allow(
            caller,
            Permissions.READ,
            "database(" # db_name # ")/collection(" # collection_name # ")/index(" # index_name # ")",
            func() : ZT.Result<?ZT.IndexStats, Text> {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                #ok(StableCollection.get_index(collection, index_name));
            },
        );
    };

    public shared query ({ caller }) func zendb_v1_collection_get_index(db_name : Text, collection_name : Text, index_name : Text) : async ZT.Result<?ZT.IndexStats, Text> {
        _zendb_collection_get_index(caller, db_name, collection_name, index_name);
    };

    public shared composite query ({ caller }) func zendb_v1_collection_get_index_composite_query(db_name : Text, collection_name : Text, index_name : Text) : async ZT.Result<?ZT.IndexStats, Text> {
        _zendb_collection_get_index(caller, db_name, collection_name, index_name);
    };

    public shared ({ caller }) func zendb_v1_collection_replace_document(db_name : Text, collection_name : Text, id : ZT.DocumentId, document_blob : Blob) : async ZT.Result<ZT.ReplaceByIdResult, Text> {
        auth.allow_rs(
            caller,
            Permissions.WRITE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<ZT.ReplaceByIdResult, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.replace_by_id(collection, id, document_blob);
            },
        );

    };

    public shared ({ caller }) func zendb_v1_collection_delete_document_by_id(db_name : Text, collection_name : Text, id : ZT.DocumentId) : async ZT.Result<ZT.DeleteByIdResult<Blob>, Text> {
        auth.allow_rs(
            caller,
            Permissions.WRITE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<ZT.DeleteByIdResult<Blob>, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.delete_by_id(collection, id);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_delete_documents(db_name : Text, collection_name : Text, db_query : ZT.StableQuery) : async ZT.Result<ZT.DeleteResult<Blob>, Text> {
        auth.allow_rs(
            caller,
            Permissions.WRITE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<ZT.DeleteResult<Blob>, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                let blobify : ZT.InternalCandify<Blob> = {
                    to_blob = func(blob : Blob) : Blob { blob };
                    from_blob = func(blob : Blob) : Blob { blob };
                };

                StableCollection.delete_documents(collection, blobify, db_query);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_update_document_by_id(db_name : Text, collection_name : Text, id : ZT.DocumentId, update_operations : [(Text, ZT.FieldUpdateOperations)]) : async ZT.Result<ZT.UpdateByIdResult, Text> {
        auth.allow_rs(
            caller,
            Permissions.WRITE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<ZT.UpdateByIdResult, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.update_by_id(collection, id, update_operations);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_update_documents(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery, update_operations : [(Text, ZT.FieldUpdateOperations)]) : async ZT.Result<ZT.UpdateResult, Text> {
        auth.allow_rs(
            caller,
            Permissions.WRITE,
            "database(" # db_name # ")/collection(" # collection_name # ")",
            func() : (ZT.Result<ZT.UpdateResult, Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                StableCollection.update_documents(collection, stable_query, update_operations);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_create_index(db_name : Text, collection_name : Text, index_name : Text, index_fields : [(Text, ZT.CreateIndexSortDirection)], options : ?ZT.CreateIndexOptions) : async ZT.Result<(), Text> {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")/index(" # index_name # ")",
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.create_and_populate_index_in_one_call(
                    collection,
                    index_name,
                    index_fields,
                    ZT.CreateIndexOptions.internal_from_opt(options),
                );
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_delete_index(db_name : Text, collection_name : Text, index_name : Text) : async ZT.Result<(), Text> {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")/index(" # index_name # ")",
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.delete_index(collection, index_name);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_delete_indexes(db_name : Text, collection_name : Text, index_names : [Text]) : async ZT.Result<(), Text> {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")/indexes",
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.delete_indexes(collection, index_names);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_hide_indexes(db_name : Text, collection_name : Text, index_names : [Text]) : async ZT.Result<(), Text> {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")/indexes",
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.hide_indexes(collection, index_names);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_unhide_indexes(db_name : Text, collection_name : Text, index_names : [Text]) : async ZT.Result<(), Text> {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")/indexes",
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.unhide_indexes(collection, index_names);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_batch_create_indexes(db_name : Text, collection_name : Text, index_configs : [ZT.CreateIndexParams]) : async ZT.Result<Nat, Text> {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")/indexes",
            func() : (ZT.Result<Nat, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                let internal_index_configs = Array.map<ZT.CreateIndexParams, ZT.CreateInternalIndexParams>(
                    index_configs,
                    func(config : ZT.CreateIndexParams) : ZT.CreateInternalIndexParams {
                        (
                            config.0,
                            config.1,
                            ZT.CreateIndexOptions.internal_from_opt(config.2),
                        );
                    },
                );

                StableCollection.batch_create_indexes(collection, internal_index_configs);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_batch_populate_indexes(db_name : Text, collection_name : Text, index_names : [Text]) : async ZT.Result<Nat, Text> {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")/indexes",
            func() : (ZT.Result<Nat, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.batch_populate_indexes_from_names(collection, index_names);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_process_index_batch(db_name : Text, collection_name : Text, batch_id : Nat) : async ZT.Result<Bool, Text> {
        auth.allow_rs(
            caller,
            Permissions.MANAGE,
            "database(" # db_name # ")/collection(" # collection_name # ")/batch(" # debug_show (batch_id) # ")",
            func() : (ZT.Result<Bool, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.populate_indexes_in_batch(collection, batch_id, null);
            },
        );
    };

    /// Cluster management functions
    /// Need to implement to be compatible with the ClusterServiceApi
    public shared query func zendb_v1_list_canisters() : async [ClusterTypes.CanisterInfo] {
        [];
    };

    public shared composite query func zendb_v1_list_canisters_composite_query() : async [ClusterTypes.CanisterInfo] {
        [];
    };

    public shared query func zendb_v1_canister_stats() : async ([ZT.InstanceStats]) {
        [];
    };

    public shared composite query func zendb_v1_canister_stats_composite_query() : async ([ZT.InstanceStats]) {
        [];
    };

    public shared ({ caller }) func zendb_v1_clear_cache() : async () {
        auth.allow<()>(
            caller,
            Permissions.MANAGE,
            "instance/cache",
            func() : () {
                ZenDB.clearCache(zendb_instance);
            },
        );
    };

};
