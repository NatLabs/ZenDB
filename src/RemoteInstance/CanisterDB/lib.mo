import Array "mo:core@2.4/Array";
import Principal "mo:core@2.4/Principal";
import Random "mo:core@2.4/Random";
import Int64 "mo:core@2.4/Int64";
import Blob "mo:core@2.4/Blob";
import Result "mo:core@2.4/Result";
import Debug "mo:core@2.4/Debug";
import Buffer "mo:base@0.16/Buffer";
import Iter "mo:core@2.4/Iter";
import Error "mo:core@2.4/Error";

import Map "mo:map@9.0/Map";
import CanisterRBAC "mo:canister-rbac@0.1";

import ClusterTypes "../Types";
import ZenDB "../../EmbeddedInstance";

import ZT "../../EmbeddedInstance/Types";
import EmbeddedInstance "../../EmbeddedInstance";
import StableDatabase "../../EmbeddedInstance/Database/StableDatabase";
import StableCollection "../../EmbeddedInstance/Collection/StableCollection";
import CollectionUtils "../../EmbeddedInstance/Collection/CollectionUtils";
import Utils "../../EmbeddedInstance/Utils";
import TypeMigrations "../../EmbeddedInstance/TypeMigrations";
import Query "../../EmbeddedInstance/Query";
import Runtime "mo:core@2.4/Runtime";
import Upgrade "./upgrade";

(
    with migration = func({
        zendb_instance : ZenDB.Types.PrevVersionedStableStore;
        canister_rbac : CanisterRBAC.Types.VersionedStableStore;
        Roles: Upgrade.Roles_v0_2_0;
        Permissions: Upgrade.Permissions_v0_2_0;
        Resource: Upgrade.Resource_v0_2_0;
    }) : ({
        zendb_instance : ZenDB.Types.VersionedStableStore;
        canister_rbac : CanisterRBAC.Types.VersionedStableStore;
    }) {
        Upgrade.applyAll({ canister_rbac; Roles; Permissions; Resource; });

        return {
            zendb_instance = ZenDB.upgrade(zendb_instance);
            canister_rbac;
        };
    }
)

shared ({ caller = owner }) persistent actor class CanisterDB() = this_canister {

    transient let Resource = {
        DATABASE = "database";
        COLLECTION = "collection";
    };

    transient let Permissions = {
        DB_READ = "db:read";
        DB_WRITE = "db:write";
        DB_MANAGE = "db:manage";

        ACCESS_CONTROL_READ = "access-control:read";
        ACCESS_CONTROL_MANAGE = "access-control:manage";
    };

    transient let Roles = {
        ADMIN = "admin";
        OBSERVER = "observer";
        WRITER = "writer";
        READER = "reader";
    };

    transient let default_roles = [
        {
            name = Roles.ADMIN;
            permissions = [
                Permissions.DB_MANAGE, Permissions.DB_WRITE, Permissions.DB_READ,
                Permissions.ACCESS_CONTROL_MANAGE, Permissions.ACCESS_CONTROL_READ
            ];
        },
        {
            name = Roles.OBSERVER;
            permissions = [Permissions.ACCESS_CONTROL_READ];
        },
        {
            name = Roles.WRITER;
            permissions = [Permissions.DB_WRITE, Permissions.DB_READ];
        },
        {
            name = Roles.READER;
            permissions = [Permissions.DB_READ];
        },
    ];

    var canister_rbac = CanisterRBAC.initRoles(default_roles);

    let canister_id = Principal.fromActor(this_canister);
    let canister_id_as_blob = Principal.toBlob(canister_id);

    Result.assertOk(
        CanisterRBAC.grantUserRole(canister_rbac, owner, Roles.ADMIN, [])
    );

    Result.assertOk(
        CanisterRBAC.grantUserRole(canister_rbac, canister_id, Roles.ADMIN, [])
    );

    var zendb_instance = ZenDB.newStableStore(canister_id, null);

    /// Principal of the db_access_registry canister that receives push notifications
    /// after every successful grant / revoke. Null means pushes are disabled.
    var db_access_registry : ?Principal = null;


    type AccessUpdateEvent = { #grant; #revoke };
    type DbAccessRegistryService = actor {
        push_user_access_update : shared (Principal, AccessUpdateEvent, [(Text, Text)], Text, [Text]) -> async ();
    };

    system func postupgrade<system>() {};

    ZenDB.setLogLevel(zendb_instance, #Debug);
    ZenDB.setIsRunLocally(zendb_instance, false);
    ZenDB.updateCacheCapacity(zendb_instance, 1_000_000);

    public shared query func zendb_v1_api_version() : async Text {
        Upgrade.CURRENT_API_VERSION 
    };

     /// Returns the version of the embedded ZenDB engine running inside this canister.
     /// Useful for determining whether an upgrade will trigger a data migration as
     /// major versions of the embedded engine may include breaking changes to the stable store format.
     public shared query func zendb_v1_embedded_version() : async Text {
          TypeMigrations.to_text(zendb_instance);
     };

    /// ::: Access Control API :::

    /// Set (or clear) the db_access_registry canister that receives push notifications.
    /// Only the canister owner may call this.
    public shared ({ caller }) func set_db_access_registry(registry : ?Principal) : async ZT.Result<(), Text> {

        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.ACCESS_CONTROL_MANAGE,
            [],
            func() : ZT.Result<(), Text> {
                db_access_registry := registry;
                #ok(())
            }
        );

    };

    /// Fire-and-forget: notify the db_access_registry of a specific access change.
    /// Sends only the single entry that was updated, tagged with the event kind so
    /// the registry knows whether to append or remove.
    /// Safe to call from any shared function context; errors are silently discarded.
    func emit_user_access_details(user : Principal, event : { #grant; #revoke }, role : Text, scope : [(Text, Text)]) : async* () {
        switch (db_access_registry) {
            case (?registry_id) {
                let permissions = switch (CanisterRBAC.getRolePermissions(canister_rbac, role)) {
                    case (#ok(p)) p;
                    case (#err(_)) [];
                };
                let reg : DbAccessRegistryService = actor(Principal.toText(registry_id));
                ignore await reg.push_user_access_update(user, event, scope, role, permissions);
            };
            case (null) {};
        };
    };

     func _grant_user_access_to(caller: Principal, target: Principal, role: Text, Resource_scope: [(Text, Text)]) : async* ZT.Result<(), Text> {

          switch(CanisterRBAC.getRole(canister_rbac, role)){
               case (#ok(_)) {};
               case (#err(msg)) return #err("grant_role() failed: " # msg)
          };

          // Requires access-control:manage at the requested scope.
          // Enforces a privilege escalation check: the caller can only grant a role
          // whose full permission set is already possessed by the caller at that scope.
          // This ensures users can only delegate access they already hold.
          let res = CanisterRBAC.allowWithResult(
               canister_rbac,
               caller,
               Permissions.ACCESS_CONTROL_MANAGE,
               Resource_scope,
               func() : ZT.Result<(), Text> {
                    let role_perms = switch (CanisterRBAC.getRolePermissions(canister_rbac, role)) {
                         case (#err(e)) return #err(e);
                         case (#ok(perms)) perms;
                    };
                    for (perm in role_perms.vals()) {
                         if (not CanisterRBAC.hasPermission(canister_rbac, caller, perm, Resource_scope)) {
                              return #err("Cannot grant role '" # role # "': you do not have permission '" # perm # "' at this scope");
                         };
                    };

                    CanisterRBAC.grantUserRole(canister_rbac, target, role, Resource_scope);

               },
          );

          switch (res){
            case (#ok(_)) await* emit_user_access_details(target, #grant, role, Resource_scope);
            case (#err(msg)) return #err("grant_role() failed: " # msg)
          };

          res
     };

     public shared ({caller}) func grant_user_access_to(target: Principal, role: Text, Resource_scope: [(Text, Text)]) : async (ZT.Result<(), Text>) {
          await* _grant_user_access_to(caller, target, role, Resource_scope)
     };

     public shared ({caller}) func grant_database_access(target: Principal, role: Text, db_name: Text) : async (ZT.Result<(), Text>) {
          await* _grant_user_access_to(caller, target, role, [(Resource.DATABASE, db_name)])
     };

     public shared ({caller}) func grant_collection_access(target: Principal, role: Text, db_name: Text, collection_name: Text) : async (ZT.Result<(), Text>) {
          await* _grant_user_access_to(caller, target, role, [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)])
     };

     public shared ({caller}) func grant_global_access(target: Principal, role: Text) : async (ZT.Result<(), Text>) {
          await* _grant_user_access_to(caller, target, role, [])
     };

     public shared ({ caller }) func grant_role(target : Principal, role : Text) : async (ZT.Result<(), Text>) {
          await* _grant_user_access_to(caller, target, role, [])
     };

     func _revoke_user_access_to(caller: Principal, target: Principal, role: Text, resource_scope: [(Text, Text)]) : async* (ZT.Result<(), Text>) {

        // Self-revocation: users may always remove their own grants without any permission check.
        let res = if (caller == target) {
            CanisterRBAC.revokeUserRole(canister_rbac, target, role, resource_scope);
        } else {
            // Revoking another user's grant requires access_control:manage at the same scope.
            CanisterRBAC.allowWithResult(
                canister_rbac,
                caller,
                Permissions.ACCESS_CONTROL_MANAGE,
                resource_scope,
                func() : ZT.Result<(), Text> {
                    CanisterRBAC.revokeUserRole(canister_rbac, target, role, resource_scope);
                },
            );
        };

        switch (res){
            case (#ok(_)) await* emit_user_access_details(target, #revoke, role, resource_scope);
            case (#err(msg)) {};
        };

        res

     };

     public shared ({ caller }) func revoke_user_access_to(target: Principal, role: Text, Resource_scope: [(Text, Text)]) : async (ZT.Result<(), Text>) {
          await* _revoke_user_access_to(caller, target, role, Resource_scope)
     };

     public shared ({ caller }) func revoke_database_access(target: Principal, role: Text, db_name: Text) : async (ZT.Result<(), Text>) {
          await* _revoke_user_access_to(caller, target, role, [(Resource.DATABASE, db_name)])
     };

     public shared ({caller}) func revoke_collection_access(target: Principal, role: Text, db_name: Text, collection_name: Text) : async (ZT.Result<(), Text>) {
          await* _revoke_user_access_to(caller, target, role, [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)])
     };

     public shared ({caller}) func revoke_global_access(target: Principal, role: Text) : async (ZT.Result<(), Text>) {
          await* _revoke_user_access_to(caller, target, role, [])
     };


     public type UserAccessDetails = ClusterTypes.UserAccessDetails;


     func _get_user_access_details(caller: Principal, user: Principal) : ZT.Result<UserAccessDetails, Text> {
          // Self-query: users can always view their own grants.
          if (caller == user) return #ok(CanisterRBAC.getUserGrants(canister_rbac, user));

          CanisterRBAC.allowWithResult(
               canister_rbac,
               caller,
               Permissions.ACCESS_CONTROL_READ,
               [],
               func() : ZT.Result<UserAccessDetails, Text> {
                    #ok(CanisterRBAC.getUserGrants(canister_rbac, user));
               },
          );
     };

     public shared query ({ caller }) func get_user_access_details(user: Principal) : async ZT.Result<UserAccessDetails, Text> {
          _get_user_access_details(caller, user);
     };

     public shared query ({ caller }) func get_my_access_details() : async ZT.Result<UserAccessDetails, Text> {
          _get_user_access_details(caller, caller);
     };


    /// ::: Admin API :::

    public shared query ({caller}) func get_all_users_access_details() : async ZT.Result<[(Principal, UserAccessDetails)], Text> {
          // Viewing all users' grants is sensitive admin information; requires access-control:read.
          CanisterRBAC.allowWithResult(
               canister_rbac,
               caller,
               Permissions.ACCESS_CONTROL_READ,
               [],
               func() : ZT.Result<[(Principal, UserAccessDetails)], Text> {
                    #ok(CanisterRBAC.getAllUserGrants(canister_rbac));
               },
          );
     };

    func throw_if_error<A>(res : ZT.Result<A, Text>) : async* () {
        switch (res) {
            case (#ok(value)) {};
            case (#err(msg)) throw Error.reject(msg);
        };
    };

    func extract_ok<A>(res : ZT.Result<A, Text>) : A {
        switch (res) {
            case (#ok(value)) value;
            case (#err(e)) Runtime.trap("Unexpected error: " # e);
        };
     };

    /// ::: ZenDB Settings API :::
    public shared ({caller}) func update_log_level(log_level: ZenDB.Types.LogLevel) : async (ZT.Result<(), Text>) {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [],
            func() : ZT.Result<(), Text> {
                ZenDB.setLogLevel(zendb_instance, log_level);
                #ok(())
            },
        );
    };



    /// ::: ZenDB Database API :::

     public shared query ({ caller }) func zendb_v1_list_database_names() : async (ZT.Result<[Text], Text>) {
          CanisterRBAC.allow(
               canister_rbac,
               caller,
               Permissions.DB_READ,
               [(Resource.DATABASE, "*")],
               func() : [Text] {
                    ZenDB.listDatabaseNames(zendb_instance);
               },
          );
     };


    public shared composite query ({ caller }) func zendb_v1_list_database_names_composite_query() : async [Text] {
        let res = CanisterRBAC.allow(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, "*")],
            func() : [Text] {
                ZenDB.listDatabaseNames(zendb_instance);
            },
        );

        await* throw_if_error(res);
        extract_ok(res)
    };

    // public shared ({ caller }) func zendb_v1_rename_database(old_name : Text, new_name : Text) : async (ZT.Result<(), Text>) {
    //     CanisterRBAC.allowWithResult(
    //         canister_rbac,
    //         caller,
    //         Permissions.DB_MANAGE,
    //         [(Resource.DATABASE, old_name)],
    //         func() : (ZT.Result<(), Text>) {
    //              // ! users who had access to the database with the previous name will lose access after this rename
    //              // ! you need a way to a Resource name so that you can update the RBAC permissions accordingly
    //             ZenDB.renameDB(zendb_instance, old_name, new_name);
    //         },
    //     );
    // };

    public shared ({ caller }) func zendb_v1_create_database(db_name : Text) : async (ZT.Result<(), Text>) {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [],
            func() : (ZT.Result<(), Text>) {
                let res = ZenDB.createDB(zendb_instance, db_name);
                switch (res) {
                    case (#err(e)) #err(e);
                    case (#ok(_)) #ok(());
                };
            },
        );
    };

    public shared ({ caller }) func zendb_v1_delete_database(db_name : Text) : async (ZT.Result<(), Text>) {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name)],
            func() : (ZT.Result<(), Text>) {
                ZenDB.deleteDB(zendb_instance, db_name);
            },
        );
    };


    public shared ({ caller }) func zendb_v1_create_collection(db_name : Text, collection_name : Text, schema : ZT.Schema, opt_options : ?ZT.CreateCollectionOptions) : async (ZT.Result<(), Text>) {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_WRITE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<ZT.DocumentId, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.insert(collection, candid_document_blob);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_insert_documents(db_name : Text, collection_name : Text, candid_document_blobs : [Blob]) : async (ZT.Result<[ZT.DocumentId], Text>) {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_WRITE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<[ZT.DocumentId], Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.insert_docs(collection, candid_document_blobs);
            },
        );
    };

    func _zendb_collection_get_document(caller : Principal, db_name : Text, collection_name : Text, document_id : ZT.DocumentId) : (ZT.Result<ZT.CandidBlob, Text>) {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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

    public shared ({ caller }) func zendb_v1_collection_search_update(
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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

    public shared ({ caller }) func zendb_v1_collection_search_for_one_update(
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
        CanisterRBAC.requirePermission<Nat>(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (Nat) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Runtime.trap("Collection not found");
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
        CanisterRBAC.requirePermission<ZT.CountResult>(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : ZT.CountResult {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else Runtime.trap("Collection not found");

                let count_result = StableCollection.count(collection, stable_query);
                let #ok(response) = count_result else {
                    let #err(err_msg) = count_result else Runtime.trap("Unexpected error");
                    Runtime.trap(err_msg);
                };

                response;
            },
        );
    };

    /// Returns the total number of documents that match the query.
    /// This ignores the limit and skip parameters.
    public shared ({ caller }) func zendb_v1_collection_count(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery) : async ZT.CountResult {
        _zendb_collection_count(caller, db_name, collection_name, stable_query);
    };
    /// Returns the total number of documents that match the query using composite query.
    /// This ignores the limit and skip parameters.
    public shared composite query ({ caller }) func zendb_v1_collection_count_composite_query(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery) : async ZT.CountResult {
        _zendb_collection_count(caller, db_name, collection_name, stable_query);
    };

    func _zendb_stats(caller : Principal) : ZenDB.Types.InstanceStats {
        CanisterRBAC.requirePermission(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [],
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
        CanisterRBAC.requirePermission(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name)],
            func() : ZT.DatabaseStats {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    Runtime.trap("Database '" # db_name # "' does not exist");
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
        CanisterRBAC.requirePermission(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, "*")],
            func() : [Text] {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    Runtime.trap("Database '" # db_name # "' does not exist");
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
        CanisterRBAC.requirePermission(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : ?ZT.CollectionStats {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    Runtime.trap("Database '" # db_name # "' does not exist");
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
        CanisterRBAC.requirePermission(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, "*")],
            func() : [(Text, ZT.CollectionStats)] {
                let sstore = TypeMigrations.get_current_state(zendb_instance);
                let ?db = Map.get<Text, ZT.StableDatabase>(sstore.databases, Map.thash, db_name) else {
                    Runtime.trap("Database '" # db_name # "' does not exist");
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
        CanisterRBAC.requirePermission(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : ZT.CollectionStats {
                let collection_result = get_collection(db_name, collection_name);
                let #ok(collection) = collection_result else {
                    let #err(err_msg) = collection_result else Runtime.trap("Unexpected error");
                    Runtime.trap(err_msg);
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_READ,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_WRITE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<ZT.ReplaceByIdResult, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.replace_by_id(collection, id, document_blob);
            },
        );

    };

    public shared ({ caller }) func zendb_v1_collection_delete_document_by_id(db_name : Text, collection_name : Text, id : ZT.DocumentId) : async ZT.Result<ZT.DeleteByIdResult<Blob>, Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_WRITE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<ZT.DeleteByIdResult<Blob>, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.delete_by_id(collection, id);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_delete_documents(db_name : Text, collection_name : Text, db_query : ZT.StableQuery) : async ZT.Result<ZT.DeleteResult<Blob>, Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_WRITE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_WRITE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<ZT.UpdateByIdResult, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.update_by_id(collection, id, update_operations);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_update_documents(db_name : Text, collection_name : Text, stable_query : ZT.StableQuery, update_operations : [(Text, ZT.FieldUpdateOperations)]) : async ZT.Result<ZT.UpdateResult, Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_WRITE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<ZT.UpdateResult, Text>) {
                let collection_res = get_collection(db_name, collection_name);
                let #ok(collection) = collection_res else return Utils.send_error(collection_res);

                StableCollection.update_documents(collection, stable_query, update_operations);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_create_index(db_name : Text, collection_name : Text, index_name : Text, index_fields : [(Text, ZT.CreateIndexSortDirection)], options : ?ZT.CreateIndexOptions) : async ZT.Result<(), Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.delete_index(collection, index_name);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_delete_indexes(db_name : Text, collection_name : Text, index_names : [Text]) : async ZT.Result<(), Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.delete_indexes(collection, index_names);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_hide_indexes(db_name : Text, collection_name : Text, index_names : [Text]) : async ZT.Result<(), Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.hide_indexes(collection, index_names);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_unhide_indexes(db_name : Text, collection_name : Text, index_names : [Text]) : async ZT.Result<(), Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.unhide_indexes(collection, index_names);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_batch_create_indexes(db_name : Text, collection_name : Text, index_configs : [ZT.CreateIndexParams]) : async ZT.Result<Nat, Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
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
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<Nat, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.batch_populate_indexes_from_names(collection, index_names);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_process_index_batch(db_name : Text, collection_name : Text, batch_id : Nat) : async ZT.Result<Bool, Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<Bool, Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.populate_indexes_in_batch(collection, batch_id, null);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_create_text_index(db_name : Text, collection_name : Text, index_name : Text, fields : [Text]) : async ZT.Result<(), Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.create_text_index(collection, index_name, fields, #basic);
            },
        );
    };

    public shared ({ caller }) func zendb_v1_collection_delete_text_index(db_name : Text, collection_name : Text) : async ZT.Result<(), Text> {
        CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, db_name), (Resource.COLLECTION, collection_name)],
            func() : (ZT.Result<(), Text>) {
                let #ok(collection) = get_collection(db_name, collection_name) else return Utils.send_error(get_collection(db_name, collection_name));

                StableCollection.delete_text_index(collection);
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
        ignore CanisterRBAC.allowWithResult(
            canister_rbac,
            caller,
            Permissions.DB_MANAGE,
            [(Resource.DATABASE, "*")],
            func() : (ZT.Result<(), Text>) {
                ZenDB.clearCache(zendb_instance);
                #ok(())
            },
        );
    };

};
