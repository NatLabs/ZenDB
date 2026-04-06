// tests/cluster-tests/UserProxy.mo
//
// A lightweight proxy canister for permission testing.
// Each deployed instance has its own unique canister principal, so calls it
// makes to CanisterDB are attributed to that principal — exactly what we need
// to simulate different users with different roles.

import Principal "mo:core@2.4/Principal";

import ZT "../../src/EmbeddedInstance/Types";
import ClusterTypes "../../src/RemoteInstance/Types";
import CanisterDBModule "../../src/RemoteInstance/CanisterDB";

persistent actor class UserProxy(db_principal : Principal) = this_proxy {

    // Persist the target principal so transient bindings can be re-derived
    // on upgrade without losing track of the database canister.
    var _db_principal = db_principal;

    transient let db : CanisterDBModule.CanisterDB =
        actor (Principal.toText(_db_principal));

    /// Returns this proxy's canister principal — the identity CanisterDB sees.
    public func whoami() : async Principal {
        Principal.fromActor(this_proxy)
    };

    // ── DB operations ──────────────────────────────────────────────────────────

    public func create_collection(
        db_name : Text,
        col_name : Text,
        schema : ZT.Schema,
    ) : async ZT.Result<(), Text> {
        await db.zendb_v1_create_collection(db_name, col_name, schema, null)
    };

    public func insert(
        db_name : Text,
        col_name : Text,
        blob : Blob,
    ) : async ZT.Result<ZT.DocumentId, Text> {
        await db.zendb_v1_collection_insert_document(db_name, col_name, blob)
    };

    public func get(
        db_name : Text,
        col_name : Text,
        doc_id : ZT.DocumentId,
    ) : async ZT.Result<Blob, Text> {
        await db.zendb_v1_collection_get_document(db_name, col_name, doc_id)
    };

    public func search(
        db_name : Text,
        col_name : Text,
        q : ZT.StableQuery,
    ) : async ZT.Result<ZT.SearchResult<Blob>, Text> {
        await db.zendb_v1_collection_search(db_name, col_name, q)
    };

    // ── Grant / revoke ─────────────────────────────────────────────────────────

    public func grant_global(
        target : Principal,
        role : Text,
    ) : async ZT.Result<(), Text> {
        await db.grant_global_access(target, role)
    };

    public func grant_db(
        target : Principal,
        role : Text,
        db_name : Text,
    ) : async ZT.Result<(), Text> {
        await db.grant_database_access(target, role, db_name)
    };

    public func grant_coll(
        target : Principal,
        role : Text,
        db_name : Text,
        col_name : Text,
    ) : async ZT.Result<(), Text> {
        await db.grant_collection_access(target, role, db_name, col_name)
    };

    public func revoke_global(
        target : Principal,
        role : Text,
    ) : async ZT.Result<(), Text> {
        await db.revoke_global_access(target, role)
    };

    // ── Access-detail queries ──────────────────────────────────────────────────

    public func get_my_access() : async ZT.Result<ClusterTypes.UserAccessDetails, Text> {
        await db.get_my_access_details()
    };

    public func get_user_access(
        user : Principal,
    ) : async ZT.Result<ClusterTypes.UserAccessDetails, Text> {
        await db.get_user_access_details(user)
    };

    public func get_all_access() : async ZT.Result<[(Principal, ClusterTypes.UserAccessDetails)], Text> {
        await db.get_all_users_access_details()
    };
};
