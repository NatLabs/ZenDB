import ZT "../../EmbeddedInstance/Types";
import Principal "mo:base@0.16.0/Principal";

import CollectionClient "Collection.Client";
import DatabaseClient "Database.Client";

import ClusterTypes "../Types";
import CanisterDBModule "../CanisterDB";

module Client {

    public type CollectionClient<Record> = CollectionClient.CollectionClient<Record>;

    public class Client(canister_id : Text) {
        let canister_db : CanisterDBModule.CanisterDB = actor (canister_id);

        public func apiVersion() : async* Text {
            await canister_db.zendb_v1_api_version();
        };

        public func getDB(db_name : Text) : DatabaseClient.DatabaseClient {
            DatabaseClient.DatabaseClient(canister_db, db_name);
        };

        public func createDB(db_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_v1_create_database(db_name);
        };

        /// List all database names
        public func listDatabaseNames() : async* ZT.Result<[Text], Text> {
            await canister_db.zendb_v1_list_database_names();
        };

        /// Rename a database
        public func renameDB(old_name : Text, new_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_v1_rename_database(old_name, new_name);
        };

        /// Delete a database
        public func deleteDB(db_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_v1_delete_database(db_name);
        };

        public func launchDefaultDB() : DatabaseClient.DatabaseClient {
            DatabaseClient.DatabaseClient(canister_db, "default");
        };

        // Helper functions for easier access

        /// List all canisters in the cluster
        public func listCanisters() : async* [ClusterTypes.CanisterInfo] {
            await canister_db.zendb_v1_list_canisters();
        };

        /// Get statistics for all canisters
        public func canisterStats() : async* [ZT.InstanceStats] {
            await canister_db.zendb_v1_canister_stats();
        };

        /// Get instance statistics
        public func stats() : async* ZT.InstanceStats {
            await canister_db.zendb_v1_stats();
        };

        // ─── Grant ───────────────────────────────────────────────────────────

        /// Grant a scoped role to a principal
        public func grantUserAccessTo(target : Principal, role : Text, resource_scope : [(Text, Text)]) : async* ZT.Result<(), Text> {
            await canister_db.grant_user_access_to(target, role, resource_scope);
        };

        /// Grant a role scoped to a specific database
        public func grantDatabaseAccess(target : Principal, role : Text, db_name : Text) : async* ZT.Result<(), Text> {
            await canister_db.grant_database_access(target, role, db_name);
        };

        /// Grant a role scoped to a specific collection
        public func grantCollectionAccess(target : Principal, role : Text, db_name : Text, collection_name : Text) : async* ZT.Result<(), Text> {
            await canister_db.grant_collection_access(target, role, db_name, collection_name);
        };

        /// Grant a role with global (canister-wide) scope
        public func grantGlobalAccess(target : Principal, role : Text) : async* ZT.Result<(), Text> {
            await canister_db.grant_global_access(target, role);
        };

        // ─── Revoke ──────────────────────────────────────────────────────────

        /// Revoke a scoped role from a principal
        public func revokeUserAccessTo(target : Principal, role : Text, resource_scope : [(Text, Text)]) : async* ZT.Result<(), Text> {
            await canister_db.revoke_user_access_to(target, role, resource_scope);
        };

        /// Revoke a role scoped to a specific database
        public func revokeDatabaseAccess(target : Principal, role : Text, db_name : Text) : async* ZT.Result<(), Text> {
            await canister_db.revoke_database_access(target, role, db_name);
        };

        /// Revoke a role scoped to a specific collection
        public func revokeCollectionAccess(target : Principal, role : Text, db_name : Text, collection_name : Text) : async* ZT.Result<(), Text> {
            await canister_db.revoke_collection_access(target, role, db_name, collection_name);
        };

        /// Revoke a role with global (canister-wide) scope
        public func revokeGlobalAccess(target : Principal, role : Text) : async* ZT.Result<(), Text> {
            await canister_db.revoke_global_access(target, role);
        };

        // ─── Registry ────────────────────────────────────────────────────────

        /// Set (or clear) the db_registry canister that receives push notifications
        /// after every successful grant or revoke. Pass `null` to disable pushes.
        public func setAccessRegistry(registry : ?Principal) : async* ZT.Result<(), Text> {
            await canister_db.set_db_access_registry(registry);
        };

        // ─── Access details ──────────────────────────────────────────────────

        /// Get access details (roles + permissions per scope) for a specific user.
        /// Self-query is always allowed; querying others requires access-control:read.
        public func getUserAccessDetails(user : Principal) : async* ZT.Result<[([(Text, Text)], Text, [Text])], Text> {
            await canister_db.get_user_access_details(user);
        };

        /// Get the caller's own access details.
        public func getMyAccessDetails() : async* ZT.Result<[([(Text, Text)], Text, [Text])], Text> {
            await canister_db.get_my_access_details();
        };

        /// Get access details for all users. Requires access-control:read.
        public func getAllUsersAccessDetails() : async* ZT.Result<[(Principal, [([(Text, Text)], Text, [Text])])], Text> {
            await canister_db.get_all_users_access_details();
        };

        /// Clear the candid map cache
        public func clearCache() : async* () {
            await canister_db.zendb_v1_clear_cache();
        };

        // Role constants for convenience
        public let ROLES = ClusterTypes.Roles;
        public let PERMISSIONS = ClusterTypes.Permissions;

    };

};
