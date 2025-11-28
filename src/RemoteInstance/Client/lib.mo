import ZT "../../EmbeddedInstance/Types";
import Principal "mo:base@0.16.0/Principal";

import CollectionClient "Collection.Client";
import DatabaseClient "Database.Client";

import ClusterTypes "../Types";

module Client {

    public type CollectionClient<Record> = CollectionClient.CollectionClient<Record>;

    public class Client(canister_id : Text) {
        let canister_db : ClusterTypes.ClusterApiService = actor (canister_id);

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
        public func listDatabaseNames() : async* [Text] {
            await canister_db.zendb_v1_list_database_names();
        };

        /// Rename a database
        public func renameDB(old_name : Text, new_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_v1_rename_database(old_name, new_name);
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

        /// Grant a role to a principal
        public func grantRole(target : Principal, role : Text) : async* ZT.Result<(), Text> {
            await canister_db.grant_role(target, role);
        };

        /// Grant multiple roles to a principal
        public func grantRoles(target : Principal, roles : [Text]) : async* ZT.Result<(), Text> {
            await canister_db.grant_roles(target, roles);
        };

        /// Revoke a role from a principal
        public func revokeRole(target : Principal, role : Text) : async* ZT.Result<(), Text> {
            await canister_db.revoke_role(target, role);
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
