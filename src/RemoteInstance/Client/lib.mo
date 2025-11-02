import CanisterDB_types "../Types";
import ZT "../../EmbeddedInstance/Types";
import Principal "mo:base@0.16.0/Principal";

import CollectionClient "Collection.Client";
import DatabaseClient "Database.Client";

import ClusterTypes "../Types";

module Client {

    public type CollectionClient<Record> = CollectionClient.CollectionClient<Record>;

    public class Client(canister_id : Text) {
        let canister_db : ClusterTypes.ClusterApiService = actor (canister_id);

        public func api_version() : async* Text {
            await canister_db.zendb_api_version();
        };

        public func get_database(db_name : Text) : DatabaseClient.DatabaseClient {
            DatabaseClient.DatabaseClient(canister_db, db_name);
        };

        public func create_database(db_name : Text) : async* (ZT.Result<(), Text>) {
            await canister_db.zendb_create_database(db_name);
        };

        public func launchDefaultDB() : DatabaseClient.DatabaseClient {
            DatabaseClient.DatabaseClient(canister_db, "default");
        };

        // Helper functions for easier access

        /// List all canisters in the cluster
        public func list_canisters() : async* [ClusterTypes.CanisterInfo] {
            await canister_db.zendb_list_canisters();
        };

        /// Get statistics for all canisters
        public func canister_stats() : async* [ZT.InstanceStats] {
            await canister_db.zendb_canister_stats();
        };

        /// Grant a role to a principal
        public func grant_role(target : Principal, role : Text) : async* ZT.Result<(), Text> {
            await canister_db.grant_role(target, role);
        };

        /// Grant multiple roles to a principal
        public func grant_roles(target : Principal, roles : [Text]) : async* ZT.Result<(), Text> {
            await canister_db.grant_roles(target, roles);
        };

        /// Revoke a role from a principal
        public func revoke_role(target : Principal, role : Text) : async* ZT.Result<(), Text> {
            await canister_db.revoke_role(target, role);
        };

        // Role constants for convenience
        public let ROLES = ClusterTypes.Roles;
        public let PERMISSIONS = ClusterTypes.Permissions;

    };

};
