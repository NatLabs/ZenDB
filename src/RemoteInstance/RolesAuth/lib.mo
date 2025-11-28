import Result "mo:base@0.16.0/Result";
import Debug "mo:base@0.16.0/Debug";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";

import Vector "mo:vector@0.4.2";
import RevIter "mo:itertools@0.2.2/RevIter";

import StableRolesAuth "StableRolesAuth";

module Roles {

    type Role = StableRolesAuth.Role;
    type InputRole = StableRolesAuth.InputRole;
    public type StableRolesAuth = StableRolesAuth.StableRolesAuth;
    public type PrevStableRolesAuth = StableRolesAuth.PrevStableRolesAuth;

    public func init_stable_store(roles : [InputRole]) : StableRolesAuth {
        let stable_roles_auth = StableRolesAuth.init(roles);
        stable_roles_auth;
    };

    public func migrate(prev : PrevStableRolesAuth) : StableRolesAuth {
        StableRolesAuth.migrate(prev);
    };

    public class RolesAuth(auth : StableRolesAuth) {

        public func add_role(role : Role) {
            StableRolesAuth.add_role(auth, role);
        };

        public func add_permissions_to_role(role_name : Text, permissions : [Text]) : Result.Result<(), Text> {
            StableRolesAuth.add_permissions_to_role(auth, role_name, permissions);
        };

        public func remove_permissions_from_role(role_name : Text, permissions : [Text]) : Result.Result<(), Text> {
            StableRolesAuth.remove_permissions_from_role(auth, role_name, permissions);
        };

        public func assign_role(user : Principal, role_name : Text) : Result.Result<(), Text> {
            StableRolesAuth.assign_role(auth, user, role_name);
        };

        public func assign_roles(user : Principal, roles : [Text]) : Result.Result<(), Text> {
            for (role in roles.vals()) {
                let #ok(_) = assign_role(user, role) else return #err("Failed to assign role");
            };

            #ok();
        };

        public func unassign_role(user_principal : Principal, role_name : Text) : Result.Result<(), Text> {
            StableRolesAuth.unassign_role(auth, user_principal, role_name);
        };

        public func user_has_permission(user : Principal, permission : Text) : Bool {
            StableRolesAuth.user_has_permission(auth, user, permission);
        };

        var on_missing_permissions = func(caller : Principal, permission : Text, resource : Text) : Text {
            "Permission denied: Caller " # debug_show (caller) # " does not have " # permission # " permission to access " # resource;
        };

        public func set_custom_error_message(
            fn : (caller : Principal, permission : Text, resource : Text) -> Text
        ) {
            on_missing_permissions := fn;
        };

        public func allow<A>(caller : Principal, permission : Text, resource : Text, fn : () -> A) : A {
            if (user_has_permission(caller, permission)) {
                fn();
            } else {
                Debug.trap(on_missing_permissions(caller, permission, resource));
            };
        };

        public func allow_rs<A>(caller : Principal, permission : Text, resource : Text, fn : () -> Result.Result<A, Text>) : Result.Result<A, Text> {
            if (user_has_permission(caller, permission)) {
                fn();
            } else {
                #err(on_missing_permissions(caller, permission, resource));
            };
        };

        // Migration functions

        public func rename_role(old_name : Text, new_name : Text) : Result.Result<(), Text> {
            StableRolesAuth.rename_role(auth, old_name, new_name);
        };

        public func rename_permission(old_name : Text, new_name : Text) : Result.Result<(), Text> {
            StableRolesAuth.rename_permission(auth, old_name, new_name);
        };

        public func delete_role(role_name : Text) : Result.Result<(), Text> {
            StableRolesAuth.delete_role(auth, role_name);
        };

        public func list_roles() : [Text] {
            StableRolesAuth.list_roles(auth);
        };

        public func list_permissions() : [Text] {
            StableRolesAuth.list_permissions(auth);
        };

        public func get_role_permissions(role_name : Text) : Result.Result<[Text], Text> {
            StableRolesAuth.get_role_permissions(auth, role_name);
        };

        public func get_user_roles(user : Principal) : [Text] {
            StableRolesAuth.get_user_roles(auth, user);
        };

        public func get_all_users_roles() : [(Principal, [Text])] {
            StableRolesAuth.get_all_users_roles(auth);
        };

    };

};
