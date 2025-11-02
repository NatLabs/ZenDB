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
    type StableRolesAuth = StableRolesAuth.StableRolesAuth;

    public func init_stable_store(roles : [InputRole]) : StableRolesAuth {
        let stable_roles_auth = StableRolesAuth.init(roles);
        stable_roles_auth;
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

        var on_missing_permissions = func(caller : Principal, permission : Text) : Text {
            "Permission denied: Caller " # debug_show (caller) # " does not have permission " # permission;
        };

        public func set_missing_permissions_error_message(
            fn : (caller : Principal, permission : Text) -> Text
        ) {
            on_missing_permissions := fn;
        };

        public func allow<A>(caller : Principal, permission : Text, fn : () -> A) : A {
            if (user_has_permission(caller, permission)) {
                fn();
            } else {
                Debug.trap(on_missing_permissions(caller, permission));
            };
        };

        public func shiiit() {};

        public func allow_rs<A>(caller : Principal, permission : Text, fn : () -> Result.Result<A, Text>) : Result.Result<A, Text> {
            if (user_has_permission(caller, permission)) {
                fn();
            } else {
                #err(on_missing_permissions(caller, permission));
            };
        };

    };

};
