import Result "mo:base/Result";
import Debug "mo:base/Debug";

import Map "mo:map/Map";
import Set "mo:map/Set";

import Vector "mo:vector";
import RevIter "mo:itertools/RevIter";

module Roles {

    let { thash; bhash; phash } = Map;

    type Map<K, V> = Map.Map<K, V>;
    type Set<K> = Set.Set<K>;
    type Vector<A> = Vector.Vector<A>;
    type Result<A, B> = Result.Result<A, B>;

    public type InputRole = {
        name : Text;
        permissions : [Text];
    };

    public type Role = {
        name : Text;
        permissions : Set<Text>;
    };

    public type StableRolesAuth = {
        permissions : Set<Text>;
        roles : Map<Text, Role>;
        users : Map<Principal, Vector<(role : Text)>>;
    };

    public func new() : StableRolesAuth {
        {
            permissions = Set.new<Text>();
            roles = Map.new<Text, Role>();
            users = Map.new<Principal, Vector<Text>>();
        };
    };

    public func init(init_roles : [InputRole]) : StableRolesAuth {
        let roles = Roles.new();

        for (role in init_roles.vals()) {

            add_role(
                roles,
                {
                    name = role.name;
                    permissions = Set.fromIter<Text>(role.permissions.vals(), thash);
                },
            );
        };

        roles;

    };

    public func add_role(auth : StableRolesAuth, role : Role) {
        ignore Map.put(auth.roles, thash, role.name, role);
    };

    public func add_permissions_to_role(auth : StableRolesAuth, role_name : Text, permissions : [Text]) : Result.Result<(), Text> {
        let ?role = Map.get(auth.roles, thash, role_name) else return #err("Role not found");

        for (permission in permissions.vals()) {
            Set.add(role.permissions, thash, permission);
        };

        #ok();

    };

    func vector_swap<T>(vec : Vector<T>, i : Nat, j : Nat) {
        let temp = Vector.get(vec, i);
        Vector.put(vec, i, Vector.get(vec, j));
        Vector.put(vec, j, temp);
    };

    public func remove_permissions_from_role(auth : StableRolesAuth, role_name : Text, permissions : [Text]) : Result.Result<(), Text> {
        let ?role = Map.get(auth.roles, thash, role_name) else return #err("Role not found");

        for (permission in permissions.vals()) {
            ignore Set.remove(role.permissions, thash, permission);
        };

        #ok()

    };

    public func assign_role(auth : StableRolesAuth, user : Principal, role_name : Text) : Result.Result<(), Text> {
        let ?_role = Map.get(auth.roles, thash, role_name) else return #err("Role not found");

        switch (Map.get(auth.users, phash, user)) {
            case (?user_roles) {
                Vector.add(user_roles, role_name);
            };
            case (null) {
                ignore Map.put<Principal, Vector<Text>>(auth.users, phash, user, Vector.fromArray<Text>([role_name]));
            };
        };

        #ok();
    };

    public func unassign_role(auth : StableRolesAuth, user_principal : Principal, role_name : Text) : Result.Result<(), Text> {
        let ?user_roles = Map.get(auth.users, phash, user_principal) else return #err("User not found");

        for (i in RevIter.range(0, Vector.size(user_roles)).rev()) {
            if (Vector.get(user_roles, i) == role_name) {
                vector_swap(user_roles, i, Vector.size(user_roles) - 1);
                ignore Vector.removeLast(user_roles);
            };
        };

        #ok();
    };

    public func user_has_permission(auth : StableRolesAuth, user : Principal, permission : Text) : Bool {
        let ?user_roles = Map.get(auth.users, phash, user) else return false;

        for (role_name in Vector.vals(user_roles)) {
            let ?role = Map.get(auth.roles, thash, role_name) else Debug.trap("Roles.user_has_permission: Role not found");

            if (Set.has(role.permissions, thash, permission)) {
                return true;
            };
        };

        false;
    };

};
