import Result "mo:base@0.16.0/Result";
import Debug "mo:base@0.16.0/Debug";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";

import Vector "mo:vector@0.4.2";
import RevIter "mo:itertools@0.2.2/RevIter";

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
        users : Map<Principal, Set<Text>>;
    };

    public type PrevStableRolesAuth = StableRolesAuth;

    public func new() : StableRolesAuth {
        {
            permissions = Set.new<Text>();
            roles = Map.new<Text, Role>();
            users = Map.new<Principal, Set<Text>>();
        };
    };

    public func migrate(prev : PrevStableRolesAuth) : StableRolesAuth {
        // no current upgrades
        prev
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

    public func remove_permissions_from_role(auth : StableRolesAuth, role_name : Text, permissions : [Text]) : Result.Result<(), Text> {
        let ?role = Map.get(auth.roles, thash, role_name) else return #err("Role not found");

        for (permission in permissions.vals()) {
            ignore Set.remove(role.permissions, thash, permission);
        };

        #ok();

    };

    public func assign_role(auth : StableRolesAuth, user : Principal, role_name : Text) : Result.Result<(), Text> {
        let ?_role = Map.get(auth.roles, thash, role_name) else return #err("Role not found");

        switch (Map.get(auth.users, phash, user)) {
            case (?user_roles) {
                Set.add(user_roles, thash, role_name);
            };
            case (null) {
                let new_roles_set = Set.new<Text>();
                Set.add(new_roles_set, thash, role_name);
                ignore Map.put<Principal, Set<Text>>(auth.users, phash, user, new_roles_set);
            };
        };

        #ok();
    };

    public func unassign_role(auth : StableRolesAuth, user_principal : Principal, role_name : Text) : Result.Result<(), Text> {
        let ?user_roles = Map.get(auth.users, phash, user_principal) else return #err("User not found");

        ignore Set.remove(user_roles, thash, role_name);

        #ok();
    };

    public func user_has_permission(auth : StableRolesAuth, user : Principal, permission : Text) : Bool {
        let ?user_roles = Map.get(auth.users, phash, user) else return false;

        for (role_name in Set.keys(user_roles)) {
            let ?role = Map.get(auth.roles, thash, role_name) else Debug.trap("Roles.user_has_permission: Role not found");

            if (Set.has(role.permissions, thash, permission)) {
                return true;
            };
        };

        false;
    };

    // Migration functions

    public func rename_role(auth : StableRolesAuth, old_name : Text, new_name : Text) : Result.Result<(), Text> {
        // Check if old role exists
        let ?old_role = Map.get(auth.roles, thash, old_name) else return #err("Role '" # old_name # "' not found");

        // Check if new name already exists
        switch (Map.get(auth.roles, thash, new_name)) {
            case (?_) return #err("Role '" # new_name # "' already exists");
            case (null) {};
        };

        // Create new role with new name
        let new_role : Role = {
            name = new_name;
            permissions = old_role.permissions;
        };

        // Add new role
        ignore Map.put(auth.roles, thash, new_name, new_role);

        // Update all users who have the old role
        for ((user, user_roles) in Map.entries(auth.users)) {
            if (Set.has(user_roles, thash, old_name)) {
                ignore Set.remove(user_roles, thash, old_name);
                Set.add(user_roles, thash, new_name);
            };
        };

        // Remove old role
        ignore Map.remove(auth.roles, thash, old_name);

        #ok();
    };

    public func rename_permission(auth : StableRolesAuth, old_name : Text, new_name : Text) : Result.Result<(), Text> {
        var found = false;

        // Update permission in all roles
        for ((role_name, role) in Map.entries(auth.roles)) {
            if (Set.has(role.permissions, thash, old_name)) {
                ignore Set.remove(role.permissions, thash, old_name);
                Set.add(role.permissions, thash, new_name);
                found := true;
            };
        };

        if (not found) {
            return #err("Permission '" # old_name # "' not found in any role");
        };

        #ok();
    };

    public func delete_role(auth : StableRolesAuth, role_name : Text) : Result.Result<(), Text> {
        // Check if role exists
        let ?_role = Map.get(auth.roles, thash, role_name) else return #err("Role '" # role_name # "' not found");

        // Remove role from all users
        for ((user, user_roles) in Map.entries(auth.users)) {
            ignore Set.remove(user_roles, thash, role_name);
        };

        // Remove role from roles map
        ignore Map.remove(auth.roles, thash, role_name);

        #ok();
    };

    public func list_roles(auth : StableRolesAuth) : [Text] {
        let roles_iter = Map.keys(auth.roles);
        let roles_buffer = Vector.new<Text>();

        for (role_name in roles_iter) {
            Vector.add(roles_buffer, role_name);
        };

        Vector.toArray(roles_buffer);
    };

    public func list_permissions(auth : StableRolesAuth) : [Text] {
        let permissions_set = Set.new<Text>();

        for ((role_name, role) in Map.entries(auth.roles)) {
            for (permission in Set.keys(role.permissions)) {
                Set.add(permissions_set, thash, permission);
            };
        };

        let permissions_iter = Set.keys(permissions_set);
        let permissions_buffer = Vector.new<Text>();

        for (permission in permissions_iter) {
            Vector.add(permissions_buffer, permission);
        };

        Vector.toArray(permissions_buffer);
    };

    public func get_role_permissions(auth : StableRolesAuth, role_name : Text) : Result.Result<[Text], Text> {
        let ?role = Map.get(auth.roles, thash, role_name) else return #err("Role '" # role_name # "' not found");

        let permissions_iter = Set.keys(role.permissions);
        let permissions_buffer = Vector.new<Text>();

        for (permission in permissions_iter) {
            Vector.add(permissions_buffer, permission);
        };

        #ok(Vector.toArray(permissions_buffer));
    };

    public func get_user_roles(auth : StableRolesAuth, user : Principal) : [Text] {
        switch (Map.get(auth.users, phash, user)) {
            case (?user_roles) {
                let roles_buffer = Vector.new<Text>();
                for (role in Set.keys(user_roles)) {
                    Vector.add(roles_buffer, role);
                };
                Vector.toArray(roles_buffer);
            };
            case (null) [];
        };
    };

    public func get_all_users_roles(auth : StableRolesAuth) : [(Principal, [Text])] {
        let users_buffer = Vector.new<(Principal, [Text])>();

        for ((user, roles_set) in Map.entries(auth.users)) {
            let roles_array_buffer = Vector.new<Text>();
            for (role in Set.keys(roles_set)) {
                Vector.add(roles_array_buffer, role);
            };
            Vector.add(users_buffer, (user, Vector.toArray(roles_array_buffer)));
        };

        Vector.toArray(users_buffer);
    };

};
