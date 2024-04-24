// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import { test; suite } "mo:test";

import HydraDb "../src";

type User = {
    name : Text;
    age : Nat;
    email : Text;
};

let candify_user = {
    from_blob = func(blob : Blob) : User {
        let ?c : ?User = from_candid (blob);
        c;
    };
    to_blob = func(c : User) : Blob {
        to_candid (c);
    };
};

let { QueryBuilder } = HydraDb;

suite(
    "HydraDb Test",
    func() {
        let hydra_db = HydraDb.new();

        let users_schema = #Record([
            ("name", #Text),
            ("age", #Nat),
            ("email", #Text),
        ]);

        ignore HydraDb.create_collection(hydra_db, "users", users_schema);

        for (i in Iter.range(0, 10)) {
            let user = {
                name = "nam-do-san";
                age = i;
                email = "email";
            };

            ignore HydraDb.put<User>(hydra_db, "users", candify_user, user);
        };

        for (i in Iter.range(0, 10)) {
            let user = {
                name = "drake";
                age = i;
                email = "email";
            };

            ignore HydraDb.put<User>(hydra_db, "users", candify_user, user);
        };

        assert #ok == HydraDb.create_index(hydra_db, "users", [("age")]);
        assert #ok == HydraDb.create_index(hydra_db, "users", [("name")]);
        assert #ok == HydraDb.create_index(hydra_db, "users", [("email")]);

        let _query = QueryBuilder()
            // .where("age", #Gt, #Nat(7))
            // ._or("age", #Lt, #Nat(3))
            // .where("age", #Gt, #Nat(0))
            .where("name", #Eq, #Text("nam-do-san"))
            .build();

        let result = HydraDb.find<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));
    },
);
