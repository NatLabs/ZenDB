// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import { test; suite } "mo:test";
import Candid "mo:serde/Candid";
import MemoryBTree "../src/memory-buffer/src/MemoryBTree/Base";

import HydraDb "../src";

type User = {
    name : Text;
    age : Nat;
    email : Text;
};

let candify_user = {
    from_blob = func(blob : Blob) : User { let ?c : ?User = from_candid (blob); c; };
    to_blob = func(c : User) : Blob { to_candid (c); };
};

type Candid = Candid.Candid;

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

        let #ok(_) = HydraDb.create_index(hydra_db, "users", [("age")]);
        let #ok(name_index) = HydraDb.create_index(hydra_db, "users", [("name")]);
        let #ok(_) = HydraDb.create_index(hydra_db, "users", [("email")]);

        // let index_data_utils = HydraDb.get_index_data_utils(name_index.key_details);
        // let entries = MemoryBTree.scan(name_index.data, index_data_utils, ?[("name", #Text("nam-do-san")), (":record_id", #Nat(0))], ?[("name", #Text("nam-do-san")), (":record_id", #Nat(2 ** 64))]);

        // Debug.print(debug_show Iter.toArray(entries));

        Debug.print("Retrieve every user with the name 'nam-do-san'");
        var _query = QueryBuilder()
            ._where("name", #Eq, #Text("nam-do-san"))
            .build();

        var result = HydraDb.find<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));

        Debug.print("Retrieve every user between the age of 3 and 7");
        _query := QueryBuilder()
            ._where("age", #Gt, #Nat(3))
            ._and("age", #Lt, #Nat(7))
            .build();

        result := HydraDb.find<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));

        Debug.print("Retrieve every user with the name 'nam-do-san' and age between 3 and 7");
        _query := QueryBuilder()
            ._where("age", #Gt, #Nat(3))
                ._and("age", #Lt, #Nat(7))
                ._and("name", #Eq, #Text("nam-do-san"))
            .build();

        result := HydraDb.find<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));
    },
);
