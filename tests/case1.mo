// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";

import { test; suite } "mo:test";
import Candid "mo:serde/Candid";
// import MemoryBTree "../src/memory-buffer/src/MemoryBTree/Base";
import Fuzz "mo:fuzz";

import ZenDB "../src";

let fuzz = Fuzz.fromSeed(0x7eadbeef);

type User = {
    name : Text;
    age : Nat;
    email : Text;
};

type User2 = {
    name : Text;
    age : Nat;
    email : Text;
    phone : ?Text;
};

let candify_user = {
    from_blob = func(blob : Blob) : User {
        let ?c : ?User = from_candid (blob);
        c;
    };
    to_blob = func(c : User) : Blob { to_candid (c) };
};

let candify_user2 = {
    from_blob = func(blob : Blob) : User2 {
        let ?c : ?User2 = from_candid (blob);
        c;
    };
    to_blob = func(c : User2) : Blob { to_candid (c) };
};

type Candid = Candid.Candid;

let { QueryBuilder } = ZenDB;

suite(
    "Single Layer Record",
    func() {

        let hydra_db = ZenDB.new();

        let users_schema = #Record([
            ("name", #Text),
            ("age", #Nat),
            ("email", #Text),
        ]);

        ignore ZenDB.create_collection(hydra_db, "users", users_schema);

        for (i in Iter.range(1, 10)) {
            let user = {
                name = "nam-do-san";
                age = i;
                email = "email";
            };

            ignore ZenDB.put<User>(hydra_db, "users", candify_user, user);
        };

        for (i in Iter.range(1, 10)) {
            let user = {
                name = "claude";
                age = i;
                email = "email";
            };

            ignore ZenDB.put<User>(hydra_db, "users", candify_user, user);
        };

        let total_records = 20;
        test(
            "scan(): retrieve all records",
            func() {
                let scanned = ZenDB.scan<User>(hydra_db, "users", candify_user, [], []); // no start or terminate query
                let results = Iter.toArray(scanned);
                Debug.print(debug_show results);
                assert results.size() == total_records;
            },
        );

        let #ok(_) = ZenDB.create_index(hydra_db, "users", [("age")]);
        let #ok(name_index) = ZenDB.create_index(hydra_db, "users", [("name")]);
        let #ok(_) = ZenDB.create_index(hydra_db, "users", [("email")]);

        // let index_data_utils = ZenDB.get_index_data_utils(name_index.key_details);
        // let entries = MemoryBTree.scan(name_index.data, index_data_utils, ?[("name", #Text("nam-do-san")), (":record_id", #Nat(0))], ?[("name", #Text("nam-do-san")), (":record_id", #Nat(2 ** 64))]);

        // Debug.print(debug_show Iter.toArray(entries));

        Debug.print("Retrieve every user with the name 'nam-do-san'");
        var _query = QueryBuilder()._where("name", #eq(#Text("nam-do-san")));

        var result = ZenDB.search<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));

        Debug.print("Retrieve every user between the age of 3 and 7");
        _query := QueryBuilder()._where("age", #gte(#Nat(3)))._and("age", #lte(#Nat(7)));

        result := ZenDB.search<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));

        Debug.print("Retrieve every user with the name 'nam-do-san' and age between 3 and 7");
        _query := QueryBuilder()._where("age", #gte(#Nat(3)))._and("age", #lte(#Nat(7)))._and("name", #eq(#Text("nam-do-san")));

        result := ZenDB.search<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));

        Debug.print("Retrieve users between the age 0 and 2 named 'nam-do-san' or between the age 8 and 10 named 'claude'");

        let q1 = QueryBuilder()._where("age", #gte(#Nat(0)))._and("age", #lte(#Nat(2)))._and("name", #eq(#Text("nam-do-san")));

        _query := QueryBuilder()._where("age", #gte(#Nat(8)))._and("age", #lte(#Nat(10)))._and("name", #eq(#Text("claude")))._or_query(q1);

        let res = ZenDB.search<User2>(hydra_db, "users", candify_user2, _query);
        // let array = Iter.toArray(res);
        Debug.print(debug_show res);

        Debug.print("Retrieve every user with an age of 0 or 10");
        _query := QueryBuilder()._where("age", #eq(#Nat(0)))._or("age", #eq(#Nat(10)));

        result := ZenDB.search<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));

        Debug.print("Update age of users named 'nam-do-san' to 0");
        _query := QueryBuilder()._where("name", #eq(#Text("nam-do-san")));

        let #ok = ZenDB.update<User>(hydra_db, "users", candify_user, _query, func(user : User) : User { { user with age = 0 } });

        Debug.print("Retrieve every user with an age of 0 or 10");
        _query := QueryBuilder()._where("age", #eq(#Nat(0)))._or("age", #eq(#Nat(10)));

        result := ZenDB.search<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));

        Debug.print("Delete every user with an age of 0");
        _query := QueryBuilder()._where("age", #eq(#Nat(0)));

        let #ok(deleted_users) = ZenDB.delete<User>(
            hydra_db,
            "users",
            candify_user,
            _query,
        );

        Debug.print("Retrieve every user with an age of 0");
        _query := QueryBuilder()._where("age", #eq(#Nat(0)));

        result := ZenDB.search<User>(hydra_db, "users", candify_user, _query);
        Debug.print(debug_show Iter.toArray(result));

    },
);
