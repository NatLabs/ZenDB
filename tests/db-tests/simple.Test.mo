// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Iter "mo:base@0.16.0/Iter";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Buffer "mo:base@0.16.0/Buffer";

import { test; suite } "mo:test";
import Candid "mo:serde@3.3.3/Candid";
import Fuzz "mo:fuzz";
import Itertools "mo:itertools@0.2.2/Iter";
import Map "mo:map@9.0.1/Map";

import ZenDB "../../src";
import ZenDBSuite "../test-utils/TestFramework";

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

let users_schema = #Record([
    ("name", #Text),
    ("age", #Nat),
    ("email", #Text),
]);

let candify_user = {
    from_blob = func(blob : Blob) : ?User {
        from_candid (blob);
    };
    to_blob = func(c : User) : Blob { to_candid (c) };
};

let candify_user2 = {
    from_blob = func(blob : Blob) : ?User2 {
        from_candid (blob);
    };
    to_blob = func(c : User2) : Blob { to_candid (c) };
};

ZenDBSuite.newSuite(
    "Simple Record Tests",
    ?{ ZenDBSuite.withAndWithoutIndex with log_level = #Error },
    func suite_setup(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {
        let #ok(users) = zendb.createCollection<User>("users", users_schema, candify_user, null) else return assert false;

        let #ok(_) = suite_utils.createIndex(users.name(), "name_idx", [("name", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(users.name(), "age_idx", [("age", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(users.name(), "email_idx", [("email", #Ascending)], null) else return assert false;

        let inputs = Map.new<ZenDB.Types.DocumentId, User>();

        for (i in Iter.range(1, 10)) {
            let user = {
                name = "nam-do-san";
                age = i;
                email = "email";
            };

            let #ok(id) = users.insert(user) else return assert false;
            // Debug.print("id: " # debug_show (id, user));
            ignore Map.put(inputs, Map.bhash, id, user);
        };

        for (i in Iter.range(1, 10)) {
            let user = {
                name = "claude";
                age = i;
                email = "email";
            };

            let #ok(id) = users.insert(user) else return assert false;
            // Debug.print("id: " # debug_show (id, user));
            ignore Map.put(inputs, Map.bhash, id, user);
        };

        let total_documents = 20;

        suite(
            "Record API Tests",
            func() {
                test(
                    "retrieve all documents",
                    func() {
                        let #ok(results) = users.search(ZenDB.QueryBuilder()) else return assert false;
                        assert results.size() == Map.size(inputs);

                        for ((id, user) in results.vals()) {
                            assert ?user == Map.get(inputs, Map.bhash, id);
                        };

                    },
                );

                test(
                    "db_query by name equality",
                    func() {
                        let db_query = ZenDB.QueryBuilder().Where("name", #eq(#Text("nam-do-san")));
                        let #ok(results) = users.search(db_query) else return assert false;

                        assert results.size() == 10;
                        for ((_, user) in results.vals()) {
                            assert user.name == "nam-do-san";
                        };
                    },
                );

                test(
                    "db_query by age range",
                    func() {
                        let db_query = ZenDB.QueryBuilder().Where("age", #gte(#Nat(3))).And("age", #lte(#Nat(7)));

                        let #ok(results) = users.search(db_query) else return assert false;
                        // Debug.print("results: " # debug_show (results));
                        assert results.size() == 10;
                        for ((_, user) in results.vals()) {
                            assert user.age >= 3 and user.age <= 7;
                        };
                    },
                );

                test(
                    "compound db_query with name and age",
                    func() {
                        let db_query = ZenDB.QueryBuilder().Where(
                            "name",
                            #eq(#Text("nam-do-san")),
                        ).And(
                            "age",
                            #gte(#Nat(3)),
                        ).And("age", #lte(#Nat(7)));

                        let #ok(results) = users.search(db_query) else return assert false;
                        assert results.size() == 5;
                        for ((_, user) in results.vals()) {
                            assert user.name == "nam-do-san";
                            assert user.age >= 3 and user.age <= 7;
                        };
                    },
                );

                test(
                    "complex OR db_query",
                    func() {
                        let q1 = ZenDB.QueryBuilder().Where(
                            "age",
                            #gte(#Nat(0)),
                        ).And(
                            "age",
                            #lte(#Nat(2)),
                        ).And(
                            "name",
                            #eq(#Text("nam-do-san")),
                        );

                        let q2 = ZenDB.QueryBuilder().Where(
                            "age",
                            #gte(#Nat(8)),
                        ).And(
                            "age",
                            #lte(#Nat(10)),
                        ).And(
                            "name",
                            #eq(#Text("claude")),
                        );

                        let db_query = q2.OrQuery(q1);

                        // We need to use User2 to check the result structure but
                        // without relying on the optional phone field
                        let #ok(results) = users.search(db_query) else return assert false;

                        assert results.size() == 5;
                        for ((_, user) in results.vals()) {
                            assert (
                                (user.name == "nam-do-san" and user.age >= 0 and user.age <= 2) or
                                (user.name == "claude" and user.age >= 8 and user.age <= 10)
                            );
                        };
                    },
                );

                test(
                    "simple OR db_query for specific ages",
                    func() {
                        let db_query = ZenDB.QueryBuilder().Where(
                            "age",
                            #eq(#Nat(1)),
                        ).Or(
                            "age",
                            #eq(#Nat(9)),
                        );

                        let #ok(results) = users.search(db_query) else return assert false;
                        assert results.size() == 4;
                        for ((_, user) in results.vals()) {
                            assert user.age == 1 or user.age == 9;
                        };
                    },
                );

                test(
                    "query documents with age == 0",
                    func() {
                        let db_query = ZenDB.QueryBuilder().Where(
                            "age",
                            #eq(#Nat(0)),
                        );

                        let #ok(results) = users.search(db_query) else return assert false;
                        assert results.size() == 0;

                    },
                );

                test(
                    "update documents matching a db_query",
                    func() {
                        let db_query = ZenDB.QueryBuilder().Where(
                            "name",
                            #eq(#Text("nam-do-san")),
                        );

                        let #ok(results) = users.search(db_query) else return assert false;
                        assert results.size() == 10;
                        for ((_, user) in results.vals()) {
                            assert user.name == "nam-do-san";
                        };

                        // Update all "nam-do-san" users to have age 0
                        let #ok(updated_documents) = users.update(db_query, [("age", #Nat(0))]) else return assert false;

                        let #ok(updated) = users.search(db_query) else return assert false;
                        assert updated.size() == 10;
                        assert updated_documents == 10;

                        for ((_, user) in updated.vals()) {
                            assert user.name == "nam-do-san";
                            assert user.age == 0;
                        };

                    },
                );

                test(
                    "delete documents matching a db_query",
                    func() {
                        // Count before deletion
                        let db_query = ZenDB.QueryBuilder().Where(
                            "age",
                            #eq(#Nat(0)),
                        );

                        let #ok(before_results) = users.search(db_query) else return assert false;
                        let before_count = before_results.size();

                        // Debug.print("results before deletion (" # debug_show (before_count) # ") " # debug_show (before_results));
                        assert before_results.size() == 10;
                        for ((_, user) in before_results.vals()) {
                            assert user.age == 0;
                        };

                        let #ok(deleted) = users.delete(db_query) else return assert false;
                        // Debug.print("deleted (" # debug_show (deleted.size()) # ") " # debug_show (deleted));
                        for ((_, user) in deleted.vals()) {
                            assert user.age == 0;
                        };

                        let #ok(after_results) = users.search(db_query) else return assert false;

                        // Debug.print("results after deletion (" # debug_show (after_results.size()) # ") " # debug_show (after_results));

                        // Assert the right number were deleted
                        assert deleted.size() == before_count;
                        assert after_results.size() == 0;
                    },
                );

                test(
                    "clear entire collection",
                    func() {

                        assert users.size() > 0;

                        users.clear();

                        assert users.size() == 0;

                    },
                );
            },
        );
    },

);
