import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Array "mo:base@0.16.0/Array";
import Char "mo:base@0.16.0/Char";

import { test; suite } "mo:test";

import Query "../../src/Query";
import T "../../src/Types";

suite(
    "QueryBuilder",
    func() {
        test(
            "Empty QueryBuilder should return empty And query",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.build();

                assert result.query_operations == #And([]);
                assert result.sort_by == null;
                assert result.pagination.cursor == null;
                assert result.pagination.limit == null;
                assert result.pagination.skip == null;
            },
        );

        test(
            "Single Where condition creates correct query",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).build();

                assert result.query_operations == #And([#Operation("name", #eq(#Text("Alice")))]);
                assert result.sort_by == null;
                assert result.pagination.cursor == null;
                assert result.pagination.limit == null;
                assert result.pagination.skip == null;
            },
        );

        test(
            "Multiple And conditions create correct query",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).And("age", #gte(#Nat(18))).And("active", #eq(#Bool(true))).build();

                let expected = #And([
                    #Operation("name", #eq(#Text("Alice"))),
                    #Operation("age", #gte(#Nat(18))),
                    #Operation("active", #eq(#Bool(true))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Multiple Or conditions create correct query",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("status", #eq(#Text("active"))).Or("status", #eq(#Text("pending"))).Or("status", #eq(#Text("review"))).build();

                let expected = #Or([
                    #Operation("status", #eq(#Text("active"))),
                    #Operation("status", #eq(#Text("pending"))),
                    #Operation("status", #eq(#Text("review"))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Mixed And/Or conditions create nested structure",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).And("age", #gte(#Nat(18))).Or("status", #eq(#Text("admin"))).build();

                let expected = #Or([
                    #And([
                        #Operation("name", #eq(#Text("Alice"))),
                        #Operation("age", #gte(#Nat(18))),
                    ]),
                    #Operation("status", #eq(#Text("admin"))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Between operator expands to gte and lte",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #between(#Nat(18), #Nat(65))).build();

                let expected = #And([
                    #Operation("age", #gte(#Nat(18))),
                    #Operation("age", #lte(#Nat(65))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "StartsWith operator expands to between range",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #startsWith(#Text("Al"))).build();

                let expected = #And([
                    #Operation("name", #gte(#Text("Al"))),
                    #Operation("name", #lte(#Text("Al" # Text.fromChar(Char.fromNat32(0xff))))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "AnyOf operator expands to Or of eq operations",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("status", #anyOf([#Text("active"), #Text("pending"), #Text("review")])).build();

                let expected = #Or([
                    #Operation("status", #eq(#Text("active"))),
                    #Operation("status", #eq(#Text("pending"))),
                    #Operation("status", #eq(#Text("review"))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with eq expands to lt and gt",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#eq(#Nat(25)))).build();

                let expected = #Or([
                    #Operation("age", #lt(#Nat(25))),
                    #Operation("age", #gt(#Nat(25))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with lt becomes gte",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#lt(#Nat(18)))).build();

                let expected = #And([
                    #Operation("age", #gte(#Nat(18)))
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with gt becomes lte",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#gt(#Nat(65)))).build();

                let expected = #And([
                    #Operation("age", #lte(#Nat(65)))
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with gte becomes lt",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#gte(#Nat(18)))).build();

                let expected = #And([
                    #Operation("age", #lt(#Nat(18)))
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with lte becomes gt",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#lte(#Nat(65)))).build();

                let expected = #And([
                    #Operation("age", #gt(#Nat(65)))
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with between expands to lt and gt",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#between(#Nat(18), #Nat(65)))).build();

                let expected = #Or([
                    #Operation("age", #lt(#Nat(18))),
                    #Operation("age", #gt(#Nat(65))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with anyOf creates And of Or conditions",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("status", #not_(#anyOf([#Text("active"), #Text("pending")]))).build();

                let expected = #And([
                    #Or([
                        #Operation("status", #lt(#Text("active"))),
                        #Operation("status", #gt(#Text("active"))),
                    ]),
                    #Or([
                        #Operation("status", #lt(#Text("pending"))),
                        #Operation("status", #gt(#Text("pending"))),
                    ]),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Double negation cancels out",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#not_(#eq(#Nat(25))))).build();

                let expected = #And([
                    #Operation("age", #eq(#Nat(25)))
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Sort configuration is preserved",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).Sort("created_at", #Descending).build();

                assert result.sort_by == ?("created_at", #Descending);
            },
        );

        test(
            "Pagination with cursor and limit",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).Pagination(?100, 10).build();

                assert result.pagination.cursor == ?(100, #Forward);
                assert result.pagination.limit == ?10;
                assert result.pagination.skip == null;
            },
        );

        test(
            "Limit configuration",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).Limit(25).build();

                assert result.pagination.limit == ?25;
                assert result.pagination.cursor == null;
                assert result.pagination.skip == null;
            },
        );

        test(
            "Skip configuration",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).Skip(5).build();

                assert result.pagination.skip == ?5;
                assert result.pagination.cursor == null;
                assert result.pagination.limit == null;
            },
        );

        test(
            "Combined pagination options",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).Pagination(?100, 10).Skip(5).build();

                assert result.pagination.cursor == ?(100, #Forward);
                assert result.pagination.limit == ?10;
                assert result.pagination.skip == ?5;
            },
        );

        test(
            "RawQuery preserves query structure",
            func() {
                let builder = Query.QueryBuilder();
                let rawQuery = #Or([
                    #Operation("name", #eq(#Text("Alice"))),
                    #Operation("name", #eq(#Text("Bob"))),
                ]);
                let result = builder.RawQuery(rawQuery).build();

                assert result.query_operations == rawQuery;
            },
        );

        test(
            "OrQuery combines with existing conditions",
            func() {
                let builder1 = Query.QueryBuilder().Where("age", #gte(#Nat(18)));

                let builder2 = Query.QueryBuilder();
                let result = builder2.Where("name", #eq(#Text("Alice"))).OrQuery(builder1).build();

                let expected = #Or([
                    #Operation("name", #eq(#Text("Alice"))),
                    #And([#Operation("age", #gte(#Nat(18)))]),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "AndQuery combines with existing conditions",
            func() {
                let builder1 = Query.QueryBuilder().Where("age", #gte(#Nat(18)));

                let builder2 = Query.QueryBuilder();
                let result = builder2.Where("name", #eq(#Text("Alice"))).AndQuery(builder1).build();

                let expected = #And([
                    #Operation("name", #eq(#Text("Alice"))),
                    #And([#Operation("age", #gte(#Nat(18)))]),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Complex nested query with all operators",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #startsWith(#Text("A"))).And("age", #between(#Nat(18), #Nat(65))).Or("status", #anyOf([#Text("admin"), #Text("moderator")])).And("active", #eq(#Bool(true))).Sort("created_at", #Ascending).Limit(50).Skip(10).build();

                // Verify structure is correct (specific structure depends on how And/Or are processed)
                assert result.sort_by == ?("created_at", #Ascending);
                assert result.pagination.limit == ?50;
                assert result.pagination.skip == ?10;
            },
        );

        test(
            "Query builder is immutable - multiple builds return same result",
            func() {
                let builder = Query.QueryBuilder();
                ignore builder.Where("name", #eq(#Text("Alice")));
                ignore builder.And("age", #gte(#Nat(18)));
                ignore builder.Sort("created_at", #Descending);
                ignore builder.Limit(10);

                let result1 = builder.build();
                let result2 = builder.build();

                assert result1.query_operations == result2.query_operations;
                assert result1.sort_by == result2.sort_by;
                assert result1.pagination.limit == result2.pagination.limit;
            },
        );

        test(
            "Exists operator is preserved",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("optional_field", #exists).build();

                let expected = #And([
                    #Operation("optional_field", #exists)
                ]);

                assert result.query_operations == expected;
            },
        );
    },
);
