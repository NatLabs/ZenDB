import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Array "mo:base@0.16.0/Array";
import Char "mo:base@0.16.0/Char";

import { test; suite } "mo:test";

import Query "../../src/EmbeddedInstance/Query";
import T "../../src/EmbeddedInstance/Types";
import Utils "../../src/EmbeddedInstance/Utils";

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
                let result = builder.Where(
                    "name",
                    #eq(#Text("Alice")),
                ).And(
                    "age",
                    #gte(#Nat(18)),
                ).And(
                    "active",
                    #eq(#Bool(true)),
                ).build();

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
                let result = builder.Where(
                    "status",
                    #eq(#Text("active")),
                ).Or(
                    "status",
                    #eq(#Text("pending")),
                ).Or(
                    "status",
                    #eq(#Text("review")),
                ).build();

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
            "BetweenExclusive operator expands to gt and lt",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #betweenExclusive(#Nat(18), #Nat(65))).build();

                let expected = #And([
                    #Operation("age", #gt(#Nat(18))),
                    #Operation("age", #lt(#Nat(65))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "BetweenLeftOpen operator expands to gt and lte",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #betweenLeftOpen(#Nat(18), #Nat(65))).build();

                let expected = #And([
                    #Operation("age", #gt(#Nat(18))),
                    #Operation("age", #lte(#Nat(65))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "BetweenRightOpen operator expands to gte and lt",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #betweenRightOpen(#Nat(18), #Nat(65))).build();

                let expected = #And([
                    #Operation("age", #gte(#Nat(18))),
                    #Operation("age", #lt(#Nat(65))),
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
            "Not operator with betweenExclusive expands to lte and gte",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#betweenExclusive(#Nat(18), #Nat(65)))).build();

                let expected = #Or([
                    #Operation("age", #lte(#Nat(18))),
                    #Operation("age", #gte(#Nat(65))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with betweenLeftOpen expands to lte and gt",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#betweenLeftOpen(#Nat(18), #Nat(65)))).build();

                let expected = #Or([
                    #Operation("age", #lte(#Nat(18))),
                    #Operation("age", #gt(#Nat(65))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with betweenRightOpen expands to lt and gte",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #not_(#betweenRightOpen(#Nat(18), #Nat(65)))).build();

                let expected = #Or([
                    #Operation("age", #lt(#Nat(18))),
                    #Operation("age", #gte(#Nat(65))),
                ]);

                assert result.query_operations == expected;
            },
        );

        test(
            "Not operator with anyOf creates And of Or conditions",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("status", #not_(#anyOf([#Text("active"), #Text("pending")]))).build();

                let expected = #Or([
                    #Operation("status", #lt(#Text("active"))),
                    #And([
                        #Operation("status", #gt(#Text("active"))),
                        #Operation("status", #lt(#Text("pending"))),
                    ]),
                    #Operation("status", #gt(#Text("pending"))),
                ]);

                Debug.print("Resulting query operations: " # debug_show result.query_operations);

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

                assert result.query_operations == #And([#Operation("name", #eq(#Text("Alice")))]);

                assert result.sort_by == ?("created_at", #Descending);
            },
        );

        test(
            "Pagination with cursor and limit",
            func() {
                let builder = Query.QueryBuilder();
                let cursor = { last_document_id = ?("\F0" : Blob) };
                let result = builder.Where("name", #eq(#Text("Alice"))).PaginationToken(cursor).Limit(10).build();

                assert result.query_operations == #And([#Operation("name", #eq(#Text("Alice")))]);

                assert result.pagination.cursor == ?cursor;
                assert result.pagination.limit == ?10;
                assert result.pagination.skip == null;
            },
        );

        test(
            "Limit configuration",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("name", #eq(#Text("Alice"))).Limit(25).build();

                assert result.query_operations == #And([#Operation("name", #eq(#Text("Alice")))]);

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

                assert result.query_operations == #And([#Operation("name", #eq(#Text("Alice")))]);
                assert result.pagination.skip == ?5;
                assert result.pagination.cursor == null;
                assert result.pagination.limit == null;
            },
        );

        test(
            "Combined pagination options",
            func() {
                let builder = Query.QueryBuilder();
                let cursor = { last_document_id = ?("\F0" : Blob) };
                let result = builder.Where("name", #eq(#Text("Alice"))).PaginationToken(cursor).Limit(10).build();

                assert result.query_operations == #And([#Operation("name", #eq(#Text("Alice")))]);

                assert result.pagination.cursor == ?cursor;
                assert result.pagination.limit == ?10;
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
                let builder1 = Query.QueryBuilder().Where("age", #gte(#Nat(18))).Or("status", #eq(#Text("admin")));

                let builder2 = Query.QueryBuilder();
                let result = builder2.Where("name", #eq(#Text("Alice"))).AndQuery(builder1).build();

                let expected = #And([
                    #Operation("name", #eq(#Text("Alice"))),
                    #Or([#Operation("age", #gte(#Nat(18))), #Operation("status", #eq(#Text("admin")))]),
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

        // ==== ADVANCED & EDGE CASE QUERIES ====

        test(
            "Multiple NOT conditions on same field with AND - should find gaps",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("category", #not_(#eq(#Text("premium")))).And("category", #not_(#eq(#Text("basic")))).build();

                // This should find all categories that are neither "premium" nor "basic"
                // Each #not_(#eq(x)) expands to #Or([#lt(x), #gt(x)])
                // When ANDed together: currently not optimized, creates nested conditions
                // TODO: Could be optimized to find the gap ranges
                let expected = #And([
                    #Or([
                        #Operation("category", #lt(#Text("premium"))),
                        #Operation("category", #gt(#Text("premium"))),
                    ]),
                    #Or([
                        #Operation("category", #lt(#Text("basic"))),
                        #Operation("category", #gt(#Text("basic"))),
                    ]),
                ]);

                Debug.print("Multiple NOT on same field: " # debug_show result.query_operations);
                assert result.query_operations == expected;
            },
        );

        test(
            "NOT of anyOf with many values - range gap optimization",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where(
                    "priority",
                    #not_(#anyOf([#Nat(1), #Nat(2), #Nat(5), #Nat(10), #Nat(20)])),
                ).build();

                // Should create gaps: <1, (1,2), (2,5), (5,10), (10,20), >20
                // Optimized to find all values NOT in the list
                let expected = #Or([
                    #Operation("priority", #lt(#Nat(1))),
                    #And([
                        #Operation("priority", #gt(#Nat(1))),
                        #Operation("priority", #lt(#Nat(2))),
                    ]),
                    #And([
                        #Operation("priority", #gt(#Nat(2))),
                        #Operation("priority", #lt(#Nat(5))),
                    ]),
                    #And([
                        #Operation("priority", #gt(#Nat(5))),
                        #Operation("priority", #lt(#Nat(10))),
                    ]),
                    #And([
                        #Operation("priority", #gt(#Nat(10))),
                        #Operation("priority", #lt(#Nat(20))),
                    ]),
                    #Operation("priority", #gt(#Nat(20))),
                ]);

                Debug.print("NOT anyOf with gaps: " # debug_show result.query_operations);
                assert result.query_operations == expected;
            },
        );

        test(
            "Complex business query - active subscriptions in price range excluding trials",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where(
                    "subscription_status",
                    #eq(#Text("active")),
                ).And(
                    "price",
                    #between(#Nat(100), #Nat(500)),
                ).And("subscription_type", #not_(#eq(#Text("trial")))).And(
                    "billing_cycle",
                    #anyOf([#Text("monthly"), #Text("yearly")]),
                ).Sort("price", #Ascending).Limit(100).build();

                // Business use case: Find active paid subscriptions in a price range
                // Excludes trial users, only monthly/yearly billing
                assert result.sort_by == ?("price", #Ascending);
                assert result.pagination.limit == ?100;

                Debug.print("Business subscription query: " # debug_show result.query_operations);

                assert result.query_operations == #And([
                    #Operation("subscription_status", #eq(#Text("active"))),
                    #Operation("price", #gte(#Nat(100))),
                    #Operation("price", #lte(#Nat(500))),
                    #Or([
                        #Operation("subscription_type", #lt(#Text("trial"))),
                        #Operation("subscription_type", #gt(#Text("trial"))),
                    ]),
                    #Or([
                        #Operation("billing_cycle", #eq(#Text("monthly"))),
                        #Operation("billing_cycle", #eq(#Text("yearly"))),
                    ]),
                ]);
            },
        );

        test(
            "E-commerce query - products with complex filtering",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where(
                    "in_stock",
                    #eq(#Bool(true)),
                ).And(
                    "price",
                    #between(#Nat(50), #Nat(200)),
                ).And(
                    "category",
                    #not_(#anyOf([#Text("clearance"), #Text("discontinued")])),
                ).And(
                    "rating",
                    #gte(#Nat(4)),
                ).Or(
                    "featured",
                    #eq(#Bool(true)),
                ).Sort("price", #Ascending).build();

                // E-commerce: In-stock products in price range, good ratings, excluding clearance
                // OR featured products (regardless of other filters)
                Debug.print("E-commerce product query: " # debug_show result.query_operations);

                assert result.query_operations == #Or([
                    #And([
                        #Operation("in_stock", #eq(#Bool(true))),
                        #Operation("price", #gte(#Nat(50))),
                        #Operation("price", #lte(#Nat(200))),
                        #Or([
                            #Operation("category", #lt(#Text("clearance"))),
                            #And([
                                #Operation("category", #gt(#Text("clearance"))),
                                #Operation("category", #lt(#Text("discontinued"))),
                            ]),
                            #Operation("category", #gt(#Text("discontinued"))),
                        ]),
                        #Operation("rating", #gte(#Nat(4))),
                    ]),
                    #Operation("featured", #eq(#Bool(true))),
                ]);
            },
        );

        test(
            "Time-based query with NOT ranges - scheduled events outside business hours",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where(
                    "event_type",
                    #eq(#Text("scheduled")),
                ).And(
                    "hour",
                    #not_(#between(#Nat(9), #Nat(17))),
                ).And(
                    "day_of_week",
                    #not_(#anyOf([#Nat(0), #Nat(6)])),
                ).build();

                Debug.print("Time-based query: " # debug_show result.query_operations);

                assert result.query_operations == #And([
                    #Operation("event_type", #eq(#Text("scheduled"))),
                    #Or([
                        #Operation("hour", #lt(#Nat(9))),
                        #Operation("hour", #gt(#Nat(17))),
                    ]),
                    #Or([
                        #Operation("day_of_week", #lt(#Nat(0))),
                        #And([
                            #Operation("day_of_week", #gt(#Nat(0))),
                            #Operation("day_of_week", #lt(#Nat(6))),
                        ]),
                        #Operation("day_of_week", #gt(#Nat(6))),
                    ]),
                ]);
            },
        );

        test(
            "User segmentation - complex demographic query",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #between(#Nat(25), #Nat(45))).And("location", #startsWith(#Text("US-"))).And("plan_type", #not_(#eq(#Text("free")))).Or("lifetime_value", #gte(#Nat(1000))).And("last_active_days", #lte(#Nat(30))).build();

                // Target users: 25-45 years, US-based, paid plans, OR high-value users
                // Recently active within 30 days
                Debug.print("User segmentation query: " # debug_show result.query_operations);

                assert result.query_operations == #And([
                    #Or([
                        #And([
                            #Operation("age", #gte(#Nat(25))),
                            #Operation("age", #lte(#Nat(45))),
                            #Operation("location", #gte(#Text("US-"))),
                            #Operation("location", #lte(#Text("US-" # Text.fromChar(Char.fromNat32(0xff))))),
                            #Or([
                                #Operation("plan_type", #lt(#Text("free"))),
                                #Operation("plan_type", #gt(#Text("free"))),
                            ]),
                        ]),
                        #Operation("lifetime_value", #gte(#Nat(1000))),
                    ]),
                    #Operation("last_active_days", #lte(#Nat(30))),
                ]);
            },
        );

        test(
            "IoT sensor query - anomaly detection pattern",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("sensor_status", #eq(#Text("active"))).And("temperature", #not_(#between(#Nat(18), #Nat(28)))).Or("humidity", #not_(#between(#Nat(30), #Nat(60)))).Or("pressure", #gt(#Nat(1100))).And("alert_acknowledged", #eq(#Bool(false))).build();

                // IoT: Find sensors with readings outside normal ranges and unacknowledged alerts
                Debug.print("IoT anomaly query: " # debug_show result.query_operations);

                assert result.query_operations == #And([
                    #Or([
                        #And([
                            #Operation("sensor_status", #eq(#Text("active"))),
                            #Or([
                                #Operation("temperature", #lt(#Nat(18))),
                                #Operation("temperature", #gt(#Nat(28))),
                            ]),
                        ]),
                        #Operation("humidity", #lt(#Nat(30))),
                        #Operation("humidity", #gt(#Nat(60))),
                        #Operation("pressure", #gt(#Nat(1100))),
                    ]),
                    #Operation("alert_acknowledged", #eq(#Bool(false))),
                ]);
            },
        );

        test(
            "Financial transactions - fraud detection pattern",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("amount", #gt(#Nat(10000))).And("transaction_hour", #not_(#between(#Nat(6), #Nat(22)))).And("location_verified", #eq(#Bool(false))).Or("velocity_score", #gt(#Nat(8))).And("user_age_days", #lt(#Nat(30))).Sort("amount", #Descending).Limit(50).build();

                // Fraud detection: Large transactions at odd hours, unverified location
                // OR high-velocity patterns, from new accounts
                Debug.print("Fraud detection query: " # debug_show result.query_operations);

                assert result.query_operations == #And([
                    #Or([
                        #And([
                            #Operation("amount", #gt(#Nat(10000))),
                            #Or([
                                #Operation("transaction_hour", #lt(#Nat(6))),
                                #Operation("transaction_hour", #gt(#Nat(22))),
                            ]),
                            #Operation("location_verified", #eq(#Bool(false))),
                        ]),
                        #Operation("velocity_score", #gt(#Nat(8))),
                    ]),
                    #Operation("user_age_days", #lt(#Nat(30))),
                ]);
            },
        );

        test(
            "Content moderation - multi-flag query",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("status", #eq(#Text("pending_review"))).And("flag_count", #gte(#Nat(3))).And("flag_types", #not_(#eq(#Text("spam")))).Or("ai_confidence", #gte(#Nat(90))).And("reviewed", #eq(#Bool(false))).Sort("flag_count", #Descending).build();

                // Content mod: Pending items with multiple flags (not just spam)
                // OR high AI confidence that needs review
                Debug.print("Content moderation query: " # debug_show result.query_operations);

                assert result.query_operations == #And([
                    #Or([
                        #And([
                            #Operation("status", #eq(#Text("pending_review"))),
                            #Operation("flag_count", #gte(#Nat(3))),
                            #Or([
                                #Operation("flag_types", #lt(#Text("spam"))),
                                #Operation("flag_types", #gt(#Text("spam"))),
                            ]),
                        ]),
                        #Operation("ai_confidence", #gte(#Nat(90))),
                    ]),
                    #Operation("reviewed", #eq(#Bool(false))),
                ]);
            },
        );

        test(
            "Inventory management - reorder point calculation",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("quantity", #lte(#Nat(100))).And("demand_level", #not_(#eq(#Text("low")))).And("supplier_status", #eq(#Text("active"))).And("category", #not_(#anyOf([#Text("discontinued"), #Text("seasonal")]))).Or("critical_item", #eq(#Bool(true))).build();

                // Inventory: Low stock items that need reordering
                // Excludes low-demand, discontinued, seasonal unless critical
                Debug.print("Inventory reorder query: " # debug_show result.query_operations);

                assert result.query_operations == #Or([
                    #And([
                        #Operation("quantity", #lte(#Nat(100))),
                        #Or([
                            #Operation("demand_level", #lt(#Text("low"))),
                            #Operation("demand_level", #gt(#Text("low"))),
                        ]),
                        #Operation("supplier_status", #eq(#Text("active"))),
                        #Or([
                            #Operation("category", #lt(#Text("discontinued"))),
                            #And([
                                #Operation("category", #gt(#Text("discontinued"))),
                                #Operation("category", #lt(#Text("seasonal"))),
                            ]),
                            #Operation("category", #gt(#Text("seasonal"))),
                        ]),
                    ]),
                    #Operation("critical_item", #eq(#Bool(true))),
                ]);
            },
        );

        // KNOWN BUG: Empty NOT anyOf causes index out of bounds
        // test(
        //     "Edge case - Empty NOT anyOf should match everything",
        //     func() {
        //         let builder = Query.QueryBuilder();
        //         let result = builder.Where("status", #not_(#anyOf([]))).build();

        //         // Edge case: NOT of empty set should theoretically match all values
        //         // Current implementation might not handle this well
        //         Debug.print("Empty NOT anyOf: " # debug_show result.query_operations);
        //     },
        // );

        test(
            "Edge case - Single value NOT anyOf reduces to NOT eq",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("status", #not_(#anyOf([#Text("inactive")]))).build();

                // Single value anyOf should optimize to simple #not_(#eq(#Text("inactive")))
                // which becomes #Or([#lt("inactive"), #gt("inactive")])
                // But current implementation still uses the range-based approach
                let expected = #Or([
                    #Operation("status", #lt(#Text("inactive"))),
                    #Operation("status", #gt(#Text("inactive"))),
                ]);

                Debug.print("Single value NOT anyOf: " # debug_show result.query_operations);
                assert result.query_operations == expected;
            },
        );

        test(
            "Edge case - Overlapping ranges with AND",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("age", #between(#Nat(18), #Nat(65))).And("age", #between(#Nat(25), #Nat(45))).build();

                // Two overlapping ranges should intersect to [25, 45]
                // Current implementation flattens but doesn't optimize intersection
                let expected = #And([
                    #Operation("age", #gte(#Nat(18))),
                    #Operation("age", #lte(#Nat(65))),
                    #Operation("age", #gte(#Nat(25))),
                    #Operation("age", #lte(#Nat(45))),
                ]);

                Debug.print("Overlapping ranges: " # debug_show result.query_operations);
                assert result.query_operations == expected;
            },
        );

        test(
            "Edge case - Contradictory conditions",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("status", #eq(#Text("active"))).And("status", #eq(#Text("inactive"))).build();

                // Impossible query: status can't be both active AND inactive
                // Should ideally be detected and optimized to empty result
                let expected = #And([
                    #Operation("status", #eq(#Text("active"))),
                    #Operation("status", #eq(#Text("inactive"))),
                ]);

                Debug.print("Contradictory conditions: " # debug_show result.query_operations);
                assert result.query_operations == expected;
            },
        );

        test(
            "Deep nesting - query builder composition",
            func() {
                let subquery1 = Query.QueryBuilder().Where("age", #gte(#Nat(18))).And("verified", #eq(#Bool(true)));

                let subquery2 = Query.QueryBuilder().Where("vip_status", #eq(#Bool(true))).Or("lifetime_purchases", #gt(#Nat(10000)));

                let builder = Query.QueryBuilder();
                let result = builder.Where("active", #eq(#Bool(true))).AndQuery(subquery1).OrQuery(subquery2).build();

                // Complex composition of sub-queries
                Debug.print("Deep nested query: " # debug_show result.query_operations);

                assert result.query_operations == #Or([
                    #And([
                        #Operation("active", #eq(#Bool(true))),
                        #Operation("age", #gte(#Nat(18))),
                        #Operation("verified", #eq(#Bool(true))),
                    ]),
                    #Operation("vip_status", #eq(#Bool(true))),
                    #Operation("lifetime_purchases", #gt(#Nat(10000))),
                ]);
            },
        );

        test(
            "Multiple startsWith on same field with OR - prefix search optimization",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("email", #startsWith(#Text("admin"))).Or("email", #startsWith(#Text("support"))).Or("email", #startsWith(#Text("help"))).build();

                // Multiple prefix searches - could be optimized to a single scan
                Debug.print("Multiple startsWith: " # debug_show result.query_operations);

                assert result.query_operations == #Or([
                    #And([
                        #Operation("email", #gte(#Text("admin"))),
                        #Operation("email", #lte(#Text("admin" # Text.fromChar(Char.fromNat32(0xff))))),
                    ]),
                    #And([
                        #Operation("email", #gte(#Text("support"))),
                        #Operation("email", #lte(#Text("support" # Text.fromChar(Char.fromNat32(0xff))))),
                    ]),
                    #And([
                        #Operation("email", #gte(#Text("help"))),
                        #Operation("email", #lte(#Text("help" # Text.fromChar(Char.fromNat32(0xff))))),
                    ]),
                ]);
            },
        );

        test(
            "NOT startsWith - inverse prefix matching",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("email", #not_(#startsWith(#Text("spam")))).build();

                // All emails that don't start with "spam"
                // Becomes: #Or([#lt("spam"), #gt("spam\xFF")])
                Debug.print("NOT startsWith: " # debug_show result.query_operations);

                assert result.query_operations == #Or([
                    #Operation("email", #lt(#Text("spam"))),
                    #Operation("email", #gt(#Text("spam" # Text.fromChar(Char.fromNat32(0xff))))),
                ]);
            },
        );

        test(
            "Chained NOT operations potential optimization failure",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("status", #not_(#eq(#Text("deleted")))).And("status", #not_(#eq(#Text("archived")))).And("status", #not_(#eq(#Text("suspended")))).build();

                // Multiple NOT eq conditions could be optimized to NOT anyOf
                // Current implementation treats them separately
                Debug.print("Chained NOT operations: " # debug_show result.query_operations);

                assert result.query_operations == #And([
                    #Or([
                        #Operation("status", #lt(#Text("deleted"))),
                        #Operation("status", #gt(#Text("deleted"))),
                    ]),
                    #Or([
                        #Operation("status", #lt(#Text("archived"))),
                        #Operation("status", #gt(#Text("archived"))),
                    ]),
                    #Or([
                        #Operation("status", #lt(#Text("suspended"))),
                        #Operation("status", #gt(#Text("suspended"))),
                    ]),
                ]);
            },
        );

        test(
            "Geo-location range query simulation",
            func() {
                let builder = Query.QueryBuilder();
                let result = builder.Where("latitude", #between(#Int(40), #Int(42))).And("longitude", #between(#Int(-75), #Int(-73))).And("active", #eq(#Bool(true))).Sort("distance", #Ascending).Limit(20).build();

                // Bounding box geo query pattern
                Debug.print("Geo-location query: " # debug_show result.query_operations);

                assert result.query_operations == #And([
                    #Operation("latitude", #gte(#Int(40))),
                    #Operation("latitude", #lte(#Int(42))),
                    #Operation("longitude", #gte(#Int(-75))),
                    #Operation("longitude", #lte(#Int(-73))),
                    #Operation("active", #eq(#Bool(true))),
                ]);
            },
        );

        test(
            "Pagination with complex sorting requirements",
            func() {
                let builder = Query.QueryBuilder();
                let cursor = { last_document_id = ?("\F0\A0\B0\C0" : Blob) };
                let result = builder.Where("category", #eq(#Text("electronics"))).And("price", #between(#Nat(100), #Nat(1000))).Sort("popularity", #Descending).PaginationToken(cursor).Limit(25).build();

                assert result.pagination.cursor == ?cursor;
                assert result.pagination.limit == ?25;
                assert result.sort_by == ?("popularity", #Descending);

                Debug.print("Pagination with sorting: " # debug_show result.query_operations);
            },
        );

        // Real-world use cases for new between variants
        test(
            "Gaming leaderboard - scores above threshold (exclusive lower bound)",
            func() {
                let builder = Query.QueryBuilder();
                // Find players with scores strictly greater than 100 and up to 1000
                let result = builder.Where("score", #betweenLeftOpen(#Nat(100), #Nat(1000))).And("active", #eq(#Bool(true))).Sort("score", #Descending).Limit(10).build();

                let expected = #And([
                    #Operation("score", #gt(#Nat(100))),
                    #Operation("score", #lte(#Nat(1000))),
                    #Operation("active", #eq(#Bool(true))),
                ]);

                assert result.query_operations == expected;
                Debug.print("Gaming leaderboard query: " # debug_show result.query_operations);
            },
        );

        test(
            "Date range - events before deadline (exclusive upper bound)",
            func() {
                let builder = Query.QueryBuilder();
                // Find events from start date up to but not including end date
                let result = builder.Where("timestamp", #betweenRightOpen(#Nat(1700000000), #Nat(1700086400))).And("status", #eq(#Text("scheduled"))).build();

                let expected = #And([
                    #Operation("timestamp", #gte(#Nat(1700000000))),
                    #Operation("timestamp", #lt(#Nat(1700086400))),
                    #Operation("status", #eq(#Text("scheduled"))),
                ]);

                assert result.query_operations == expected;
                Debug.print("Date range query: " # debug_show result.query_operations);
            },
        );

        test(
            "Temperature monitoring - strictly within safe range",
            func() {
                let builder = Query.QueryBuilder();
                // Temperature strictly between 0 and 100 degrees (exclusive bounds)
                let result = builder.Where("temperature", #betweenExclusive(#Int(0), #Int(100))).And("sensor_active", #eq(#Bool(true))).build();

                let expected = #And([
                    #Operation("temperature", #gt(#Int(0))),
                    #Operation("temperature", #lt(#Int(100))),
                    #Operation("sensor_active", #eq(#Bool(true))),
                ]);

                assert result.query_operations == expected;
                Debug.print("Temperature monitoring query: " # debug_show result.query_operations);
            },
        );
    },
);
