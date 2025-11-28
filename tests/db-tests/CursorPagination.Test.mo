// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Iter "mo:base@0.16.0/Iter";
import Array "mo:base@0.16.0/Array";
import Buffer "mo:base@0.16.0/Buffer";
import Blob "mo:base@0.16.0/Blob";

import { test; suite } "mo:test";
import Map "mo:map@9.0.1/Map";

import ZenDB "../../src/EmbeddedInstance";
import ZenDBSuite "../test-utils/TestFramework";

type User = {
    name : Text;
    age : Nat;
    category : Text;
};

let users_schema = #Record([
    ("name", #Text),
    ("age", #Nat),
    ("category", #Text),
]);

let candify_user = {
    from_blob = func(blob : Blob) : ?User {
        from_candid (blob);
    };
    to_blob = func(c : User) : Blob { to_candid (c) };
};

ZenDBSuite.newSuite(
    "Cursor Pagination Tests",
    ?{ ZenDBSuite.withAndWithoutIndex with log_level = #Debug },
    func suite_setup(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {
        let #ok(users) = zendb.createCollection<User>("users", users_schema, candify_user, null) else return assert false;

        let #ok(_) = suite_utils.createIndex(users.name(), "name_idx", [("name", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(users.name(), "age_idx", [("age", #Ascending)], null) else return assert false;
        let #ok(_) = suite_utils.createIndex(users.name(), "category_idx", [("category", #Ascending)], null) else return assert false;

        let inputs = Map.new<ZenDB.Types.DocumentId, User>();

        // Insert 100 documents with varying data for pagination testing
        for (i in Iter.range(1, 100)) {
            let user = {
                name = "user_" # debug_show i;
                age = i;
                category = if (i % 3 == 0) "premium" else if (i % 2 == 0) "basic" else "free";
            };

            let #ok(id) = users.insert(user) else return assert false;
            ignore Map.put(inputs, Map.bhash, id, user);
        };

        // Helper function to collect all pages using skip pagination
        func skip_paginated_query(db_query : ZenDB.QueryBuilder, page_size : Nat) : [[ZenDB.Types.WrapId<User>]] {
            let pages = Buffer.Buffer<[(ZenDB.Types.DocumentId, User)]>(10);
            var offset = 0;
            var continue_pagination = true;

            label pagination_loop while (continue_pagination) {
                let query_with_pagination = db_query.Skip(offset).Limit(page_size);
                let #ok(results) = users.search(query_with_pagination) else return Buffer.toArray(pages);

                if (results.documents.size() == 0) {
                    continue_pagination := false;
                } else {

                    pages.add(results.documents);

                    if (results.documents.size() < page_size) {
                        continue_pagination := false;
                    } else {
                        offset += page_size;
                    };
                };
            };

            Buffer.toArray(pages);
        };

        // Helper function to collect all pages using cursor pagination
        func cursor_paginated_query(db_query : ZenDB.QueryBuilder, page_size : Nat) : [[(ZenDB.Types.DocumentId, User)]] {
            let pages = Buffer.Buffer<[(ZenDB.Types.DocumentId, User)]>(10);
            var cursor : ?ZenDB.Types.PaginationToken = null;
            var continue_pagination = true;

            label pagination_loop while (continue_pagination) {
                let query_with_pagination = switch (cursor) {
                    case (null) db_query.Limit(page_size);
                    case (?c) db_query.PaginationToken(c).Limit(page_size);
                };
                let #ok(results) = users.search(query_with_pagination) else return Buffer.toArray(pages);

                if (results.documents.size() > 0) pages.add(results.documents);
                if (cursor == ?results.pagination_token) {
                    Debug.print("Cursor did not advance, potential infinite loop detected");
                    break pagination_loop;
                };

                cursor := ?results.pagination_token;
                continue_pagination := results.has_more;
            };

            Buffer.toArray(pages);
        };

        func log_pagination_comparison(skip_pages : [[ZenDB.Types.WrapId<User>]], cursor_pages : [[ZenDB.Types.WrapId<User>]]) {
            Debug.print("Skip pages: " # debug_show skip_pages.size());
            Debug.print("Cursor pages: " # debug_show cursor_pages.size());

            for (i in Iter.range(0, skip_pages.size() - 1)) {
                Debug.print("\n=== Page " # debug_show (i + 1) # " ===");

                Debug.print("Skip page entries:");
                for (entry in skip_pages[i].vals()) {
                    Debug.print("  " # debug_show entry);
                };

                Debug.print("Cursor page entries:");
                for (entry in cursor_pages[i].vals()) {
                    Debug.print("  " # debug_show entry);
                };
            };
        };

        suite(
            "Cursor Pagination Tests",
            func() {

                test(
                    "basic pagination - no filters",
                    func() {
                        let page_size = 15;
                        let db_query = ZenDB.QueryBuilder();

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);

                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "filtered query - age greater than 50",
                    func() {
                        let page_size = 10;
                        let db_query = ZenDB.QueryBuilder().Where("age", #gt(#Nat(50)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);

                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "filtered query - category equals premium",
                    func() {
                        let page_size = 7;
                        let db_query = ZenDB.QueryBuilder().Where("category", #eq(#Text("premium")));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);

                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "range query - age between 20 and 80",
                    func() {
                        let page_size = 12;
                        let db_query = ZenDB.QueryBuilder().Where("age", #between(#Nat(20), #Nat(80)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);

                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "sorted query - ascending by age",
                    func() {
                        let page_size = 20;
                        let db_query = ZenDB.QueryBuilder().SortBy("age", #Ascending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);

                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "sorted query - descending by name",
                    func() {
                        let page_size = 13;
                        let db_query = ZenDB.QueryBuilder().SortBy("name", #Descending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);

                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "filtered and sorted - category basic, sorted by age desc",
                    func() {
                        let page_size = 8;
                        let db_query = ZenDB.QueryBuilder().Where("category", #eq(#Text("basic"))).SortBy("age", #Descending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);

                        log_pagination_comparison(skip_pages, cursor_pages);
                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "complex query - age less than 30 or greater than 70",
                    func() {
                        let page_size = 10;
                        let db_query = ZenDB.QueryBuilder().Where("age", #lt(#Nat(30))).Or("age", #gt(#Nat(70)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);

                        log_pagination_comparison(skip_pages, cursor_pages);
                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "anyOf query - multiple categories",
                    func() {
                        let page_size = 15;
                        let db_query = ZenDB.QueryBuilder().Where("category", #anyOf([#Text("premium"), #Text("basic")]));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "small page size - single result per page",
                    func() {
                        let page_size = 1;
                        let db_query = ZenDB.QueryBuilder().Where("age", #lte(#Nat(10)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "large page size - more than total results",
                    func() {
                        let page_size = 200;
                        let db_query = ZenDB.QueryBuilder();

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "combined filters - age range and category",
                    func() {
                        let page_size = 5;
                        let db_query = ZenDB.QueryBuilder().Where("age", #gte(#Nat(25))).And("age", #lte(#Nat(75))).And("category", #eq(#Text("free")));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "not equal filter - category not premium",
                    func() {
                        let page_size = 12;
                        let db_query = ZenDB.QueryBuilder().Where("category", #not_(#eq(#Text("premium"))));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "exists filter - name exists",
                    func() {
                        let page_size = 20;
                        let db_query = ZenDB.QueryBuilder().Where("name", #exists);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "startsWith filter - name starts with 'user_1'",
                    func() {
                        let page_size = 5;
                        let db_query = ZenDB.QueryBuilder().Where("name", #startsWith(#Text("user_1")));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "complex OR chain - multiple age conditions",
                    func() {
                        let page_size = 8;
                        let db_query = ZenDB.QueryBuilder().Where("age", #eq(#Nat(10))).Or("age", #eq(#Nat(25))).Or("age", #eq(#Nat(50))).Or("age", #eq(#Nat(75))).Or("age", #eq(#Nat(100)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "complex AND chain - narrow age range",
                    func() {
                        let page_size = 5;
                        let db_query = ZenDB.QueryBuilder().Where("age", #gte(#Nat(40))).And("age", #lte(#Nat(60))).And("age", #not_(#eq(#Nat(50))));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "filter with sort - age less than 30 sorted by name desc",
                    func() {
                        let page_size = 6;
                        let db_query = ZenDB.QueryBuilder().Where("age", #lt(#Nat(30))).SortBy("name", #Descending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "between with sort - age 30-70 sorted by category",
                    func() {
                        let page_size = 15;
                        let db_query = ZenDB.QueryBuilder().Where("age", #between(#Nat(30), #Nat(70))).SortBy("category", #Ascending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "empty result set - age greater than 1000",
                    func() {
                        let page_size = 10;
                        let db_query = ZenDB.QueryBuilder().Where("age", #gt(#Nat(1000)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                        assert skip_pages.size() == 0;
                        assert cursor_pages.size() == 0;
                    },
                );

                test(
                    "boundary test - age equals 1 (minimum)",
                    func() {
                        let page_size = 5;
                        let db_query = ZenDB.QueryBuilder().Where("age", #eq(#Nat(1)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "boundary test - age equals 100 (maximum)",
                    func() {
                        let page_size = 5;
                        let db_query = ZenDB.QueryBuilder().Where("age", #eq(#Nat(100)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "odd page size - 7 items per page with sort",
                    func() {
                        let page_size = 7;
                        let db_query = ZenDB.QueryBuilder().SortBy("age", #Ascending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "multiple between conditions - age 10-40 with category filter",
                    func() {
                        let page_size = 8;
                        let db_query = ZenDB.QueryBuilder().Where("age", #between(#Nat(10), #Nat(40))).And("category", #eq(#Text("premium")));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "text comparison - name less than 'user_5'",
                    func() {
                        let page_size = 10;
                        let db_query = ZenDB.QueryBuilder().Where("name", #lt(#Text("user_5")));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "text comparison - name greater than or equal 'user_9'",
                    func() {
                        let page_size = 12;
                        let db_query = ZenDB.QueryBuilder().Where("name", #gte(#Text("user_9")));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "descending sort only - no filters",
                    func() {
                        let page_size = 11;
                        let db_query = ZenDB.QueryBuilder().SortBy("age", #Descending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "ascending sort only - no filters",
                    func() {
                        let page_size = 9;
                        let db_query = ZenDB.QueryBuilder().SortBy("name", #Ascending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "sort by category ascending",
                    func() {
                        let page_size = 14;
                        let db_query = ZenDB.QueryBuilder().SortBy("category", #Ascending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "sort by category descending",
                    func() {
                        let page_size = 16;
                        let db_query = ZenDB.QueryBuilder().SortBy("category", #Descending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "anyOf with sort - categories sorted by age",
                    func() {
                        let page_size = 11;
                        let db_query = ZenDB.QueryBuilder().Where("category", #anyOf([#Text("premium"), #Text("free")])).SortBy("age", #Ascending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "age divisible by 5 - using anyOf",
                    func() {
                        let page_size = 6;
                        let ages = Array.tabulate<ZenDB.Types.Candid>(20, func(i : Nat) : ZenDB.Types.Candid { #Nat((i + 1) * 5) });
                        let db_query = ZenDB.QueryBuilder().Where("age", #anyOf(ages));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "very small result set - age between 48 and 52",
                    func() {
                        let page_size = 3;
                        let db_query = ZenDB.QueryBuilder().Where("age", #between(#Nat(48), #Nat(52)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "complex query - OR with sorts",
                    func() {
                        let page_size = 7;
                        let db_query = ZenDB.QueryBuilder().Where("age", #lt(#Nat(20))).Or("age", #gt(#Nat(80))).SortBy("age", #Ascending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "edge case - page size equals total results",
                    func() {
                        let page_size = 100;
                        let db_query = ZenDB.QueryBuilder();

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                        assert skip_pages.size() == 1;
                        assert skip_pages[0].size() == 100;
                    },
                );

                test(
                    "edge case - page size is 2",
                    func() {
                        let page_size = 2;
                        let db_query = ZenDB.QueryBuilder().Where("age", #lte(#Nat(20)));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "age divisible by 10 with ascending sort",
                    func() {
                        let page_size = 4;
                        let ages = Array.tabulate<ZenDB.Types.Candid>(10, func(i : Nat) : ZenDB.Types.Candid { #Nat((i + 1) * 10) });
                        Debug.print("Ages for anyOf filter: " # debug_show ages);
                        let db_query = ZenDB.QueryBuilder().Where("age", #anyOf(ages)).SortBy("age", #Ascending);

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "age divisible by 10 with descending sort",
                    func() {
                        let page_size = 4;
                        let ages = Array.tabulate<ZenDB.Types.Candid>(10, func(i : Nat) : ZenDB.Types.Candid { #Nat((i + 1) * 10) });
                        Debug.print("Ages for anyOf filter: " # debug_show ages);
                        let db_query = ZenDB.QueryBuilder().Where("age", #anyOf(ages)).SortBy("age", #Descending);

                        Debug.print(
                            "result array: " # debug_show (
                                users.search(db_query)
                            )
                        );

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                test(
                    "not anyOf filter - exclude multiple categories",
                    func() {
                        let page_size = 10;
                        let db_query = ZenDB.QueryBuilder().Where("category", #not_(#anyOf([#Text("premium"), #Text("basic")])));

                        let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                        let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                        log_pagination_comparison(skip_pages, cursor_pages);

                        assert skip_pages == cursor_pages;
                    },
                );

                // test(
                //     "combined not filters - not premium and not basic",
                //     func() {
                //         let page_size = 10;
                //         let db_query = ZenDB.QueryBuilder().Where("category", #not_(#eq(#Text("premium")))).And("category", #not_(#eq(#Text("basic"))));

                //         let skip_pages = skip_paginated_query(db_query.clone(), page_size);
                //         let cursor_pages = cursor_paginated_query(db_query.clone(), page_size);
                //         log_pagination_comparison(skip_pages, cursor_pages);

                //         assert skip_pages == cursor_pages;
                //     },
                // );

            },
        );
    },
);
