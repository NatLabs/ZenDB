---
sidebar-position: 2
---

# Query

The search method allows users to query the database.

## Search

```motoko
    let db_query : ZenDB.Query = ZenDB.QueryBuilder()
        .Where("<field>", #<operator>);

    let #ok(users) = zendb.get_collection("users", candify_users);

    let #ok(results) = users.search(db_query);

```

## Query Builder

ZenDB provides a Query builder to make it easier to create queries.
The query builder has 3 methods that accept the nested field name and the query operator:

### And Condition

`.Where()` -

```motoko
     let #ok(results) = users.search(
         ZenDB.QueryBuilder()
             .Where("name", #eq(#Text("zendb_enjoyer")))
     );
```

`.And()` -

```motoko
     let #ok(results) = users.search(
         ZenDB.QueryBuilder()
             .Where("name", #eq(#Text("zendb_enjoyer")))
             .And("age", #gte(#Nat(18)))
     );
```

### Or Condition

`.Or()` -

```motoko
    let #ok(results) = users.search(
        ZenDB.QueryBuilder()
            .Where("age", #gt(#Nat(30)))
            .Or("account.balance", #gte(#Nat(100_000)))
    );
```

### Pagination

- `Limit()` and `Skip()`

### Sort

- `Sort()`

### Query Operators

These operators are used as conditionals in query calls

| Operator         | Description                                                                                                                 |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `#eq()`          | Equality operator - Query Matches if the specified field is equal to the Candid value passes to the operator                |
| `#gt()`          | Greater Than - Matches if the specified field's value is greater than the given value                                       |
| `#gte()`         | Matches if the specified field's values is greater than or equal to the given value                                         |
| `#lt()`          | Matches if the specified field's value is less than the given value                                                         |
| `#lte()`         | Matches if the specified field's value is less than or equal to the given value                                             |
| `#between(a, b)` | Matches if the specified field's value is between the between or equal to the first and second value passes in the operator |
| `#in([])`        | Matches if the specified field's value matches any of the elements given in the array                                       |
| `#startsWith()`  | Matches any specified field that starts with the given value                                                                |
| `#not()`         | Negates any of the operators in this table                                                                                  |
