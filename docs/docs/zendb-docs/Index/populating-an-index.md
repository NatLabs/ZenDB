---
sidebar-position: 2
---

# Populate Index

Populating an index with a large number of documents will be a very expensive operation. As a result populating an index is not an operation that can be done in a single call so all the methods are async calls that split parts of their call into several nested calls to execute the operation

- `create_and_populate_index()`
- `populate_index()`
- `populate_indexes()`

## Populate Indexes

```motoko

    let #ok(users) = zendb.get_collection("users", candify_users);

    let list_of_users = [...];

    let #ok(ids : [Nat]) = users.insert_all(list_of_users);

    let #ok(_) = users.create_index(["name"]);
    let #ok(_) = users.create_index(["account.currency", "account.balance"]);

    let #ok(_) = await* users.populate_indexes([
        ["name"],
        ["account.currency", "account.balance"]
    ]);

```

## Checking the status of a populating index

The populated percentage of the index can be found in the index stats.
The value is a `Float` from `0.00` to `100.00`.

```motoko

    let #ok(index_stats) = users.get_index_stats(["name"]);

    Debug.print(debug_show(index_stats.populated_percentage));

```
