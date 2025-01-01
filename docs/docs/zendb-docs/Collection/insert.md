---
sidebar-position: 1
---

# Insert

To insert a document in a collection, call the `insert()` method on a collection instance. You can retrieve an existing collection via `get_collection()` or create a new collection (link to creating a new collection).

Using the User example from the `creating a collection`(link to page) we will show examples of how to insert some items into it. (show User and candify_users below)

## Insert single document

```motoko
    let #ok(users) = zendb.get_collection("users", candify_users);

    let user1 : User = {
        name = "zendb_enjoyer";
        email = "avg@gmail.com";
        age = 20;
        created_at = Time.now();
        account = {
            balance = 1_000;
            currency = "usd";
        };
    };

    let #ok(id : Nat) = users.insert(user1);

```

### Insert multiple documents

```motoko

    let users = [user1, user2, user3, ...];

    let #ok(ids : [Nat]) = users.insert_all(users);
```
