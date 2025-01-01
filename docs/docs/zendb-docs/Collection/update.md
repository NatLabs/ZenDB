---
sidebar-position: 3
---

# Update

To update an entry you need to the `update()` method on the collection instance. Currently, you can't update individual fields, instead you need to replace the entire document.

### Update a single entry

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

    let #ok(id) = users.insert(user1);

    let new_user1 : User = {
        name = "zendb_enjoyer";
        email = "avg@gmail.com";
        age = 20;
        created_at = user1.created_at;
        account = {
            balance = 4_000;
            currency = "usd";
        }
    };

    let #ok(_) = users.updateById(id, )

```

### Update multiple entries
