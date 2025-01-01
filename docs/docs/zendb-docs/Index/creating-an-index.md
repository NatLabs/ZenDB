---
sidebar-position: 1
---

# Index

Indexes are required to make queries faster. Otherwise each query would have to scan every single element in the database. The indexes help us narrow down the required documents to scan.

## Creating an Index

Creating an index is as easy as calling the `create_index` method on a collection instance. At the moment every index is a composite / multi-field index. In addition all the indexed fields are sorted in `#Ascending` order. Support for `#Descending` will be added soon.

```motoko

    let #ok(users) = zendb.get_collection("users", candify_users);

    let #ok(_) = users.create_index(["name"]);
    let #ok(_) = users.create_index(["account.currency", "account.balance"]);

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

    let #ok(_) = users.insert(user1);

```

The first index stores the name (`"zendb_enjoyer"`) as the key and the id to the record as the value. The second index stores a composite of the currency and balance as the key and the record id as the value.

This way if we make a query for the same name we can search the sorted name index for all the matching record ids.

Creating an index at any point results in an empty index. Documents that are inserted after the index has been created are automatically added to the index. However, any documents inserted to the database before the index was created will not be automatically added.

## Renaming an index

## Clear an index

In case the index gets corrupted or needs to be migrated

## Deleting an index
