---
sidebar-position: 3
---

# Getting Started

## Creating a Database

```motoko
    import ZenDB "mo:zendb";

    stable var zendb_sstore = ZenDB.init_stable_store();
    zendb_sstore := ZenDB.update(zendb_sstore);

    let zendb = ZenDB.launch(zendb_sstore);

```

## Collection

A collection is a set of documents. A collection here is akin to a table in relational databases. You can create a collection by calling `create_collection()` and retrieve an existing collection by calling `get_collection()`.

## Document

A document is an entry stored in a collection. In most document databases these documents are usually in json but in zendb the documents are encoded to candid, in order to support native motoko types. Unlike other document databases that allow you to store single values, In ZenDB every document is a record.

## Creating a Collection

Due to how candid handles serialization we can't allow our collections to be schema-less and flexible like most document databases. The main reason for this is we want to be able to deserialize the data stored in our collections back to a motoko type. This is only possible if the data type is enforced accross all the documents in the collection. If multiple non-compatible candid types are stored in the collection we would get errors when deserializing back to motoko.
This is not the end of the world as candid has a method for handline non-compatible types by using `variants` (Link to find out more about variants).

This is all to say that we need a schema and when updating the schema, the updated version will have to be backward compatible with the previous one.

- Defining our data-type in motoko

  ```motoko
      type User = {
          id: Nat;
          name: Text;
          email: Text;
          age: Nat;
          created_at: Int;
          account: {
              balance: Nat;
              currency: Text;
          };
      };
  ```

- Creating an equivalent schema definition for ZenDB.

  > ZenDB Schema supports all of motoko's primitive and compound types. Look here for the full list.

  ```motoko
      let UsersSchema : ZenDB.Schema = #Record([
          ("id", #Nat),
          ("name", #Text),
          ("email", #Text),
          ("age", #Nat),
          ("created_at", #Int),
          ("account", #Record([
              ("balance", #Nat),
              ("currency", #Text),
          ])),
      ]);
  ```

> We have not added suport for single value documents yet, so every schema has to be defined as a #Record.

- Candify

  Define the methods for converting the specific data-type from candid to motoko and vice-versa.
  This is a generic function that needs to be added to every collection so the motoko compiler knows at compiler time what data types we are deserializing to.

  ```motoko

  let candify_users : ZenDB.Candify<User> = {
    to_blob = func (user: User) : Blob { to_candid(user); };
    from_blob = func (blob: Blob) : User {
        switch(from_candid(blob) : ?User){
            case (?user) { user };
            case (null) Debug.trap("users collection: failed to decode user");
        };
    };
  }



  ```

- Putting it all together

  We define a unique name for our collection. In our case `"users"` makes the most sense. Then we add the `UsersSchema` and `candify_users` methods defined earlier and create the collection. This returns a result containing an error message if it fails or an instance of the collection class we can use to make collection level calls if it succeeds.

  ```motoko
    let #ok(users) = zendb.create_collection("users", UsersSchema, candify_users);
  ```
