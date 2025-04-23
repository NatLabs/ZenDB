## ZenDB

> Very much still in development, Highly volatile API

A single canister, document database that natively supports candid encoding, inspired by MongoDB, for Motoko developers. Leverages multi-field indexes for performant query execution, scaling up to the Internet Computer's 500GB canister limit.

### Getting started

- Install from mops - [zenDB](https://mops.one)

### Usage

- Importing and initializing the ZenDB library

```motoko
import ZenDB "mo:zendb";

stable var zendb_sstore = ZenDB.init_stable_store();
zendb_sstore := ZenDB.upgrade(zendb_sstore);

let zendb = ZenDB.launchDefaultDB(zendb_sstore);
```

- Creating a collection

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

let users_to_candid_blob : ZenDB.Candify<User> = {
  to_blob = func (user: User) : Blob { to_candid(user); };
  from_blob = func (blob: Blob) : User {
    switch(from_candid(blob) : ?User){
      case (?user) { user };
      case (null) Debug.trap("users collection: failed to decode user");
    };
  };
}

let #ok(users) = zendb.create_collection("users", UsersSchema, users_to_candid_blob);

```

- Creating an index

Ideally, creating an index should be done at initialization after the collection is created.
Creating an index after the collection has been populated is a blocking operation that can take a long time and has the potential to exceed the canister's instruction limit.

```motoko

let #ok(_) = users.create_index(["account.balance"]);
let #ok(_) = users.create_index(["account.currency", "created_at"]);

```

- Inserting documents into the collection

There are two ways to insert documents into a collection.
The first is to insert the document and let the system generate an id for the document.
The second is to insert the document with a specific id (`insert_with_id()`).

```motoko
public func add_user(name: Text, email: Text, age: Nat, balance: Nat, currency: Text) : async () {
  let user : User = {
    name;
    email;
    age;
    created_at = Time.now();
    account = { balance; currency; };
  };

  switch(await* users.insert(user)){
    case #ok(_) {};
    case #err(msg) { Debug.print("Error: " # msg); };
  };

};
```

- Querying the collection

QueryBuilder methods: `Where`, `Sort`, `Limit`, `Skip`, `And`, `Or`
Supported operators: #eq(val), #gte(val), #lte(val), #gt(val), #lt(val), #in([val]), #not(nested_operator)

```motoko

func get_usernames_with_balance_between(min: Nat, max: Nat) : [Text] {
  let users_query = ZenDB.QueryBuilder()
    .Where("account.balance", #gte(#Nat(min)))
    .Where("account.balance", #lte(#Nat(max)));

  let #ok(results) = users.find(users_query);

  let usernames = Array.map(
    results,
    func(id: Nat, user: User): Text { user.name; }
  );

  return usernames;

};

func get_most_recent_users_for_currency(currency: Text, limit: Nat) : [User] {
  let users_query = ZenDB.QueryBuilder()
    .Where("account.currency", #eq(currency))
    .Sort("created_at", #Descending)
    .Limit(limit);

  let #ok(results) = users.find(users_query);

  return results;
};


```

### Features

- [x] Candid serialization for all types
- [ ] Query Caching
- [ ] Indexes
  - [x] Multiple field index (Compound Index)
  - [ ] Multi-key array index
- [ ] Zen Query Language
  - [x] operators (and, or, not, eq, gte, lte, gt, lt, in, nin)
  - [x] modifiers (limit, skip, sort)
  - [ ] functions (count, sum, avg, min, max)
- [ ] Schema
  - [x] Schema Validation on insert/update
  - [x] Backwards compatibility on schema changes
  - [ ] Schema Constraints (required, unique, enum, min, max)
- [ ] Transactions
  - [ ] Single document transactions
    - [ ] Document versioning
- [ ] Backup and Restore
  - [ ] Copy on Write Snapshots
  - [ ] External canister Backups
  - [ ] Consistent incremental backups
- [x] Garbage collection of regions from deleted collections
