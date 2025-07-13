# Documentation and Core Modules

## Quick Reference
- [Schema Types](#schema-table) 
- [Schema Constraints](#schema-constraints-table)
- [Query Operators](#query-operators)
- [Update Operations](#update-table)

## Terms
List of terms as they are used in this library.

| Term                                  | Description                                                                                                                                                                         |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Document                              | The smallest entity of our database. It's the data that is defined and stored by the user.                                                                                          |
| Collection                            | This is the same as a table in SQL databases. A collection is a group of records with the same schema definition.                                                                   |
| Schema                                | How to define a schema? The schema must match the Motoko type you are trying to store. Our schema definition is a list of Motoko types.                                             |
| Database                              | A database is a collection of collections.                                                                                                                                          |
| Index                                 | An index is a data structure that improves the speed of data retrieval operations on a database table.                                                                              |
| Query                                 | A query is a request for data or information from a database table or collection.                                                                                                   |
| Query Builder                         | A fluent interface for building complex queries.                                                                                                                                    |
| Query Planner                         | The component that determines the most efficient way to execute a query.                                                                                                            |
| Query Execution                       | The process of executing a query and returning the results.                                                                                                                         |
| Candid                                | The interface description language used by the Internet Computer.                                                                                                                   |
| Candid Encoding                       | The process of converting a Motoko type to Candid's binary format as defined by the Candid specification.                                                                           |
| Candid Decoding                       | The process of converting a Candid binary format back to a Motoko type.                                                                                                             |
| SchemaMap                             | A mapping of all possible field names that can be defined from the schema to their corresponding candid types.                                                                      |
| Candify                               | A type that defines how to convert a Motoko type to and from Candid's binary format.                                                                                                |
| CandidMap                             | A mapping of all possible field names that can be defined from the schema to their corresponding candid values.                                                                     |
| Stable Memory                         | A feature in the Internet Computer that allows us to persist data across upgrades and grants us access to a larger amount of memory. Akin to disk storage in traditional computing. |
| Stable Store                          | A stable store is the internal state representation of a motoko utility or module that allows it to be stable (persist on upgrades).                                                |
| stable var                            | A stable variable is a variable that is stored in stable memory and persists across upgrades.                                                                                       |
| Stable Heap                           | Stable variables can also be stored on the heap by using the `stable var` keyword. This does not require serializing the variable to bytes.                                         |
| Enhanced Orthogonal Persistence (EOP) | A feature in the Internet Computer that allows us to heap data in stable memory by using the `stable var` keyword. Prevents the need to serialize the variable to bytes.            |



## ZenDB Instance 
A ZenDB instance is collection of databases.

```motoko
let zendb = ZenDB.newStableStore(null);
```

### Heap vs Stable Memory
ZenDB supports two memory types: `#heap` and `#stableMemory`.
Just as their names suggest, choosing `#heap` memory means all the data stored in your database collections and indexes would be stored on the heap and choosing `#stableMemory` means all your data would be serialized and stored in stable memory as bytes.

There are performance differences between these two different memory types, as shown in our benchmarks. For read operations, heap memory is typically 15-30% faster, while for write operations with indexes, the difference can be even more significant. 

By default, we select `#stableMemory` for new ZenDB instances but you have the option to change it to heap using this command:
```motoko
let zendb = ZenDB.newStableStore(?{
  ZenDB.defaultSettings with
  memory_type = #heap; // or #stableMemory;
});
```
The benefit of the `#heap` memory type is that we avoid the extra overhead required to serialize to bytes and store in stable memory, resulting in more performant database operations. However, the heap memory is currently limited to 4GB which limits the amount of data that we can store in our databases. This is where stable memory comes in, as we can store up to 500GB of data in a single canister, giving us so much more room to store content.

There is a feature in beta currently to increase the current heap limit to that of stable memory by upgrading the current canister system to store variables on the heap directly in stable memory and keep this process completely invisible to the users. This feature is called Enhanced Orthogonal Persistence (EOP).

### Updating canister size restrictions


## Error Handling
Our design philosophy is to ensure that every foreseeable error can be handled by the user by returning a `Result` type and to minimize the use of Debug.trap() to terminate the execution of the current process.

## Logging
We return extensive logs from the ZenDB library to help with debugging issues in the library.

Each of our logs follow the popular log level system that defines the level of importance a specific log has.
Log levels from lowest to highest priority: `#Debug`, `#Info`, `#Warn`, `#Error`, `#Trap`

By default, when defining your ZenDB instance, the `#Warn` option is chosen as this is the lowest log level that may require action from the user. You have the option, at any time, to select the minimum log level you want to receive logs from the library.

```motoko
let zendb = ZenDB.newStableStore(null);
ZenDB.setLogLevel(zendb, #Info);
```

> Warning: Setting the log level to `#Debug` may lead to a log overload as there are significantly more logs outputted at this log level than all the others.

For easy identification, our logs start with the library name, `ZenDB` and the name of the public function called. For example a failed call to insert an element into the database would output similar logs to:
```
[ERROR] ZenDB.insert(): Insertion of record value #Record([("name", "James")]) failed because another record with id = 72, has the same value for field 'name' -> "James"
```

## Database
A database is a collection of collections.
For the purpose of this library, a database is a namespace for a set of collections. 
On instantiation of the zendb instance the default database (a database named 'default') is created and can be accessed via the `launchDefaultDB()` method.

```motoko
let db = ZenDB.launchDefaultDB(zendb);
```

Ideally, only one database would be used per canister but depending on your specific usecase, you have the option to create multiple databases by calling `createDB()` and specifying a unique db name:

```motoko
let #ok(cycles_db) = ZenDB.createDB(zendb, "cycles");
```

An existing database can also be retrieved by calling `getDB()` with the database's unique name:

```motoko
let #ok(cycles_db) = ZenDB.getDB(zendb, "cycles");
```

## Collection
This is the same as a table in SQL databases.
A collection is a group of documents with the same schema definition.

### Characteristics
- A single Motoko type (document) per collection. This is a strict rule because it follows the strict typing system in Motoko and Candid.
- Collections are not flexible in that their schema cannot be changed to a completely different schema after creation. We only allow backward compatible schema upgrades.
- Each document is dynamically assigned an id that is returned after it is inserted into the collection.
- The internal representation of our collections is a B+Tree with the document id as its key and the candid representation of the document as the value.
- Allows duplicate documents unless a unique constraint is defined.

Creating a collection requires two key parameters: the schema and the candify definition.

### Size limitations

- The max size of any record stored in a collection is 4GB
- The max size for any field that will be stored in an index is only 64 KB. Actually the total size of the composite fields that would be stored in the index should be less than or equal to 64 KB.

## Schema
How to define a schema? The schema must match the Motoko type you are trying to store. Our schema definition is a list of Motoko types.

### Schema Table
A schema can contain any combination of the following types:
| Type                                        | Description                 |
| ------------------------------------------- | --------------------------- |
| #Nat                                        | A natural number            |
| #Int                                        | A signed integer            |
| Bounded Nats: #Nat8, #Nat16, #Nat32, #Nat64 | Bounded natural numbers     |
| Bounded Ints: #Int8, #Int16, #Int32, #Int64 | Bounded signed integers     |
| #Text                                       | A string                    |
| #Bool                                       | A boolean                   |
| #Float                                      | A floating point number     |
| #Blob                                       | A binary blob               |
| #Principal                                  | A principal                 |
| #Option(SchemaType)                         | An optional value           |
| #Array(SchemaType)                          | An array of values          |
| #Record([("field_name", SchemaType)])       | A record with named fields  |
| #Variant([("variant_name", SchemaType)])    | A variant with named fields |



These schema definitions have to match the Motoko types you are trying to store.

This file [Documents.Test.mo](./tests/Documents.Test.mo) has a few examples of complex schema and documents.

#### Tuple Schema Types
We support tuple schema types, but they need special handling in the schema definition.

```motoko
 let QuadrupleSchema = ZenDB.Schema.Quadruple(#Nat, #Text, #Nat, #Blob);

  let #ok(quadruples) = zendb.createCollection<Quadruple>(
      "quadruples",
      QuadrupleSchema,
      {
          from_blob = func(blob : Blob) : ?Quadruple = from_candid (blob);
          to_blob = func(c : Quadruple) : Blob = to_candid (c);
      },
      ?{
          schemaConstraints = [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Field("2", [#Min(1)]), #Field("3", [#MinSize(1)]), #Unique(["0"]), #Unique(["1"]), #Unique(["2"]), #Unique(["3"])];
      },
  ) else return assert false;

  let #ok(id) = quadruples.insert(ZenDB.Quadruple(42, "hello", 100, Blob.fromArray([0, 1, 2]))) else return assert false;
  assert quadruples.size() == 1;
  assert quadruples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42)))) == #ok([(0, ZenDB.Quadruple(42, "hello", 100, Blob.fromArray([0, 1, 2])))]);
  assert quadruples.get(id) == ?(ZenDB.Quadruple(42, "hello", 100, Blob.fromArray([0, 1, 2])));
  assert quadruples.get(id) == ?({
      _0_ = 42;
      _1_ = "hello";
      _2_ = 100;
      _3_ = Blob.fromArray([0, 1, 2]);
  });

  assert switch (quadruples.get(id)) {
      case (?q) ZenDB.fromQuadruple(q) == (42, "hello", 100, "\00\01\02");
      case (_) false;
  };
```

**Supported Tuple Types**
| Type                                                                                      | Description                 |
| ----------------------------------------------------------------------------------------- | --------------------------- |
| `ZenDB.Schema.Tuple(SchemaType1, SchemaType2)`                                            | A tuple with two elements   |
| `ZenDB.Schema.Triple(SchemaType1, SchemaType2, SchemaType3)`                              | A tuple with three elements |
| `ZenDB.Schema.Quadruple(SchemaType1, SchemaType2, SchemaType3, SchemaType4)`              | A tuple with four elements  |
| `ZenDB.Schema.Quintuple(SchemaType1, SchemaType2, SchemaType3, SchemaType4, SchemaType5)` | A tuple with five elements  |


### Schema Constraints
Schema constraints are used to define the structure of the documents that can be stored in a collection.

#### Schema Constraints Table
| Constraint          | Description                                                                       | Supported Types  | Example                                              |
| ------------------- | --------------------------------------------------------------------------------- | ---------------- | ---------------------------------------------------- |
| `#Unique([fields])` | Ensures that the field has a unique value across all documents in the collection  | All types        | `#Unique(["email"])`, `#Unique(["name", "address"])` |
| `#Max(value)`       | Ensures that the field value does not exceed the specified maximum value          | All Number types | `#Max(#Nat(100))`, `#Max(#Int(1000))`                |
| `#Min(value)`       | Ensures that the field value is not less than the specified minimum value         | All Number types | `#Min(#Nat(1))`, `#Min(#Int(-100))`                  |
| `#MaxSize(value)`   | Ensures that the field value does not exceed the specified maximum size in bytes  | `#Text`, `#Blob` | `#MaxSize(#Nat(256))`                                |
| `#MinSize(value)`   | Ensures that the field value is not less than the specified minimum size in bytes | `#Text`, `#Blob` | `#MinSize(#Nat(1))`                                  |



### Candify 
The Candify definition is a record that contains functions to serialize and deserialize the specified Motoko type for the collection.
The actual serialization is handled for us by `to_candid()` and `from_candid()`, we just need to give them a hand by manually specifying the Motoko type representation of our database schema.

For this we have a simple boilerplate that you should use when creating this record:
```motoko
let candify : ZenDB.Types.Candify<User> = {
  to_blob = func(motoko: User) : Blob { to_candid(motoko) };
  from_blob = func(candid: Blob) : ?User { from_candid(candid) };
};
```

To use this candify record for your own collection, all you need to do is replace `User` with the type for your schema. For example, if your collection stores only Text values then your candify function would be:
```motoko
let candify : ZenDB.Types.Candify<Text> = {
  to_blob = func(motoko: Text) : Blob { to_candid(motoko) };
  from_blob = func(candid: Blob) : ?Text { from_candid(candid) };
};
```

### Creating Collection
Once you have decided on the type of data you want to store in your collection, you will need to generate your ZenDB compatible schema and candify record. 
We are going to use the following Motoko record type with user information for our collection:

```motoko
type User = {
  id : Principal;
  name : Text;
  age : Nat;
};
```
To get the schema for this type, we look at the Schema Types available in the [schema table](#schema-table).
The top level type for the `User` is a `#Record` type with the entries: a `#Principal` with field 'id', a `#Text` with field 'name`, and a `#Nat` with field 'age'.

```motoko
let UserSchema = #Record([
  ("id", #Principal),
  ("name", #Text),
  ("age", #Nat)
]);
```

Take care to verify that your defined schema type explicitly matches the Motoko type, as the creation of the collection will fail if it doesn't. 

The candify record would just be the boilerplate from [earlier](#candify) with the `User` type. 

Now let's create our collection:
```motoko
let #ok(users_collection) = db.createCollection("unique_collection_name", UserSchema, candify, null);
```

### Inserting records into Collection
To insert a record into a collection, you use the `insert` method:

```motoko
let user : User = {
  id = Principal.fromText("2vxsx-fae");
  name = "Alice";
  age = 30;
};

let #ok(userId) = users_collection.insert(user);
```

This returns the unique ID assigned to the document, which you can use later to retrieve, update, or delete the record.

### Retrieving records from Collection

### Queries
> move query here

### Updating existing records
ZenDB provides powerful update capabilities through the `updateById` method:

```motoko
// Basic update
let #ok(_) = users_collection.updateById(userId, [
  ("name", #Text("Alice Smith")),
  ("age", #Nat(31))
]);

// Nested update
let #ok(_) = users_collection.updateById(userId, [
  ("profile.location", #Text("New York"))
]);

// Arithmetic operations
let #ok(_) = users_collection.updateById(userId, [
  ("age", #add(#currValue, #Nat(1))),  // Increment age
  ("login_count", #mul(#currValue, #Nat(2)))  // Double login count
]);
```

### Update Table
| Operation                         | Description                                                  | Supported Types                                         | Example                                                        |
| --------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------- | -------------------------------------------------------------- |
| `#currValue`                      | Represents the current value of the field being updated      | Supports all types                                      | `collection.update([("age", #add(#currValue, #Nat(1)))])`      |
| `#get(field)`                     | Gets the current value of the specified field                | Supports all types                                      | `#add(#get("age"), #Nat(1))`                                   |
| **Number Operations**             |                                                              |                                                         |                                                                |
| `#add(n1, n2)`                    | Adds two values                                              | Supports all Number types and update operations on them | `#add(#currValue, #Nat(1))`                                    |
| `#sub(n1, n2)`                    | Subtracts second value from first value                      | Supports all Number types and update operations on them | `#sub(#currValue, #Nat(1))`                                    |
| `#mul(n1, n2)`                    | Multiplies two values                                        | Supports all Number types and update operations on them | `#mul(#currValue, #Nat(2))`                                    |
| `#div(n1, n2)`                    | Divides first value by second value                          | Supports all Number types and update operations on them | `#div(#currValue, #Nat(2))`                                    |
| `#abs(n)`                         | Returns absolute value                                       | Supports all Number types and update operations on them | `#abs(#currValue)`                                             |
| `#neg(n)`                         | Returns negative value                                       | Supports all Number types and update operations on them | `#neg(#currValue)`                                             |
| `#floor(n)`                       | Returns floor value                                          | Supports Float and update operations on them            | `#floor(#currValue)`                                           |
| `#ceil(n)`                        | Returns ceiling value                                        | Supports Float and update operations on them            | `#ceil(#currValue)`                                            |
| `#sqrt(n)`                        | Returns square root                                          | Supports all Number types and update operations on them | `#sqrt(#currValue)`                                            |
| `#pow(base, exponent)`            | Returns base raised to the power of exponent                 | Supports all Number types and update operations on them | `#pow(#currValue, #Nat(2))`                                    |
| `#min(n1, n2)`                    | Returns minimum of two values                                | Supports all Number types and update operations on them | `#min(#currValue, #Nat(100))`                                  |
| `#max(n1, n2)`                    | Returns maximum of two values                                | Supports all Number types and update operations on them | `#max(#currValue, #Nat(0))`                                    |
| `#mod(n1, n2)`                    | Returns remainder of division                                | Supports all Number types and update operations on them | `#mod(#currValue, #Nat(10))`                                   |
| **Multi-Value Number Operations** |                                                              |                                                         |                                                                |
| `#addAll([n1, n2, ...])`          | Adds all values in the array                                 | Supports all Number types and update operations on them | `#addAll([#Nat(1), #Nat(2), #Nat(3)])`                         |
| `#subAll([n1, n2, ...])`          | Subtracts all values in the array from the first value       | Supports all Number types and update operations on them | `#subAll([#currValue, #Nat(1), #Nat(2)])`                      |
| `#mulAll([n1, n2, ...])`          | Multiplies all values in the array                           | Supports all Number types and update operations on them | `#mulAll([#currValue, #Nat(2), #Nat(3)])`                      |
| `#divAll([n1, n2, ...])`          | Divides first value by all subsequent values                 | Supports all Number types and update operations on them | `#divAll([#currValue, #Nat(2), #Nat(3)])`                      |
| **Text Operations**               |                                                              |                                                         |                                                                |
| `#concat(t1, t2)`                 | Concatenates two text values                                 | Supports `#Text` and `#Blob` types                      | `#concat(#currValue, #Text(" suffix"))`                        |
| `#concatAll([t1, t2, ...])`       | Concatenates all text values in the array                    | Supports `#Text` and `#Blob` types                      | `#concatAll([#Text("prefix "), #currValue, #Text(" suffix")])` |
| `#lowercase(t)`                   | Converts text to lowercase                                   | Supports only `#Text` type                              | `#lowercase(#currValue)`                                       |
| `#uppercase(t)`                   | Converts text to uppercase                                   | Supports only `#Text` type                              | `#uppercase(#currValue)`                                       |
| `#trim(t, chars)`                 | Trims specified characters from text                         | Supports only `#Text` type                              | `#trim(#currValue, " \t\n")`                                   |
| `#replaceSubText(t, old, new)`    | Replaces all occurrences of old substring with new substring | Supports only `#Text` type                              | `#replaceSubText(#currValue, "old", "new")`                    |
| `#slice(t, start, end)`           | Extracts substring from start to end position                | Supports only `#Text` type                              | `#slice(#currValue, 0, 5)`                                     |


### Deleting records from collection
To delete records, use one of the delete methods:

```motoko
// Delete by ID
let #ok(deletedUser) = users_collection.deleteById(userId);

// Delete many records matching criteria
let #ok(deletedUsers) = users_collection.deleteMany(
  ZenDB.QueryBuilder()
    .Where("age", #lt(#Nat(18)))
);
```

## Queries


ZenDB offers a powerful query system through its QueryBuilder API:

```motoko
// Basic query
let #ok(results) = users_collection.search(
  ZenDB.QueryBuilder()
    .Where("age", #gte(#Nat(21)))
    .Sort("name", #Ascending)
);

// Complex query with multiple conditions
let #ok(results) = users_collection.search(
  ZenDB.QueryBuilder()
    .Where("status", #eq(#Text("active")))
    .And("age", #between(#Nat(25), #Nat(50)))
    .OrQuery(
      ZenDB.QueryBuilder()
        .Where("role", #eq(#Text("admin")))
    )
    .Sort("last_login", #Descending)
    .Skip(20)
    .Limit(10)
);
```

Query execution is optimized by the internal query planner, which analyzes available indexes and query patterns to determine the most efficient execution path.

#### Query Operators

ZenDB provides a rich set of query operators to build expressive queries:

| Operator      | Description           | Example                                                         |
| ------------- | --------------------- | --------------------------------------------------------------- |
| `#eq`         | Equality              | `.Where("status", #eq(#Text("active")))`                        |
| `#lt`         | Less than             | `.Where("age", #lt(#Nat(30)))`                                  |
| `#gt`         | Greater than          | `.Where("score", #gt(#Nat(100)))`                               |
| `#lte`        | Less than or equal    | `.Where("priority", #lte(#Nat(3)))`                             |
| `#gte`        | Greater than or equal | `.Where("reputation", #gte(#Nat(500)))`                         |
| `#between`    | Range (inclusive)     | `.Where("age", #between(#Nat(18), #Nat(65)))`                   |
| `#exists`     | Field exists          | `.Where("profile.avatar", #exists)`                             |
| `#startsWith` | Text starts with      | `.Where("name", #startsWith(#Text("A")))`                       |
| `#anyOf`      | Value in set          | `.Where("status", #anyOf([#Text("active"), #Text("pending")]))` |
| `#not_`       | Negates operator      | `.Where("role", #not_(#eq(#Text("admin"))))`                    |

Logical operators allow combining conditions:

- `.And(field, operator)` - Field must match this condition AND previous conditions
- `.Or(field, operator)` - Field must match this condition OR previous conditions
- `.AndQuery(queryBuilder)` - Combines with another query using AND
- `.OrQuery(queryBuilder)` - Combines with another query using OR


## Index
An index is a data structure that improves the speed of data retrieval operations on a database table.
Indexes are created on fields in the schema and can be used to speed up queries.
Indexes can be created on any field in the schema but must be defined as a list of tuples.
The first element of the tuple is the field name and the second element is the order of the index. The order can be either `#Ascending` or `#Descending`.

```motoko
let #ok(_) = users_collection.createIndex("name_idx", [("name", #Ascending)], false);
```

Now searches for records with a specific name will be much faster as we have a sorted index that stores all the names of the records in the collection and does a quick binary search to retrieve matching records.

We also support multi-field indexes to allow indexing on multiple fields in a single index:
```motoko
let #ok(_) = users_collection.createIndex("name_age_idx", [("name", #Ascending), ("age", #Ascending)], false);
```

This type of index can help optimize sorting operations and queries that filter on both fields. The internal query planner will use these indexes automatically when they match your query patterns.

### Index Composite Fields Encoding Format