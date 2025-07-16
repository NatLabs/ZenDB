# ZenDB Documentation

## Quick Reference
- [Memory Types](#memory-types-heap-vs-stable-memory)
- [Schema Types](#schema-types) 
- [Schema Constraints](#schema-constraints)
- [Creating Collections](#creating-a-collection)
- [Document Operations](#document-operations)
- [Query Operators](#query-operators)
- [QueryBuilder Methods](#querybuilder-methods)
- [Update Operations](#update-operations)
- [Index Overview](#index-overview)
- [Collection Statistics](#collection-statistics)
- [Memory Statistics](#memory-statistics)

## Glossary

List of terms as they are used in this library.

| Term                                  | Description                                                                                                                                                                         |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ZenDB Instance**                    | The main database container that holds multiple databases. Ideally one instance per canister.                                                                                       |
| **Database**                          | A namespace containing multiple collections. Provides logical grouping of related collections.                                                                                      |
| **Collection**                        | Equivalent to a table in SQL databases. A group of documents with the same schema definition and type safety.                                                                        |
| **Document**                          | The smallest data entity in ZenDB. User-defined data that conforms to a collection's schema.                                                                                        |
| **Schema**                            | Type definition that matches Motoko types. Defines the structure, field types, and constraints for documents in a collection.                                                       |
| **Schema Constraints**                | Validation rules applied to collections and fields (e.g., unique constraints, min/max values, size limits).                                                                        |
| **Memory Types**                      | Storage options for collections: `#heap` (faster, 6GB limit) or `#stableMemory` (slower, 500GB limit, persists across upgrades).                                                  |
| **B-Tree**                            | The underlying data structure used for document storage and indexes. Provides sorted, efficient data access.                                                                        |
| **Index**                             | A B-Tree data structure that stores document fields in sorted order to accelerate query performance. Can be single-field or composite.                                             |
| **Composite Index**                   | Multi-field index that optimizes queries filtering or sorting on multiple fields simultaneously.                                                                                    |
| **Query**                             | A request for data from a collection using filtering, sorting, and pagination criteria.                                                                                             |
| **Query Builder**                     | A fluent interface for constructing complex queries with automatic logical grouping when switching between AND/OR operations.                                                       |
| **Logical Group**                     | Automatic bracketing of query conditions that occurs when switching between AND/OR operations in QueryBuilder.                                                                     |
| **Query Planner**                     | Internal component that analyzes queries and selects optimal indexes for execution.                                                                                                 |
| **Update Operations**                 | Functions for modifying document fields, including arithmetic operations (add, multiply), text operations (concat, trim), and field references.                                     |
| **Orchid**                            | ZenDB's custom binary encoding format for composite index keys, ensuring proper sort order and efficient B-tree operations.                                                        |
| **Candid**                            | The Interface Description Language (IDL) used by the Internet Computer for cross-language communication.                                                                            |
| **Candify**                           | Type definition containing functions to serialize/deserialize Motoko types to/from Candid binary format.                                                                            |
| **Stable Memory**                     | Internet Computer feature providing persistent storage across canister upgrades with larger capacity (up to 500GB).                                                                |
| **Stable Store**                      | Internal state representation that enables ZenDB components to persist across canister upgrades.                                                                                    |
| **Enhanced Orthogonal Persistence (EOP)** | A Beta feature in motoko that allows heap variables to be stored directly in stable memory while maintaining heap-like access patterns.                                                      |

## Getting Started

### ZenDB Instance 
A ZenDB instance is a collection of databases. You should typically create only one ZenDB instance per canister.

```motoko
let zendb = ZenDB.newStableStore(null);
```

### Memory Types: Heap vs Stable Memory
ZenDB supports two memory types: `#heap` and `#stableMemory`. As their names suggest, `#heap` stores your database collections and indexes in heap memory, while `#stableMemory` stores data in stable memory.

These memory types have different performance characteristics, as shown in these [benchmarks results](https://github.com/NatLabs/ZenDB/tree/benchmark-results). Heap memory requires 20-30% fewer instructions than stable memory in both read and write situations, resulting in heap memory being slightly more efficient.

By default, `#stableMemory` is selected for new ZenDB instances, but you can change this to heap memory:

```motoko
let zendb = ZenDB.newStableStore(?{
  ZenDB.defaultSettings with
  memory_type = #heap; // or #stableMemory;
});
```

**Heap memory** offers better performance by avoiding the overhead of stable memory operations. However, it's currently limited to 6GB, which restricts the amount of data you can store.

**Stable memory** doesn't have this space limitation and can store up to 500GB of data in a single canister, providing much more storage capacity.

> **Note:** Enhanced Orthogonal Persistence (EOP) is a beta feature that will allow heap variables to be stored directly in stable memory while maintaining heap-like performance. This will effectively remove the 6GB heap limitation.

### Canister Configuration
When using `#stableMemory`, you need to configure your canister's stable memory limit. Canisters have a default stable memory limit of 4GB, but you can increase this up to 500GB for larger datasets.

The limit is measured in pages, where each page is 64KiB. For example, to set a 200GB limit: `200 * (1024 * 1024) / 64 = 3276800` pages.

Configure this in your `dfx.json` file:

```json
  "canisters": {
    "stress-test" : {
      "type": "motoko",
      "main": "./tests/Stress.Test.mo",
      "args": "--max-stable-pages 3276800" 
    }
  },
```

### Error Handling
ZenDB follows a robust error handling philosophy: every foreseeable error returns a `Result` type, allowing you to handle errors gracefully. This minimizes the use of `Debug.trap()` which would terminate execution.

### Logging
ZenDB provides extensive logging to help you debug issues. The library uses standard log levels from lowest to highest priority: `#Debug`, `#Info`, `#Warn`, `#Error`, `#Trap`.

By default, the minimum log level is set to `#Warn` (the lowest level that typically requires user action). You can adjust this at any time:

```motoko
let zendb = ZenDB.newStableStore(null);
ZenDB.setLogLevel(zendb, #Info);
```

> **Warning:** Setting the log level to `#Debug` may generate excessive output, as this level produces significantly more logs than others.

All ZenDB logs start with `ZenDB` and the function name for easy identification. For example:
```
[ERROR] ZenDB.insert(): Insertion of document value #Record([("name", "James")]) failed because another document with id = 72, has the same value for field 'name' -> "James"
```

## Database Management

### Database
A database serves as a namespace for organizing related collections. When you instantiate a ZenDB instance, a default database is automatically created and can be accessed via the `launchDefaultDB()` method:

```motoko
let db = ZenDB.launchDefaultDB(zendb);
```

While you'll typically use only one database per canister, you can create additional databases for specific use cases by calling `createDB()` with a unique name:

```motoko
let #ok(cycles_db) = ZenDB.createDB(zendb, "cycles");
```

You can retrieve an existing database using `getDB()` with the database's name:

```motoko
let #ok(cycles_db) = ZenDB.getDB(zendb, "cycles");
```

## Schema Definition

### Schema Types
A schema defines the structure and types of data in your collection. It must match the Motoko type you want to store and consists of a combination of the following types:

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
| #Record([("field_name", SchemaType)])       | A document with named fields  |
| #Variant([("variant_name", SchemaType)])    | A variant with named fields |

Your schema definitions must exactly match the Motoko types you're storing. For examples of complex schemas and documents, see [Documents.Test.mo](./tests/Documents.Test.mo).

#### Tuple Schema Types
We support tuple schema types, but they need special handling as they are not directly supported by Candid. Instead, tuples are represented as numbered fields in Candid, which can be accessed by their index. As a result of the tuple being represented as a record, the way it's accessed changes from the usual `tuple.0` to `tuple._0_`. 
We have provided a few helpers for the most common tuple types to make it easier to work with them:
- `ZenDB.Schema.Tuple(SchemaType1, SchemaType2)` - The type to use when defining a tuple with two elements.
- `ZenDB.Tuple<Type1, Type2>` - The tuple type represented as a record with numbered fields.
- `ZenDB.Tuple(Value1, Value2)` - A function to create a tuple type with two elements in the supported record format.
- `ZenDB.fromTuple(Tuple)` - A function to convert the record representation of a tuple back to the motoko tuple type.

These helpers are also available for tuples with more than two elements. You can access them by replacing the `Tuple` name with `Triple`, `Quadruple`, or `Quintuple` as needed.

Here's an example of how to use the tuple schema types in a collection:

```motoko
    // Tuples are converted to documents in Candid
    // They become documents with numbered fields, that can be accessed by their index
    // e.g. (Nat, Text) becomes { _0_ : Nat; _1_ : Text }
    //
    // ZenDB provides helpers for the most common tuple types

    type Tuple = ZenDB.Types.Tuple<Nat, Text>;

    let TupleSchema = ZenDB.Schema.Tuple(#Nat, #Text);

    let candify : ZenDB.Types.Candify<Tuple> = {
        from_blob = func(blob : Blob) : ?Tuple = from_candid (blob);
        to_blob = func(c : Tuple) : Blob = to_candid (c);
    };

    let #ok(tuples) = zendb.createCollection<Tuple>(
        "tuples",
        TupleSchema,
        candify,
        ?{
            schemaConstraints = [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Unique(["0"]), #Unique(["1"])];
        },
    ) else return assert false;

    let #ok(id) = tuples.insert(ZenDB.Tuple(42, "hello")) else return assert false;
    assert tuples.size() == 1;
    assert tuples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42)))) == #ok([(0, ZenDB.Tuple(42, "hello"))]);
    assert tuples.get(id) == ?(ZenDB.Tuple(42, "hello"));
    assert tuples.get(id) == ?({ _0_ = 42; _1_ = "hello" });
    assert switch (tuples.get(id)) {
        case (?t) ZenDB.fromTuple(t) == (42, "hello");
        case (_) false;
    };
```

### Candify 
The Candify definition contains functions to serialize and deserialize your Motoko types for the collection. While the actual serialization is handled by `to_candid()` and `from_candid()`, you need to specify the Motoko type representation for your schema.

Use this boilerplate when creating your Candify definition:

```motoko
let candify : ZenDB.Types.Candify<User> = {
  to_blob = func(motoko: User) : Blob { to_candid(motoko) };
  from_blob = func(candid: Blob) : ?User { from_candid(candid) };
};
```

Simply replace `User` with your collection's type. For example, if your collection stores Text values:

```motoko
let candify : ZenDB.Types.Candify<Text> = {
  to_blob = func(motoko: Text) : Blob { to_candid(motoko) };
  from_blob = func(candid: Blob) : ?Text { from_candid(candid) };
};
```

### Schema Constraints
Schema constraints allow you to define rules and restrictions on the values stored in your collections. These constraints are specified in the `schemaConstraints` field when creating a collection and are automatically enforced during insert and update operations.

#### Collection-Level Constraints
| Constraint          | Description                                                                       | Supported Types  | Example                                              |
| ------------------- | --------------------------------------------------------------------------------- | ---------------- | ---------------------------------------------------- |
| `#Unique([fields])` | Ensures that the specified field(s) have unique values across all documents in the collection | All types        | `#Unique(["email"])`, `#Unique(["name", "address"])` |

#### Field-Level Constraints
Use `#Field(field, constraints)` to apply specific validation rules to individual fields:

| Constraint          | Description                                                                       | Supported Types  | Example                                              |
| ------------------- | --------------------------------------------------------------------------------- | ---------------- | ---------------------------------------------------- |
| `#Max(value)`       | Sets a maximum value limit for the field                                         | All Number types | `#Max(#Nat(100))`, `#Max(#Int(1000))`                |
| `#Min(value)`       | Sets a minimum value limit for the field                                         | All Number types | `#Min(#Nat(1))`, `#Min(#Int(-100))`                  |
| `#MaxSize(value)`   | Sets a maximum size limit in bytes for the field content                         | `#Text`, `#Blob` | `#MaxSize(#Nat(256))`                                |
| `#MinSize(value)`   | Sets a minimum size limit in bytes for the field content                         | `#Text`, `#Blob` | `#MinSize(#Nat(1))`                                  |

#### Example Usage
```motoko
let #ok(users_collection) = db.createCollection(
  "users",
  UserSchema,
  candify,
  ?{
    schemaConstraints = [
      #Unique(["email"]),                    // Email must be unique
      #Unique(["username"]),                 // Username must be unique  
      #Field("age", [#Min(#Nat(0)), #Max(#Nat(120))])   // Age between 0-120
      #Field("bio", [#MaxSize(#Nat(500))])   // Bio max 500 bytes
    ];
  }
);
```

## Collection Management

### Collection Overview
A collection is equivalent to a table in SQL databases - it's a group of documents that share the same schema definition.

#### Key Characteristics
- **Single type per collection**: Each collection stores only one Motoko type, following Motoko's strict typing system
- **Schema immutability**: Once created, schema fields cannot be changed to completely different primitive types. While not supported yet, once upgrades are shipped in a future release, only backward-compatible upgrades will be allowed.
- **Automatic ID assignment**: Each document receives a unique ID when inserted
- **B-Tree storage**: Collections use B-Tree structures with document IDs as keys and Candid-encoded documents as values
- **Duplicate handling**: Two identical documents can be stored in a collection without issues, unless a unique constraint is defined on a field. In that case, the second document with the same value will fail to insert.
 

#### Size Limitations
- Maximum document size: 4GB
- Maximum indexed field size: 64KB (including composite fields)

### Creating a Collection
To create a collection, you need to define both the schema and Candify configuration for your data type. Let's walk through creating a collection for user information:

First, define your Motoko type:
```motoko
type User = {
  id : Principal;
  name : Text;
  age : Nat;
};
```

Next, create the corresponding schema using the [schema types](#schema-types). The `User` type becomes a `#Record` with three fields:

```motoko
let UserSchema = #Record([
  ("id", #Principal),
  ("name", #Text),
  ("age", #Nat)
]);
```

**Important**: Verify that your schema exactly matches your Motoko type, as collection creation will fail if they don't match.

Create the Candify definition using the [boilerplate](#candify) with your `User` type:

```motoko
let candify : ZenDB.Types.Candify<User> = {
  to_blob = func(motoko: User) : Blob { to_candid(motoko) };
  from_blob = func(candid: Blob) : ?User { from_candid(candid) };
};
```

Finally, create your collection:
```motoko
let #ok(users_collection) = db.createCollection("users", UserSchema, candify, null);
```

### Document Operations

#### Inserting Documents
To insert a document into a collection, you use the `insert` method:

```motoko
let user : User = {
  id = Principal.fromText("2vxsx-fae");
  name = "Alice";
  age = 30;
};

let #ok(userId) = users_collection.insert(user);
```

This returns the unique ID assigned to the document, which you can use later to retrieve, update, or delete the document.

#### Retrieving Documents
Given a collection and the document ID, you can retrieve a document using the `get` method:

```motoko
let #ok(user) = users_collection.get(userId);

assert user == ?{
  id = Principal.fromText("2vxsx-fae");
  name = "Alice";
  age = 30;
};
```

#### Updating Documents
ZenDB provides powerful update capabilities through two methods:
- `updateById()` - Updates a specific document by its ID
- `update()` - Updates multiple documents matching query criteria

```motoko
// Basic field updates
let #ok(_) = users_collection.updateById(userId, [
  ("name", #Text("Alice Smith")),
  ("age", #Nat(31))
]);

// Nested field updates using dot notation
let #ok(_) = users_collection.updateById(userId, [
  ("profile.location", #Text("New York"))
]);

// Arithmetic operations on multiple documents
let #ok(_) = users_collection.update(
  ZenDB.QueryBuilder().Where("status", #eq(#Text("active"))),
  [
    ("age", #add(#currValue, #Nat(1))),           // Increment age
    ("api_limits", #mul(#currValue, #Nat(2)))     // Double API limits
  ]
);
```

##### Update Operations
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

#### Deleting Documents
To delete documents, use one of the delete methods:

```motoko
// Delete by ID
let #ok(deletedUser) = users_collection.deleteById(userId);

// Delete many documents matching criteria
let #ok(deletedUsers) = users_collection.deleteMany(
  ZenDB.QueryBuilder()
    .Where("age", #lt(#Nat(18)))
);
```

## Querying

### Query System Overview
ZenDB provides a powerful and flexible query system through its QueryBuilder API. You can construct simple or complex queries to search for documents based on various criteria, with automatic query optimization for performance.

#### Basic Query Example
```motoko
let #ok(results) = users_collection.search(
  ZenDB.QueryBuilder()
    .Where("age", #gt(#Nat(18)))              // Find users older than 18
    .And("name", #startsWith(#Text("A")))     // AND whose name starts with "A"
    .Sort("age", #Ascending)                  // Sort by age in ascending order
    .Limit(10)                                // Return maximum 10 results
);
```

#### Advanced Query Example
```motoko
let #ok(results) = users_collection.search(
  ZenDB.QueryBuilder()
    .Where("status", #eq(#Text("active")))
    .And("age", #between(#Nat(25), #Nat(50)))
    .OrQuery(
      ZenDB.QueryBuilder()
        .Where("role", #eq(#Text("admin")))
        .Or("permissions", #anyOf([#Text("write"), #Text("admin")]))
    )
    .Sort("last_login", #Descending)
    .Skip(20)
    .Limit(10)
);
```

The `search` method returns a list of tuples containing the document ID and the document itself: `[(DocumentId, Document)]`. Query execution is automatically optimized by the internal query planner, which analyzes available indexes and determines the most efficient execution path.

#### Nested Field Queries
ZenDB also supports querying nested fields using dot notation:

```motoko
let #ok(results) = users_collection.search(
  ZenDB.QueryBuilder()
    .Where("profile.address.city", #eq(#Text("New York")))
    .And("profile.preferences.notifications", #eq(#Bool(true)))
);
```

### QueryBuilder Methods

The QueryBuilder provides condition methods that you chain together to build complex queries. When you switch between AND and OR operations during chaining, ZenDB automatically organizes your conditions into logical groups to ensure proper query evaluation.

**Logical Groups:** A logical group is a set of conditions that are evaluated together, similar to parentheses in mathematical expressions. 

#### Condition Methods
- `.Where(field, operator)` - Sets the initial condition for the query
- `.And(field, operator)` - Applies AND to the current logical group
- `.Or(field, operator)` - Applies OR to the current logical group
- `.AndQuery(queryBuilder)` - Combines with another complete query using AND logic
- `.OrQuery(queryBuilder)` - Combines with another complete query using OR logic

```motoko
// Example 1: AND then OR
ZenDB.QueryBuilder()
  .Where("age", #gt(#Nat(18)))      // age > 18
  .And("status", #eq(#Text("active"))) // AND status = "active"  
  .Or("role", #eq(#Text("admin")))     // OR role = "admin"
// Becomes: (age > 18 AND status = "active") OR (role = "admin")

// Example 2: OR then AND
ZenDB.QueryBuilder()
  .Where("role", #eq(#Text("admin")))  // role = "admin"
  .Or("role", #eq(#Text("moderator"))) // OR role = "moderator"
  .And("status", #eq(#Text("active"))) // AND status = "active"
// Becomes: (role = "admin" OR role = "moderator") AND (status = "active")
```

#### Result Control Methods
- `.Sort(field, #Ascending | #Descending)` - Sorts results by the specified field
- `.Limit(count)` - Limits the number of results returned
- `.Skip(count)` - Skips the first N results (useful for pagination)

### Query Operators

| Operator      | Description                    | Supported Types  | Example                                                         |
| ------------- | ------------------------------ | ---------------- | --------------------------------------------------------------- |
| `#eq`         | Exact equality match           | All types        | `.Where("status", #eq(#Text("active")))`                        |
| `#lt`         | Less than                      | Number types     | `.Where("age", #lt(#Nat(30)))`                                  |
| `#gt`         | Greater than                   | Number types     | `.Where("score", #gt(#Nat(100)))`                               |
| `#lte`        | Less than or equal to          | Number types     | `.Where("priority", #lte(#Nat(3)))`                             |
| `#gte`        | Greater than or equal to       | Number types     | `.Where("reputation", #gte(#Nat(500)))`                         |
| `#between`    | Range (inclusive on both ends) | Number types     | `.Where("age", #between(#Nat(18), #Nat(65)))`                   |
| `#exists`     | Field exists and is not null   | All types        | `.Where("profile.avatar", #exists)`                             |
| `#startsWith` | Text starts with substring     | `#Text` only     | `.Where("name", #startsWith(#Text("John")))`                    |
| `#anyOf`      | Value matches any in the list  | All types        | `.Where("status", #anyOf([#Text("active"), #Text("pending")]))` |
| `#not_`       | Negates any other operator     | All types        | `.Where("role", #not_(#eq(#Text("admin"))))`                    |

## Indexing

### Index Overview
Indexes are data structures that store document fields in sorted order, enabling faster data retrieval during queries. They're essential for optimizing database performance.

You can create indexes on any field in your schema by defining them as tuples, where the first element is the field name and the second is the sort order (`#Ascending` or `#Descending`):

```motoko
let #ok(_) = users_collection.createIndex("name_idx", [("name", #Ascending)], false);
```

This creates a sorted index containing all document names, allowing quick binary searches when querying by name.

### Index Characteristics
- **B-Tree structure**: Indexes use B-Trees with document fields as keys and document IDs as values
- **Field support**: Works with any primitive field, including nested fields using dot notation
- **Uniqueness options**: Can be unique (no duplicate values) or non-unique (allowing duplicates)
- **Internal usage**: ZenDB automatically creates unique indexes to enforce schema constraints

### Composite Indexes
You can create multi-field indexes to optimize queries that filter or sort on multiple fields:

```motoko
let #ok(_) = users_collection.createIndex("name_age_idx", [("name", #Ascending), ("age", #Ascending)], false);
```

The query planner automatically uses these indexes when they match your query patterns, optimizing both filtering and sorting operations.

### Index Encoding Format

ZenDB uses a custom binary encoding format, defined in the "[Orchid](./src/Collection/Orchid.mo)" module, to store composite index keys efficiently in B-tree structures. This encoding ensures proper sorting order while maintaining compact storage.

#### Encoding Structure

Each composite index key is encoded as a concatenation of:
1. **Field count** - Single byte indicating the number of fields in the composite key
2. **Field encodings** - Sequential encoding of each field value with its type information

The complete key becomes: `[count_byte][field1_encoding][field2_encoding]...[fieldN_encoding]`

*Note: All multi-byte sequences (sizes, numeric values) are stored in big-endian format.*

#### Type Codes and Encoding

| Type Code | Type | Encoding Format |
|-----------|------|----------------|
| 1 | Minimum | Type code only (query-only, not stored in indexes) |
| 2 | Null | Type code only |
| 3 | Empty | Type code only |
| 4 | Bool | Type code + 1 byte (0 or 1) |
| 5-8 | Nat8/Nat16/Nat32/Nat64 | Type code + big-endian bytes |
| 9-12 | Int8/Int16/Int32/Int64 | Type code + big-endian encoding |
| 13 | Nat (as Nat64) | Type code + 8 bytes big-endian |
| 14 | Int (as Int64) | Type code + 8 bytes big-endian |
| 15 | Float | Type code + 8 bytes big-endian |
| 16 | Principal | Type code + size byte + principal bytes |
| 17 | Text | Type code + 2-byte big-endian size + UTF-8 bytes |
| 18 | Blob | Type code + 2-byte big-endian size + blob bytes |
| 19 | Option | Type code + encoded inner value |
| 255 | Maximum | Type code only (query-only, not stored in indexes) |

#### Sorting Properties

The encoding ensures lexicographic byte comparison produces correct sort order:
- **Direct binary comparison**: Each field can be compared directly in its binary format without deserializing, enabling efficient B-tree operations
- **Type precedence**: Minimum < Null < regular types < Maximum  
- **Text/Blob**: Size-prefixed with big-endian length encoding followed by lexicographic byte comparison
- **Composite keys**: Field-by-field comparison proceeds left to right until a difference is found
- **Query boundaries**: Minimum and Maximum type codes are used only in search queries, and are not stored as index entries.

#### Example

For an index with fields `[("status", #Text), ("age", #Nat)]` and values `("active", 25)`:

```
[0x02]           // 2 fields
[0x11]           // Text type code
[0x00, 0x06]     // Text size (6 bytes) in big-endian
[0x61, 0x63, 0x74, 0x69, 0x76, 0x65]  // "active" in UTF-8
[0x0D]           // Nat type code  
[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19]  // 25 as big-endian Nat64
```

This binary encoding allows efficient B-tree operations while maintaining proper sort order for all supported data types.

## Statistics and Monitoring

### Collection Statistics

ZenDB provides detailed statistics about your collections to help you monitor performance, memory usage, and optimize your database operations. Use the `stats()` method to get comprehensive information about a collection's current state.

#### Usage

```motoko
let stats = users_collection.stats();
```

#### CollectionStats Structure

The `stats()` method returns a `CollectionStats` record containing:

##### Collection Information
- **`name`** - The collection's name
- **`schema`** - The collection's schema definition
- **`entries`** - Total number of documents in the collection
- **`memoryType`** - Memory type used (`#heap` or `#stableMemory`)

##### Document Storage Metrics
- **`memory`** - [Memory statistics](#memory-statistics) for the main document storage B-tree
- **`avgDocumentIdSize`** - Average size of document IDs in bytes
- **`totalDocumentIdSize`** - Total memory used by all document IDs
- **`avgDocumentSize`** - Average size of documents in bytes  
- **`totalDocumentSize`** - Total memory used by all documents

##### Index Statistics
- **`indexes`** - Array of [IndexStats](#indexstats-structure) for each index in the collection

#### IndexStats Structure

Each index provides detailed statistics:

##### Index Information
- **`name`** - The index name
- **`fields`** - Array of indexed field definitions with sort order
- **`entries`** - Number of entries in the index
- **`isUnique`** - Whether the index enforces uniqueness
- **`usedInternally`** - Whether the index is used internally by ZenDB

##### Index Memory Metrics
- **`memory`** - [Memory statistics](#memory-statistics) for the index B-tree
- **`avgIndexKeySize`** - Average size of composite index keys in bytes
- **`totalIndexKeySize`** - Total memory used by index keys
- **`avgDocumentIdSize`** - Average size of document ID values in bytes
- **`totalDocumentIdSize`** - Total memory used by document ID references

### Memory Statistics

### Memory Statistics

Memory statistics provide detailed information about memory usage in stable memory collections:

#### Memory Fields

- **allocatedPages**: Total memory pages allocated to the B-tree
- **bytesPerPage**: Size of each memory page (always 65,536 bytes)  
- **allocatedBytes**: Total bytes available from allocated pages
- **usedBytes**: Bytes currently in use by the B-tree
- **freeBytes**: Allocated but unused bytes (reserved for future use, cannot be deallocated)
- **dataBytes**: Total bytes used for storing actual data (keys + values)
- **metadataBytes**: Bytes used for B-tree structure (nodes, pointers, headers)
- **leafBytes**: Memory used by leaf node structures (excluding key/value data)
- **branchBytes**: Memory used by branch node structures
- **keyBytes**: Total memory used for storing all keys
- **valueBytes**: Total memory used for storing all values
- **leafCount**: Number of leaf nodes in the B-tree
- **branchCount**: Number of branch (internal) nodes in the B-tree
- **totalNodeCount**: Total number of nodes (leaves + branches)

> **Note:** For heap-based collections, all memory metrics will be zero as memory tracking is not supported for heap storage.

### Performance Monitoring

Use collection statistics to:
- **Monitor growth**: Track document count and memory usage over time
- **Optimize indexes**: Identify unused or oversized indexes  
- **Plan capacity**: Estimate memory requirements for scaling
- **Debug performance**: Analyze index efficiency and document sizes
- **Memory management**: Compare heap vs stable memory usage patterns

### Best Practices

1. **Regular monitoring**: Check stats periodically in production
2. **Index efficiency**: Remove unused indexes to save memory
3. **Document optimization**: Monitor average document size for schema efficiency
4. **Memory planning**: Use total memory metrics for capacity planning
5. **Index selection**: Compare index memory costs vs query performance benefits
