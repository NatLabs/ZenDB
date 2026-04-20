# ZenDB: Document Database for the Internet Computer

[![MOPS](https://img.shields.io/badge/MOPS-zendb-blue)](https://mops.one/zendb)
[![License](https://img.shields.io/badge/License-AGPL--3.0-blue)](LICENSE)
[![Motoko](https://img.shields.io/badge/Language-Motoko-orange)](https://github.com/dfinity/motoko)


**ZenDB** is a document database for Motoko that leverages stable memory to store and query large datasets efficiently. It provides a familiar document-oriented API, similar to MongoDB, allowing developers to store nested records, create collections, define indexes and perform complex queries on their data.

### Key Features
- **Indexes**: Support for multi-field indexes to speed up complex queries
- **Full Candid Integration**: Native support for Candid that allows you to insert and retrieve any motoko data type without creating custom serialization or type mappings
- **Rich Query Language And Query Builder**: Comprehensive set of operators including equality, range, and logical operations with an intuitive fluent API for building queries
- **Query Execution Engine**: Performance optimized query planner programmed to search for the best execution path that minimizes in-memory data processing.
- **Pagination**: Supports both offset and cursor pagination
- **Partial Field Updates**: Update any nested field without having to re-insert the entire document
- **Schema Validation And Constraints**: Add restrictions on what specific values can be stored in each collection


**Important Note**: Currently, no automatic indexes are created for your data. You'll need to create your own indexes when defining your collection and schema. If no indexes exist that can satisfy a query, ZenDB will run a full collection scan, which is likely to hit the instruction limit for a dataset with as little as ten thousand records.


## Getting Started

### Installation

#### Setup the CLI & Create a remote canister

- Install the [ZenDB CLI](https://www.npmjs.com/package/zendb-cli)
  ```bash
  npm install -g zendb-cli
  ```

- Setup the user identity by importing from dfx.
  ```bash
  zendb user import --data "$(dfx identity export <identity-name>)" --mode keyring
  ```

- create a canister 

  ```bash
  zendb canister create dev --ic --release latest

  # requires dfx running via `dfx start`
  zendb canister create local-dev --local --release latest 
  ```

- create a database and collection 
  ```bash
  zendb db create myapp --canister dev
  zendb collection create users --canister dev --db myapp \
    --schema 'record {id: nat; name: text; email: text; profile: record {age: opt nat; location: text; interests: vec text}; created_at: int; updated_at: int;}'
  ```

  <details>
    <summary>Equivalent Motoko type for the collection schema</summary>

    ```motoko
    type User = {
      id: Nat;
      name: Text;
      email: Text;
      profile: {
        age: ?Nat;
        location: Text;
        interests: [Text];
        bio: Text;
      };
      created_at: Int;
      updated_at: Int;
    };
    ```
  </details>

#### Connect to the remote canister from your motoko code
- Install the ZenDB motoko package

  ```bash
  mops add zendb
  ```

- Get the remote canister id 
  ```bash
  zendb canister list
  ```

- Connect to the database in your motoko code. The candify function handles the conversion between your motoko type and the candid blob format used by ZenDB, allowing you to work with native motoko types without needing to manually serialize/deserialize them.

  ```motoko
  import ZenDB "mo:zendb";

  let zendbClient = ZenDB.Client("<canister-id>");
  let myappDB = zendbClient.getDB("myapp");

  let candify : ZenDB.Types.Candify<User> = {
    from_blob = func(blob : Blob) : ?User = from_candid(blob);
    to_blob = func(value : User) : Blob = to_candid(value);
  };

  let users = myappDB.getCollection("users", candify);
  ```

### Basic Usage

#### Insert a document

- Each document returns a unique DocumentId that can be used to retrieve or update the document later.
  ```motoko

  let alice : User = {
    id = 1;
    name = "Alice";
    email = "alice@example.com";
    profile = {
      age = ?35;
      location = "San Francisco";
      interests = ["hiking", "photography"];
      bio = "Software engineer and avid traveler.";
    };
    created_at = Time.now();
    updated_at = Time.now();
  };

  let #ok(aliceId) = users.insert(alice);
  assert (await users.get(aliceId)) == ?alice;
  ```

#### Creating Indexes

- Composite Indexes

  ```motoko
  // Create composite indexes for your query patterns
  let #ok(_) = users.createIndex("name_idx", [("name", #Ascending)], null);

  let #ok(_) = users.createIndex(
    "location_created_at_idx", 
    [
      ("profile.location", #Ascending), 
      ("created_at", #Descending)
    ],
    null
  );
  ```

- Text Index 

  ```motoko

  let #ok(_) = users.createTextIndex("text_idx", ["name", "email", "bio"]);

  ```

- via the cli
  ```bash
  zendb index create name_idx --canister dev --db myapp --collection users --fields "name"
  zendb index create location_created_at_idx --canister dev --db myapp --collection users --fields "profile.location:asc" "created_at:desc"
  zendb index create-text text_idx --canister dev --db myapp --collection users --fields name,email,bio
  ```

#### Search For Documents

```motoko

let #ok(userId) = users.insert(user);

// Query with the QueryBuilder API
let #ok(queryResults) = users.search(
  ZenDB.QueryBuilder()
    .Where("profile.location", #eq(#Text("San Francisco")))
    .And("profile.age", #gte(#Nat(30)))
);

// Search returns a SearchResult with documents and instruction count
assert queryResults.documents == [(userId, user)];
```

```motoko

let #ok(queryResults2) = users.search(
  ZenDB.QueryBuilder()
    .Where("bio", #text(#startsWith("travel")))
);

Debug.print(debug_show(queryResults2.documents)); // Should include Alice

```

#### Field Updates & Transformations

- Update specific fields in a document
  ```motoko
  let #ok(_) = users.updateById(
    userId,
    [
      ("profile.location", #Text("New York")),
      ("profile.interests", #Array([#Text("coding"), #Text("reading")]))
    ]
  );

  let ?updatedUser1 = users.get(userId);

  assert updatedUser1.profile.location == "New York";
  assert updatedUser1.profile.interests == ["coding", "reading"];

  ```

- Update multiple fields referencing the current value of a field
  
  ```motoko
  let currTime = Time.now();
  let #ok(_) = users.update(
    ZenDB.QueryBuilder().Where("email", #eq(#Text(""))),
    [
      ("name", #uppercase(#currValue)),
      ("age", #add(#currValue, #Nat(1))),
      ("updated_at", #Int(currTime)),
      ("email", #lowercase(
        #concatAll([
          #concat(#get("name"), #Text("-in-")),
          #replaceSubText(#get("profile.location"), " ", "-"),
          #Text("@example.com")
        ])
      ))
    ]
  );

  let updatedUser2 = users.get(userId);

  assert updatedUser2.name == "ALICE";
  assert updatedUser2.age == ?36;
  assert updatedUser2.updated_at == currTime;
  assert updatedUser2.email == "alice-in-san-francisco@example.com";

  ```

#### Statistics & Monitoring

Monitor your collections to understand performance characteristics:

```motoko
let stats = users.stats();
```


### Advanced Usage

#### Compound Filtering with Logical Operators

```motoko
// Find active premium users who joined recently
let #ok(search_result) = users.search(
  ZenDB.QueryBuilder()
    .Where("status", #eq(#Text("active")))
    .And("account_type", #eq(#Text("premium")))
    .And("joined_date", #gte(#Int(oneWeekAgo)))
    .SortBy("activity_score", #Descending)
    .Limit(25)
);

let activeRecentPremiumUsers = search_result.documents;

// Find users matching any of several criteria
let #ok(search_result2) = users.search(
  ZenDB.QueryBuilder()
    .Where("role", #eq(#Text("admin")))
    .OrQuery(
      ZenDB.QueryBuilder()
        .Where("subscription_tier", #eq(#Text("enterprise")))
        .And("usage", #gte(#Nat(highUsageThreshold)))
    )
);
```

### Examples

The repository includes several examples demonstrating ZenDB's capabilities:

- [**Simple Notes Dapp**](./example/notes/lib.mo#L9): Example Notes app, with simple CRUD operations
- [**ICP Txs explorer**](https://2yfll-4qaaa-aaaap-anvaq-cai.icp0.io/): ZenDB test app that indexes ICP transactions
  - https://2yfll-4qaaa-aaaap-anvaq-cai.icp0.io/
- [**Flying Ninja**](./example/flying_ninja/backend/app.mo): Dapp from the [dfinity/examples](https://github.com/dfinity/examples/tree/master/motoko) repo ported to use ZenDB.


For more detailed examples and advanced usage, see the [**Complete Documentation**](./zendb-doc.md).

## Documentation

**[ZenDB Documentation](https://github.com/NatLabs/ZenDB/blob/main/zendb-doc.md)** - Comprehensive guide covering:

- **Getting Started**: ZenDB instances, memory types, configuration
- **Schema Definition**: Type system, constraints, validation  
- **Collection Management**: Creating collections, CRUD operations
- **Advanced Querying**: QueryBuilder API, logical grouping, operators
- **Indexing**: B-Tree indexes, composite indexes, Orchid encoding
- **Performance**: Query optimization, index selection strategies
- **Monitoring**: Collection statistics, memory usage

Related tools and client libraries:

- **ZenDB CLI**: https://www.npmjs.com/package/zendb-cli
- **ZenDB TypeScript Client**: https://www.npmjs.com/package/zendb-client

The documentation includes detailed examples, performance optimization tips, and best practices to help you get the most out of ZenDB.


## Performance Optimization

### Index Selection Strategy

ZenDB uses a sophisticated query planner to determine the most efficient indexes for each query. 
To get the best performance from ZenDB, create indexes that:

1. Match your most common query patterns
2. Include fields used in sorting operations
3. Support your filtering operations (equality, range conditions)

#### Composite Index Field Ordering

For optimal query performance, order fields in composite indexes by priority:
1. **Equality filters** - Fields with exact matches (`#eq`) come first
2. **Sort fields** - Fields used for ordering results come second  
3. **Range filters** - Fields with range queries (`#gt`, `#lt`, `#between`) come last

This ordering is crucial because ZenDB stores composite indexes as concatenated keys in a B-tree structure. When equality filters come first, the query engine can combine these conditions into a single key prefix for efficient B-tree scanning. This allows the system to quickly narrow down to the smallest possible result set before applying range operations. If range fields were placed first, the index couldn't be fully utilized since range operations break the prefix matching pattern, forcing expensive full index scans.

Example query using ZenDB QueryBuilder:
```motoko
let #ok(results) = users.search(
  ZenDB.QueryBuilder()
    .Where("age", #gt(#Nat(18)))                  // Range filter  
    .And("status", #eq(#Text("active")))          // Equality filter
    .SortBy("created_at", #Descending)              // Sort operation
);
```

Optimal index for this query:
```motoko
let #ok(_) = users.createIndex(
  "status_date_age_idx", 
  [
    ("status", #Ascending),       // High selectivity field first
    ("created_at", #Descending),  // Sort field for efficient ordering
    ("age", #Ascending)           // Range field with lower selectivity
  ]
);
```

### Search Result Type

The `search()` method returns a `Result<SearchResult<Record>, Text>` where `SearchResult` contains:
- **`documents`**: An array of tuples `[(DocumentId, Document)]` - the document ID and the document itself
- **`instructions`**: The number of instructions used to execute the query

```motoko
let #ok(search_result) = users.search(query);
let documents = search_result.documents;
let instructions_used = search_result.instructions;
```

This allows you to monitor query performance and access both the results and execution metrics.

## Limitations

- Limited array support - Can store arrays in collections, but cannot create indexes on array fields or perform operations on specific array elements. In addition, indexes cannot be created on fields nested within an `#Array`.

- Complex queries with many OR conditions may have suboptimal performance

- The query planner may not always select the optimal index for complex queries. It is recommended to analyze query performance and adjust indexes accordingly.

- Schema updates and migrations not yet supported. As a result, changing the schema of an existing collection requires creating a new collection and migrating the data manually.

- Using Limit/Skip Pagination can be inefficient and may hit the instruction limits if the result set is too large. It is recommended to create indexes that fully cover your queries where possible, to avoid this limitation.

- **Index Type Range Limitations**: When indexing `#Nat` or `#Int` types, values are stored using 64-bit encoding:
  - `#Nat` values must be ≤ 2^64-1 
  - `#Int` values must be within Int64 range: -2^63 to 2^63-1
  - Attempting to index values outside these ranges will trap during encoding


## Roadmap

- [x] Multi-field compound indexes
- [x] Powerful query language with logical operators
- [x] Schema validation and Schema constraints
- [x] Full Text search capabilities by implementing an inverted text index
- [x] Dedicated database canister for use by clients in other languages
- [x] Database management tools to handle collection creation, index management, and data migrations
- [ ] Fully support Array fields and operations on them
- [ ] Multi-key array indexes - for indexing fields within arrays
- [ ] Backward compatible schema upgrades and versioning
- [ ] Aggregation functions (min, max, sum, avg, etc.)
- [ ] Support for migrations
- [ ] Periodic backups to external canisters
- [ ] Database canister monitoring and analytics tools

## Contributing

Contributions are welcome! Please feel free to create an issue to report a bug or submit a Pull Request.
For features, please create an issue first to discuss supporting it in this project.

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0) - see the [LICENSE](LICENSE) file for details.