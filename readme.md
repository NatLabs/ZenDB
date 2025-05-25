# ZenDB: Document Database for the Internet Computer

[![MOPS](https://img.shields.io/badge/MOPS-zendb-blue)](https://mops.one/zendb)
[![License](https://img.shields.io/badge/License-AGPL--3.0-blue)](LICENSE)
[![Motoko](https://img.shields.io/badge/Language-Motoko-orange)](https://github.com/dfinity/motoko)

> A high-performance document database built specifically for the Internet Computer ecosystem


ZenDB is an embedded document database that leverages the Internet Computer's unique features to provide a powerful, scalable, and efficient data storage solution for Motoko applications. With advanced querying capabilities, users can perform complex queries on large datasets efficiently, while also benefiting from the simplicity and safety of Motoko's type system.
It is designed to work seamlessly with stable memory, allowing developers to store and query complex data models with a storage capacity of up to 500GB in a single canister. 

## Key Features
- **Full Candid Integration**: Native support for candid which allows users to store all Motoko data types
- **Compound Indexes**: Support for compound multi-field indexes to accelerate complex queries
- **Rich Query Language**: Comprehensive set of operators including equality, range, logical operations
- **Query Builder API**: Intuitive fluent interface for building complex queries
- **Query Execution Engine**: Performanced optimized Query planner programmed to search for the path with the smallest result size to filter/traverse.
- **Sorting & Pagination**: Efficient ordered result sets with skip/limit pagination
- **Schema Validation**: Ensure data integrity with schema-based validation for each entry
- **Schema Constraints**: Add limits on what can be stored in the db

## Internal Workflow

When executing a query, ZenDB follows this optimized workflow:

1. The query is parsed and validated against the collection schema
2. The query plan generator analyzes available indexes and query patterns
3. For indexed queries, the system chooses between:
   - **Index Scan**: Direct B-tree traversal for equality or simple range queries
   - **Bitmap Intersection**: Converting multiple index scans to bitmaps and intersecting them
   - **Hybrid Approach**: Combining index scans with in-memory filtering for complex queries
4. Results are processed, sorted if needed, and paginated according to query parameters

This architecture allows ZenDB to handle complex queries efficiently, even with large datasets, by minimizing the amount of data that needs to be deserialized from stable memory.

## Getting Started

### Installation

Requires `moc` version `0.14.9"` or higher to run
Install with mops: `mops toolchain use moc 0.14.9`  

#### Install Directly from Mops (Recommended)

```bash
mops add zendb
```

#### Install Specific Github Branch/Commit
- Replace the value after the pound sign `#` with the branch or the commit hash

```bash
mops add https://github.com/NatLabs/ZenDB#<branch/commit-hash>
```

### Basic Usage

#### 1. Initialize the Database

```motoko
import ZenDB "mo:zendb";

actor {
  stable var zendb = ZenDB.newStableStore(null);
  
  system func preupgrade() {
    // Store is automatically persisted in stable memory
  }
  
  system func postupgrade() {
    zendb := ZenDB.upgrade(zendb);
  }
  
  let db = ZenDB.launchDefaultDB(zendb);
}
```

#### 2. Define the Collection's Schema

```motoko
// motoko type
type User = {
  id: Nat;
  name: Text;
  email: Text;
  profile: {
    age: ?Nat;
    location: Text;
    interests: [Text];
  };
  created_at: Int;
};

// corresponding schema type
let UsersSchema : ZenDB.Types.Schema = #Record([
  ("id", #Nat),
  ("name", #Text),
  ("email", #Text),
  ("profile", #Record([
    ("age", #Option(#Nat)),
    ("location", #Text),
    ("interests", #Array(#Text)),
  ])),
  ("created_at", #Int),
]);

// serializer
let candify_users : ZenDB.Types.Candify<User> = {
  to_blob = func(user: User) : Blob { to_candid(user) };
  from_blob = func(blob: Blob) : ?User { from_candid(blob) };
};
```

#### 3. Create a Collection & Indexes

```motoko
// Create collection
let #ok(users) = db.createCollection("users", UsersSchema, candify_users, null);

// Create optimal indexes for your query patterns
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

#### 4. Insert & Query Data

```motoko
// Insert a document
let user : User = {
  id = 1;
  name = "Alice";
  email = "alice@example.com";
  profile = {
    age = ?35;
    location = "San Francisco";
    interests = ["coding", "hiking", "photography"];
  };
  created_at = Time.now();
};

let #ok(userId) = users.insert(user);

// Query with the QueryBuilder API
let #ok(queryResults) = users.search(
  ZenDB.QueryBuilder()
    .Where("profile.location", #eq(#Text("San Francisco")))
    .And("profile.age", #gte(#Nat(30)))
    .Sort("created_at", #Descending)
    .Limit(10)
);

assert queryResults == [user];
```

#### 5. Field Updates & Transformations

```motoko
// Atomic field updates
let #ok(_) = users.updateById(
  userId,
  [
    ("profile.location", #Text("New York")),
    ("profile.interests", #Array([#Text("coding"), #Text("reading")]))
  ]
);

// Field transformations
let #ok(_) = users.updateById(
  userId, 
  [
    ("login_count", #add(#currValue, #Nat(1))),
    ("last_login", #Int(Time.now())),
    ("name", #uppercase(#currValue))
  ]
);
```

#### 6. Statistics & Monitoring

Monitor your collections to understand performance characteristics:

```motoko
let stats = users.stats();
```

### Advanced Usage

#### Compound Filtering with Logical Operators

```motoko
// Find active premium users who joined recently
let #ok(activeRecentPremiumUsers) = users.search(
  ZenDB.QueryBuilder()
    .Where("status", #eq(#Text("active")))
    .And("account_type", #eq(#Text("premium")))
    .And("joined_date", #gte(#Int(oneWeekAgo)))
    .Sort("activity_score", #Descending)
    .Limit(25)
);

// Find users matching any of several criteria
let #ok(specialCaseUsers) = users.search(
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
- [**ICP Txs explorer**](./example/react-project/backend/Backend.mo): ZenDB test app to index ICP transactions
  - https://2yfll-4qaaa-aaaap-anvaq-cai.icp0.io/
- [**Flying Ninja**](./example/flying_ninja/backend/app.mo): Dapp from the [dfinity/examples](https://github.com/dfinity/examples/tree/master/motoko) repo migrated to use ZenDB.


For more detailed examples and advanced usage, see the [Documentation](./zendb-doc.md) section below.



## Performance Optimization

### Index Selection Strategy

ZenDB uses a sophisticated query planner to determine the most efficient indexes for each query. 
To get the best performance from ZenDB, create indexes that:

1. Match your most common query patterns
2. Include fields used in sorting operations
3. Support your filtering operations (equality, range conditions)

```motoko
// Great for queries filtering on status and sorting by date
let #ok(_) = users.createIndex(
  "status_date_idx", [
    ("status", #Ascending), 
    ("created_at", #Descending)
  ]
);
```

### Statistics & Monitoring

Monitor your collections to understand performance characteristics:

```motoko
let stats = users.stats();
```

## Limitations

- Limited array support - Can store arrays in db but cannot create indexes on array fields or perform operations on specific array elements
- No support for text-based full-text search or pattern matching within indexes

- Cannot create indexes on fields nested within an `#Array`

- Complex queries with many OR conditions may have suboptimal performance
- The query planner may not always select the optimal index for complex queries

- Schema updates and migrations not yet supported. As a result, changing the schema of an existing collection requires creating a new collection and migrating data

- Using Limit/Skip Pagination can be inefficient and may hit the instruction limits if the result set is large enough. It is recommended to created indexes that fully cover your queries where possible, to avoid this limitation.

## Roadmap

- [x] Multi-field compound indexes
- [x] Powerful query language with logical operators
- [x] Schema validation and Schema constraints (required, unique, enum)
- [ ] Data Certification of all database records, using the [ic-certification](https://mops.one/ic-certification) motoko library
- [ ] Fully support Array fields and operations on them
- [ ] Multi-key array indexes - for indexing fields within arrays
- [ ] Backward compatible schema updates and versioning
- [ ] Aggregation functions (min, max, sum, avg, etc.)
- [ ] Better support for migrations
- [ ] Enhanced fulltext search capabilities
- [ ] Support for transactions
- [ ] Dedicated database canister for use by clients in other languages (e.g. JavaScript, Rust)
- [ ] Database management tools
- [ ] Periodic backups to external canisters
- [ ] Improved monitoring and analytics tools

## Contributing

Contributions are welcome! Please feel free to create an isssue to report a bug or submit a Pull Request.
For features, please create an issue first to discuss supporting it in this project.

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0) - see the [LICENSE](LICENSE) file for details.