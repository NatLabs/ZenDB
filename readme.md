# ZenDB: Document Database for the Internet Computer

[![MOPS](https://img.shields.io/badge/MOPS-zendb-blue)](https://mops.one/zendb)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Motoko](https://img.shields.io/badge/Language-Motoko-orange)](https://github.com/dfinity/motoko)

> A high-performance document database built specifically for the Internet Computer ecosystem

ZenDB is an embedded document database that leverages the Internet Computer's stable memory to provide scalable, performant data persistence with advanced query capabilities. Engineered to handle complex data models with up to 500GB per canister, it bridges the gap between Motoko's type system and Candid encoding to offer a seamless developer experience.

## Key Features

### 🚀 Performance-Optimized
- **B-Tree Indexes**: Sophisticated multi-field indexes for lightning-fast queries
- **Query Execution Engine**: Intelligent query planner with cost-based optimization
- **Memory Efficiency**: Carefully designed to maximize stable memory usage patterns

### 💾 Data Architecture
- **Document-Oriented Storage**: Flexible schema design for complex data structures
- **Compound Indexes**: Support for multi-field indexes to accelerate complex queries
- **Full Candid Integration**: Native support for all Motoko and Candid data types

### 🔍 Advanced Querying
- **Rich Query Language**: Comprehensive set of operators including equality, range, logical operations
- **Query Builder API**: Intuitive fluent interface for building complex queries
- **Sorting & Pagination**: Efficient ordered result sets with skip/limit functionality

### 🔒 Data Integrity
- **Schema Validation**: Ensure data integrity with schema-based validation
- **Backward Compatibility**: Safe schema evolution with compatibility checking
- **Transactional Operations**: Consistent data operations with rollback capability

## Technical Architecture

ZenDB is designed around a layered architecture with clear separation of concerns:

### Core Components

1. **Storage Engine**
   - **BTree**: Implementation of ordered key-value storage (interfaces with both heap and stable memory)
   - **Memory Management**: Automatic optimization of memory usage with freed memory reuse
   - **Disk Paging**: Custom implementation that efficiently manages large datasets in stable memory

2. **Query Processing**
   - **QueryBuilder**: Fluent API that constructs complex query expressions
   - **QueryPlan**: Analyzes query expressions and available indexes to determine execution strategy
   - **QueryExecution**: Executes the most efficient query plan using bitmap intersections or index scans

3. **Schema System**
   - **SchemaMap**: Maps Candid types to internal schema representation
   - **Candify**: Handles serialization/deserialization between Motoko types and storage
   - **CandidMap**: Provides efficient access to nested fields in documents

4. **Indexing Subsystem**
   - **Index Creation**: Indexes are B-trees maintained alongside the main collection
   - **Orchid**: Special type utility that orders Candid values without full deserialization
   - **Bitmap Operations**: Uses bitmap intersections for efficient filtering operations

### Internal Workflow

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

### 1. Installation

```bash
mops add zendb
```

> Requires `moc` version `0.14.9"` or higher to run

### 2. Initialize Your Database

```motoko
import ZenDB "mo:zendb";

actor {
  stable var zendb_store = ZenDB.newStableStore();
  
  system func preupgrade() {
    // Store is automatically persisted in stable memory
  }
  
  system func postupgrade() {
    zendb_store := ZenDB.upgrade(zendb_store);
  }
  
  let db = ZenDB.launchDefaultDB(zendb_store);
}
```

### 3. Define Your Schema

```motoko
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

let candify_users : ZenDB.Types.Candify<User> = {
  to_blob = func(user: User) : Blob { to_candid(user) };
  from_blob = func(blob: Blob) : ?User { from_candid(blob) };
};
```

### 4. Create a Collection & Indexes

```motoko
// Create collection
let #ok(users) = db.create_collection("users", UsersSchema, candify_users);

// Create optimal indexes for your query patterns
let #ok(_) = users.create_index("name_idx", [("name", #Ascending)]);
let #ok(_) = users.create_index("location_created_idx", [
  ("profile.location", #Ascending), 
  ("created_at", #Descending)
]);
```

### 5. Insert & Query Data

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

// Query with the fluent QueryBuilder API
let #ok(queryResults) = users.search(
  ZenDB.QueryBuilder()
    .Where("profile.location", #eq(#Text("San Francisco")))
    .And("profile.age", #gte(#Nat(30)))
    .Sort("created_at", #Descending)
    .Limit(10)
);
```

## Advanced Usage

### Compound Filtering with Logical Operators

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

### Field Updates & Transformations

```motoko
// Atomic field updates
let #ok(_) = users.updateById(userId, [
  ("profile.location", #Text("New York")),
  ("profile.interests", #Array([#Text("coding"), #Text("reading")]))
]);

// Field transformations
let #ok(_) = users.updateById(userId, [
  ("login_count", #add(#currValue, #Nat(1))),
  ("last_login", #Int(Time.now())),
  ("name", #uppercase(#currValue))
]);
```


## Query Operators

ZenDB provides a rich set of query operators to build expressive queries:

| Operator | Description | Example |
|----------|-------------|---------|
| `#eq` | Equality | `.Where("status", #eq(#Text("active")))` |
| `#lt` | Less than | `.Where("age", #lt(#Nat(30)))` |
| `#gt` | Greater than | `.Where("score", #gt(#Nat(100)))` |
| `#lte` | Less than or equal | `.Where("priority", #lte(#Nat(3)))` |
| `#gte` | Greater than or equal | `.Where("reputation", #gte(#Nat(500)))` |
| `#between` | Range (inclusive) | `.Where("age", #between(#Nat(18), #Nat(65)))` |
| `#exists` | Field exists | `.Where("profile.avatar", #exists)` |
| `#startsWith` | Text starts with | `.Where("name", #startsWith(#Text("A")))` |
| `#anyOf` | Value in set | `.Where("status", #anyOf([#Text("active"), #Text("pending")]))` |
| `#not_` | Negates operator | `.Where("role", #not_(#eq(#Text("admin"))))` |

Logical operators allow combining conditions:

- `.And(field, operator)` - Field must match this condition AND previous conditions
- `.Or(field, operator)` - Field must match this condition OR previous conditions
- `.AndQuery(queryBuilder)` - Combines with another query using AND
- `.OrQuery(queryBuilder)` - Combines with another query using OR

## Performance Optimization

### Index Selection Strategy

ZenDB uses a sophisticated query planner to determine the most efficient indexes for each query. Create indexes that:

1. Match your most common query patterns
2. Include fields used in sorting operations
3. Support your filtering operations (equality, range conditions)

```motoko
// Great for queries filtering on status and sorting by date
let #ok(_) = users.create_index("status_date_idx", [
  ("status", #Ascending), 
  ("created_at", #Descending)
]);
```

### Statistics & Monitoring

Monitor your collections to understand performance characteristics:

```motoko
let stats = users.stats();
Debug.print("Records: " # Nat.toText(stats.records));
Debug.print("Indexes: " # Nat.toText(stats.indexes.size()));
Debug.print("Memory Used: " # Nat.toText(stats.memoryUsed));
```

## Examples

The repository includes several examples demonstrating ZenDB's capabilities:

- [**Simple Notes Dapp**](./example/notes/): Complete CRUD application with task management functionality
- **ICP Txs explorer**: High-performance financial transaction indexing
- **React Integration**: Frontend integration example with React UI

## Documentation 
![](./zendb-doc.md)

## Limitations

#### Indexes
- Not capable of creating indexes on fields with `#Float` data type
- Cannot create indexes on fields nested within an `#Array`
- No support for text-based full-text search or pattern matching within indexes
- Limited support for indexing variant types - must reference specific variant paths

#### Query Execution
- Complex queries with many OR conditions may have suboptimal performance
- No built-in support for geospatial queries or operations
- Computation can spike during complex sorting operations on large result sets
- The query planner may not always select the optimal index for complex queries

#### Data Management
- No automatic schema migration tools when changing collection schemas
- Update operations on large collections with many indexes can be resource-intensive
- Limited batch operation support for mass updates or deletes
- No built-in support for time-to-live (TTL) or automatic document expiration

#### Resource Constraints
- Large result sets may encounter instruction limits on query operations
- Pagination with skip can be inefficient for large offsets - cursor-based pagination recommended
- No built-in horizontal scaling across multiple canisters
- Complex aggregation operations must be implemented at the application level

#### Size limitations
- The max size of any record stored in a collection is 4GB
- The max size for any field that will be stored in an index is only 64 KB. Actually the total size of the composite fields that would be stored in the index should be less than or equal to 64 KB.
- 

## Roadmap

- [x] Multi-field compound indexes
- [x] Powerful query language with logical operators
- [x] Schema validation and backward compatibility
- [x] Efficient memory usage with garbage collection
- [ ] Data Certification of all database records, using the [ic-certification](https://mops.one/ic-certification) motoko library
  - [ ] https://forum.dfinity.org/t/do-we-still-need-bigmap-nosql-database/13133/5
- [ ] Multi-key array indexes and nested array support
- [ ] Aggregation functions (count, sum, avg, etc.)
- [ ] Schema constraints (required, unique, enum)
- [ ] Support for transactions
- [ ] Enhanced fulltext search capabilities
- [ ] Distributed querying across multiple canisters

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request to fix a bug.
For features, please create an issue first to discuss supporting it in this project.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
