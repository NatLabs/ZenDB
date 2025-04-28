# ZenDB: Enterprise-Grade Document Database for the Internet Computer

[![MOPS](https://img.shields.io/badge/MOPS-zendb-blue)](https://mops.one/zendb)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Motoko](https://img.shields.io/badge/Language-Motoko-orange)](https://github.com/dfinity/motoko)

> A high-performance document database built specifically for the Internet Computer ecosystem

ZenDB is a sophisticated, embedded document database that leverages the Internet Computer's stable memory to provide scalable, performant data persistence with advanced query capabilities. Engineered to handle complex data models with up to 500GB per canister, it bridges the gap between Motoko's type system and Candid encoding to offer a seamless developer experience.

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

## Getting Started

### 1. Installation

```bash
mops add zendb
```

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
  from_blob = func(blob: Blob) : User {
    switch(from_candid(blob) : ?User) {
      case (?user) user;
      case null Debug.trap("Failed to decode user");
    };
  };
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
let queryResults = users.search(
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
let activeRecentPremiumUsers = users.search(
  ZenDB.QueryBuilder()
    .Where("status", #eq(#Text("active")))
    .And("account_type", #eq(#Text("premium")))
    .And("joined_date", #gte(#Int(oneWeekAgo)))
    .Sort("activity_score", #Descending)
    .Limit(25)
);

// Find users matching any of several criteria
let specialCaseUsers = users.search(
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
users.updateById(userId, [
  ("profile.location", #Text("New York")),
  ("profile.interests", #Array([#Text("coding"), #Text("reading")]))
]);

// Field transformations
users.updateById(userId, [
  ("login_count", #add(#currValue, #Nat(1))),
  ("last_login", #Int(Time.now())),
  ("name", #uppercase(#currValue))
]);
```

### Pagination with Cursor Support

```motoko
func getPaginatedResults(cursor: ?Nat, pageSize: Nat) : [User] {
  let query = ZenDB.QueryBuilder()
    .Where("status", #eq(#Text("active")))
    .Sort("created_at", #Descending);
    
  if (cursor != null) {
    query.Cursor(cursor, #Forward);
  };
  
  query.Limit(pageSize);
  
  let #ok(results) = users.search(query);
  return Array.map<(Nat, User), User>(results, func(entry) = entry.1);
}
```

## Performance Optimization

### Index Selection Strategy

ZenDB uses a sophisticated query planner to determine the most efficient indexes for each query. Create indexes that:

1. Match your most common query patterns
2. Include fields used in sorting operations
3. Support your filtering operations (equality, range conditions)

```motoko
// Great for queries filtering on status and sorting by date
users.create_index("status_date_idx", [
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
```

## Examples

The repository includes several examples demonstrating ZenDB's capabilities:

- **Task Manager**: Complete CRUD application with task management functionality
- **Transaction Ledger**: High-performance financial transaction indexing
- **React Integration**: Frontend integration example with React UI

## Roadmap

- [x] Multi-field compound indexes
- [x] Powerful query language with logical operators
- [x] Schema validation and backward compatibility
- [x] Efficient memory usage with garbage collection
- [ ] Query result caching
- [ ] Multi-key array indexes
- [ ] Aggregation functions (count, sum, avg, etc.)
- [ ] Schema constraints (required, unique, enum)
- [ ] Advanced transaction support

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
