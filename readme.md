## ZenDB

A single canister document database currently in development.

### Features

- [x] Candid serialization for all types
- [ ] Query Caching
- [ ] Indexes
  - [x] Single field index
  - [x] Multiple field index (Compound Index)
  - [ ] Multi-key array index
- [ ] Hydra Query Language
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
- [ ] Garbage collection of regions from deleted collections
