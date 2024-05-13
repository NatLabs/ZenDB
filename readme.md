## Hydra DB

A single canister document database currently in development.

### Features

- [x] Candid serialization for all types
- [ ] Query Caching
- [ ] Indexes
  - [x] Single field index
  - [ ] Multiple field index
  - [ ] Multi-key array index
  - [ ] Text index (full text search)
- [ ] Hydra Query Language
  - [ ] operators (and, or, not, eq, gte, lte, gt, lt, in, nin)
  - [ ] functions (count, sum, avg, min, max)
  - [ ] modifiers (limit, skip, sort)
- [ ] Schema 
  - [x] Schema Validation on insert/update
  - [x] Backwards compatibility on schema changes
  - [ ] Schema Constraints (required, unique, enum, min, max)
- [ ] Transactions
  - [ ] Single document transactions
  - [ ] Document versioning
- [] Backup and Restore
  - [] Copy on Write Snapshots
  - [] External canister Backup and Restore
  - [] Consistent incremental backups
- [ ] Garbage collection of regions from deleted collections