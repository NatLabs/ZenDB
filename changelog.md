# Changelog

All notable changes to ZenDB stable types will be documented in this file.

## [2.0.0] - 2026-03-07

### Breaking Changes from v1.0.0

#### Critical Index Encoding Changes
- **Orchid (Composite Index Encoder)**: Complete rewrite to lexicographic escape-based encoding
  - Variable-length types (Text, Blob, Principal) now use escape-based encoding instead of size prefixes
  - Enables tail compression in B+Tree for improved memory efficiency
  - **Impact**: All existing indexes are incompatible and must be rebuilt

#### Technical Improvements
- Raw byte comparison now produces correct semantic ordering for all types
- Simplified `btree_cmp` from custom implementation to use Motoko's native `Prim.blobCompare`
- Type codes 15-16 reserved for future variable-length Nat/Int encoding

### Migration Notes
- v1.0.0 **NOT directly upgradeable** to v2.0.0
- **Manual migration required**: All indexes must be rebuilt
- Document data remains compatible (no document structure changes)
- Migration path:
  1. Export all documents from v1.0.0 collections
  2. Create new v2.0.0 collections with same schemas
  3. Re-insert documents (indexes rebuild automatically)
  4. Verify data integrity before deleting v1.0.0 collections

## [1.0.0] - 2025-11-23

### Breaking Changes from v0.1.0

#### Critical Type Changes
- **DocumentId**: `Nat` → `Blob` (enables cross-canister unique identifiers)
- **BitMap**: `BitMap.BitMap` → `SparseBitMap64.SparseBitMap64` (improved memory efficiency)

#### New Stable Structure Fields

**StableStore**
- `canister_id`, `instance_id` - Cross-canister identity
- `candid_map_cache` - Document caching layer

**StableDatabase**
- `instance_id` - Cross-canister identity
- `candid_map_cache` - Document caching layer

**StableCollection**
- `instance_id` - Cross-canister identity
- `indexes_in_batch_operations`, `populate_index_batches` - Asynchronous index building
- `hidden_indexes` - Internal index management
- `candid_serializer` - Optimized serialization
- `candid_map_cache` - Document caching layer

#### Major New Features
- **Batch Index Operations**: Background index population with progress tracking
- **Caching**: `TwoQueueCache` for improved read performance
- **Stats & Monitoring**: Comprehensive memory and instruction tracking types

### Migration Notes
- v0.1.0 preserved for reference but not directly upgradeable to v1.0.0
- Manual migration required due to DocumentId and BitMap type changes
