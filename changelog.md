# Changelog

All notable changes to ZenDB stable types will be documented in this file.

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
