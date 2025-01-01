---
sidebar-position: 4
---

# Schema

## Schema Types

| Candid Type    | Equivalent motoko type / Description                                                                                 |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `#Empty`       | ()                                                                                                                   |
| `#Null`        | null                                                                                                                 |
| `#Bool`        | Bool                                                                                                                 |
| `#Nat`         | Nat                                                                                                                  |
| `#Nat8`        | Nat8                                                                                                                 |
| `#Nat16`       | Nat16                                                                                                                |
| `#Nat32`       |                                                                                                                      |
| `#Nat64`       |                                                                                                                      |
| `#Int`         |                                                                                                                      |
| `#Int8`        |                                                                                                                      |
| `#Int16`       |                                                                                                                      |
| `#Int32`       |                                                                                                                      |
| `#Int64`       |                                                                                                                      |
| `#Text`        |                                                                                                                      |
| `#Blob`        |                                                                                                                      |
| `#Array()`     | Accepts the type for all the elements in the array. For example an array of numbers would be `#Array(#Nat)` == [Nat] |
| `#Tuple([])`   |                                                                                                                      |
| `#Record([])`  |                                                                                                                      |
| `#Variant([])` |                                                                                                                      |

## Candid Values

| Candid Type    | Equivalent motoko type                                           |
| -------------- | ---------------------------------------------------------------- |
| `#Empty`       | ()                                                               |
| `#Null`        | null                                                             |
| `#Blob`        | Blob                                                             |
| `#Nat`         | Nat                                                              |
| `#Nat8`        | Nat8                                                             |
| `#Nat16`       | Nat16                                                            |
| `#Nat32`       |                                                                  |
| `#Nat64`       |                                                                  |
| `#Int`         |                                                                  |
| `#Int8`        |                                                                  |
| `#Int16`       |                                                                  |
| `#Int32`       |                                                                  |
| `#Int64`       |                                                                  |
| `#Text`        |                                                                  |
| `#Blob`        |                                                                  |
| `#Array()`     | Accepts an array of values: `#Array([#Nat(1), #Nat(2), #Nat(3)]) |
| `#Tuple([])`   |                                                                  |
| `#Record([])`  |                                                                  |
| `#Variant([])` |                                                                  |
