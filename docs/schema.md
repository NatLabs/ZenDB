## Schema

ZenDB uses

Supported schema types are:

- primitive types:
  - `#Int`, `#Int8`, `#Int16`, `#Int32`, `#Int64`, `#Nat`, `#Nat8`, `#Nat16`, `#Nat32`, `#Nat64`, `#Bool`, `#Float`, `#Text`, `#Blob`, `#Null`, `#Empty`, `#Principal`
- compound types:
  - `#Option(nested_type)`, `#Array(nested_type)`, `#Map or #Record([("field_name", field_type)])`, `#Tuple([tuple_types])`, `#Variant([("variant_name", variant_type)])`

At the top level every schema is a `#Record` type with a list of field tuples, indicating the field name and the field type.
When defining your schema, you must start with a `#Record` type.

```motoko
let UsersSchema : ZenDB.Schema = #Record([
  ("id", #Nat),
  ("name", #Text),
  ("email", #Text),
  ("age", #Nat),
  ("created_at", #Int),
  ("account", #Record([
    ("balance", #Nat),
    ("currency", #Text),
  ])),
]);
```

All the schema types can be queried but only the primitive types and #Option() can be indexed.
