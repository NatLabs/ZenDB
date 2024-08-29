/// A collection is a set of records of the same type.
///
/// ```motoko
/// type User = { name: Text, age: Nat };
/// let hydra_db = HydraDB();
/// let db = hydra_db.getDB("my_db");
///
/// let candify_users = {
///     to_blob = func(user: User) : Blob { to_candid(user) };
///     from_blob = func(blob: Blob) : User { let ?user : ?User = from_candid(blob); user; };
/// };
///
/// let users = db.getCollection<User>("users", candify_users);
///
/// let alice = { name = "Alice", age = 30 };
/// let bob = { name = "Bob", age = 25 };
///
/// let alice_id = users.put(alice);
/// let bob_id = users.put(bob);
///
/// ```
import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Result "mo:base/Result";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Hash "mo:base/Hash";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Int32 "mo:base/Int32";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Int16 "mo:base/Int16";
import Int64 "mo:base/Int64";
import Int8 "mo:base/Int8";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Tag "mo:candid/Tag";
import BitMap "mo:bit-map";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "Types";
import Query "Query";
import Utils "Utils";
import CandidMap "CandidMap";

module {

    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; nhash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    type QueryBuilder = Query.QueryBuilder;

    // public type MemoryBTree = MemoryBTree.VersionedMemoryBTree;
    public type BTreeUtils<K, V> = MemoryBTree.BTreeUtils<K, V>;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type RecordPointer = Nat;
    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type HydraQueryLang = T.HydraQueryLang;

    public type Candify<A> = {
        from_blob : Blob -> A;
        to_blob : A -> Blob;
    };

    public type StableCollection = {
        var schema : Schema;
        schema_keys : [Text];
        schema_keys_set : Set<Text>;
        main : MemoryBTree.StableMemoryBTree;
        indexes : Map<Text, Index>;
    };

    public type IndexKeyFields = [(Text, Candid)];

    let DEFAULT_BTREE_ORDER = 256;

    func is_schema_backward_compatible(curr : Schema, new : Schema) : Bool {
        switch (curr, new) {
            case (#Empty, #Empty) true;
            case (#Null, #Null) true;
            case (#Text, #Text) true;
            case (#Nat, #Nat) true;
            case (#Int, #Int) true;
            case (#Float, #Float) true;
            case (#Bool, #Bool) true;
            case (#Principal, #Principal) true;
            case (#Option(inner_curr), #Option(inner_new)) is_schema_backward_compatible(inner_curr, inner_new);
            // types can be updated to become optional but not the other way around
            case (curr, #Option(inner_new)) is_schema_backward_compatible(curr, inner_new);
            case (#Array(inner_curr), #Array(inner_new)) is_schema_backward_compatible(inner_curr, inner_new);
            case (#Tuple(curr), #Tuple(new)) {
                if (curr.size() != new.size()) return false;
                for ((a, b) in Itertools.zip(curr.vals(), new.vals())) {
                    if (not is_schema_backward_compatible(a, b)) return false;
                };
                true;
            };
            case (#Record(fields_curr), #Record(fields_new)) {
                let sorted_fields_new = Array.sort(
                    fields_new,
                    func(a : (Text, Schema), b : (Text, Schema)) : Order {
                        let ?i = Array.indexOf<(Text, Schema)>(a, fields_curr, func(a : (Text, Schema), b : (Text, Schema)) : Bool { a.0 == b.0 }) else return #greater;
                        let ?j = Array.indexOf(b, fields_curr, func(a : (Text, Schema), b : (Text, Schema)) : Bool { a.0 == b.0 }) else return #less;

                        Nat.compare(i, j);
                    },
                );

                for (i in Itertools.range(0, fields_curr.size())) {
                    let (name_curr, schema_curr) = fields_curr[i];
                    let (name_new, schema_new) = sorted_fields_new[i];
                    if (name_curr != name_new) return false;
                    if (not is_schema_backward_compatible(schema_curr, schema_new)) return false;
                };

                for (i in Itertools.range(fields_curr.size(), sorted_fields_new.size())) {
                    let (_, schema_new) = sorted_fields_new[i];

                    // new fields must be optional so they are backward compatible
                    let #Option(_) = schema_new else return false;
                };

                true;
            };
            case (#Variant(variants_curr), #Variant(variants_new)) {

                let sorted_variants_new = Array.sort(
                    variants_new,
                    func(a : (Text, Schema), b : (Text, Schema)) : Order {
                        let ?i = Array.indexOf(a, variants_curr, func(a : (Text, Schema), b : (Text, Schema)) : Bool { a.0 == b.0 }) else return #greater;
                        let ?j = Array.indexOf(b, variants_curr, func(a : (Text, Schema), b : (Text, Schema)) : Bool { a.0 == b.0 }) else return #less;

                        Nat.compare(i, j);
                    },
                );

                for (i in Itertools.range(0, variants_curr.size())) {
                    let (name_curr, schema_curr) = variants_curr[i];
                    let (name_new, schema_new) = variants_new[i];
                    if (name_curr != name_new) return false;
                    if (not is_schema_backward_compatible(schema_curr, schema_new)) return false;
                };

                // no need to validate new variants
                true;
            };
            case (_) false;
        };
    };

    func validate_record(schema : Schema, record : Candid) : Result<(), Text> {

        // var var_schema = schema;
        // var var_record = record;

        func _validate(schema : Schema, record : Candid) : Result<(), Text> {
            switch (schema, record) {
                case (#Empty, #Empty) #ok;
                case (#Null, #Null) #ok;
                case (#Text, #Text(_)) #ok;
                case (#Nat, #Nat(_)) #ok;
                case (#Int, #Int(_)) #ok;
                case (#Float, #Float(_)) #ok;
                case (#Bool, #Bool(_)) #ok;
                case (#Principal, #Principal(_)) #ok;
                case (#Blob, #Blob(_)) #ok;
                case (#Option(inner), #Null) #ok;
                case (#Option(inner), record) {
                    // it should pass in
                    // the case where you update a schema type to be optional
                    return _validate(inner, record);
                };
                case (schema, #Option(inner)) {
                    if (inner == #Null) return #ok;

                    _validate(schema, inner);
                };
                case (#Tuple(tuples), #Record(records)) {
                    if (records.size() != tuples.size()) return #err("Tuple size mismatch: expected " # debug_show (tuples.size()) # ", got " # debug_show (records.size()));

                    for ((i, (key, _)) in Itertools.enumerate(records.vals())) {
                        if (key != debug_show (i)) return #err("Tuple key mismatch: expected " # debug_show (i) # ", got " # debug_show (key));
                    };

                    for ((i, (key, value)) in Itertools.enumerate(records.vals())) {
                        let res = _validate(tuples[i], value);
                        let #ok(_) = res else return send_error(res);
                    };

                    #ok;

                };
                case (#Record(fields), #Record(records)) {
                    if (fields.size() != records.size()) {
                        return #err("Record size mismatch: " # debug_show (("schema", fields.size()), ("record", records.size())));
                    };

                    let sorted_fields = Array.sort(
                        fields,
                        func(a : (Text, Schema), b : (Text, Schema)) : Order {
                            Text.compare(a.0, b.0);
                        },
                    );

                    let sorted_records = Array.sort(
                        records,
                        func(a : (Text, Candid), b : (Text, Candid)) : Order {
                            Text.compare(a.0, b.0);
                        },
                    );

                    // should sort fields and records
                    var i = 0;
                    while (i < fields.size()) {
                        let field = sorted_fields[i];
                        let record = sorted_records[i];

                        if (field.0 != record.0) return #err("Record field mismatch: " # debug_show (("field", field.0), ("record", record.0)) # debug_show (fields, records));

                        let res = _validate(field.1, record.1);
                        let #ok(_) = res else return send_error(res);

                        i += 1;
                    };

                    #ok;
                };
                case (#Array(inner), #Array(records)) {
                    var i = 0;
                    while (i < records.size()) {
                        let res = _validate(inner, records[i]);
                        let #ok(_) = res else return send_error(res);
                        i += 1;
                    };
                    #ok;
                };
                case (#Variant(variants), #Variant((record_key, nested_record))) {

                    let result = Array.find<(Text, Schema)>(
                        variants,
                        func((variant_name, _) : (Text, Schema)) : Bool {
                            variant_name == record_key;
                        },
                    );

                    switch (result) {
                        case (null) return #err("Variant not found in schema");
                        case (?(name, variant)) return _validate(variant, nested_record);
                    };
                };

                case (a, b) return #err("validate_record(): schema and record mismatch: " # debug_show (a, b));
            };
        };

        switch (schema) {
            case (#Record(fields)) _validate(schema, record);
            case (_) #err("validate_schema(): schema is not a record");
        };
    };

    func cmp_candid(schema : Schema, a : Candid, b : Candid) : Int8 {
        // Debug.print("cmp_candid: " # debug_show (schema, a, b));
        switch (schema, a, b) {
            // The #Minimum variant is used in queries to represent the minimum value
            case (_, #Minimum, _) -1;
            case (_, _, #Minimum) 1;

            // The #Maximum variant is used in queries to represent the maximum value
            case (_, #Maximum, _) 1;
            case (_, _, #Maximum) -1;

            case (_, #Null, #Null) 0;
            case (_, #Empty, #Empty) 0;

            case (_, _, #Null) 1;
            case (_, #Null, _) -1;

            case (#Text, #Text(a), #Text(b)) Int8Cmp.Text(a, b);
            case (#Blob, #Blob(a), #Blob(b)) Int8Cmp.Blob(a, b);
            case (#Nat, #Nat(a), #Nat(b)) Int8Cmp.Nat(a, b);
            case (#Nat8, #Nat8(a), #Nat8(b)) Int8Cmp.Nat8(a, b);
            case (#Nat16, #Nat16(a), #Nat16(b)) Int8Cmp.Nat16(a, b);
            case (#Nat32, #Nat32(a), #Nat32(b)) Int8Cmp.Nat32(a, b);
            case (#Nat64, #Nat64(a), #Nat64(b)) Int8Cmp.Nat64(a, b);
            case (#Principal, #Principal(a), #Principal(b)) Int8Cmp.Principal(a, b);
            case (_, #Float(a), #Float(b)) Int8Cmp.Float(a, b);
            case (_, #Bool(a), #Bool(b)) Int8Cmp.Bool(a, b);
            case (_, #Int(a), #Int(b)) Int8Cmp.Int(a, b);
            case (_, #Int8(a), #Int8(b)) Int8Cmp.Int8(a, b);
            case (_, #Int16(a), #Int16(b)) Int8Cmp.Int16(a, b);
            case (_, #Int32(a), #Int32(b)) Int8Cmp.Int32(a, b);
            case (_, #Int64(a), #Int64(b)) Int8Cmp.Int64(a, b);

            case (#Option(schema), #Option(a), #Option(b)) {
                switch (a, b) {
                    case (#Null, #Null) 0;
                    case (#Null, _) -1;
                    case (_, #Null) 1;
                    case (_, _) cmp_candid(schema, a, b);
                };
            };
            case (#Variant(schema), #Variant(a), #Variant(b)) {

                let ?i = Array.indexOf<(Text, Any)>(
                    a,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("cmp_candid: variant not found in schema");

                let ?j = Array.indexOf<(Text, Any)>(
                    b,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("cmp_candid: variant not found in schema");

                let res = Int8Cmp.Nat(i, j);

                if (res == 0) {
                    cmp_candid(schema[i].1, a.1, b.1);
                } else {
                    res;
                };

            };

            // case (#Array(a), #Array(b)) {
            //     // compare the length of the arrays
            //     let len_cmp = Int8Cmp.Nat(a.size(), b.size());
            //     if (len_cmp != 0) return len_cmp;

            //     let min_len = Nat.min(a.size(), b.size());
            //     for (i in Iter.range(0, min_len - 1)) {
            //         let cmp_result = cmp_candid(a[i], b[i]);
            //         if (cmp_result != 0) return cmp_result;
            //     };
            //     Int8Cmp.Nat32(a.size(), b.size());
            // };

            case (schema, a, b) {
                Debug.print(debug_show (a, b));
                Debug.trap("cmp_candid: unexpected candid type " # debug_show (schema, a, b));
            };
        };
    };

    func send_error<A, B, C>(res : Result<A, B>) : Result<C, B> {
        switch (res) {
            case (#ok(_)) Debug.trap("send_error: unexpected error type");
            case (#err(err)) return #err(err);
        };
    };

    module IdMapping {

        // mapping calculated using the size of each key-value block
        // from the documentation we know that each block uses 15 bytes + the size of the serialized key
        // in our case our key is always a Nat64 which is 8 bytes
        // so each block uses 23 bytes
        // https://github.com/NatLabs/memory-collection/blob/main/src/MemoryBTree/readme.md#key-value-region

        let REGION_HEADER = 64;
        let KEY_BLOCK_SIZE = 23;

        public func to_pointer(id : Nat) : Nat {
            (id * KEY_BLOCK_SIZE) + REGION_HEADER;
        };

        public func from_pointer(n : Nat) : Nat {
            (n - REGION_HEADER) / KEY_BLOCK_SIZE;
        };

    };

    func lookup_record<Record>(collection : T.StableCollection, blobify : T.Candify<Record>, id : Nat) : Record {
        let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);

        let pointer = IdMapping.to_pointer(id);
        let ?record_candid_blob = MemoryBTree.lookupVal(collection.main, btree_main_utils, pointer);
        let record = blobify.from_blob(record_candid_blob);
        record;
    };

    func lookup_candid_record(collection : StableCollection, id : Nat) : ?Candid {
        let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);
        let pointer = IdMapping.to_pointer(id);
        let ?candid_blob = MemoryBTree.lookupVal(collection.main, btree_main_utils, pointer);
        let candid = decode_candid_blob(collection, candid_blob);

        ?candid;
    };

    public func get_index_data_utils(collection : StableCollection, index_key_details : [(Text, SortDirection)]) : MemoryBTree.BTreeUtils<[Candid], RecordPointer> {

        let key_utils = get_index_key_utils(collection, index_key_details);
        let value_utils = TypeUtils.Nat;

        MemoryBTree.createUtils(key_utils, value_utils);

    };

    public let CandidTypeCode = {
        // primitive types
        // Null : Nat8 = 0x7f;
        Bool : Nat8 = 0x7e;
        Nat : Nat8 = 0x7d;
        Int : Nat8 = 0x7c;
        Nat8 : Nat8 = 0x7b;
        Nat16 : Nat8 = 0x7a;
        Nat32 : Nat8 = 0x79;
        Nat64 : Nat8 = 0x78;
        Int8 : Nat8 = 0x77;
        Int16 : Nat8 = 0x76;
        Int32 : Nat8 = 0x75;
        Int64 : Nat8 = 0x74;
        // Float32 : Nat8 = 0x73;
        Float : Nat8 = 0x72;
        Text : Nat8 = 0x71;
        // Reserved : Nat8 = 0x70;
        Empty : Nat8 = 0x6f;
        Principal : Nat8 = 0x68;

        // compound types

        Option : Nat8 = 0x6e;
        Array : Nat8 = 0x6d;
        Record : Nat8 = 0x6c;
        Variant : Nat8 = 0x6b;
        // Func : Nat8 = 0x6a;
        // Service : Nat8 = 0x69;

        // custom types
        Blob : Nat8 = 0x5f;
        Null : Nat8 = 0x60;

    };

    public let Orchid : TypeUtils.TypeUtils<[Candid]> = {
        blobify = {
            to_blob = func(candid_values : [Candid]) : Blob {
                let buffer = Buffer.Buffer<Nat8>(100);

                func encode(buffer : Buffer.Buffer<Nat8>, candid : Candid) {

                    switch (candid) {
                        case (#Minimum) buffer.add(0);
                        case (#Array(_) or #Record(_) or #Map(_) or #Variant(_) or #Tuple(_)) Debug.trap("Orchid does not support compound types: " # debug_show (candid));
                        case (#Option(option_type)) {
                            buffer.add(CandidTypeCode.Option);
                            encode(buffer, option_type);
                        };

                        case (#Principal(p)) {
                            buffer.add(CandidTypeCode.Principal);

                            let blob = Principal.toBlob(p);
                            let bytes = Blob.toArray(blob);

                            let size = Nat8.fromNat(blob.size()); // -> Are principals only limited to 29 bytes? or just Principals for user and canister ids?

                            buffer.add(size);

                            var i = 0;
                            while (i < bytes.size()) {
                                buffer.add(bytes[i]);
                                i += 1;
                            };

                        };
                        case (#Text(t)) {
                            let utf8 = Text.encodeUtf8(t);
                            let bytes = Blob.toArray(utf8);
                            // let size = Nat8.fromNat(utf8.size()); -> the size will throw of the comparison, because text should be compared in lexicographic order and not by size
                            buffer.add(CandidTypeCode.Text);

                            var i = 0;
                            while (i < bytes.size()) {
                                buffer.add(bytes[i]);
                                i += 1;
                            };

                            buffer.add(0); // null terminator, helps with lexicographic comparison, if the text ends before the other one, it will be considered smaller because the null terminator is smaller than any other byte

                        };
                        case (#Blob(b)) {

                            let bytes = Blob.toArray(b);
                            let size = Nat32.fromNat(b.size());

                            buffer.add(CandidTypeCode.Blob);

                            buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat(size & 0xff)));

                            var i = 0;
                            while (i < bytes.size()) {
                                buffer.add(bytes[i]);
                                i += 1;
                            };

                        };
                        case (#Float(f)) Debug.trap("Orchid does not support Float type");
                        case (#Int(int)) {
                            buffer.add(CandidTypeCode.Int);

                            let sign : Nat8 = if (int < 0) 0 else 1;

                            var num = Int.abs(int);
                            var size : Nat32 = 0;

                            while (num > 0) {
                                num /= 255;
                                size += 1;
                            };

                            buffer.add(sign);

                            buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat(size & 0xff)));

                            num := Int.abs(int);

                            var i : Nat32 = 0;

                            while (i < size) {
                                let tmp = num % 255;
                                num /= 255;
                                buffer.add(Nat8.fromNat(tmp));
                                i += 1;
                            };

                        };
                        case (#Int64(i)) {
                            buffer.add(CandidTypeCode.Int64);

                            let n = Int64.toNat64(i);

                            let msbyte = Nat8.fromNat(Nat64.toNat(n >> 56));
                            let msbyte_with_flipped_msbit = msbyte ^ 0x80;

                            buffer.add(msbyte_with_flipped_msbit);
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat(n & 0xff)));

                        };
                        case (#Int32(i)) {
                            buffer.add(CandidTypeCode.Int32);

                            let n = Int32.toNat32(i);

                            let msbyte = Nat8.fromNat(Nat32.toNat(n >> 24));
                            let msbyte_with_flipped_msbit = msbyte ^ 0x80;

                            buffer.add(msbyte_with_flipped_msbit);
                            buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));

                        };
                        case (#Int16(i)) {
                            buffer.add(CandidTypeCode.Int16);

                            let n = Int16.toNat16(i);

                            let most_significant_byte = Nat8.fromNat(Nat16.toNat(n >> 8));
                            let msbyte_with_flipped_msbit = most_significant_byte ^ 0x80;

                            buffer.add(msbyte_with_flipped_msbit);
                            buffer.add(Nat8.fromNat(Nat16.toNat(n & 0xff)));
                        };
                        case (#Int8(i)) {
                            buffer.add(CandidTypeCode.Int8);

                            let byte = Int8.toNat8(i);
                            let int8_with_flipped_msb = byte ^ 0x80;

                            buffer.add(int8_with_flipped_msb);
                        };

                        case (#Nat64(n)) {
                            buffer.add(CandidTypeCode.Nat64);

                            buffer.add(Nat8.fromNat(Nat64.toNat(n >> 56)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat(n & 0xff)));

                        };
                        case (#Nat32(n)) {
                            buffer.add(CandidTypeCode.Nat32);

                            buffer.add(Nat8.fromNat(Nat32.toNat(n >> 24)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));

                        };
                        case (#Nat16(n)) {
                            buffer.add(CandidTypeCode.Nat16);

                            buffer.add(Nat8.fromNat(Nat16.toNat(n >> 8)));
                            buffer.add(Nat8.fromNat(Nat16.toNat(n & 0xff)));

                        };
                        case (#Nat8(n)) {
                            buffer.add(CandidTypeCode.Nat8);
                            buffer.add(n);
                        };
                        case (#Nat(n)) {
                            var num = n;
                            var size : Nat32 = 0;

                            while (num > 0) {
                                num /= 255;
                                size += 1;
                            };

                            buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat(size & 0xff)));

                            num := n;

                            var i : Nat32 = 0;
                            while (i < size) {
                                let tmp = num % 255;
                                num /= 255;
                                buffer.add(Nat8.fromNat(tmp));
                                i += 1;
                            };

                        };
                        case (#Bool(b)) {
                            buffer.add(CandidTypeCode.Bool);
                            buffer.add(if (b) 1 else 0);
                        };
                        case (#Empty) buffer.add(CandidTypeCode.Empty);
                        case (#Null) buffer.add(CandidTypeCode.Null);
                        case (#Maximum) buffer.add(255);
                    };
                };

                var i = 0;
                while (i < candid_values.size()) {
                    encode(buffer, candid_values[i]);
                    i += 1;
                };

                Blob.fromArray(
                    Buffer.toArray(buffer)
                );

            };
            from_blob = func(blob : Blob) : [Candid] {
                // Debug.trap("Orchid does not support deserialization");
                [];
            };
        };
        cmp = TypeUtils.MemoryCmp.Default;

        hash = func(a : Candid, b : Candid) : Nat64 = Debug.trap("Orchid does not support hashing");
    };

    public func get_index_key_utils(collection : StableCollection, index_key_details : [(Text, SortDirection)]) : TypeUtils<[Candid]> {
        Orchid;
    };

    func main_btree_utils() : BTreeUtils<Nat, Blob> {
        MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);
    };

    func decode_candid_blob(collection : StableCollection, candid_blob : Blob) : Candid.Candid {
        let candid_result = Candid.decode(candid_blob, collection.schema_keys, null);
        let #ok(candid_values) = candid_result;
        let candid = candid_values[0];
        candid;
    };

    func get_index_columns(collection : StableCollection, index_key_details : [(Text, SortDirection)], id : Nat, records : [(Text, Candid)]) : [Candid] {
        let buffer = Buffer.Buffer<Candid>(8);

        for ((index_key, dir) in index_key_details.vals()) {
            for ((key, value) in records.vals()) {
                if (key == ":record-id") {
                    buffer.add(#Nat(id));
                } else if (key == index_key) {
                    buffer.add(value);
                };
            };
        };

        Buffer.toArray(buffer);
    };

    type BestIndexResult = {
        index : Index;
        requires_additional_sorting : Bool;
        requires_additional_filtering : Bool;
    };

    func get_best_index(collection : StableCollection, operations : [(Text, T.HqlOperators)], sort_field : ?(Text, T.SortDirection)) : ?BestIndexResult {
        let equal_fields = Set.new<Text>();
        let sort_fields = Buffer.Buffer<(Text, T.SortDirection)>(8);
        let range_fields = Set.new<Text>();

        func fill_field_maps(equal_fields : Set.Set<Text>, sort_fields : Buffer<(Text, T.SortDirection)>, range_fields : Set.Set<Text>, operations : [(Text, T.HqlOperators)], sort_field : ?(Text, T.SortDirection)) {

            sort_fields.clear();

            switch (sort_field) {
                case (?(field, direction)) sort_fields.add(field, direction);
                case (null) {};
            };

            // sort_fields.reverse(); or add in reverse order

            for ((field, op) in operations.vals()) {
                switch (op) {
                    case (#eq(_)) ignore Set.put(equal_fields, thash, field);
                    case (_) ignore Set.put(range_fields, thash, field);
                };
            };
        };

        fill_field_maps(equal_fields, sort_fields, range_fields, operations, sort_field);

        var best_score = 0;
        var best_index : ?Index = null;
        var best_requires_additional_sorting = false;
        var best_requires_additional_filtering = false;

        var num_of_equal_fields_evaluated = 0;
        var num_of_sort_fields_evaluated = 0;
        var num_of_range_fields_evaluated = 0;

        for (index in Map.vals(collection.indexes)) {
            var index_score = 0;
            var requires_additional_filtering = false;

            label scoring_indexes for ((index_key, direction) in index.key_details.vals()) {
                var matches_at_least_one_column = false;

                switch (Set.has(equal_fields, thash, index_key)) {
                    case (true) {
                        index_score += 3;
                        num_of_equal_fields_evaluated += 1;
                        matches_at_least_one_column := true;
                    };
                    case (false) {};
                };

                if (num_of_sort_fields_evaluated < sort_fields.size()) {
                    let i = sort_fields.size() - 1 - num_of_sort_fields_evaluated;
                    if (index_key == sort_fields.get(i).0 and direction == sort_fields.get(i).1) {
                        index_score += 2;
                        num_of_sort_fields_evaluated += 1;
                        matches_at_least_one_column := true;
                    };
                };

                switch (Set.remove(range_fields, thash, index_key)) {
                    case (true) {
                        index_score += 1;
                        num_of_range_fields_evaluated += 1;
                        matches_at_least_one_column := true;
                        break scoring_indexes;
                    };
                    case (false) {};
                };

                if (not matches_at_least_one_column) break scoring_indexes;

            };

            if (num_of_range_fields_evaluated < Set.size(range_fields) or num_of_equal_fields_evaluated < Set.size(equal_fields)) {
                requires_additional_filtering := true;
            };

            if (index_score > best_score) {
                best_score := index_score;
                best_index := ?index;
                best_requires_additional_filtering := requires_additional_filtering;
                best_requires_additional_sorting := num_of_sort_fields_evaluated < sort_fields.size();
            };

        };

        let index = switch (best_index) {
            case (null) return null;
            case (?index) index;
        };

        let index_response = {
            index;
            requires_additional_sorting = best_requires_additional_sorting;
            requires_additional_filtering = best_requires_additional_filtering;
        };

        ?index_response;

    };

    func get_nested_candid_field(_candid_record : Candid, key : Text) : ?Candid {
        let nested_field_keys = Text.split(key, #text("."));

        var candid_record = _candid_record;

        for (key in nested_field_keys) {
            let #Record(record_fields) or #Option(#Record(record_fields)) = candid_record else return null;

            let ?found_field = Array.find<(Text, Candid)>(
                record_fields,
                func((variant_name, _) : (Text, Candid)) : Bool {
                    variant_name == key;
                },
            ) else return null;

            candid_record := found_field.1;

            // return #Null if the nested field was terminated early
            if (candid_record == #Null) return ? #Null;
        };

        return ?candid_record;
    };

    func get_nested_candid_type(_schema : Schema, key : Text) : ?Schema {
        let nested_field_keys = Text.split(key, #text("."));

        var schema = _schema;

        for (key in nested_field_keys) {
            let #Record(record_fields) or #Option(#Record(record_fields)) = schema else return null;

            let ?found_field = Array.find<(Text, Schema)>(
                record_fields,
                func((variant_name, _) : (Text, Schema)) : Bool {
                    variant_name == key;
                },
            ) else return null;

            schema := found_field.1;
        };

        return ?schema;
    };

    func candid_record_filter_condition(collection : StableCollection, candid_record : [(Text, Candid.Candid)], lower : [(Text, ?T.State<Candid>)], upper : [(Text, ?T.State<Candid>)]) : Bool {

        for (((key, opt_lower_val), (upper_key, opt_upper_val)) in Itertools.zip(lower.vals(), upper.vals())) {
            assert key == upper_key;

            let ?field_value = get_nested_candid_field(#Record(candid_record), key) else Debug.trap("filter: field '" # debug_show key # "' not found in record");

            switch (opt_lower_val) {
                case (?(#True(lower_val))) {
                    if (cmp_candid(collection.schema, field_value, lower_val) == -1) return false;
                };
                case (?(#False(lower_val))) {
                    if (cmp_candid(collection.schema, field_value, lower_val) < 1) return false;
                };
                case (null) {};
            };

            switch (opt_upper_val) {
                case (?(#True(upper_val))) {
                    if (cmp_candid(collection.schema, field_value, upper_val) == 1) return false;
                };
                case (?(#False(upper_val))) {
                    if (cmp_candid(collection.schema, field_value, upper_val) > -1) return false;
                };
                case (null) {};
            };

        };

        true;
    };

    func multi_filter(
        collection : StableCollection,
        records : Iter<Nat>,
        bounds : Buffer.Buffer<(lower : [(Text, ?T.State<Candid>)], upper : [(Text, ?T.State<Candid>)])>,
    ) : Iter<Nat> {
        Iter.filter<Nat>(
            records,
            func(id : Nat) : Bool {
                let ?candid_lookup_res = lookup_candid_record(collection, id) else Debug.trap("multi_filter: record not found");
                let #Record(candid_record) = candid_lookup_res else Debug.trap("multi_filter: record is not a record");

                var result = true;

                for ((lower, upper) in bounds.vals()) {
                    result := result and candid_record_filter_condition(collection, candid_record, lower, upper);
                };

                result;
            },
        );
    };

    func filter(collection : StableCollection, records : Iter<Nat>, lower : [(Text, ?T.State<Candid>)], upper : [(Text, ?T.State<Candid>)]) : Iter<Nat> {

        Iter.filter<Nat>(
            records,
            func(id : Nat) : Bool {
                let ?candid_lookup_res = lookup_candid_record(collection, id) else Debug.trap("filter: record not found");
                let #Record(candid_record) = candid_lookup_res else Debug.trap("filter: record is not a record");

                candid_record_filter_condition(collection, candid_record, lower, upper);

            },
        );
    };

    func id_to_record_iter<Record>(collection : StableCollection, blobify : Candify<Record>, iter : Iter<Nat>) : Iter<(Nat, Record)> {
        Iter.map<Nat, (Nat, Record)>(
            iter,
            func(id : Nat) : (Nat, Record) {
                let record = lookup_record<Record>(collection, blobify, id);
                (id, record);
            },
        );
    };

    func memorybtree_scan_interval<K, V>(
        btree : MemoryBTree.StableMemoryBTree,
        btree_utils : MemoryBTree.BTreeUtils<K, V>,
        start_key : ?K,
        end_key : ?K,
    ) : (Nat, Nat) {

        let start_rank = switch (start_key) {
            case (?key) switch (MemoryBTree.getExpectedIndex(btree, btree_utils, key)) {
                case (#Found(rank)) rank;
                case (#NotFound(rank)) rank;
            };
            case (null) 0;
        };

        let end_rank = switch (end_key) {
            case (?key) switch (MemoryBTree.getExpectedIndex(btree, btree_utils, key)) {
                case (#Found(rank)) rank + 1;
                case (#NotFound(rank)) rank;
            };
            case (null) MemoryBTree.size(btree);
        };

        (start_rank, end_rank);

    };

    func scan<Record>(
        collection : T.StableCollection,
        index : Index,
        blobify : Candify<Record>,
        start_query : [(Text, ?T.State<Candid>)],
        end_query : [(Text, ?T.State<Candid>)],
    ) : (Nat, Nat) {
        // Debug.print("start_query: " # debug_show start_query);
        // Debug.print("end_query: " # debug_show end_query);

        let index_data_utils = get_index_data_utils(collection, index.key_details);

        func sort_by_key_details(a : (Text, Any), b : (Text, Any)) : Order {
            let pos_a = switch (Array.indexOf<(Text, SortDirection)>((a.0, #Asc), index.key_details, Utils.tuple_eq(Text.equal))) {
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            let pos_b = switch (Array.indexOf<(Text, SortDirection)>((b.0, #Asc), index.key_details, Utils.tuple_eq(Text.equal))) {
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            if (pos_a > pos_b) return #greater;
            if (pos_a < pos_b) return #less;
            #equal;
        };

        let sorted_start_query = Array.sort(start_query, sort_by_key_details);
        let sorted_end_query = Array.sort(end_query, sort_by_key_details);

        let full_start_query = do {

            Array.tabulate<(Candid)>(
                index.key_details.size(),
                func(i : Nat) : (Candid) {
                    if (i >= sorted_start_query.size()) {
                        return (#Minimum);
                    };

                    let key = sorted_start_query[i].0;
                    let ?(#True(val)) or ?(#False(val)) = sorted_start_query[i].1 else return (#Minimum);

                    (val);
                },
            );
        };

        let full_end_query = do {

            Array.tabulate<Candid>(
                index.key_details.size(),
                func(i : Nat) : (Candid) {
                    if (i >= sorted_end_query.size()) {
                        return (#Maximum);
                    };

                    let key = sorted_end_query[i].0;
                    let ?(#True(val)) or ?(#False(val)) = sorted_end_query[i].1 else return (#Maximum);

                    (val);
                },
            );
        };

        let intervals = memorybtree_scan_interval(index.data, index_data_utils, ?full_start_query, ?full_end_query);
        intervals

        // let records_iter = MemoryBTree.scan(index.data, index_data_utils, ?full_start_query, ?full_end_query);

        // let record_ids_iter = Iter.map<([Candid], Nat), Nat>(
        //     records_iter,
        //     func((_, id) : ([Candid], Nat)) : (Nat) { id },
        // );

        // record_ids_iter;

        // return id_to_record_iter(collection, blobify,record_ids_iter );

    };

    func get_best_index_from_query(collection : StableCollection, _query : T.HydraQueryLang, sort_field : ?(Text, T.SortDirection)) : ?Index {
        let db_scan_query = _query;

        let index_frequencies = Map.new<Text, Nat>();

        func explore_operations(db_scan_query : T.HydraQueryLang) {

            switch (db_scan_query) {
                case (#Operation(field, op)) {
                    Debug.trap("don't explore operations directly");
                };

                case (#And(ops)) {
                    let operations = Buffer.Buffer<(Text, T.HqlOperators)>(8);
                    for (op in ops.vals()) {
                        switch (op) {
                            case (#Operation(field, op)) operations.add((field, op));
                            case (#Or(_)) {
                                explore_operations(op);
                            };
                            case (_) Debug.trap("unexpected operation in And");
                        };
                    };

                    let best_index = get_best_index(collection, Buffer.toArray(operations), sort_field);
                    switch (best_index) {
                        case (?best_index) {
                            switch (Map.get(index_frequencies, thash, best_index.index.name)) {
                                case (null) ignore Map.put(index_frequencies, thash, best_index.index.name, 1);
                                case (?prev) ignore Map.put(index_frequencies, thash, best_index.index.name, prev + 1);
                            };
                        };
                        case (null) {};
                    };

                };

                case (#Or(ops)) {
                    for (op in ops.vals()) {
                        switch (op) {
                            case (#Operation(field, op)) {
                                let best_index = get_best_index(collection, [(field, op)], sort_field);

                                switch (best_index) {
                                    case (?best_index) {
                                        switch (Map.get<Text, Nat>(index_frequencies, thash, best_index.index.name)) {
                                            case (null) ignore Map.put(index_frequencies, thash, best_index.index.name, 1);
                                            case (?prev) ignore Map.put(index_frequencies, thash, best_index.index.name, prev + 1);
                                        };
                                    };
                                    case (null) {};
                                };

                            };
                            case (#And(_)) {
                                explore_operations(op);
                            };
                            case (_) Debug.trap("unexpected operation in Or");
                        };

                    };
                };
            };

        };

        explore_operations(db_scan_query);

        if (Map.size(index_frequencies) == 0) {
            null;
        } else {
            var best_index_name = "";
            var best_index_freq = 0;

            for ((index_name, freq) in Map.entries(index_frequencies)) {
                if (freq > best_index_freq) {
                    best_index_name := index_name;
                    best_index_freq := freq;
                };
            };

            Map.get(collection.indexes, thash, best_index_name);

        };
    };

    public class Collection<Record>(collection_name : Text, collection : StableCollection, blobify : T.Candify<Record>) = self {

        public func name() : Text = collection_name;

        public func filter_iter(condition : (Record) -> Bool) : Iter<Record> {

            let iter = MemoryBTree.vals(collection.main, main_btree_utils());
            let records = Iter.map<Blob, Record>(iter, blobify.from_blob);
            let filtered = Iter.filter<Record>(records, condition);

        };

        public func filter(condition : (Record) -> Bool) : [Record] {
            Iter.toArray(filter_iter(condition));
        };

        /// Clear all the data in the collection.
        public func clear() {
            MemoryBTree.clear(collection.main);

            for (index in Map.vals(collection.indexes)) {
                MemoryBTree.clear(index.data);
            };
        };

        public func update_schema(schema : Schema) : Result<(), Text> {

            let is_compatible = is_schema_backward_compatible(collection.schema, schema);
            if (not is_compatible) return #err("Schema is not backward compatible");

            collection.schema := schema;
            // Debug.print("Schema Updated: Ensure to update your Record type as well.");
            #ok;
        };

        public func create_index(_index_key_details : [(Text)]) : Result<(), Text> {

            let index_key_details : [(Text, SortDirection)] = Array.append(
                Array.map<Text, (Text, SortDirection)>(
                    _index_key_details,
                    func(key : Text) : (Text, SortDirection) { (key, #Asc) },
                ),
                [(":record-id", #Asc)],
            );

            // let sorted_index_key_details = Array.sort(index_key_details, func(a : (Text, SortDirection), b : (Text, SortDirection)) : Order { Text.compare(a.0, b.0) });

            let index_name = Text.join(
                "_",
                Iter.map<(Text, SortDirection), Text>(
                    index_key_details.vals(),
                    func((name, dir) : (Text, SortDirection)) : Text {
                        name # (debug_show dir);
                    },
                ),
            );

            switch (Map.get(collection.indexes, thash, index_name)) {
                case (?_) return #err("Index already exists");
                case (null) {};
            };

            let index_data = MemoryBTree.new(?DEFAULT_BTREE_ORDER);

            let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);

            let index_data_utils = get_index_data_utils(collection, index_key_details);

            let candid_map = CandidMap.CandidMap(#Record([]));

            for ((id, candid_blob) in MemoryBTree.entries(collection.main, btree_main_utils)) {
                let candid = decode_candid_blob(collection, candid_blob);
                candid_map.reload(candid);

                let buffer = Buffer.Buffer<(Candid)>(8);

                switch (candid) {
                    case (#Record(records)) {};
                    case (_) return #err("Couldn't get records");
                };

                for ((index_key, dir) in index_key_details.vals()) {

                    if (index_key == ":record-id") {
                        buffer.add(#Nat(id));
                    } else {
                        let ?value = candid_map.get(index_key) else return #err("Couldn't get value for index key: " # debug_show index_key);

                        buffer.add(value);
                    };

                };

                let index_key_values = Buffer.toArray(buffer);
                ignore MemoryBTree.insert(index_data, index_data_utils, index_key_values, id);
            };

            let index : Index = {
                name = index_name;
                key_details = index_key_details;
                data = index_data;
            };

            ignore Map.put<Text, Index>(collection.indexes, thash, index_name, index);

            #ok();
        };

        public func insert(record : Record) : Result<(Nat), Text> {
            put(record);
        };

        public func put(record : Record) : Result<(Nat), Text> {

            let candid_blob = blobify.to_blob(record);
            let candid = decode_candid_blob(collection, candid_blob);

            switch (candid) {
                case (#Record(_)) {};
                case (_) return #err("Values inserted into the collection must be #Records");
            };

            // Debug.print("validate: " # debug_show (collection.schema) #debug_show (candid));
            Utils.assert_result(validate_record(collection.schema, candid));

            let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);

            let id = MemoryBTree.size(collection.main);
            assert null == MemoryBTree.insert<Nat, Blob>(collection.main, btree_main_utils, id, candid_blob);
            // assert MemoryBTree.getId(collection.main, btree_main_utils, id) == ?id;

            if (Map.size(collection.indexes) == 0) return #ok(id);

            let candid_map = CandidMap.CandidMap(candid);

            for (index in Map.vals(collection.indexes)) {

                let buffer = Buffer.Buffer<Candid>(8);

                for ((index_key, dir) in index.key_details.vals()) {

                    if (index_key == ":record-id") {
                        buffer.add(#Nat(id));
                    } else {
                        let ?value = candid_map.get(index_key) else return #err("Couldn't get value for index key: " # debug_show index_key);

                        buffer.add(value);
                    };

                };

                let index_key_values = Buffer.toArray(buffer);

                let index_data_utils = get_index_data_utils(collection, index.key_details);
                ignore MemoryBTree.insert(index.data, index_data_utils, index_key_values, id);
            };

            #ok(id);

        };

        public func get(id : Nat) : Result<Record, Text> {

            let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);

            let ?candid_blob = MemoryBTree.lookupVal(collection.main, btree_main_utils, id) else return #err("Couldn't get record");
            let record = blobify.from_blob(candid_blob);

            #ok(record);
        };

        // func internal_find_best_index(query_builder) : Result<Iter<T.WrapId<Record>>, Text> {
        // };
        func interval_union(a : (Nat, Nat), b : (Nat, Nat)) : (Nat, Nat) {

            let start = Nat.min(a.0, b.0);
            let end = Nat.max(a.1, b.1);

            (start, end);

        };

        func interval_intersect(a : (Nat, Nat), b : (Nat, Nat)) : (Nat, Nat) {

            let start = Nat.max(a.0, a.1);
            let end = Nat.min(a.1, b.1);

            (start, end);

        };

        func intervals_intersect(intervals : Buffer.Buffer<(Nat, Nat)>) : ?(Nat, Nat) {

            var start = intervals.get(0).0;
            var end = intervals.get(0).1;

            var i = 1;

            while (i < intervals.size()) {
                start := Nat.max(start, intervals.get(i).0);
                end := Nat.min(end, intervals.get(i).1);
            };

            if (end < start) return null;

            ?(start, end);
        };

        func intervals_union(intervals : Buffer.Buffer<(Nat, Nat)>) : (Nat, Nat) {

            var start = intervals.get(0).0;
            var end = intervals.get(0).1;

            var i = 1;

            while (i < intervals.size()) {
                start := Nat.min(start, intervals.get(i).0);
                end := Nat.max(end, intervals.get(i).1);
            };

            (start, end);
        };

        func operation_eval(field : Text, op : T.HqlOperators, lower : Map<Text, T.State<Candid>>, upper : Map<Text, T.State<Candid>>) {
            switch (op) {
                case (#eq(candid)) {
                    ignore Map.put(lower, thash, field, #True(candid));
                    ignore Map.put(upper, thash, field, #True(candid));
                };
                case (#In(_) or #Not(_)) {
                    Debug.trap(debug_show op # " not allowed in this context. Should have been expanded by the query builder");
                };
                case (#gte(candid)) {
                    ignore Map.put(lower, thash, field, #True(candid));
                };
                case (#lte(candid)) {
                    ignore Map.put(upper, thash, field, #True(candid));
                };
                case (#lt(candid)) {
                    ignore Map.put(upper, thash, field, #False(candid));
                };
                case (#gt(candid)) {
                    ignore Map.put(lower, thash, field, #False(candid));
                };
            };
        };

        type RecordLimits = [(Text, ?State<Candid>)];
        type FieldLimit = (Text, ?State<Candid>);

        func extract_lower_upper_bounds(lower : Map<Text, T.State<Candid>>, upper : Map<Text, T.State<Candid>>) : ([(Text, ?State<Candid>)], [(Text, ?State<Candid>)]) {
            let lower_bound_size = Map.size(lower);
            let upper_bound_size = Map.size(upper);

            let is_lower_bound_larger = lower_bound_size > upper_bound_size;
            let max_size = Nat.max(lower_bound_size, upper_bound_size);

            let (a, b) = if (is_lower_bound_larger) (lower, upper) else (upper, lower);

            let iter = Map.entries(a);
            let arr1 = Array.tabulate<(Text, ?State<Candid>)>(
                max_size,
                func(i : Nat) : (Text, ?State<Candid>) {
                    let ?(key, value) = iter.next();
                    (key, ?value);
                },
            );

            let iter_2 = Map.entries(a);
            let arr2 = Array.tabulate<(Text, ?State<Candid>)>(
                max_size,
                func(i : Nat) : (Text, ?State<Candid>) {
                    let ?(key, _) = iter_2.next();
                    let value = Map.get(b, thash, key);
                    (key, value);
                },
            );

            if (is_lower_bound_larger) (arr1, arr2) else (arr2, arr1);

        };

        // func best_index_interval_eval(expr : HydraQueryLang, sort_column : ?(Text, T.SortDirection)) : EvalResult {

        //     let best_index = get_best_index_from_query(collection, expr, sort_column);

        //     switch (expr) {
        //         case (#Operation(field, op)) {
        //             Debug.trap("Operation not allowed in this context");
        //         };
        //         case (#And(and_operations)) {
        //             let new_lower = Map.new<Text, State<Candid>>();
        //             let new_upper = Map.new<Text, State<Candid>>();

        //             let bitmaps = Buffer.Buffer<T.BitMap>(8);
        //             let intervals_by_index = Map.new<Text, Buffer.Buffer<(Nat, Nat)>>();
        //             let iterators = Buffer.Buffer<Iter<Nat>>(8);
        //             let full_scan_bounds = Buffer.Buffer<([(Text, ?State<Candid>)], [(Text, ?State<Candid>)])>(8);

        //             var is_sorted = false;
        //             var requires_sorting : Bool = Option.isSome(sort_column);
        //             var operations = Buffer.Buffer<(Text, T.HqlOperators)>(8);

        //             var nested_or_operations = 0;

        //             for (nested_expr in and_operations.vals()) {
        //                 switch (nested_expr) {
        //                     case (#Operation(field, op)) {
        //                         operations.add(field, op);
        //                         operation_eval(field, op, new_lower, new_upper);
        //                     };
        //                     case (#And(_)) Debug.trap("And not allowed in this context");
        //                     case (#Or(_)) {

        //                         nested_or_operations += 1;

        //                         let eval_result = best_index_interval_eval(nested_expr, sort_column);
        //                         switch (eval_result) {
        //                             case (#Empty) return #Empty; // return early if we encounter an empty set
        //                             case (#Ids(iter)) {
        //                                 let bitmap = BitMap.fromIter(iter);
        //                                 bitmaps.add(bitmap);
        //                             };
        //                             case (#BitMap(bitmap)) {
        //                                 bitmaps.add(bitmap);
        //                             };
        //                             case (#Interval(index, intervals)) {
        //                                 for (interval in intervals.vals()) {
        //                                     add_interval(intervals_by_index, index, interval);
        //                                 };
        //                             };
        //                         };
        //                     };
        //                 };
        //             };

        //             if (nested_or_operations < and_operations.size()) {
        //                 let (lower_bound_as_array, upper_bound_as_array) = extract_lower_upper_bounds(new_lower, new_upper);

        //                 switch (best_index) {
        //                     case (?best_index_result) {
        //                         let index = best_index_result.index;
        //                         let requires_additional_filtering = best_index_result.requires_additional_filtering;
        //                         let requires_additional_sorting = best_index_result.requires_additional_sorting;

        //                         let interval = scan(collection, index, blobify, lower_bound_as_array, upper_bound_as_array);

        //                         if (requires_additional_filtering) {
        //                             let record_ids_in_interval = record_ids_from_index_interval(index, interval);
        //                             let filtered_ids = multi_filter(collection, record_ids_in_interval, Buffer.fromArray([(lower_bound_as_array, upper_bound_as_array)]));
        //                             let bitmap = BitMap.fromIter(filtered_ids);
        //                             bitmaps.add(bitmap);
        //                         } else {
        //                             add_interval(intervals_by_index, index.name, interval);
        //                             requires_sorting := requires_sorting or requires_additional_sorting;
        //                         };
        //                     };
        //                     case (null) {
        //                         full_scan_bounds.add((lower_bound_as_array, upper_bound_as_array));
        //                     };
        //                 };
        //             };

        //             for ((index_name, interval_buffer) in Map.entries(intervals_by_index)) {
        //                 switch (intervals_intersect(interval_buffer)) {
        //                     case (?interval) {
        //                         interval_buffer.clear();
        //                         interval_buffer.add(interval);
        //                     };
        //                     case (null) ignore Map.remove(intervals_by_index, thash, index_name);
        //                 };
        //             };

        //             if (bitmaps.size() == 0 and full_scan_bounds.size() == 0 and Map.size(intervals_by_index) <= 1) {

        //                 let merged_results = if (Map.size(intervals_by_index) == 1) {
        //                     let ?(index_name, interval_buffer) = Map.entries(intervals_by_index).next() else Debug.trap("No elements in map when size is greater than 0");
        //                     let interval = interval_buffer.get(0);
        //                     #Interval(index_name, [interval]);
        //                 } else {
        //                     #Empty;
        //                 };

        //                 return merged_results;

        //             };

        //             /**

        //             ! - feature: reduce full scan range by only scanning the intersection with the smallest interval range

        //             var smallest_interval_start = 0;
        //             var smallest_interval_end = 2 ** 64;

        //             var index_with_smallest_interval_range = "";

        //             */

        //             if (full_scan_bounds.size() > 0) {

        //                 var smallest_interval_index = "";
        //                 var smallest_interval_start = 0;
        //                 var smallest_interval_end = 0;
        //                 if (Map.size(intervals_by_index) > 0) {

        //                     var smallest_interval_range = 2 ** 64;

        //                     for ((index_name, interval_buffer) in Map.entries(intervals_by_index)) {
        //                         let interval = interval_buffer.get(0);
        //                         let range = interval.1 - interval.0;
        //                         if (range < smallest_interval_range) {
        //                             smallest_interval_range := range;
        //                             smallest_interval_start := interval.0;
        //                             smallest_interval_end := interval.1;
        //                             smallest_interval_index := index_name;
        //                         };
        //                     };

        //                     for ((index_name, interval_buffer) in Map.entries(intervals_by_index)) {
        //                         if (index_name != smallest_interval_index) {
        //                             let interval = interval_buffer.get(0);
        //                             let intersection = interval_intersect((smallest_interval_start, smallest_interval_end), interval);
        //                             interval_buffer.clear();
        //                             interval_buffer.add(intersection);
        //                         };
        //                     };

        //                 } else {
        //                     let (lower_bound_as_array, upper_bound_as_array) = full_scan_bounds.get(0);
        //                     let full_scan_interval = scan(collection, collection.main, blobify, lower_bound_as_array, upper_bound_as_array);
        //                     smallest_interval_start := full_scan_interval.0;
        //                     smallest_interval_end := full_scan_interval.1;
        //                 };

        //             };
        //         };
        //         case (#Or(_)) Debug.trap("Or not allowed in this context");
        //     };

        //     return #Empty;

        // };

        type EvalResult = {
            #Empty;
            #Ids : Iter<Nat>;
            #BitMap : T.BitMap;
            #Interval : (index : Text, interval : [(Nat, Nat)]);
        };

        func index_intersection_eval(expr : HydraQueryLang, sort_column : ?(Text, T.SortDirection)) : EvalResult {

            func add_interval(intervals_by_index : Map<Text, Buffer.Buffer<(Nat, Nat)>>, index : Text, interval : (Nat, Nat)) {
                let buffer = switch (Map.get(intervals_by_index, thash, index)) {
                    case (?buffer) buffer;
                    case (null) {
                        let buffer = Buffer.Buffer<(Nat, Nat)>(8);
                        ignore Map.put(intervals_by_index, thash, index, buffer);
                        buffer;
                    };
                };

                buffer.add(interval);
            };

            switch (expr) {
                case (#Operation(field, op)) {
                    Debug.trap("Operation not allowed in this context");
                };
                case (#And(and_operations)) {
                    let new_lower = Map.new<Text, State<Candid>>();
                    let new_upper = Map.new<Text, State<Candid>>();

                    let bitmaps = Buffer.Buffer<T.BitMap>(8);
                    let intervals_by_index = Map.new<Text, Buffer.Buffer<(Nat, Nat)>>();
                    let iterators = Buffer.Buffer<Iter<Nat>>(8);
                    let full_scan_bounds = Buffer.Buffer<([(Text, ?State<Candid>)], [(Text, ?State<Candid>)])>(8);

                    var is_sorted = false;
                    var requires_sorting : Bool = Option.isSome(sort_column);
                    var operations = Buffer.Buffer<(Text, T.HqlOperators)>(8);

                    var nested_or_operations = 0;

                    for (nested_expr in and_operations.vals()) {
                        switch (nested_expr) {
                            case (#Operation(field, op)) {
                                operations.add(field, op);
                                operation_eval(field, op, new_lower, new_upper);
                            };
                            case (#And(_)) Debug.trap("And not allowed in this context");
                            case (#Or(_)) {

                                nested_or_operations += 1;

                                let eval_result = index_intersection_eval(nested_expr, sort_column);
                                switch (eval_result) {
                                    case (#Empty) return #Empty; // return early if we encounter an empty set
                                    case (#Ids(iter)) {
                                        // if (requires_sorting) {
                                        // iterators.add(iter);

                                        // } else {
                                        let bitmap = BitMap.fromIter(iter);
                                        bitmaps.add(bitmap);

                                        // };
                                    };
                                    case (#BitMap(bitmap)) {
                                        bitmaps.add(bitmap);
                                    };
                                    case (#Interval(index, intervals)) {
                                        for (interval in intervals.vals()) {
                                            add_interval(intervals_by_index, index, interval);
                                        };
                                    };
                                };
                            };
                        };
                    };

                    if (nested_or_operations < and_operations.size()) {
                        let (lower_bound_as_array, upper_bound_as_array) = extract_lower_upper_bounds(new_lower, new_upper);

                        switch (get_best_index(collection, Buffer.toArray(operations), sort_column)) {
                            case (?best_index_result) {
                                let index = best_index_result.index;
                                let requires_additional_filtering = best_index_result.requires_additional_filtering;
                                let requires_additional_sorting = best_index_result.requires_additional_sorting;

                                let interval = scan(collection, index, blobify, lower_bound_as_array, upper_bound_as_array);

                                if (requires_additional_filtering) {
                                    let record_ids_in_interval = record_ids_from_index_interval(index, interval);
                                    let filtered_ids = multi_filter(collection, record_ids_in_interval, Buffer.fromArray([(lower_bound_as_array, upper_bound_as_array)]));
                                    let bitmap = BitMap.fromIter(filtered_ids);
                                    bitmaps.add(bitmap);
                                } else {
                                    add_interval(intervals_by_index, index.name, interval);
                                    requires_sorting := requires_sorting or requires_additional_sorting;
                                };
                            };
                            case (null) {
                                full_scan_bounds.add((lower_bound_as_array, upper_bound_as_array));
                            };
                        };
                    };

                    for ((index_name, interval_buffer) in Map.entries(intervals_by_index)) {
                        switch (intervals_intersect(interval_buffer)) {
                            case (?interval) {
                                interval_buffer.clear();
                                interval_buffer.add(interval);
                            };
                            case (null) ignore Map.remove(intervals_by_index, thash, index_name);
                        };
                    };

                    if (bitmaps.size() == 0 and full_scan_bounds.size() == 0 and Map.size(intervals_by_index) <= 1) {

                        let merged_results = if (Map.size(intervals_by_index) == 1) {
                            let ?(index_name, interval_buffer) = Map.entries(intervals_by_index).next() else Debug.trap("No elements in map when size is greater than 0");
                            let interval = interval_buffer.get(0);
                            #Interval(index_name, [interval]);
                        } else {
                            #Empty;
                        };

                        return merged_results;

                    };

                    /**
                    ! - feature: reduce full scan range by only scanning the intersection with the smallest interval range

                    var smallest_interval_start = 0;
                    var smallest_interval_end = 2 ** 64;

                    var index_with_smallest_interval_range = "";
                    */

                    if (full_scan_bounds.size() > 0) {

                        var smallest_interval_index = "";
                        var smallest_interval_start = 0;
                        var smallest_interval_end = 0;
                        if (Map.size(intervals_by_index) > 0) {

                            var smallest_interval_range = 2 ** 64;

                            for ((index_name, interval_buffer) in Map.entries(intervals_by_index)) {
                                let interval = interval_buffer.get(0);
                                let range = interval.1 - interval.0 : Nat;

                                if (range < smallest_interval_range) {
                                    smallest_interval_range := range;
                                    smallest_interval_index := index_name;

                                    smallest_interval_start := interval.0;
                                    smallest_interval_end := interval.1;
                                };
                            };
                        };

                        let filtered_ids = if (smallest_interval_index == "") {
                            let filtered_ids = multi_filter(collection, MemoryBTree.keys(collection.main, main_btree_utils()), full_scan_bounds);
                        } else {
                            let ?index = Map.get(collection.indexes, thash, smallest_interval_index) else Debug.trap("Unreachable: IndexMap not found for index: " # smallest_interval_index);
                            let record_ids_in_interval = record_ids_from_index_interval(index, (smallest_interval_start, smallest_interval_end));
                            let filtered_ids = multi_filter(collection, record_ids_in_interval, full_scan_bounds);
                        };

                        let bitmap = BitMap.fromIter(filtered_ids);
                        bitmaps.add(bitmap);

                        // full_scan_bounds.clear();
                    };

                    for ((index_name, interval_buffer) in Map.entries(intervals_by_index)) {
                        let interval = interval_buffer.get(0);
                        let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);

                        let index_data_utils = get_index_data_utils(collection, index.key_details);
                        let record_ids = MemoryBTree.rangeVals(index.data, index_data_utils, interval.0, interval.1);
                        let bitmap = BitMap.fromIter(record_ids);
                        bitmaps.add(bitmap);
                    };

                    if (bitmaps.size() == 0) {
                        #Empty;
                    } else {
                        let bitmap = BitMap.multiIntersect(bitmaps.vals());
                        #BitMap(bitmap);
                    };
                };
                case (#Or(buffer)) {
                    let bitmaps = Buffer.Buffer<T.BitMap>(8);
                    let intervals_by_index = Map.new<Text, Buffer.Buffer<(Nat, Nat)>>();
                    let full_scan_bounds = Buffer.Buffer<([(Text, ?State<Candid>)], [(Text, ?State<Candid>)])>(8);

                    label resolving_or_operations for (expr in buffer.vals()) {
                        let new_lower = Map.new<Text, State<Candid>>();
                        let new_upper = Map.new<Text, State<Candid>>();

                        switch (expr) {
                            case (#Operation(field, op)) {
                                operation_eval(field, op, new_lower, new_upper);
                                let (lower_bound_as_array, upper_bound_as_array) = extract_lower_upper_bounds(new_lower, new_upper);

                                let opt_index = get_best_index(collection, [(field, op)], sort_column);

                                let iter = switch (opt_index) {
                                    case (?best_index_info) {
                                        let index = best_index_info.index;
                                        let requires_additional_filtering = best_index_info.requires_additional_filtering;
                                        let requires_additional_sorting = best_index_info.requires_additional_sorting;

                                        assert requires_additional_filtering == false;

                                        let interval = scan(collection, index, blobify, lower_bound_as_array, upper_bound_as_array);
                                        add_interval(intervals_by_index, index.name, interval);
                                    };
                                    case (null) {
                                        full_scan_bounds.add((lower_bound_as_array, upper_bound_as_array));
                                        continue resolving_or_operations;
                                    };
                                };
                            };
                            case (#And(_)) switch (index_intersection_eval(expr, sort_column)) {
                                case (#Empty) {}; // do nothing if empty set
                                case (#Ids(iter)) {
                                    let bitmap = BitMap.fromIter(iter);
                                    bitmaps.add(bitmap);
                                };
                                case (#BitMap(bitmap)) {
                                    bitmaps.add(bitmap);
                                };
                                case (#Interval(index, intervals)) {
                                    for (interval in intervals.vals()) {
                                        add_interval(intervals_by_index, index, interval);
                                    };
                                    continue resolving_or_operations;
                                };
                            };
                            case (#Or(_)) Debug.trap("Directly nested #Or not allowed in this context");
                        };

                    };

                    //! - feature: merge overlapping intervals

                    if (bitmaps.size() == 0 and full_scan_bounds.size() == 0 and Map.size(intervals_by_index) <= 1) {
                        if (Map.size(intervals_by_index) == 0) return #Ids(Itertools.empty<Nat>());

                        let ?(index_name, interval_buffer) = Map.entries(intervals_by_index).next() else Debug.trap("No elements in map when size is greater than 0");

                        let intervals = Buffer.toArray(interval_buffer);
                        // let non_overlapping_intervals = intervals_union(intervals);
                        return #Interval(index_name, intervals);

                    };

                    for ((index_name, interval_buffer) in Map.entries(intervals_by_index)) {
                        let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);
                        let index_data_utils = get_index_data_utils(collection, index.key_details);

                        for (interval in interval_buffer.vals()) {

                            let record_ids = MemoryBTree.rangeVals(index.data, index_data_utils, interval.0, interval.1);
                            let bitmap = BitMap.fromIter(record_ids);
                            bitmaps.add(bitmap);

                        };
                    };

                    if (full_scan_bounds.size() > 0) {
                        let filtered_ids = multi_filter(collection, MemoryBTree.keys(collection.main, main_btree_utils()), full_scan_bounds);

                        let bitmap = BitMap.fromIter(filtered_ids);
                        bitmaps.add(bitmap);
                    };

                    if (bitmaps.size() == 0) {
                        #Empty;
                    } else {
                        let bitmap = BitMap.multiUnion(bitmaps.vals());
                        #BitMap(bitmap);
                    };

                };
            };
        };

        func record_ids_from_index_interval(index : Index, interval : (Nat, Nat)) : Iter<Nat> {
            let index_data_utils = get_index_data_utils(collection, index.key_details);
            let record_ids = MemoryBTree.rangeVals(index.data, index_data_utils, interval.0, interval.1);
            record_ids;
        };

        func evaluate_query(query_builder : QueryBuilder) : Result<EvalResult, Text> {
            let db_query = query_builder.build();
            let db_scan_query = db_query.query_search;
            let pagination = db_query.pagination;

            switch (Query.validate_query(collection, db_scan_query)) {
                case (#err(err)) return #err("Invalid Query: " # err);
                case (#ok(_)) ();
            };

            let skip = switch (pagination.skip) {
                case (?skip) skip;
                case (null) 0;
            };

            let limit = switch (pagination.limit) {
                case (?limit) limit;
                case (null) 2 ** 64;
            };

            let eval_result = switch (index_intersection_eval(db_scan_query, db_query.sort_by)) {
                case (#Empty) #Empty;
                case (#BitMap(bitmap)) {
                    let bitmap_iter = bitmap.vals();

                    let iter_with_offset = Itertools.skip(bitmap_iter, skip);

                    switch (pagination.limit) {
                        case (?limit) {
                            let iter_with_limit = Itertools.take(iter_with_offset, limit);
                            #Ids(iter_with_limit);
                        };
                        case (null) #Ids(iter_with_offset);
                    };
                };
                case (#Ids(iter)) {
                    let iter_with_offset = Itertools.skip(iter, skip);

                    switch (pagination.limit) {
                        case (?limit) {
                            let iter_with_limit = Itertools.take(iter_with_offset, limit);
                            #Ids(iter_with_limit);
                        };
                        case (null) #Ids(iter_with_offset);
                    };
                };
                case (#Interval(index_name, intervals)) {
                    let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);
                    let index_data_utils = get_index_data_utils(collection, index.key_details);

                    let intervals_in_pagination = Itertools.mapFilter(
                        intervals.vals(),
                        func(interval : (Nat, Nat)) : ?(Nat, Nat) {
                            if (interval.1 > skip and interval.0 < skip + limit) return ?interval;
                            if (interval.0 >= skip + limit) return null;
                            if (interval.1 <= skip) return null;

                            let start = Nat.max(interval.0, skip);
                            let end = Nat.min(interval.1, skip + limit);

                            ?(start, end);
                        },
                    );

                    let record_ids_from_returned_intervals = Itertools.flatten(
                        Iter.map(
                            intervals_in_pagination,
                            func(interval : (Nat, Nat)) : Iter<(Nat)> {
                                let record_ids = MemoryBTree.rangeVals(index.data, index_data_utils, interval.0, interval.1);
                                record_ids;
                            },
                        )
                    );

                    #Ids(record_ids_from_returned_intervals);
                };
            };

            #ok(eval_result);
        };

        func internal_find(query_builder : QueryBuilder) : Result<Iter<Nat>, Text> {

            let eval_result = switch (evaluate_query(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(eval_result)) eval_result;
            };

            let record_ids_iter = switch (eval_result) {
                case (#Empty) Itertools.empty<Nat>();
                case (#BitMap(bitmap)) bitmap.vals();
                case (#Ids(iter)) iter;
                case (#Interval(index_name, intervals)) {
                    let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);
                    let index_data_utils = get_index_data_utils(collection, index.key_details);

                    let record_ids_from_returned_intervals = Itertools.flatten(
                        Iter.map(
                            intervals.vals(),
                            func(interval : (Nat, Nat)) : Iter<(Nat)> {
                                let record_ids = MemoryBTree.rangeVals(index.data, index_data_utils, interval.0, interval.1);
                                record_ids;
                            },
                        )
                    );
                };
            };

            #ok(record_ids_iter);
        };

        public func find_iter(query_builder : QueryBuilder) : Result<Iter<T.WrapId<Record>>, Text> {
            switch (internal_find(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = id_to_record_iter(collection, blobify, record_ids_iter);
                    #ok(record_iter);
                };
            };
        };

        public func find(query_builder : QueryBuilder) : Result<[T.WrapId<Record>], Text> {
            switch (internal_find(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(record_ids_iter)) {
                    let record_iter = id_to_record_iter(collection, blobify, record_ids_iter);
                    let records = Iter.toArray(record_iter);
                    #ok(records);
                };
            };
        };

        public func getBestIndex(db_query : QueryBuilder) : ?Index {
            let _query = db_query.build();

            get_best_index_from_query(collection, _query.query_search, _query.sort_by);
        };

        public func count(query_builder : QueryBuilder) : Result<Nat, Text> {
            let eval_result = switch (evaluate_query(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(eval_result)) {
                    eval_result;
                };
            };

            let count = switch (eval_result) {
                case (#Empty) 0;
                case (#BitMap(bitmap)) bitmap.size();
                case (#Ids(iter)) Iter.size(iter);
                case (#Interval(index_name, intervals)) {

                    var i = 0;
                    var sum = 0;
                    while (i < intervals.size()) {
                        sum += intervals.get(i).1 - intervals.get(i).0;
                        i := i + 1;
                    };

                    sum;
                };
            };

            #ok(count);
        };

        public func updateById(id : Nat, update_fn : (Record) -> Record) : Result<(), Text> {
            let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);

            let ?prev_candid_blob = MemoryBTree.lookupVal(collection.main, btree_main_utils, id);
            let prev_record = blobify.from_blob(prev_candid_blob);
            // let prev_record = lookup_record<Record>(collection, blobify, id);

            let new_record = update_fn(prev_record);

            let new_candid_blob = blobify.to_blob(new_record);
            let new_candid = decode_candid_blob(collection, new_candid_blob);

            // not needed since it uses the same record type
            Utils.assert_result(validate_record(collection.schema, new_candid));

            assert ?prev_candid_blob == MemoryBTree.insert<Nat, Blob>(collection.main, btree_main_utils, id, new_candid_blob);
            let prev_candid = decode_candid_blob(collection, prev_candid_blob);

            let #Record(prev_records) = prev_candid else return #err("Couldn't get records");
            let #Record(new_records) = new_candid else return #err("Couldn't get records");

            for (index in Map.vals(collection.indexes)) {

                let prev_index_key_values = get_index_columns(collection, index.key_details, id, prev_records);
                let index_data_utils = get_index_data_utils(collection, index.key_details);

                assert ?id == MemoryBTree.remove(index.data, index_data_utils, prev_index_key_values);

                let new_index_key_values = get_index_columns(collection, index.key_details, id, new_records);
                ignore MemoryBTree.insert(index.data, index_data_utils, new_index_key_values, id);
            };

            #ok;
        };

        public func update(query_builder : QueryBuilder, update_fn : (Record) -> Record) : Result<(), Text> {

            let records_iter = switch (internal_find(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            for ((id) in records_iter) {
                let #ok(_) = updateById(id, update_fn);
            };

            #ok;
        };

        public func deleteById(id : Nat) : Result<Record, Text> {
            let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);

            let ?prev_candid_blob = MemoryBTree.remove<Nat, Blob>(collection.main, btree_main_utils, id);
            let prev_candid = decode_candid_blob(collection, prev_candid_blob);

            let #Record(prev_records) = prev_candid else return #err("Couldn't get records");
            // Debug.print("prev_records: " # debug_show prev_records);
            for (index in Map.vals(collection.indexes)) {

                let prev_index_key_values = get_index_columns(collection, index.key_details, id, prev_records);
                let index_data_utils : BTreeUtils<[Candid], RecordPointer> = get_index_data_utils(collection, index.key_details);

                assert ?id == MemoryBTree.remove<[Candid], RecordPointer>(index.data, index_data_utils, prev_index_key_values);
            };

            let prev_record = blobify.from_blob(prev_candid_blob);
            #ok(prev_record);
        };

        public func delete(query_builder : QueryBuilder) : Result<[Record], Text> {

            // let db_query = query_builder.build();
            let results_iter = switch (internal_find(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            let buffer = Buffer.Buffer<Record>(8);
            for ((id) in results_iter) {
                // Debug.print("deleting record: " # debug_show (id));
                let #ok(record) = deleteById(id);
                buffer.add(record);
            };

            #ok(Buffer.toArray(buffer));
        };

        // public func find()
    };

};
