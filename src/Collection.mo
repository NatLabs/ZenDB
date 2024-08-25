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
    public type Direction = T.Direction;
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

    func lookup_record<Record>(collection : T.StableCollection, blobify : T.Candify<Record>, id : Nat) : Record {
        let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);
        let ?record_candid_blob = MemoryBTree.lookupVal(collection.main, btree_main_utils, id);
        let record = blobify.from_blob(record_candid_blob);
        record;
    };

    func lookup_candid_record(collection : StableCollection, id : Nat) : ?Candid {
        let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);
        let ?candid_blob = MemoryBTree.lookupVal(collection.main, btree_main_utils, id);
        let candid = decode_candid_blob(collection, candid_blob);

        ?candid;
    };

    public func get_index_data_utils(collection : StableCollection, index_key_details : [(Text, Direction)]) : MemoryBTree.BTreeUtils<IndexKeyFields, RecordPointer> {

        let key_utils = get_index_key_utils(collection, index_key_details);
        let value_utils = TypeUtils.BigEndian.Nat;

        MemoryBTree.createUtils(key_utils, value_utils);

    };

    public func get_index_key_utils(collection : StableCollection, index_key_details : [(Text, Direction)]) : TypeUtils<IndexKeyFields> {
        let index_key_utils : TypeUtils.TypeUtils<IndexKeyFields> = {
            blobify = {
                from_blob = func(b : Blob) : IndexKeyFields {
                    let ?res : ?IndexKeyFields = from_candid (b);
                    res;
                };
                to_blob = func(index_key_detail : IndexKeyFields) : Blob {
                    to_candid (index_key_detail);
                };
            };
            cmp = #GenCmp(
                func(a : IndexKeyFields, b : IndexKeyFields) : Int8 {
                    var cmp_result : Int8 = 0;
                    // Debug.print("index key details: " # debug_show (index_key_details));
                    // Debug.print("a: " # debug_show (a));
                    // Debug.print("b: " # debug_show (b));

                    for (i in Itertools.range(0, Nat.min(a.size(), b.size()))) {
                        let (key, val_a) = a[i];
                        let (key2, val_b) = b[i];

                        let (index_key, dir) = index_key_details[i];
                        // Debug.print(
                        //     debug_show ("cmp keys: ", key, key2, index_key)
                        // );

                        assert key == index_key or key2 == index_key;

                        cmp_result := switch (val_a, val_b) {
                            case (#Array(_) or #Record(_) or #Tuple(_), _) {
                                Debug.print("index key details: " # debug_show (index_key_details));

                                Debug.trap("cmp: unexpected candid type in index key: " # debug_show (val_a, val_b));
                            };
                            case (_, #Array(_) or #Record(_) or #Tuple(_)) {
                                Debug.print("index key details: " # debug_show (index_key_details));
                                Debug.trap("cmp: unexpected candid type in index key: " # debug_show (val_a, val_b));
                            };
                            case (val_a, val_b) {
                                let #Record(schema) = collection.schema else Debug.trap("cmp: schema is not a record");

                                // Debug.print("extracting nested schema for key: " # debug_show index_key);

                                if (index_key == ":record-id") {
                                    cmp_candid(#Nat, a[i].1, b[i].1);
                                } else {
                                    let ?nested_schema_type = get_nested_candid_type(#Record(schema), index_key) else Debug.trap("cmp: nested schema not found");

                                    // Debug.print("nested_schema_type: " # debug_show nested_schema_type);

                                    cmp_candid(nested_schema_type, val_a, val_b);
                                };

                            };
                        };

                        if (cmp_result != 0) {
                            if (dir == #Desc) return -cmp_result;
                            return cmp_result;
                        };
                    };

                    cmp_result;
                }
            );
        };

    };

    func main_btree_utils() : BTreeUtils<Nat, Blob> {
        MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);
    };

    func decode_candid_blob(collection : StableCollection, candid_blob : Blob) : Candid {
        let candid_result = Decoder.one_shot(candid_blob, collection.schema_keys, null);
        let #ok(candid_values) = candid_result;
        let candid = candid_values[0];
        candid;
    };

    func get_index_columns(collection : StableCollection, index_key_details : [(Text, Direction)], id : Nat, records : [(Text, Candid)]) : IndexKeyFields {
        let buffer = Buffer.Buffer<(Text, Candid)>(8);

        for ((index_key, dir) in index_key_details.vals()) {
            for ((key, value) in records.vals()) {
                if (key == index_key) {
                    buffer.add((key, value));
                };
            };
        };

        buffer.add((":record-id", #Nat(id)));

        Buffer.toArray(buffer);
    };

    func get_best_index(collection : StableCollection, _query : [(Text, Any)]) : ?Index {

        var best_score = 0;
        var best_index : ?Index = null;

        for (index in Map.vals(collection.indexes)) {
            var index_score = 0;

            for ((query_key, query_value) in _query.vals()) {
                label nested_for_loop for ((index_key, _) in index.key_details.vals()) {
                    if (query_key == index_key) {
                        index_score += 1;
                    } else break nested_for_loop;
                };
            };

            if (index_score > best_score) {
                best_score := index_score;
                best_index := ?index;
            };
        };

        if (best_score == 0) return null;

        switch (best_index) {
            case (null) return null;
            case (?index) ?(index);
        };
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

    func filter(collection : StableCollection, records : Iter<Nat>, lower : [(Text, ?T.State<Candid>)], upper : [(Text, ?T.State<Candid>)]) : Iter<Nat> {
        // Debug.print("filter: lower: " # debug_show lower);
        // Debug.print("filter: upper: " # debug_show upper);

        Iter.filter<Nat>(
            records,
            func(id : Nat) : Bool {

                let ?candid_lookup_res = lookup_candid_record(collection, id) else Debug.trap("filter: record not found");
                let #Record(candid_record) = candid_lookup_res else Debug.trap("filter: record is not a record");

                for (((key, opt_lower_val), (upper_key, opt_upper_val)) in Itertools.zip(lower.vals(), upper.vals())) {
                    assert key == upper_key;

                    let ?field_value = get_nested_candid_field(candid_lookup_res, key) else Debug.trap("filter: field '" # debug_show key # "' not found in record");

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
            case (?key) MemoryBTree.getIndex(btree, btree_utils, key);
            case (null) 0;
        };

        let end_rank = switch (end_key) {
            case (?key) MemoryBTree.getIndex(btree, btree_utils, key) + 1; // +1 because the end is exclusive
            case (null) MemoryBTree.size(btree);
        };

        (start_rank, end_rank);

    };

    // func scan_interval<Record>(
    //     collection : T.StableCollection,
    //     blobify : Candify<Record>,
    //     start_query : [(Text, ?T.State<Candid>)],
    //     end_query : [(Text, ?T.State<Candid>)],
    // ) : (Nat, Nat) {
    //     // Debug.print("start_query: " # debug_show start_query);
    //     // Debug.print("end_query: " # debug_show end_query);

    //     let opt_start_index = get_best_index(collection, start_query);
    //     let opt_end_index = get_best_index(collection, end_query);

    //     // Debug.print("opt_start_index: " # debug_show Option.map(opt_start_index, func(index : Index) : Text { index.name }));
    //     // Debug.print("opt_end_index: " # debug_show Option.map(opt_end_index, func(index : Index) : Text { index.name }));

    //     let index = switch (opt_start_index, opt_end_index) {
    //         case (?(start_index), ?(end_index)) {
    //             start_index;
    //         };
    //         case (?(index), _) index;
    //         case (_, ?(index)) index;
    //         case (_) {
    //             // Debug.print("No index found. Attempting to scan main collection");
    //             let keys = MemoryBTree.keys(collection.main, MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob));
    //             // Debug.print("start_query: " # debug_show start_query);
    //             // Debug.print("end_query: " # debug_show end_query);

    //             let filtered = filter(collection, keys, start_query, end_query);
    //             return id_to_record_iter(collection, blobify, filtered);
    //         };
    //     };

    //     let index_data_utils = get_index_data_utils(collection, index.key_details);

    //     func sort_by_key_details(a : (Text, Any), b : (Text, Any)) : Order {
    //         let pos_a = switch (Array.indexOf<(Text, Direction)>((a.0, #Asc), index.key_details, Utils.tuple_eq(Text.equal))) {
    //             case (?pos) pos;
    //             case (null) index.key_details.size();
    //         };

    //         let pos_b = switch (Array.indexOf<(Text, Direction)>((b.0, #Asc), index.key_details, Utils.tuple_eq(Text.equal))) {
    //             case (?pos) pos;
    //             case (null) index.key_details.size();
    //         };

    //         if (pos_a > pos_b) return #greater;
    //         if (pos_a < pos_b) return #less;
    //         #equal;
    //     };

    //     let sorted_start_query = Array.sort(start_query, sort_by_key_details);
    //     let sorted_end_query = Array.sort(end_query, sort_by_key_details);

    //     let full_start_query = do {
    //         let ?(min_key_fields, min_id) = MemoryBTree.getMin(index.data, index_data_utils);

    //         Array.tabulate<(Text, Candid)>(
    //             index.key_details.size(),
    //             func(i : Nat) : (Text, Candid) {
    //                 if (i >= sorted_start_query.size()) {
    //                     return (index.key_details[i].0, #Minimum);
    //                 };
    //                 // if (i == sorted_start_query.size()) return (":record-id", #Nat(0));

    //                 let key = sorted_start_query[i].0;
    //                 let ?(#True(val)) or ?(#False(val)) = sorted_start_query[i].1 else return (key, min_key_fields[i].1);

    //                 (key, val);
    //             },
    //         );
    //     };

    //     let full_end_query = do {
    //         let ?(max_key_fields, max_id) = MemoryBTree.getMax(index.data, index_data_utils);

    //         Array.tabulate<(Text, Candid)>(
    //             index.key_details.size(),
    //             func(i : Nat) : (Text, Candid) {
    //                 // if (i == sorted_end_query.size()) return (":record-id", #Nat(2 ** 64));

    //                 if (i >= sorted_end_query.size()) {
    //                     return (index.key_details[i].0, #Maximum);
    //                 };

    //                 let key = sorted_end_query[i].0;
    //                 let ?(#True(val)) or ?(#False(val)) = sorted_end_query[i].1 else return (key, max_key_fields[i].1);

    //                 (key, val);
    //             },
    //         );
    //     };

    //     let intervals = memorybtree_scan_interval(index.data, index_data_utils, ?full_start_query, ?full_end_query);
    //     intervals

    // };

    func scan<Record>(
        collection : T.StableCollection,
        blobify : Candify<Record>,
        start_query : [(Text, ?T.State<Candid>)],
        end_query : [(Text, ?T.State<Candid>)],
    ) : Iter<Nat> {
        // Debug.print("start_query: " # debug_show start_query);
        // Debug.print("end_query: " # debug_show end_query);

        let opt_start_index = get_best_index(collection, start_query);
        let opt_end_index = get_best_index(collection, end_query);

        // Debug.print("opt_start_index: " # debug_show Option.map(opt_start_index, func(index : Index) : Text { index.name }));
        // Debug.print("opt_end_index: " # debug_show Option.map(opt_end_index, func(index : Index) : Text { index.name }));

        let index : Index = switch (opt_start_index, opt_end_index) {
            case (?(index), _) index;
            case (_, ?(index)) index;
            case (_) {
                // Debug.print("No index found. Attempting to scan main collection");
                let keys = MemoryBTree.keys(collection.main, MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob));
                // Debug.print("start_query: " # debug_show start_query);
                // Debug.print("end_query: " # debug_show end_query);

                let filtered_ids = filter(collection, keys, start_query, end_query);

                return filtered_ids;

                // return id_to_record_iter(collection, blobify, filtered_ids);
            };
        };

        let index_data_utils = get_index_data_utils(collection, index.key_details);

        func sort_by_key_details(a : (Text, Any), b : (Text, Any)) : Order {
            let pos_a = switch (Array.indexOf<(Text, Direction)>((a.0, #Asc), index.key_details, Utils.tuple_eq(Text.equal))) {
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            let pos_b = switch (Array.indexOf<(Text, Direction)>((b.0, #Asc), index.key_details, Utils.tuple_eq(Text.equal))) {
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
            let ?(min_key_fields, min_id) = MemoryBTree.getMin(index.data, index_data_utils);

            Array.tabulate<(Text, Candid)>(
                index.key_details.size(),
                func(i : Nat) : (Text, Candid) {
                    if (i >= sorted_start_query.size()) {
                        return (index.key_details[i].0, #Minimum);
                    };

                    let key = sorted_start_query[i].0;
                    let ?(#True(val)) or ?(#False(val)) = sorted_start_query[i].1 else return (key, min_key_fields[i].1);

                    (key, val);
                },
            );
        };

        let full_end_query = do {
            let ?(max_key_fields, max_id) = MemoryBTree.getMax(index.data, index_data_utils);

            Array.tabulate<(Text, Candid)>(
                index.key_details.size(),
                func(i : Nat) : (Text, Candid) {
                    if (i >= sorted_end_query.size()) {
                        return (index.key_details[i].0, #Maximum);
                    };

                    let key = sorted_end_query[i].0;
                    let ?(#True(val)) or ?(#False(val)) = sorted_end_query[i].1 else return (key, max_key_fields[i].1);

                    (key, val);
                },
            );
        };

        let records_iter = MemoryBTree.scan(index.data, index_data_utils, ?full_start_query, ?full_end_query);

        let record_ids_iter = Iter.map<(IndexKeyFields, Nat), Nat>(
            records_iter,
            func((_, id) : (IndexKeyFields, Nat)) : (Nat) { id },
        );

        record_ids_iter;

        // return id_to_record_iter(collection, blobify,record_ids_iter );

    };

    public class Collection<Record>(collection_name : Text, collection : StableCollection, blobify : T.Candify<Record>) = self {

        public func name() : Text = collection_name;

        public func filter(condition : (Record) -> Bool) : [Record] {

            let iter = MemoryBTree.vals(collection.main, main_btree_utils());
            let records = Iter.map<Blob, Record>(iter, blobify.from_blob);
            let filtered = Iter.filter<Record>(records, condition);

            Iter.toArray(filtered);
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

            let index_key_details : [(Text, Direction)] = Array.append(
                Array.map<Text, (Text, Direction)>(
                    _index_key_details,
                    func(key : Text) : (Text, Direction) { (key, #Asc) },
                ),
                [(":record-id", #Asc)],
            );

            // let sorted_index_key_details = Array.sort(index_key_details, func(a : (Text, Direction), b : (Text, Direction)) : Order { Text.compare(a.0, b.0) });

            let index_name = Text.join(
                "_",
                Iter.map<(Text, Direction), Text>(
                    index_key_details.vals(),
                    func((name, dir) : (Text, Direction)) : Text {
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

            for ((id, candid_blob) in MemoryBTree.entries(collection.main, btree_main_utils)) {
                let candid = decode_candid_blob(collection, candid_blob);

                let buffer = Buffer.Buffer<(Text, Candid)>(8);

                let records = switch (candid) {
                    case (#Record(records)) records;
                    case (_) return #err("Couldn't get records");
                };

                for ((index_key, dir) in index_key_details.vals()) {

                    for ((key, value) in records.vals()) {

                        if (key == index_key) {
                            buffer.add((key, value));
                        };
                    };
                };

                buffer.add((":record-id", #Nat(id)));

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

            // Debug.print("validate: " # debug_show (collection.schema) #debug_show (candid));
            Utils.assert_result(validate_record(collection.schema, candid));

            let btree_main_utils = MemoryBTree.createUtils(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);

            let id = MemoryBTree.nextId(collection.main);
            assert null == MemoryBTree.insert<Nat, Blob>(collection.main, btree_main_utils, id, candid_blob);
            // assert MemoryBTree.getId(collection.main, btree_main_utils, id) == ?id;

            let records = switch (candid) {
                case (#Record(records)) records;
                case (_) return #err("Couldn't get records");
            };

            for (index in Map.vals(collection.indexes)) {

                let buffer = Buffer.Buffer<(Text, Candid)>(8);

                for ((index_key, dir) in index.key_details.vals()) {
                    for ((key, value) in records.vals()) {

                        if (key == index_key) {
                            buffer.add((key, value));
                        } else if (Text.startsWith(index_key, #text(key)) and Text.contains(index_key, #text("."))) {
                            // nested field
                            let opt_nested_field_value = get_nested_candid_field(candid, index_key);

                            // Debug.print("nested_field_value: " # debug_show (index_key, opt_nested_field_value));

                            switch (opt_nested_field_value) {
                                case (?nested_field_value) {
                                    buffer.add((index_key, nested_field_value));
                                };
                                case (null) Debug.trap("Couldn't get nested field value for index key: " # debug_show (index_key, candid));
                            };

                        };
                    };
                };

                buffer.add((":record-id", #Nat(id)));

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

        func interval_intersection(a : (Nat, Nat), b : (Nat, Nat)) : (Nat, Nat) {

            let start = Nat.max(a.0, a.1);
            let end = Nat.min(a.1, b.1);

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

        // func best_index_interval_eval(expr : HydraQueryLang) : (Nat, Nat) {
        //     switch (expr) {
        //         case (#Operation(field, op)) {
        //             Debug.trap("Operation not allowed in this context");
        //         };
        //         case (#And(and_operations)) {
        //             let group_intervals_by_index = Map.new<Text, Buffer.Buffer<(Nat, Nat)>>(8);
        //             let new_lower = Map.new<Text, State<Candid>>();
        //             let new_upper = Map.new<Text, State<Candid>>();

        //             let intervals = Buffer.Buffer<(Nat, Nat)>(8);
        //             var nested_or_operations = 0;

        //             for (expr in and_operations.vals()) {
        //                 switch (expr) {
        //                     case (#Operation(field, op)) {
        //                         operation_eval(field, op, new_lower, new_upper);
        //                     };
        //                     case (#And(_)) Debug.trap("And not allowed in this context");
        //                     case (#Or(_)) {
        //                         let or_interval = best_index_interval_eval(expr);
        //                         intervals.add(or_interval);
        //                         nested_or_operations += 1;
        //                     };
        //                 };

        //             };

        //             if (nested_or_operations < and_operations.size()) {
        //                 let (lower_bound_as_array, upper_bound_as_array) = extract_lower_upper_bounds(new_lower, new_upper);
        //                 let interval = scan_interval(collection, blobify, lower_bound_as_array, upper_bound_as_array);
        //                 intervals.add(interval);
        //             };

        //             interval_intersection(intervals);
        //         };
        //         case (#Or(or_operations)) {

        //             let intervals = Buffer.Buffer<(Nat, Nat)>(8);

        //             for (expr in or_operations.vals()) {
        //                 let new_lower = Map.new<Text, State<Candid>>();
        //                 let new_upper = Map.new<Text, State<Candid>>();

        //                 let interval = switch (expr) {
        //                     case (#Operation(field, op)) {
        //                         operation_eval(field, op, new_lower, new_upper);
        //                         let (lower_bound_as_array, upper_bound_as_array) = extract_lower_upper_bounds(new_lower, new_upper);
        //                         scan_interval(collection, blobify, lower_bound_as_array, upper_bound_as_array);
        //                     };
        //                     case (#And(_)) best_index_interval_eval(expr);
        //                     case (#Or(_)) Debug.trap("Directly nested #Or not allowed in this context");
        //                 };

        //                 intervals.add(interval);

        //             };

        //             interval_union(intervals);

        //         };
        //     };
        // };

        type EvalResult = {
            #Ids : Iter<Nat>;
            #Interval : (index : Text, interval : (Nat, Nat));
        };

        func index_intersection_eval(expr : HydraQueryLang, id_encoder : IdEncoder) : Iter<Nat> {
            switch (expr) {
                case (#Operation(field, op)) {
                    Debug.trap("Operation not allowed in this context");
                };
                case (#And(and_operations)) {
                    let new_lower = Map.new<Text, State<Candid>>();
                    let new_upper = Map.new<Text, State<Candid>>();

                    let bitmaps = Buffer.Buffer<T.BitMap>(8);
                    let intervals_by_index = Map.new<Text, Buffer.Buffer<(Nat, Nat)>>();

                    var nested_or_operations = 0;

                    for (nested_expr in and_operations.vals()) {
                        switch (nested_expr) {
                            case (#Operation(field, op)) {
                                operation_eval(field, op, new_lower, new_upper);
                            };
                            case (#And(_)) Debug.trap("And not allowed in this context");
                            case (#Or(_)) {
                                let encoded_iter = index_intersection_eval(nested_expr, id_encoder);
                                let bitmap = BitMap.fromIter(encoded_iter);
                                bitmaps.add(bitmap);
                                nested_or_operations += 1;
                            };
                        };

                    };

                    if (nested_or_operations < and_operations.size()) {
                        let (lower_bound_as_array, upper_bound_as_array) = extract_lower_upper_bounds(new_lower, new_upper);

                        let and_id_iter_results = scan(collection, blobify, lower_bound_as_array, upper_bound_as_array);
                        let encoded_iter = id_encoder.encode_iter(and_id_iter_results);
                        let bitmap = BitMap.fromIter(encoded_iter);
                        bitmaps.add(bitmap);
                    };

                    if (bitmaps.size() == 0) {
                        Itertools.empty<Nat>();
                    } else if (bitmaps.size() == 1) {
                        bitmaps.get(0).vals();
                    } else {
                        let bitmap = bitmaps.get(0);

                        var i = 1;
                        while (i < bitmaps.size()) {
                            bitmap.intersect(bitmaps.get(i));
                            i += 1;
                        };

                        bitmap.vals();

                    };
                };
                case (#Or(buffer)) {
                    let bitmaps = Buffer.Buffer<T.BitMap>(8);

                    for (expr in buffer.vals()) {
                        let new_lower = Map.new<Text, State<Candid>>();
                        let new_upper = Map.new<Text, State<Candid>>();

                        switch (expr) {
                            case (#Operation(field, op)) {
                                operation_eval(field, op, new_lower, new_upper);
                                let (lower_bound_as_array, upper_bound_as_array) = extract_lower_upper_bounds(new_lower, new_upper);
                                let iter = scan(collection, blobify, lower_bound_as_array, upper_bound_as_array);

                                let encoded_iter = id_encoder.encode_iter(iter);
                                let bitmap = BitMap.fromIter(encoded_iter);
                                bitmaps.add(bitmap);
                            };
                            case (#And(_)) {
                                let encoded_iter = index_intersection_eval(expr, id_encoder);
                                let bitmap = BitMap.fromIter(encoded_iter);
                                bitmaps.add(bitmap);
                            };
                            case (#Or(_)) Debug.trap("Directly nested #Or not allowed in this context");
                        };

                    };

                    if (bitmaps.size() == 0) {
                        Itertools.empty<Nat>();
                    } else if (bitmaps.size() == 1) {
                        bitmaps.get(0).vals();
                    } else {
                        let bitmap = bitmaps.get(0);

                        var i = 1;
                        while (i < bitmaps.size()) {
                            bitmap.union(bitmaps.get(i));
                            i += 1;
                        };

                        bitmap.vals();

                    };

                };
            };
        };

        class IdEncoder() {

            // mapping calculated using the size of each key-value block
            // from the documentation we know that each block uses 15 bytes + the size of the serialized key
            // in our case our key is always a Nat64 which is 8 bytes
            // so each block uses 23 bytes
            // https://github.com/NatLabs/memory-collection/blob/main/src/MemoryBTree/readme.md#key-value-region

            let REGION_HEADER = 64;
            let KEY_BLOCK_SIZE = 23;

            public func encode(id : Nat) : Nat {
                (id - REGION_HEADER) / KEY_BLOCK_SIZE;
            };

            public func decode(n : Nat) : Nat {
                (n * KEY_BLOCK_SIZE) + REGION_HEADER;
            };

            public func encode_iter(iter : Iter<Nat>) : Iter<Nat> {
                Iter.map<Nat, Nat>(
                    iter,
                    func(id : Nat) : Nat {
                        encode(id);
                    },
                );
            };

            public func decode_iter(iter : Iter<Nat>) : Iter<Nat> {
                Iter.map<Nat, Nat>(
                    iter,
                    func(n : Nat) : Nat {
                        decode(n);
                    },
                );
            };

        };

        func internal_find(query_builder : QueryBuilder) : Result<Iter<T.WrapId<Record>>, Text> {
            var limit = 1000;
            var batch_size = 100;
            var skip = 0;

            let db_query = query_builder.build();

            switch (Query.validate_query(collection, db_query)) {
                case (#err(err)) return #err("Invalid Query: " # err);
                case (#ok(_)) ();
            };

            let id_encoder = IdEncoder();
            let encoded_record_ids_iter = index_intersection_eval(db_query, id_encoder);
            let decoded_record_ids_iter = id_encoder.decode_iter(encoded_record_ids_iter);
            let id_record_pairs_iter = id_to_record_iter(collection, blobify, decoded_record_ids_iter);

            #ok(id_record_pairs_iter);

        };

        public func find(query_builder : QueryBuilder) : Result<[T.WrapId<Record>], Text> {
            switch (internal_find(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(iter)) {
                    #ok(Iter.toArray(iter));
                };
            };
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

            let db_query = query_builder.build();
            let records_iter = switch (internal_find(query_builder)) {
                case (#err(err)) return #err(err);
                case (#ok(records_iter)) records_iter;
            };

            for ((id, record) in records_iter) {
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
                let index_data_utils : BTreeUtils<IndexKeyFields, RecordPointer> = get_index_data_utils(collection, index.key_details);

                assert ?id == MemoryBTree.remove<IndexKeyFields, RecordPointer>(index.data, index_data_utils, prev_index_key_values);
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
            for ((id, record) in results_iter) {
                // Debug.print("deleting record: " # debug_show (id));
                let #ok(_) = deleteById(id);
                buffer.add(record);
            };

            #ok(Buffer.toArray(buffer));
        };

        // public func find()
    };

};
