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
import Serde "mo:serde";
import Record "mo:serde/Candid/Text/Parser/Record";
import Variant "mo:serde/Candid/Text/Parser/Variant";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Tag "mo:candid/Tag";

import MemoryIdBTree "memory-buffer/src/MemoryIdBTree/Versioned";
import BTreeUtils "memory-buffer/src/MemoryBTree/BTreeUtils";
import MemoryBTreeIndex "memory-buffer/src/MemoryBTreeIndex/Versioned";
import IndexUtils "memory-buffer/src/MemoryBTreeIndex/IndexUtils";
import Int8Cmp "memory-buffer/src/Int8Cmp";

module {
    public type Map<K, V> = Map.Map<K, V>;
    let { thash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    public type MemoryBTreeIndex = MemoryBTreeIndex.VersionedMemoryBTreeIndex;
    public type MemoryIdBTree = MemoryIdBTree.VersionedMemoryIdBTree;
    public type BTreeUtils<K, V> = BTreeUtils.BTreeUtils<K, V>;
    public type IndexUtils<A> = IndexUtils.IndexUtils<A>;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = {
        #Text;
        #Nat;
        #Int;
        #Float;
        #Bool;
        #Option : Schema;
        #Array : Schema;
        #Tuple : (Schema, Schema);
        #Triple : (Schema, Schema, Schema);
        #Quadruple : (Schema, Schema, Schema, Schema);
        #Record : [(Text, Schema)];
        #Variant : [(Text, Schema)];
        #Principal;
        #Empty; // used to represent the absence of a schema. the reflected type in motoko is -> `()`
        #Null;
    };

    public type Tuple<A, B> = { _0_ : A; _1_ : B };
    public func Tuple<A, B>(a : A, b : B) : Tuple<A, B> {
        { _0_ = a; _1_ = b };
    };

    public type Triple<A, B, C> = { _0_ : A; _1_ : B; _2_ : C };
    public func Triple<A, B, C>(a : A, b : B, c : C) : Triple<A, B, C> {
        { _0_ = a; _1_ = b; _2_ = c };
    };

    public type Quadruple<A, B, C, D> = { _0_ : A; _1_ : B; _2_ : C; _3_ : D };
    public func Quadruple<A, B, C, D>(a : A, b : B, c : C, d : D) : Quadruple<A, B, C, D> {
        { _0_ = a; _1_ = b; _2_ = c; _3_ = d };
    };

    public type Direction = {
        #Asc;
        #Desc;
    };

    public type Index = {
        name : Text;
        key_details : [(Text, Direction)];
        data : MemoryBTreeIndex;
    };

    public type Collection = {
        var schema : Schema;
        schema_keys : [Text];
        main : MemoryIdBTree;
        indexes : Map<Text, Index>;
    };

    public type HydraDB = {
        collections : Map<Text, Collection>;
    };

    public let DEFAULT_BTREE_ORDER = 256;

    public func new() : HydraDB {
        let hydra_db = {
            collections = Map.new<Text, Collection>();
        };

        hydra_db;
    };

    public func extract_schema_keys(schema : Schema) : [Text] {
        let buffer = Buffer.Buffer<Text>(8);

        func extract(schema : Schema) {
            switch (schema) {
                case (#Record(fields)) {
                    for ((name, value) in fields.vals()) {
                        buffer.add(name);
                        extract(value);
                    };
                };
                case (#Variant(variants)) {
                    for ((name, value) in variants.vals()) {
                        buffer.add(name);
                        extract(value);
                    };
                };
                case (#Tuple(a, b)) { extract(a); extract(b) };
                case (#Triple(a, b, c)) { extract(a); extract(b); extract(c) };
                case (#Quadruple(a, b, c, d)) {
                    extract(a);
                    extract(b);
                    extract(c);
                    extract(d);
                };
                case (#Option(inner)) { extract(inner) };
                case (#Array(inner)) { extract(inner) };
                case (_) {};
            };
        };

        extract(schema);

        Buffer.toArray(buffer);
    };

    public func create_collection(hydra_db : HydraDB, name : Text, schema : Schema) : Result<Collection, Text> {

        switch (Map.get<Text, Collection>(hydra_db.collections, thash, name)) {
            case (?_) return #err("Collection already exists");
            case (null) ();
        };

        let #Record(_) = schema else return #err("Schema error: schema type is not a record");

        let collection = {
            var schema = schema;
            schema_keys = extract_schema_keys(schema);
            main = MemoryIdBTree.new(?DEFAULT_BTREE_ORDER);
            indexes = Map.new<Text, Index>();
        };

        ignore Map.put<Text, Collection>(hydra_db.collections, thash, name, collection);
        #ok(collection);
    };

    // clear data in collection
    public func clear_collection(hydra_db : HydraDB, name : Text) {
        let ?collection = Map.get<Text, Collection>(hydra_db.collections, thash, name);
        MemoryIdBTree.clear(collection.main);

        for (index in Map.vals(collection.indexes)) {
            MemoryBTreeIndex.clear(index.data);
        };
    };

    public func is_schema_backward_compatible(curr : Schema, new : Schema) : Bool {
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
                is_schema_backward_compatible(curr.0, new.0) and is_schema_backward_compatible(curr.1, new.1);
            };
            case (#Triple(curr), #Triple(new)) {
                is_schema_backward_compatible(curr.0, new.0) and is_schema_backward_compatible(curr.1, new.1) and is_schema_backward_compatible(curr.2, new.2);
            };
            case (#Quadruple(curr), #Quadruple(new)) {
                is_schema_backward_compatible(curr.0, new.0) and is_schema_backward_compatible(curr.1, new.1) and is_schema_backward_compatible(curr.2, new.2) and is_schema_backward_compatible(curr.3, new.3);
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

    public func update_collection_schema(hydra_db : HydraDB, name : Text, schema : Schema) : Result<(), Text> {
        let ?collection = Map.get<Text, Collection>(hydra_db.collections, thash, name) else return #err("Collection not found");

        let is_compatible = is_schema_backward_compatible(collection.schema, schema);
        if (not is_compatible) return #err("Schema is not backward compatible");

        collection.schema := schema;
        Debug.print("Schema Updated: Ensure to update your Record type and Blobify functions accordingly.");
        #ok;
    };

    public type Candid = Serde.Candid;

    public type Candify<A> = {
        from_blob : Blob -> A;
        to_blob : A -> Blob;
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
                case (#Tuple((v1, v2)), #Record(records)) {
                    if (records.size() != 2) return #err("Tuple size mismatch: expected 2, got " # debug_show (records.size()));

                    for ((i, (key, _)) in Itertools.enumerate(records.vals())) {
                        if (key != debug_show (i)) return #err("Tuple key mismatch: expected " # debug_show (i) # ", got " # debug_show (key));
                    };

                    let r1 = _validate(v1, records[0].1);
                    let r2 = _validate(v2, records[1].1);

                    let #ok(_) = r1 else return send_error(r1);
                    let #ok(_) = r2 else return send_error(r2);
                };

                case (#Triple((v1, v2, v3)), #Record(records)) {
                    if (records.size() != 3) return #err("Triple size mismatch: expected 3, got " # debug_show (records.size()));
                    for ((i, (key, _)) in Itertools.enumerate(records.vals())) {
                        if (key != debug_show (i)) return #err("Tuple key mismatch: expected " # debug_show (i) # ", got " # debug_show (key));
                    };

                    let r1 = _validate(v1, records[0].1);
                    let r2 = _validate(v2, records[1].1);
                    let r3 = _validate(v3, records[2].1);

                    let #ok(_) = r1 else return send_error(r1);
                    let #ok(_) = r2 else return send_error(r2);
                    let #ok(_) = r3 else return send_error(r3);
                };

                case (#Quadruple((v1, v2, v3, v4)), #Record(records)) {
                    if (records.size() != 4) return #err("Quadruple size mismatch: expected 4, got " # debug_show (records.size()));
                    for ((i, (key, _)) in Itertools.enumerate(records.vals())) {
                        if (key != debug_show (i)) return #err("Tuple key mismatch: expected " # debug_show (i) # ", got " # debug_show (key));
                    };
                    
                    let r1 = _validate(v1, records[0].1);
                    let r2 = _validate(v2, records[1].1);
                    let r3 = _validate(v3, records[2].1);
                    let r4 = _validate(v4, records[3].1);

                    let #ok(_) = r1 else return send_error(r1);
                    let #ok(_) = r2 else return send_error(r2);
                    let #ok(_) = r3 else return send_error(r3);
                    let #ok(_) = r4 else return send_error(r4);
                };

                case (#Record(fields), #Record(records)) {
                    if (fields.size() != records.size()) {
                        return #err("Record size mismatch: " # debug_show (("shema", fields.size()), ("record", records.size())));
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
        switch (schema, a, b) {
            case (_, #Empty, #Empty) 0;
            case (_, #Null, #Null) 0;
            case (_, #Text(a), #Text(b)) Int8Cmp.Text(a, b);
            case (_, #Nat(a), #Nat(b)) Int8Cmp.Nat(a, b);
            case (_, #Nat8(a), #Nat8(b)) Int8Cmp.Nat8(a, b);
            case (_, #Nat16(a), #Nat16(b)) Int8Cmp.Nat16(a, b);
            case (_, #Nat32(a), #Nat32(b)) Int8Cmp.Nat32(a, b);
            case (_, #Nat64(a), #Nat64(b)) Int8Cmp.Nat64(a, b);
            case (_, #Principal(a), #Principal(b)) Int8Cmp.Principal(a, b);
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

            case (schema, a, b) Debug.trap("cmp_candid: unexpected candid type " # debug_show (a, b));
        };
    };

    // func eq_candid(a : Candid, b : Candid) : Bool {
    //     cmp_candid(a, b) == 0;
    // };

    func send_error<A, B, C>(res : Result<A, B>) : Result<C, B> {
        switch (res) {
            case (#ok(_)) Debug.trap("send_error: unexpected error type");
            case (#err(err)) return #err(err);
        };
    };

    func get_collection(hydra_db : HydraDB, collection_name : Text) : ?Collection {
        Map.get<Text, Collection>(hydra_db.collections, thash, collection_name);
    };

    func reverse_order(order : Order) : Order {
        switch (order) {
            case (#less) #greater;
            case (#greater) #less;
            case (#equal) #equal;
        };
    };

    public type IndexKeyDetails = [(Text, Candid)];

    public func lookup_record<Record>(collection : Collection, blobify : Candify<Record>, id : Nat) : Record {
        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);
        let ?record_candid_blob = MemoryIdBTree.lookupVal(collection.main, btree_main_utils, id);
        let record = blobify.from_blob(record_candid_blob);
        record;
    };

    func lookup_candid_record(collection : Collection, id : Nat) : ?Candid {
        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);
        let ?candid_blob = MemoryIdBTree.lookupVal(collection.main, btree_main_utils, id);
        let candid = decode_candid_blob(collection, candid_blob);

        ?candid;
    };

    public func get_index_data_utils(collection : Collection, index_key_details : [(Text, Direction)]) : IndexUtils<IndexKeyDetails> {
        let index_key_utils : IndexUtils.IndexUtils<IndexKeyDetails> = {
            blobify = {
                from_blob = func(b : Blob) : IndexKeyDetails {
                    let ?res : ?IndexKeyDetails = from_candid (b);
                    res;
                };
                to_blob = func(index_key_detail : IndexKeyDetails) : Blob {
                    to_candid (index_key_detail);
                };
            };
            cmp = #cmp(
                func(a : IndexKeyDetails, b : IndexKeyDetails) : Int8 {
                    var cmp_result : Int8 = 0;

                    for (i in Iter.range(0, a.size() - 1)) {
                        let (_, val_a) = a[i];
                        let (_, val_b) = b[i];

                        let (_, dir) = index_key_details[i];

                        cmp_result := switch (val_a, val_b) {
                            case (#Array(_) or #Record(_) or #Tuple(_) or #Triple(_) or #Quadruple(_), _) {
                                Debug.trap("cmp: unexpected candid type");
                            };
                            case (_, #Array(_) or #Record(_) or #Tuple(_) or #Triple(_) or #Quadruple(_)) {
                                Debug.trap("cmp: unexpected candid type");
                            };
                            case (val_a, val_b) {
                                let #Record(schema) = collection.schema else Debug.trap("cmp: schema is not a record");
                                cmp_candid(schema[i].1, val_a, val_b);
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

    public func create_index(hydra_db : HydraDB, collection_name : Text, _index_key_details : [(Text)]) : Result<Index, Text> {
        let ?collection = get_collection(hydra_db, collection_name);

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

        let index_data = MemoryBTreeIndex.new(?DEFAULT_BTREE_ORDER);

        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);

        let index_data_utils = get_index_data_utils(collection, index_key_details);

        for ((id, candid_blob) in MemoryIdBTree.entries(collection.main, btree_main_utils)) {
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
            ignore MemoryBTreeIndex.insert<IndexKeyDetails>(index_data, index_data_utils, index_key_values, id);
        };

        let index : Index = {
            name = index_name;
            key_details = index_key_details;
            data = index_data;
        };

        ignore Map.put<Text, Index>(collection.indexes, thash, index_name, index);

        #ok(index);
    };

    func unwrap_or_err<A>(res : Result<A, Text>) : A {
        switch (res) {
            case (#ok(success)) success;
            case (#err(err)) Debug.trap("unwrap_or_err: " # err);
        };
    };

    func assert_result<A>(res : Result<A, Text>) {
        switch (res) {
            case (#ok(_)) ();
            case (#err(err)) Debug.trap("assert_result: " # err);
        };
    };

    func main_btree_utils() : BTreeUtils<Nat, Blob> {
        BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);
    };

    func decode_candid_blob(collection : Collection, candid_blob : Blob) : Candid {
        let candid_result = Serde.Candid.decode(candid_blob, collection.schema_keys, null);
        let #ok(candid_values) = candid_result;
        let candid = candid_values[0];
        candid;
    };

    public func put<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, record : Record) : Result<(Nat), Text> {
        let ?collection = get_collection(hydra_db, collection_name);

        let candid_blob = blobify.to_blob(record);
        let candid = decode_candid_blob(collection, candid_blob);

        // Debug.print("validate: " # debug_show (collection.schema) #debug_show (candid));
        assert_result(validate_record(collection.schema, candid));

        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);

        let id = MemoryIdBTree.nextId(collection.main);
        assert null == MemoryIdBTree.insert<Nat, Blob>(collection.main, btree_main_utils, id, candid_blob);
        // assert MemoryIdBTree.getId(collection.main, btree_main_utils, id) == ?id;

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
                    };
                };
            };

            buffer.add((":record-id", #Nat(id)));

            let index_key_values = Buffer.toArray(buffer);

            let index_data_utils = get_index_data_utils(collection, index.key_details);

            ignore MemoryBTreeIndex.insert<IndexKeyDetails>(index.data, index_data_utils, index_key_values, id);
        };

        #ok(id);

    };

    func get_index_key_values(collection : Collection, index_key_details : [(Text, Direction)], id : Nat, records : [(Text, Candid)]) : IndexKeyDetails {
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

    func get_best_index(collection : Collection, _query : [(Text, Candid)]) : ?Index {

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

    public func get<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, _query : [(Text, Candid)]) : Result<Record, Text> {
        let ?collection = get_collection(hydra_db, collection_name);
        let opt_index = get_best_index(collection, _query);

        let index = switch (opt_index) {
            case (?(index)) index;
            case (null) return #err("Couldn't get index");
        };

        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);
        let index_data_utils = get_index_data_utils(collection, index.key_details);

        let ?record_id = MemoryBTreeIndex.get(index.data, index_data_utils, _query);
        let ?record_candid_blob = MemoryIdBTree.lookupVal(collection.main, btree_main_utils, record_id);
        let record = blobify.from_blob(record_candid_blob);
        #ok(record);
    };

    func tuple_cmp<A, B>(cmp : (A, A) -> Order) : ((A, B), (A, B)) -> Order {
        func(a : (A, B), b : (A, B)) : Order {
            cmp(a.0, b.0);
        };
    };

    func tuple_eq<A, B>(eq : (A, A) -> Bool) : ((A, B), (A, B)) -> Bool {
        func(a : (A, B), b : (A, B)) : Bool {
            eq(a.0, b.0);
        };
    };

    func filter(collection : Collection, records : Iter<Nat>, filter_index : Nat, lower : [(Text, Candid)], upper : [(Text, Candid)]) : Iter<Nat> {

        let filtered_records_ids = if (filter_index >= lower.size()) {
            records;
        } else {
            Debug.print("filter_index: " # debug_show filter_index);

            Iter.filter<Nat>(
                records,
                func(id : Nat) : Bool {
                    let ? #Record(candid_record) = lookup_candid_record(collection, id);

                    for ((a, b) in Itertools.zip(lower.vals(), upper.vals())) {
                        let ?field = Array.find<(Text, Candid)>(
                            candid_record,
                            func((variant_name, _) : (Text, Candid)) : Bool {
                                variant_name == a.0;
                            },
                        );

                        if (cmp_candid(collection.schema, field.1, a.1) < 0) return false;
                        if (cmp_candid(collection.schema, field.1, b.1) > 0) return false;
                    };

                    true;
                },
            );

        };
    };

    func id_to_record_iter<Record>(collection : Collection, blobify : Candify<Record>, iter : Iter<Nat>) : Iter<(Nat, Record)> {
        Iter.map<Nat, (Nat, Record)>(
            iter,
            func(id : Nat) : (Nat, Record) {
                let record = lookup_record<Record>(collection, blobify, id);
                (id, record);
            },
        );
    };

    public func scan<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, start_query : [(Text, Candid)], end_query : [(Text, Candid)]) : Iter<(Nat, Record)> {
        Debug.print("start_query: " # debug_show start_query);
        Debug.print("end_query: " # debug_show end_query);

        let ?collection = get_collection(hydra_db, collection_name);
        let opt_start_index = get_best_index(collection, start_query);
        let opt_end_index = get_best_index(collection, end_query);

        Debug.print("opt_start_index: " # debug_show Option.map(opt_start_index, func(index : Index) : Text { index.name }));
        Debug.print("opt_end_index: " # debug_show Option.map(opt_end_index, func(index : Index) : Text { index.name }));

        let index = switch (opt_start_index, opt_end_index) {
            case (?(start_index), ?(end_index)) {
                start_index;
            };
            case (?(index), _) index;
            case (_, ?(index)) index;
            case (_) {
                Debug.print("No index found. Attempting to scan main collection");
                let keys = MemoryIdBTree.keys(collection.main, BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob));
                let filtered = filter(collection, keys, 0, start_query, end_query);
                return id_to_record_iter(collection, blobify, filtered);
            };
        };

        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);
        let index_data_utils = get_index_data_utils(collection, index.key_details);

        func sort_by_key_details(a : (Text, Candid), b : (Text, Candid)) : Order {
            let pos_a = switch (Array.indexOf<(Text, Direction)>((a.0, #Asc), index.key_details, tuple_eq(Text.equal))) {
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            let pos_b = switch (Array.indexOf<(Text, Direction)>((b.0, #Asc), index.key_details, tuple_eq(Text.equal))) {
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
            let ?(min_key_fields, min_id) = MemoryBTreeIndex.getMin(index.data, index_data_utils);
            // let min_record = lookup_candid_record(collection, min_id);

            let buffer = Buffer.Buffer<(Text, Candid)>(8);
            var j = 0;

            if (j == (sorted_start_query.size())) {
                Debug.print("overwritten by min_key_fields");
                for (k in Itertools.range(0, min_key_fields.size() - 1)) {
                    buffer.add(min_key_fields[k]);
                };
            } else label for_loop for (i in Itertools.range(0, min_key_fields.size() - 1)) {
                let a = min_key_fields[i];
                let b = sorted_start_query[j];
                Debug.print("(a, b): " # debug_show (a, b));

                if (a.0 == b.0) {
                    buffer.add(b);
                    j += 1;
                } else {
                    buffer.add(a);
                };

                if (j == (sorted_start_query.size())) {
                    for (k in Itertools.range(i + 1, min_key_fields.size() - 1)) {
                        buffer.add(min_key_fields[k]);
                    };

                    break for_loop;
                };

            };

            Buffer.toArray(buffer);
        };

        let full_end_query = do {
            let ?(max_key_fields, max_id) = MemoryBTreeIndex.getMax(index.data, index_data_utils);

            let buffer = Buffer.Buffer<(Text, Candid)>(8);
            var j = 0;

            if (j == (sorted_end_query.size())) {
                for (k in Itertools.range(0, max_key_fields.size() - 1)) {
                    buffer.add(max_key_fields[k]);
                };
            } else label for_loop for (i in Itertools.range(0, max_key_fields.size() - 1)) {

                let a = max_key_fields[i];
                let b = sorted_end_query[j];

                if (a.0 == b.0) {
                    buffer.add(b);
                    j += 1;
                } else {
                    buffer.add(a);
                };

                if (j == (sorted_end_query.size())) {
                    for (k in Itertools.range(i + 1, max_key_fields.size() - 1)) {
                        buffer.add(max_key_fields[k]);
                    };

                    break for_loop;
                };

            };

            Buffer.toArray(buffer);
        };

        let scan_lower_bound = Array.append(full_start_query, [(":record-id", #Nat(0))]);
        let scan_upper_bound = Array.append(full_end_query, [(":record-id", #Nat(2 ** 64))]);

        let records_iter = MemoryBTreeIndex.scan(index.data, index_data_utils, ?(scan_lower_bound), ?scan_upper_bound);

        func get_filter_bounds_index(left : [(Text, Candid)], right : [(Text, Candid)]) : Nat {
            Debug.print("left: " # debug_show left);
            Debug.print("right: " # debug_show right);

            var i = 0;
            while (i < left.size()) {
                let (_, val1) = left[i];
                let (_, val2) = right[i];

                if (cmp_candid(collection.schema, val1, val2) != 0) return i + 1;
                i += 1;
            };

            i;
        };

        // the elements not in the index
        let extended_start_query : [(Text, Candid)] = if (sorted_start_query.size() > full_start_query.size()) {
            let b = Buffer.fromArray<(Text, Candid)>(sorted_start_query);
            for (i in Itertools.range(0, full_start_query.size())) {
                b.put(i, full_start_query.get(i));
            };
            Buffer.toArray(b);
        } else full_start_query;

        let extended_end_query : [(Text, Candid)] = if (sorted_end_query.size() > full_end_query.size()) {
            let b = Buffer.fromArray<(Text, Candid)>(sorted_end_query);
            for (i in Itertools.range(0, full_end_query.size())) {
                b.put(i, full_end_query.get(i));
            };
            Buffer.toArray(b);
        } else full_end_query;

        let record_ids_iter = Iter.map<(IndexKeyDetails, Nat), Nat>(
            records_iter,
            func((_, id) : (IndexKeyDetails, Nat)) : (Nat) { id },
        );

        let filtered_records_ids = filter(
            collection,
            record_ids_iter,
            get_filter_bounds_index(extended_start_query, extended_end_query),
            extended_start_query,
            extended_end_query,
        );

        return id_to_record_iter(collection, blobify, filtered_records_ids);

        // #ok(record);
    };

    public type HqlOperators = {
        #eq : Candid;
        #gte : Candid;
        #lte : Candid;
    };

    public type HydraQueryLang = {

        #Operation : (Text, HqlOperators);
        #And : Buffer<HydraQueryLang>;
        #Or : Buffer<HydraQueryLang>;

        // #Limit : (Nat, HydraQueryLang);
        // #Skip : (Nat, HydraQueryLang);
        // #BatchSize : (Nat, HydraQueryLang);

        // #Regex : (Text, Text);
        // #Not : HydraQueryLang;

        // #In : (Text, [Candid]);
        // #Between : (Text, Candid, Candid);
        // #All : (Text, HydraQueryLang);
        // #Intersect : (HydraQueryLang, HydraQueryLang);
        // #Union : (HydraQueryLang, HydraQueryLang);
    };

    public type Operator = {
        #Eq;
        #Gt;
        #Lt;
    };

    public class QueryBuilder() = self {
        var _query : HydraQueryLang = #And(Buffer.Buffer<HydraQueryLang>(4));

        public func _where(key : Text, op : HqlOperators) : QueryBuilder {
            return _and(key, op);
        };

        public func _and(key : Text, op : HqlOperators) : QueryBuilder {

            let and_buffer = switch (_query) {
                case (#And(buffer)) buffer;
                case (#Or(_)) {
                    let and_buffer = Buffer.Buffer<HydraQueryLang>(8);
                    and_buffer.add(_query);
                    _query := #And(and_buffer);
                    and_buffer;
                };
                case (#Operation(_)) Debug.trap("Operation not allowed in this context");
            };

            and_buffer.add(#Operation(key, op));

            self;
        };

        public func _or(key : Text, op : HqlOperators) : QueryBuilder {
            let or_buffer = switch (_query) {
                case (#Or(or_buffer)) or_buffer;
                case (#Operation(_)) Debug.trap("Operation not allowed in this context");
                case (#And(_)) {
                    let or_buffer = Buffer.Buffer<HydraQueryLang>(8);
                    or_buffer.add(_query);
                    _query := #Or(or_buffer);
                    or_buffer;
                };
            };

            or_buffer.add(#Operation(key, op));
            self;
        };

        public func _or_query(new_query : QueryBuilder) : QueryBuilder {
            let or_buffer = switch (_query) {
                case (#Or(or_buffer)) or_buffer;
                case (#Operation(_)) Debug.trap("Operation not allowed in this context");
                case (#And(_)) {
                    let or_buffer = Buffer.Buffer<HydraQueryLang>(8);
                    or_buffer.add(_query);
                    _query := #Or(or_buffer);
                    or_buffer;
                };
            };

            or_buffer.add(new_query.build());

            self;
        };

        public func build() : HydraQueryLang {
            _query;
        };

    };

    public func find<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, query_builder : QueryBuilder) : Iter<(Nat, Record)> {
        var limit = 1000;
        var batch_size = 100;
        var skip = 0;

        func eval_op(field : Text, op : HqlOperators, lower : Map<Text, Candid>, upper : Map<Text, Candid>) {
            switch (op) {
                case (#eq(candid)) {
                    ignore Map.put(lower, thash, field, candid);
                    ignore Map.put(upper, thash, field, candid);
                };
                case (#gte(candid)) {
                    ignore Map.put(lower, thash, field, candid);
                };
                case (#lte(candid)) {
                    ignore Map.put(upper, thash, field, candid);
                };
            };
        };

        func eval(expr : HydraQueryLang) : Iter<(Nat, Record)> {
            switch (expr) {
                case (#Operation(field, op)) {
                    Debug.trap("Operation not allowed in this context");
                };
                case (#And(buffer)) {
                    let new_lower = Map.new<Text, Candid>();
                    let new_upper = Map.new<Text, Candid>();

                    var res = Itertools.empty<(Nat, Record)>();

                    for (expr in buffer.vals()) {
                        switch (expr) {
                            case (#Operation(field, op)) {
                                eval_op(field, op, new_lower, new_upper);
                            };
                            case (#And(_)) Debug.trap("And not allowed in this context");
                            case (#Or(_)) {
                                res := Itertools.chain(res, eval(expr));
                            };
                        };

                    };

                    let lower_bound_as_array = Map.toArray(new_lower);
                    let upper_bound_as_array = Map.toArray(new_upper);

                    res := scan(hydra_db, collection_name, blobify, lower_bound_as_array, upper_bound_as_array);

                    let hash_fn = func((id, record) : (Nat, Record)) : Hash {
                        Nat32.fromNat(id);
                    };

                    let is_eq = func(a : (Nat, Record), b : (Nat, Record)) : Bool {
                        a.0 == b.0;
                    };

                    Itertools.unique<(Nat, Record)>(res, hash_fn, is_eq);
                };
                case (#Or(buffer)) {

                    var res = Itertools.empty<(Nat, Record)>();

                    for (expr in buffer.vals()) {
                        let new_lower = Map.new<Text, Candid>();
                        let new_upper = Map.new<Text, Candid>();

                        let iter = switch (expr) {
                            case (#Operation(field, op)) {
                                eval_op(field, op, new_lower, new_upper);
                                scan(hydra_db, collection_name, blobify, Map.toArray(new_lower), Map.toArray(new_upper));
                            };
                            case (#And(_)) eval(expr);
                            case (#Or(_)) Debug.trap("Or not allowed in this context");
                        };

                        res := Itertools.chain(res, iter);
                    };

                    res;
                };
            };
        };

        let db_query = query_builder.build();
        return eval(db_query);

    };

    public func updateById<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, id : Nat, update_fn : (Record) -> Record) : Result<(), Text> {
        let ?collection = get_collection(hydra_db, collection_name);
        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);

        let ?prev_candid_blob = MemoryIdBTree.lookupVal(collection.main, btree_main_utils, id);
        let prev_record = blobify.from_blob(prev_candid_blob);
        // let prev_record = lookup_record<Record>(collection, blobify, id);

        let new_record = update_fn(prev_record);

        let new_candid_blob = blobify.to_blob(new_record);
        let new_candid = decode_candid_blob(collection, new_candid_blob);

        // not needed since it uses the same record type
        assert_result(validate_record(collection.schema, new_candid));

        assert ?prev_candid_blob == MemoryIdBTree.insert<Nat, Blob>(collection.main, btree_main_utils, id, new_candid_blob);
        let prev_candid = decode_candid_blob(collection, prev_candid_blob);

        let #Record(prev_records) = prev_candid else return #err("Couldn't get records");
        let #Record(new_records) = new_candid else return #err("Couldn't get records");

        for (index in Map.vals(collection.indexes)) {

            let prev_index_key_values = get_index_key_values(collection, index.key_details, id, prev_records);
            let index_data_utils = get_index_data_utils(collection, index.key_details);

            assert ?id == MemoryBTreeIndex.remove(index.data, index_data_utils, prev_index_key_values);

            let new_index_key_values = get_index_key_values(collection, index.key_details, id, new_records);
            ignore MemoryBTreeIndex.insert<IndexKeyDetails>(index.data, index_data_utils, new_index_key_values, id);
        };

        #ok;
    };

    public func update<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, query_builder : QueryBuilder, update_fn : (Record) -> Record) : Result<(), Text> {
        let ?collection = get_collection(hydra_db, collection_name);
        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);

        let db_query = query_builder.build();
        let records_iter = find(hydra_db, collection_name, blobify, query_builder);

        for ((id, record) in records_iter) {
            let #ok(_) = updateById(hydra_db, collection_name, blobify, id, update_fn);
        };

        #ok;
    };

    public func deleteById<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, id : Nat) : Result<Record, Text> {
        let ?collection = get_collection(hydra_db, collection_name);
        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);

        let ?prev_candid_blob = MemoryIdBTree.remove<Nat, Blob>(collection.main, btree_main_utils, id);
        let prev_candid = decode_candid_blob(collection, prev_candid_blob);

        let #Record(prev_records) = prev_candid else return #err("Couldn't get records");
        Debug.print("prev_records: " # debug_show prev_records);
        for (index in Map.vals(collection.indexes)) {

            let prev_index_key_values = get_index_key_values(collection, index.key_details, id, prev_records);
            let index_data_utils = get_index_data_utils(collection, index.key_details);

            assert ?id == MemoryBTreeIndex.remove(index.data, index_data_utils, prev_index_key_values);
        };

        let prev_record = blobify.from_blob(prev_candid_blob);
        #ok(prev_record);
    };

    public func delete<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, query_builder : QueryBuilder) : Result<[Record], Text> {
        let ?collection = get_collection(hydra_db, collection_name);
        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);

        let db_query = query_builder.build();
        let results_iter = find(hydra_db, collection_name, blobify, query_builder);

        let buffer = Buffer.Buffer<Record>(8);
        for ((id, record) in results_iter) {
            Debug.print("deleting record: " # debug_show (id));
            let #ok(_) = deleteById(hydra_db, collection_name, blobify, id);
            buffer.add(record);
        };

        #ok(Buffer.toArray(buffer));
    };

    // let db = hydra.db("natlabs");
    // let users = db.collection("users");

    // users.put("users", #Text);

    // public func collection_create(db : HydraDB, name : Text, schema : Schema) {
    //     let collection = {
    //         schema = schema;
    //         main = Map.new<Blob, Blob>();
    //         indexes = Map.new<Text, Map<Blob, Blob>>();
    //     };
    //     ignore Map.put<Text, Collection>(db.collections, thash, name, collection);
    // };

    // public func collection_put(db : HydraDB, name : Text, key : Blob, value : Blob) {
    //     let ?collection = Map.get<Text, Collection>(db.collections, thash, name) else return;
    //     ignore Map.put<Blob, Blob>(collection.main, bhash, key, value);
    // };

    // public func collection_get(db : HydraDB, name : Text, key : Blob) : ?Blob {
    //     let ?collection = Map.get<Text, Collection>(db.collections, thash, name) else return null;
    //     return Map.get<Blob, Blob>(collection.main, bhash, key);
    // };

    // public func collection_index_create(db : HydraDB, name : Text, index_name: Text, records : [(Text, Text)]) {
    //     let ?collection = Map.get<Text, Collection>(db.collections, thash, name) else return;
    //     ignore Map.put<Blob, Map<Blob, Blob>>(collection.indexes, thash, index_name, Map.new<Blob, Blob>());
    // };

    // public func collection_index_drop(db : HydraDB, name : Text, index_name: Text) {
    //     let ?collection = Map.get<Text, Collection>(db.collections, thash, name) else return;
    //     ignore Map.remove<Blob, Map<Blob, Blob>>(collection.indexes, thash, index_name);
    // };

    // let q = #And(#Not(#Eq("name", "bob")), #Eq("age", 42));

    // public func collection_query(db : HydraDB, name : Text, cols : [Text], key : Blob) : [Blob] {
    //     let ?collection = Map.get<Text, Collection>(db.collections, thash, name) else return [];
    //     let ?index = Map.get<Text, Index>(collection.indexes, thash, cols[0]) else return [];
    //     return [];
    // };

};
