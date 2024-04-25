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

import Map "mo:map/Map";
import Serde "mo:serde";
import Record "mo:serde/Candid/Text/Parser/Record";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";

import MemoryIdBTree "memory-buffer/src/MemoryIdBTree/Base";
import MemoryBTree "memory-buffer/src/MemoryBTree/Base";
import BTreeUtils "memory-buffer/src/MemoryBTree/BTreeUtils";
import Int8Cmp "memory-buffer/src/Int8Cmp";

module {
    public type Map<K, V> = Map.Map<K, V>;
    let { thash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    public type MemoryBTree = MemoryBTree.MemoryBTree;
    public type MemoryIdBTree = MemoryIdBTree.MemoryIdBTree;
    public type BTreeUtils<K, V> = BTreeUtils.BTreeUtils<K, V>;
    public type SingleUtil<A> = BTreeUtils.SingleUtil<A>;

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
        #Tuple : [Schema];
        #Record : [(Text, Schema)];
        #Variant : [(Text, Schema)];
        #Principal;
    };

    public type Direction = {
        #Asc;
        #Desc;
    };

    public type Index = {
        name : Text;
        key_details : [(Text, Direction)];
        data : MemoryBTree;
    };

    public type Collection = {
        schema : Schema;
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

    public func create_collection(hydra_db : HydraDB, name : Text, schema : Schema) : Result<Collection, Text> {

        switch (Map.get<Text, Collection>(hydra_db.collections, thash, name)) {
            case (?_) return #err("Collection already exists");
            case (null) ();
        };

        switch (schema) {
            case (#Record(_)) {};
            case (_) return #err("Schema error: schema type is not a record");
        };

        let collection = {
            schema = schema;
            main = MemoryIdBTree.new(?DEFAULT_BTREE_ORDER);
            indexes = Map.new<Text, Index>();
        };

        ignore Map.put<Text, Collection>(hydra_db.collections, thash, name, collection);
        #ok(collection);
    };

    public type Candid = Serde.Candid;

    public type Candify<A> = {
        from_blob : Blob -> A;
        to_blob : A -> Blob;
    };

    func validate_record(schema : Schema, record : Candid) : Bool {

        // var var_schema = schema;
        // var var_record = record;

        func _validate(schema : Schema, record : Candid) : Bool {
            switch (schema, record) {
                case (#Text, #Text(_)) true;
                case (#Nat, #Nat(_)) true;
                case (#Int, #Int(_)) true;
                case (#Float, #Float(_)) true;
                case (#Bool, #Bool(_)) true;
                case (#Principal, #Principal(_)) true;
                case (#Option(inner), #Null) true;
                case (#Option(inner), record) {
                    return _validate(inner, record);
                };

                case (#Tuple(schemas), #Record(records)) {
                    let tuple_schema_as_record : [(Text, Schema)] = Array.tabulate<(Text, Schema)>(
                        schemas.size(),
                        func(i : Nat) : (Text, Schema) {
                            let key = Char.toText(Char.fromNat32(Nat32.fromNat(i)));
                            let schema = schemas[i];
                            (key, schema);
                        },
                    );

                    _validate(#Record(tuple_schema_as_record), #Record(records));
                };

                case (#Record(fields), #Record(records)) {
                    if (fields.size() != records.size()) {
                        return false;
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

                        if (field.0 != record.0) return false;

                        if (not _validate(field.1, record.1)) return false;
                        i += 1;
                    };

                    true;
                };
                case (#Array(inner), #Array(records)) {
                    var i = 0;
                    while (i < records.size()) {
                        if (not _validate(inner, records[i])) return false;
                        i += 1;
                    };
                    true;
                };
                case (#Variant(variants), #Variant((record_key, nested_record))) {

                    let result = Array.find<(Text, Schema)>(
                        variants,
                        func((variant_name, _) : (Text, Schema)) : Bool {
                            variant_name == record_key;
                        },
                    );

                    switch (result) {
                        case (null) return false;
                        case (?(name, variant)) return _validate(variant, nested_record);
                    };
                };

                case (_) return false;
            };
        };

        switch (schema) {
            case (#Record(fields)) _validate(schema, record);
            case (_) Debug.trap("validate_schema(): schema is not a record");
        };
    };

    func cmp_candid(a : Candid, b : Candid) : Int8 {
        switch (a, b) {
            case (#Text(a), #Text(b)) Int8Cmp.Text(a, b);
            case (#Nat(a), #Nat(b)) Int8Cmp.Nat(a, b);
            case (_, _) Debug.trap("cmp_candid: unexpected candid type");
        };
    };

    func eq_candid(a : Candid, b : Candid) : Bool {
        cmp_candid(a, b) == 0;
    };

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

    func lookup_candid_record<Record>(collection : Collection, id : Nat) : ?Candid {
        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);
        let ?candid_blob = MemoryIdBTree.lookupVal(collection.main, btree_main_utils, id);

        let record_keys : [Text] = switch (collection.schema) {
            case (#Record(fields)) {
                Array.map(
                    fields,
                    func((name, _) : (Text, Schema)) : Text {
                        name;
                    },
                );
            };
            case (_) return null;
        };

        let candid_result = Serde.Candid.decode(candid_blob, record_keys, null);

        let #ok(candid_values) = candid_result;

        let candid = candid_values[0];
        ?candid;
    };

    public func get_index_data_utils(index_key_details: [(Text, Direction)]) : BTreeUtils<IndexKeyDetails, Nat>   {
        let index_key_utils : BTreeUtils.SingleUtil<IndexKeyDetails> = {
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

                        cmp_result := cmp_candid(val_a, val_b);

                        if (cmp_result != 0) {
                            if (dir == #Desc) return -cmp_result;
                            return cmp_result;
                        };
                    };

                    cmp_result;
                }
            );
        };

        let index_data_utils : BTreeUtils<IndexKeyDetails, Nat> = BTreeUtils.createUtils(index_key_utils, BTreeUtils.Nat);

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

        let index_data = MemoryBTree.new(?DEFAULT_BTREE_ORDER);

        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);

        let record_keys : [Text] = switch (collection.schema) {
            case (#Record(fields)) {
                Array.map(
                    fields,
                    func((name, _) : (Text, Schema)) : Text {
                        name;
                    },
                );
            };
            case (_) return #err("Schema error: schema is not a record");
        };

        let index_data_utils = get_index_data_utils(index_key_details);

        for ((id, record) in MemoryIdBTree.entries(collection.main, btree_main_utils)) {
            let #ok(candid_values) = Serde.Candid.decode(record, record_keys, null);
            let candid = candid_values[0];

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
            ignore MemoryBTree.insert<IndexKeyDetails, Nat>(index_data, index_data_utils, index_key_values, id);
        };

        let index : Index = {
            name = index_name;
            key_details = index_key_details;
            data = index_data;
        };

        ignore Map.put<Text, Index>(collection.indexes, thash, index_name, index);

        #ok(index);
    };

    public func put<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, record : Record) : Result<(Nat), Text> {
        let ?collection = get_collection(hydra_db, collection_name);

        let candid_blob = blobify.to_blob(record);

        let record_keys : [Text] = switch (collection.schema) {
            case (#Record(fields)) {
                Array.map(
                    fields,
                    func((name, _) : (Text, Schema)) : Text {
                        name;
                    },
                );
            };
            case (_) return #err("Schema error: schema is not a record");
        };

        let candid_result = Serde.Candid.decode(candid_blob, record_keys, null);

        let #ok(candid_values) = candid_result;

        let candid = candid_values[0];
        assert (validate_record(collection.schema, candid));

        // let candid_with_id = switch(candid){
        //     case (#Record(records)) {
        //         let id = MemoryIdBTree.next_id(collection.main);
        //         #Record([(":record-id", #Nat(id))] + records);
        //     };
        //     case (_) return #err("put(): unexpected candid type");
        // };

        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);

        let id = MemoryIdBTree.nextId(collection.main);
        ignore MemoryIdBTree.insert<Nat, Blob>(collection.main, btree_main_utils, id, candid_blob);
        assert MemoryIdBTree.getId(collection.main, btree_main_utils, id) == ?id;

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

            let index_data_utils = get_index_data_utils(index.key_details);

            ignore MemoryBTree.insert<IndexKeyDetails, Nat>(index.data, index_data_utils, index_key_values, id);
        };

        #ok(id);

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
        let index_data_utils = get_index_data_utils(index.key_details);


        let ?record_id = MemoryBTree.get(index.data, index_data_utils, _query);
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
            case (_) Debug.trap("scan(): couldn't get index");
        };

        let btree_main_utils = BTreeUtils.createUtils(BTreeUtils.Nat, BTreeUtils.Blob);
        let index_data_utils = get_index_data_utils(index.key_details);

        func sort_by_key_details(a : (Text, Candid), b : (Text, Candid)) : Order {
            let pos_a = switch(Array.indexOf<(Text, Direction)>((a.0, #Asc), index.key_details, tuple_eq(Text.equal))){
                case (?pos) pos;
                case (null) index.key_details.size();
            };

            let pos_b = switch(Array.indexOf<(Text, Direction)>((b.0, #Asc), index.key_details, tuple_eq(Text.equal))){
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
            // let min_record = lookup_candid_record<Record>(collection, min_id);

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

                if (j == (sorted_start_query.size() )) {
                    for (k in Itertools.range(i + 1, min_key_fields.size() - 1)) {
                        buffer.add(min_key_fields[k]);
                    };

                    break for_loop;
                };

            };

            Buffer.toArray(buffer);
        };

        let full_end_query = do {
            let ?(max_key_fields, max_id) = MemoryBTree.getMax(index.data, index_data_utils);

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

        let record_ids = MemoryBTree.scan(index.data, index_data_utils, ?(scan_lower_bound), ?scan_upper_bound);

        func get_filter_bounds_index(left : [(Text, Candid)], right : [(Text, Candid)]) : Nat {
            Debug.print("left: " # debug_show left);
            Debug.print("right: " # debug_show right);
            var i = 0;
            while (i < left.size()) {
                let (_, val1) = left[i];
                let (_, val2) = right[i];

                if (cmp_candid(val1, val2) != 0) return i + 1;
                i += 1;
            };

            i;
        };

        let filter_start_index = get_filter_bounds_index(full_start_query, full_end_query);

        let filtered_records_ids = if (filter_start_index >= full_start_query.size()) {
            record_ids;
        } else {
            Debug.print("filter_start_index: " # debug_show filter_start_index);

            Iter.filter(
                record_ids,
                func((index_keys, id) : (IndexKeyDetails, Nat)) : Bool {

                    for ((a, b, c) in Itertools.zip3(index_keys.vals(), full_start_query.vals(), full_end_query.vals())) {
                        if (cmp_candid(a.1, b.1) < 0) return false;
                        if (cmp_candid(a.1, c.1) > 0) return false;
                    };

                    true;
                },
            );

        };

        Iter.map<(IndexKeyDetails, Nat), (Nat, Record)>(
            filtered_records_ids,
            func((_, id) : (IndexKeyDetails, Nat)) : (Nat, Record) {
                let record = lookup_record<Record>(collection, blobify, id);
                (id, record);
            },
        )

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
        var _query : HydraQueryLang = #And(Buffer.Buffer<HydraQueryLang>(8));

        public func _where(key : Text, op : HqlOperators) : QueryBuilder {
            return _and(key, op);
        };

        public func _and(key : Text, op : HqlOperators) : QueryBuilder {

            let and_buffer = switch(_query){
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
            let or_buffer = switch(_query){
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

        public func build() : HydraQueryLang {
            _query;
        };

    };

    public func find<Record>(hydra_db : HydraDB, collection_name : Text, blobify : Candify<Record>, db_query : HydraQueryLang) : Iter<(Nat, Record)> {
        var limit = 1000;
        var batch_size = 100;
        var skip = 0;

        func eval_op(field: Text, op: HqlOperators, lower: Map<Text, Candid>, upper : Map<Text, Candid>) {
            switch(op){
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

        func eval(expr: HydraQueryLang, lower: Map<Text, Candid>, upper : Map<Text, Candid>) : ?Iter<(Nat, Record)>{
            switch(expr){
                case(#Operation(field, op)) {
                    eval_op(field, op, lower, upper);
                    null;
                };
                case (#And(buffer)){
                    let new_lower = Map.new<Text, Candid>();
                    let new_upper = Map.new<Text, Candid>();

                    var res = Itertools.empty<(Nat, Record)>();

                    for (expr in buffer.vals()){
                        switch(eval(expr, new_lower, new_upper)){
                            case (?iter) res := Itertools.chain(res, iter);
                            case (_){};
                        };
                    };

                    let lower_bound_as_array = Map.toArray(new_lower);
                    let upper_bound_as_array = Map.toArray(new_upper);

                    ?scan(hydra_db, collection_name, blobify,lower_bound_as_array, upper_bound_as_array);
                };
                case (#Or(buffer)){
                    
                    var res = Itertools.empty<(Nat, Record)>();

                    for (expr in buffer.vals()){
                        let new_lower = Map.new<Text, Candid>();
                        let new_upper = Map.new<Text, Candid>();

                        switch(eval(expr, new_lower, new_upper)){
                            case (?iter) res := Itertools.chain(res, iter);
                            case (_){};
                        };
                        
                    };

                    let hash_fn = func((id, record) : (Nat, Record)) : Hash {
                        Nat32.fromNat(id)
                    };

                    let is_eq = func(a : (Nat, Record), b : (Nat, Record)) : Bool {
                        a.0 == b.0
                    };

                    ?Itertools.unique<(Nat, Record)>(res, hash_fn, is_eq);
                };
            };
        };


        func evaluate(expr : HydraQueryLang, lower : Buffer<(Text, Candid)>, upper : Buffer<(Text, Candid)>) : ?Iter<(Nat, Record)> {
            switch (expr) {
                // ------------------------------ Comparison ------------------------------
                
                // ------------------------------ Logical ------------------------------
                case (#And(expr1, expr2)) {
                    Debug.print("expr1: " # debug_show expr1);
                    Debug.print("expr2: " # debug_show expr2);

                    let new_lower = Buffer.clone(lower);
                    let new_upper = Buffer.clone(upper);

                    let opt_left = evaluate(expr1, lower, upper);
                    let opt_right = switch(opt_left){
                        case (null) evaluate(expr2, lower, upper);
                        case (?left) evaluate(expr2, new_lower, new_upper);
                    };

                    func union(left: Iter<(Nat, Record)>, right: Iter<(Nat, Record)>) : Iter<(Nat, Record)> {
                        let merged = Itertools.chain(left, right);
                        let hash_fn = func((id, record) : (Nat, Record)) : Hash {
                            Nat32.fromNat(id)
                        };

                        let is_eq = func(a : (Nat, Record), b : (Nat, Record)) : Bool {
                            a.0 == b.0
                        };

                        Itertools.unique<(Nat, Record)>(merged, hash_fn, is_eq);
                    };
                    
                    switch(opt_left, opt_right){
                        case (null, null) {
                            ?scan(hydra_db, collection_name, blobify, Buffer.toArray(lower), Buffer.toArray(upper));
                        };
                        case (?left, null) {
                            let right = scan(hydra_db, collection_name, blobify, Buffer.toArray(new_lower), Buffer.toArray(new_upper));
                            ?union(left, right);
                        };
                        case (null, ?right) {
                            let left = scan(hydra_db, collection_name, blobify, Buffer.toArray(new_lower), Buffer.toArray(new_upper));
                            ?union(left, right);
                        };
                        case (?left, ?right) {
                            ?union(left, right);
                        };
                    };
                };
                case (#Or(expr1, expr2)) {
                    // create a union of the two queries
                    let new_lower = Buffer.clone(lower);
                    let new_upper = Buffer.clone(upper);

                    let opt_left = evaluate(expr1, lower, upper);
                    let opt_right = evaluate(expr2, new_lower, new_upper);

                    switch(opt_left, opt_right){
                        case(?left, ?right) {
                            ?Itertools.chain(left, right);
                        };
                        case(?left, null){
                            let right = scan(hydra_db, collection_name, blobify, Buffer.toArray(new_lower), Buffer.toArray(new_upper));
                            ?Itertools.chain(left, right);
                        };
                        case(null, ?right){
                            let left = scan(hydra_db, collection_name, blobify, Buffer.toArray(lower), Buffer.toArray(upper));
                            ?Itertools.chain(left, right);
                        };
                        case(null, null){
                            let left = scan(hydra_db, collection_name, blobify, Buffer.toArray(lower), Buffer.toArray(upper));
                            let right = scan(hydra_db, collection_name, blobify, Buffer.toArray(new_lower), Buffer.toArray(new_upper));
                            ?Itertools.chain(left, right);
                        };
                    };
                };

                // ------------------------------ Pagination ------------------------------
                case (#Limit(n, expr)) {
                    limit := n;
                    evaluate(expr, lower, upper);
                };
                case (#Skip(n, expr)) {
                    skip := n;
                    evaluate(expr, lower, upper);
                };
                case (#BatchSize(n, expr)) {
                    batch_size := n;
                    evaluate(expr, lower, upper);
                };
            };
        };

        let res = evaluate(db_query, lower, upper);

        switch (res) {
            case (null) {
                scan<Record>(hydra_db, collection_name, blobify, Buffer.toArray(lower), Buffer.toArray(upper));
            };
            case (?iter) return iter;
        };

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
