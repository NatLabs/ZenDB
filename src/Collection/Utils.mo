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
import InternetComputer "mo:base/ExperimentalInternetComputer";

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

import T "../Types";
import Query "../Query";
import Utils "../Utils";
import CandidMap "../CandidMap";
import ByteUtils "../ByteUtils";

import Orchid "Orchid";
import Schema "Schema";
import C "../Constants";

module CollectionUtils {

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    // public type MemoryBTree = MemoryBTree.VersionedMemoryBTree;
    public type BTreeUtils<K, V> = MemoryBTree.BTreeUtils<K, V>;
    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type Index = T.Index;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type Candify<A> = T.Candify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;

    public let { thash; bhash } = Map;

    public func get_index_data_utils(
        collection : StableCollection,
        index_key_details : [(Text, SortDirection)],
    ) : MemoryBTree.BTreeUtils<[Candid], T.RecordId> {

        let key_utils = get_index_key_utils(collection, index_key_details);
        let value_utils = TypeUtils.Nat;

        MemoryBTree.createUtils(key_utils, value_utils);

    };

    public func get_index_key_utils(collection : StableCollection, index_key_details : [(Text, SortDirection)]) : TypeUtils.TypeUtils<[Candid]> {
        Orchid.Orchid;
    };

    public func get_main_btree_utils() : BTreeUtils<Nat, Blob> {
        MemoryBTree.createUtils<Nat, Blob>(Utils.typeutils_nat_as_nat64, TypeUtils.Blob);
    };

    public func get_index_columns(collection : StableCollection, index_key_details : [(Text, SortDirection)], id : Nat, records : [(Text, Candid)]) : [Candid] {
        let buffer = Buffer.Buffer<Candid>(8);

        for ((index_key, dir) in index_key_details.vals()) {
            for ((key, value) in records.vals()) {
                if (key == C.RECORD_ID_FIELD) {
                    buffer.add(#Nat(id));
                } else if (key == index_key) {
                    buffer.add(value);
                };
            };
        };

        Buffer.toArray(buffer);
    };

    public func memorybtree_scan_interval<K, V>(
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

    public func lookup_record<Record>(collection : T.StableCollection, blobify : T.Candify<Record>, id : Nat) : Record {

        let ?record_details = MemoryBTree.get(collection.main, get_main_btree_utils(), id);
        let record = blobify.from_blob(record_details);
        record;
    };

    public func lookup_candid_blob(collection : StableCollection, id : Nat) : Blob {
        let ?record_details = MemoryBTree.get(collection.main, get_main_btree_utils(), id);
        record_details;
    };

    public func decode_candid_blob(collection : StableCollection, candid_blob : Blob) : Candid.Candid {
        let candid_result = Candid.decode(candid_blob, collection.schema_keys, null);
        let #ok(candid_values) = candid_result;
        let candid = candid_values[0];
        candid;
    };

    public func lookup_candid_record(collection : StableCollection, id : Nat) : ?Candid.Candid {
        let ?record_details = MemoryBTree.get(collection.main, get_main_btree_utils(), id);
        let candid = decode_candid_blob(collection, record_details);

        ?candid;
    };

    // public func lookup_candid_map_bytes(collection : StableCollection, id : Nat) : ?[Nat8] {
    //     let ?record_details = MemoryBTree.get(collection.main, get_main_btree_utils(), id) else return null;
    //     let bytes = record_details.1;

    //     ?bytes;
    // };

    public func candid_map_filter_condition(collection : StableCollection, candid_record : Candid.Candid, lower : [(Text, ?T.State<Candid>)], upper : [(Text, ?T.State<Candid>)]) : Bool {

        let candid_map = CandidMap.CandidMap(candid_record);

        for (((key, opt_lower_val), (upper_key, opt_upper_val)) in Itertools.zip(lower.vals(), upper.vals())) {
            assert key == upper_key;

            let ?field_value = candid_map.get(key) else Debug.trap("filter: field '" # debug_show key # "' not found in record");

            switch (opt_lower_val) {
                case (?(#True(lower_val))) {
                    if (Schema.cmp_candid(collection.schema, field_value, lower_val) == -1) return false;
                };
                case (?(#False(lower_val))) {
                    if (Schema.cmp_candid(collection.schema, field_value, lower_val) < 1) return false;
                };
                case (null) {};
            };

            switch (opt_upper_val) {
                case (?(#True(upper_val))) {
                    if (Schema.cmp_candid(collection.schema, field_value, upper_val) == 1) return false;
                };
                case (?(#False(upper_val))) {
                    if (Schema.cmp_candid(collection.schema, field_value, upper_val) > -1) return false;
                };
                case (null) {};
            };

        };

        true;

    };

    public func record_ids_from_index_intervals(collection : StableCollection, index_name : Text, _intervals : [(Nat, Nat)], sorted_in_reverse : Bool) : Iter<Nat> {

        let intervals = if (sorted_in_reverse) {
            Array.reverse(_intervals);
        } else {
            _intervals;
        };

        if (index_name == C.RECORD_ID_FIELD) {
            let main_btree_utils = get_main_btree_utils();

            let record_ids = Itertools.flatten(
                Iter.map(
                    intervals.vals(),
                    func(interval : (Nat, Nat)) : Iter<(Nat)> {
                        let record_ids = MemoryBTree.rangeKeys(collection.main, main_btree_utils, interval.0, interval.1);

                        if (sorted_in_reverse) {
                            return record_ids.rev();
                        };

                        record_ids;
                    },
                )
            );

            return record_ids;
        };

        let ?index = Map.get(collection.indexes, thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);

        let index_data_utils = CollectionUtils.get_index_data_utils(collection, index.key_details);

        Itertools.flatten(
            Iter.map(
                intervals.vals(),
                func(interval : (Nat, Nat)) : Iter<(Nat)> {
                    let record_ids = MemoryBTree.rangeVals(index.data, index_data_utils, interval.0, interval.1);

                    if (sorted_in_reverse) {
                        return record_ids.rev();
                    };
                    record_ids;
                },
            )
        );
    };

    public func multi_filter(
        collection : StableCollection,
        records : Iter<Nat>,
        bounds : Buffer.Buffer<(lower : [(Text, ?T.State<Candid>)], upper : [(Text, ?T.State<Candid>)])>,
    ) : Iter<Nat> {
        Iter.filter<Nat>(
            records,
            func(id : Nat) : Bool {
                let ?candid = CollectionUtils.lookup_candid_record(collection, id) else Debug.trap("multi_filter: candid_map_bytes not found");

                var result = true;

                for ((lower, upper) in bounds.vals()) {
                    result := result and candid_map_filter_condition(collection, candid, lower, upper);
                };

                result;
            },
        );
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
            if (candid_record == #Null) return ?#Null;
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

};
