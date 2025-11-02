import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Result "mo:base@0.16.0/Result";
import Order "mo:base@0.16.0/Order";
import Iter "mo:base@0.16.0/Iter";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Hash "mo:base@0.16.0/Hash";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.3.3";
import Decoder "mo:serde@3.3.3/Candid/Blob/Decoder";
import Candid "mo:serde@3.3.3/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";
import BitMap "mo:bit-map@0.1.2";
import Vector "mo:vector@0.4.2";

import TypeUtils "mo:memory-collection@0.3.2/TypeUtils";
import Int8Cmp "mo:memory-collection@0.3.2/TypeUtils/Int8Cmp";

import T "../Types";
import Query "../Query";
import Utils "../Utils";

import CompositeIndex "Index/CompositeIndex";
import Orchid "Orchid";
import Schema "Schema";
import CollectionUtils "CollectionUtils";
import DocumentStore "DocumentStore";
import QueryPlan "QueryPlan";
import C "../Constants";
import BTree "../BTree";

module {

    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; nhash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;
    type QueryBuilder = Query.QueryBuilder;

    public type TypeUtils<A> = TypeUtils.TypeUtils<A>;

    public type Order = Order.Order;
    public type Hash = Hash.Hash;

    public type Schema = Candid.CandidType;

    public type CompositeIndex = T.CompositeIndex;
    public type Candid = T.Candid;
    public type SortDirection = T.SortDirection;
    public type State<R> = T.State<R>;
    public type ZenQueryLang = T.ZenQueryLang;

    public type InternalCandify<A> = T.Candify<A>;

    public type StableCollection = T.StableCollection;

    public type IndexKeyFields = T.IndexKeyFields;

    // Returns the range that is common to all intervals
    public func intersect(intervals : Buffer.Buffer<T.Interval>) : ?T.Interval {

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

    // merges adjacent or overlapping intervals
    // - done in place
    public func union(intervals : Buffer.Buffer<T.Interval>) {

        func tuple_sort(a : T.Interval, b : T.Interval) : Order {
            Nat.compare(a.0, b.0);
        };

        intervals.sort(tuple_sort);

        var start = intervals.get(0).0;
        var end = intervals.get(0).1;

        var scan = 1;
        var insert = 0;

        while (scan < intervals.size()) {

            let interval = intervals.get(scan);
            let l = interval.0;
            let r = interval.1;

            if (l <= end) {
                end := Nat.max(end, r);
            } else {
                intervals.put(insert, (start, end));
                insert += 1;
                start := l;
                end := r;
            };

            scan += 1;
        };

        intervals.put(insert, (start, end));

        for (_ in Itertools.range(insert + 1, intervals.size())) {
            ignore intervals.removeLast();
        };

    };

    // assumes there are no duplicate or overlapping intervals
    public func count(intervals : Buffer.Buffer<T.Interval>) : Nat {
        var count = 0;
        for (interval in intervals.vals()) {
            count += interval.1 - interval.0;
        };
        count;
    };

    // tries to skip the number of documents requested within the instruction limit
    // returns the number of documents skipped
    public func extract_document_ids_in_pagination_range(collection : T.StableCollection, skip : Nat, opt_limit : ?Nat, index_name : Text, intervals : [T.Interval], sorted_in_reverse : Bool) : Iter<T.DocumentId> {
        // Debug.print("skip, opt_limit: " # debug_show (skip, opt_limit));

        var skipped = 0;
        var prev = 0;
        var i = 0;

        label searching_for_skip_index while (i < intervals.size()) {
            let size = intervals.get(i).1 - intervals.get(i).0;

            skipped += size;

            let remaining_skip = skip - prev;

            if (skipped > skip) {
                break searching_for_skip_index;
            };

            prev := skipped;
            i += 1;
        };

        if (i == intervals.size()) {
            return Itertools.empty();
        };

        let remaining_skip = skip - prev;

        let new_intervals_first_start = intervals.get(i).0 + remaining_skip;

        // Debug.print("old_intervals: " # debug_show intervals);

        let new_intervals = Array.tabulate<T.Interval>(
            intervals.size() - i,
            func(j : Nat) : T.Interval {
                if (j == 0) {
                    (new_intervals_first_start, intervals.get(i).1);
                } else {
                    intervals.get(i + j);
                };
            },
        );

        if (new_intervals.size() == 1 and new_intervals.get(0).0 == new_intervals.get(0).1) {
            return Itertools.empty();
        };

        // Debug.print("new_intervals: " # debug_show new_intervals);

        let limit = switch (opt_limit) {
            case (null) {
                return document_ids_from_index_intervals(collection, index_name, new_intervals, sorted_in_reverse);
            };
            case (?limit) limit;
        };

        i := 0;
        prev := 0;
        var add = 0;

        label finding_limit_index while (i < new_intervals.size()) {
            prev := add;
            let interval = new_intervals.get(i);
            let size = interval.1 - interval.0;

            add += size;

            if (add >= limit or (add < limit and i == (new_intervals.size() - 1))) {
                break finding_limit_index;
            };

            i += 1;
        };

        let remaining_limit = limit - prev;

        // Debug.print("remaining_limit: " # debug_show remaining_limit);
        // Debug.print("i: " # debug_show i);

        if (i == intervals.size()) {
            return document_ids_from_index_intervals(collection, index_name, new_intervals, sorted_in_reverse);
        };

        let even_newer_intervals = Array.tabulate<T.Interval>(
            i + 1,
            func(j : Nat) : T.Interval {
                if (j == i) {
                    (
                        new_intervals.get(j).0,
                        Nat.min(new_intervals.get(j).0 + remaining_limit, new_intervals.get(j).1),
                    );
                } else {
                    new_intervals.get(j);
                };
            },
        );

        // Debug.print("even_newer_intervals: " # debug_show even_newer_intervals);

        return document_ids_from_index_intervals(collection, index_name, even_newer_intervals, sorted_in_reverse);

    };

    public func extract_document_ids_in_pagination_range_for_reversed_intervals(
        collection : T.StableCollection,
        skip : Nat,
        opt_limit : ?Nat,
        index_name : Text,
        intervals : [T.Interval],
        sorted_in_reverse : Bool,
    ) : Iter<T.DocumentId> {
        // Debug.print("extract_document_ids_in_pagination_range_for_reversed_intervals");
        // Debug.print("skip, opt_limit: " # debug_show (skip, opt_limit));

        var skipped = 0;
        var prev = 0;

        var i = intervals.size();

        // Debug.print("old_intervals: " # debug_show intervals);

        //! calculate the total size and use it as the max bound when calculating the remaining skip
        label searching_for_skip_index while (i > 0) {

            prev := skipped;

            let interval = intervals.get(i - 1);
            let size = interval.1 - interval.0;
            skipped += size;

            if (skipped > skip) {
                break searching_for_skip_index;
            };

            i -= 1;

        };

        if (i == 0) {
            return Itertools.empty();
        };

        // Debug.print("skipped: " # debug_show skipped);
        // Debug.print("prev: " # debug_show prev);
        // Debug.print("i: " # debug_show i);

        let remaining_skip = skip - prev;

        let new_intervals_last_end = intervals.get(i - 1).1 - remaining_skip;

        let new_intervals = Array.tabulate<T.Interval>(
            i,
            func(j : Nat) : T.Interval {
                if (j == (intervals.size() - i)) {
                    (intervals.get(i - 1).0, new_intervals_last_end);
                } else {
                    intervals.get(i + j);
                };
            },
        );

        // Debug.print("new_intervals: " # debug_show new_intervals);

        let limit = switch (opt_limit) {
            case (null) {
                return document_ids_from_index_intervals(collection, index_name, new_intervals, sorted_in_reverse);
            };
            case (?limit) limit;
        };

        i := new_intervals.size();

        prev := 0;
        var add = 0;

        label searching_for_limit_index while (i > 0) {

            prev := add;
            let interval = new_intervals.get(i - 1);
            let size = interval.1 - interval.0;

            add += size;

            if (add >= limit or (add < limit and i == 1)) {
                break searching_for_limit_index;
            };

            i -= 1;
        };

        // Debug.print("i: " # debug_show i);

        if (i == 0) {
            return document_ids_from_index_intervals(collection, index_name, new_intervals, sorted_in_reverse);
        };

        let remaining_limit = limit - prev;
        // Debug.print("remaining_limit: " # debug_show remaining_limit);

        let even_newer_intervals = Array.tabulate<T.Interval>(
            new_intervals.size() - i + 1,
            func(j : Nat) : T.Interval {
                if (j == (new_intervals.size() - i)) {
                    let updated_start_interval = if (new_intervals.get(j).1 < remaining_limit) {
                        0;
                    } else {
                        new_intervals.get(j).1 - remaining_limit;
                    };

                    (
                        Nat.max(
                            new_intervals.get(j).0,
                            updated_start_interval,
                        ),
                        new_intervals.get(j).1,
                    );
                } else {
                    new_intervals.get(j);
                };
            },
        );

        // Debug.print("even_newer_intervals: " # debug_show even_newer_intervals);

        return document_ids_from_index_intervals(collection, index_name, even_newer_intervals, sorted_in_reverse);

    };

    public func document_ids_from_index_intervals(collection : T.StableCollection, index_name : Text, _intervals : [T.Interval], sorted_in_reverse : Bool) : Iter<T.DocumentId> {

        let intervals = if (sorted_in_reverse) {
            Array.reverse(_intervals);
        } else {
            _intervals;
        };

        // Debug.print("document_ids_from_index_intervals: intervals: " # debug_show intervals);

        if (index_name == C.DOCUMENT_ID) {
            let main_btree_utils = DocumentStore.getBtreeUtils(collection.documents);

            let document_ids = Itertools.flatten(
                Iter.map(
                    intervals.vals(),
                    func(interval : T.Interval) : Iter<(T.DocumentId)> {
                        let document_ids = DocumentStore.range_keys(collection.documents, main_btree_utils, interval.0, interval.1);

                        if (sorted_in_reverse) {
                            return document_ids.rev();
                        };

                        document_ids;
                    },
                )
            );

            return document_ids;
        };

        let ?index = Map.get(collection.indexes, Map.thash, index_name) else Debug.trap("Unreachable: IndexMap not found for index: " # index_name);

        let internal_index = switch (index) {
            case (#text_index(text_index)) text_index.internal_index;
            case (#composite_index(composite_index)) composite_index;
        };

        let index_data_utils = CompositeIndex.get_index_data_utils(collection);

        Itertools.flatten(
            Iter.map(
                intervals.vals(),
                func(interval : T.Interval) : Iter<(T.DocumentId)> {
                    let document_ids = BTree.range_vals(internal_index.data, index_data_utils, interval.0, interval.1);

                    if (sorted_in_reverse) {
                        return document_ids.rev();
                    };
                    document_ids;
                },
            )
        );
    };

};
