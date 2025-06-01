import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import BitMap "mo:bit-map";

import T "../../src/Types";

module {

    // record ids should match the index of the record in the data buffer
    public func validate_records<A>(data : Buffer.Buffer<A>, records : [(Nat, A)], pred : (Nat, A) -> Bool, print : (A) -> Text) {

        let expected_bitmap = BitMap.BitMap(100);
        let actual_bitmap = BitMap.BitMap(100);

        for ((id, record) in records.vals()) {
            if (not pred(id, record)) {
                Debug.print("record does not match query: " # debug_show (id, print(record)));
                assert false;
            };

            if (actual_bitmap.get(id)) {
                Debug.print("duplicate record: " # debug_show (id, print(record)));
                assert false;
            };

            actual_bitmap.set(id, true);

        };

        var count = 0;

        var i = 0;

        for ((record) in data.vals()) {
            if (pred(i, record)) {
                count += 1;
                expected_bitmap.set(i, true);
            };
            i += 1;
        };

        if (count != records.size()) {
            Debug.print("size mismatch (expected, actual): " # debug_show (count, records.size()));

            actual_bitmap.difference(expected_bitmap);

            let difference = actual_bitmap;

            for (id in difference.vals()) {
                Debug.print("expected record not found in actual data: " # debug_show (id, print(data.get(id))));
            };

            for ((id, record) in records.vals()) {
                if (difference.get(id)) {
                    Debug.print("actual data record not found in expected: " # debug_show (id, print(record)));
                };
            };

            assert false;
        };

    };

    public func validate_sorted_records<A>(data : Buffer.Buffer<A>, records : [(Nat, A)], pred : (Nat, A) -> Bool, sorted : (A, A) -> Bool, print : (A) -> Text) {
        validate_records<A>(data, records, pred, print);

        if (records.size() == 0) return;

        var prev = records[0].1;
        for ((id, record) in records.vals()) {
            if (not sorted(prev, record)) {
                Debug.print("records are not sorted: " # debug_show (print(prev), print(record)));
                assert false;
            };
            prev := record;
        };
    };

};
