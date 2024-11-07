import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";

module {

    public func validate_records<A>(data : Buffer.Buffer<A>, records : [(Nat, A)], pred : (Nat, A) -> Bool, print : (A) -> Text) {

        // todo - check that there are no duplicates in records

        for ((id, record) in records.vals()) {
            if (not pred(id, record)) {
                Debug.print("record does not match query: " # debug_show (id, print(record)));
                assert false;
            };
        };

        var count = 0;

        var i = 0;

        for ((record) in data.vals()) {
            if (pred(i, record)) {
                count += 1;
            };
            i += 1;
        };

        if (count != records.size()) {
            Debug.print("size mismatch (expected, actual): " # debug_show (count, records.size()));
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
