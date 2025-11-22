import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";

import MemoryBTree "mo:memory-collection@0.3.2/MemoryBTree/Stable";
import SparseBitMap64 "mo:bit-map@0.1.2/SparseBitMap64";

import T "../../src/EmbeddedInstance/Types";

module {

    // document ids should match the index of the document in the data buffer
    public func validate_documents<A>(data : Buffer.Buffer<A>, documents : [(Nat, A)], pred : (Nat, A) -> Bool, print : (A) -> Text) {

        let expected_bitmap = SparseBitMap64.new();
        let actual_bitmap = SparseBitMap64.new();

        for ((id, document) in documents.vals()) {
            if (not pred(id, document)) {
                Debug.print("document does not match query: " # debug_show (id, print(document)));
                assert false;
            };

            if (SparseBitMap64.get(actual_bitmap, id)) {
                Debug.print("duplicate document id: " # debug_show (id));
                assert false;
            };

            SparseBitMap64.set(actual_bitmap, id, true);

        };

        var count = 0;

        var i = 0;

        for ((document) in data.vals()) {
            if (pred(i, document)) {
                count += 1;
                SparseBitMap64.set(expected_bitmap, i, true);
            };
            i += 1;
        };

        if (count != documents.size()) {
            Debug.print("size mismatch (expected, actual): " # debug_show (count, documents.size()));

            let difference = SparseBitMap64.difference(actual_bitmap, expected_bitmap);

            for (id in SparseBitMap64.vals(difference)) {
                Debug.print("expected document not found in actual data: " # debug_show (id, print(data.get(id))));
            };

            for ((id, document) in documents.vals()) {
                if (SparseBitMap64.get(difference, id)) {
                    Debug.print("actual data document not found in expected: " # debug_show (id, print(document)));
                };
            };

            assert false;
        };

    };

    public func validate_sorted_documents<A>(data : Buffer.Buffer<A>, documents : [(Nat, A)], pred : (Nat, A) -> Bool, sorted : (A, A) -> Bool, print : (A) -> Text) {
        validate_documents<A>(data, documents, pred, print);

        if (documents.size() == 0) return;

        var prev = documents[0].1;
        for ((id, document) in documents.vals()) {
            if (not sorted(prev, document)) {
                Debug.print("documents are not sorted: " # debug_show (print(prev), print(document)));
                assert false;
            };
            prev := document;
        };
    };

};
