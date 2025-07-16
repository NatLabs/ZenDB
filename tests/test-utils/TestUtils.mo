import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import BitMap "mo:bit-map";

import T "../../src/Types";

module {

    // document ids should match the index of the document in the data buffer
    public func validate_documents<A>(data : Buffer.Buffer<A>, documents : [(Nat, A)], pred : (Nat, A) -> Bool, print : (A) -> Text) {

        let expected_bitmap = BitMap.BitMap(100);
        let actual_bitmap = BitMap.BitMap(100);

        for ((id, document) in documents.vals()) {
            if (not pred(id, document)) {
                Debug.print("document does not match query: " # debug_show (id, print(document)));
                assert false;
            };

            if (actual_bitmap.get(id)) {
                Debug.print("duplicate document: " # debug_show (id, print(document)));
                assert false;
            };

            actual_bitmap.set(id, true);

        };

        var count = 0;

        var i = 0;

        for ((document) in data.vals()) {
            if (pred(i, document)) {
                count += 1;
                expected_bitmap.set(i, true);
            };
            i += 1;
        };

        if (count != documents.size()) {
            Debug.print("size mismatch (expected, actual): " # debug_show (count, documents.size()));

            actual_bitmap.difference(expected_bitmap);

            let difference = actual_bitmap;

            for (id in difference.vals()) {
                Debug.print("expected document not found in actual data: " # debug_show (id, print(data.get(id))));
            };

            for ((id, document) in documents.vals()) {
                if (difference.get(id)) {
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
