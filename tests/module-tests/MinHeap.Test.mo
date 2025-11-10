import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Order "mo:base/Order";

import MinHeap "../../src/EmbeddedInstance/MinHeap";
import { test; suite } "mo:test";

suite(
    "MinHeap Tests",
    func() {
        test(
            "new - creates empty heap",
            func() {
                let heap = MinHeap.new<Nat>();
                assert MinHeap.size(heap) == 0;
                assert MinHeap.isEmpty(heap) == true;
                assert MinHeap.peekMin(heap) == null;
            },
        );

        test(
            "newWithCapacity - creates empty heap with capacity",
            func() {
                let heap = MinHeap.newWithCapacity<Nat>(10);
                assert MinHeap.size(heap) == 0;
                assert MinHeap.isEmpty(heap) == true;
            },
        );

        test(
            "put and peekMin - single element",
            func() {
                let heap = MinHeap.new<Nat>();
                MinHeap.put(heap, 5, Nat.compare);

                assert MinHeap.size(heap) == 1;
                assert MinHeap.isEmpty(heap) == false;
                assert MinHeap.peekMin(heap) == ?5;
            },
        );

        test(
            "put - maintains min heap property",
            func() {
                let heap = MinHeap.new<Nat>();
                let values = [5, 3, 7, 1, 9, 2, 8];

                for (val in values.vals()) {
                    MinHeap.put(heap, val, Nat.compare);
                };

                assert MinHeap.size(heap) == 7;
                assert MinHeap.peekMin(heap) == ?1;
            },
        );

        test(
            "removeMin - single element",
            func() {
                let heap = MinHeap.new<Nat>();
                MinHeap.put(heap, 5, Nat.compare);

                let min = MinHeap.removeMin(heap, Nat.compare);
                assert min == ?5;
                assert MinHeap.size(heap) == 0;
                assert MinHeap.isEmpty(heap) == true;
                assert MinHeap.peekMin(heap) == null;
            },
        );

        test(
            "removeMin - returns elements in sorted order",
            func() {
                let heap = MinHeap.new<Nat>();
                let values = [5, 3, 7, 1, 9, 2, 8];

                for (val in values.vals()) {
                    MinHeap.put(heap, val, Nat.compare);
                };

                let sorted = Array.tabulate<Nat>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap, Nat.compare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                assert sorted == [1, 2, 3, 5, 7, 8, 9];
                assert MinHeap.isEmpty(heap) == true;
            },
        );

        test(
            "removeMin - empty heap returns null",
            func() {
                let heap = MinHeap.new<Nat>();
                let result = MinHeap.removeMin(heap, Nat.compare);
                assert result == null;
            },
        );

        test(
            "clear - removes all elements",
            func() {
                let heap = MinHeap.new<Nat>();
                let values = [5, 3, 7, 1, 9];

                for (val in values.vals()) {
                    MinHeap.put(heap, val, Nat.compare);
                };

                assert MinHeap.size(heap) == 5;

                MinHeap.clear(heap);

                assert MinHeap.size(heap) == 0;
                assert MinHeap.isEmpty(heap) == true;
                assert MinHeap.peekMin(heap) == null;
            },
        );

        test(
            "fromArray - creates heap from array",
            func() {
                let values = [5, 3, 7, 1, 9, 2, 8];
                let heap = MinHeap.fromArray(values, Nat.compare);

                assert MinHeap.size(heap) == 7;
                assert MinHeap.peekMin(heap) == ?1;

                // Verify all elements come out in sorted order
                let sorted = Array.tabulate<Nat>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap, Nat.compare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                assert sorted == [1, 2, 3, 5, 7, 8, 9];
            },
        );

        test(
            "fromArray - empty array",
            func() {
                let values : [Nat] = [];
                let heap = MinHeap.fromArray(values, Nat.compare);

                assert MinHeap.size(heap) == 0;
                assert MinHeap.isEmpty(heap) == true;
            },
        );

        test(
            "heapify - converts existing heap",
            func() {
                let heap = MinHeap.new<Nat>();
                MinHeap.put(heap, 10, Nat.compare);
                MinHeap.put(heap, 20, Nat.compare);

                assert MinHeap.size(heap) == 2;

                let newValues = [5, 3, 7, 1];
                MinHeap.heapify(heap, newValues, Nat.compare);

                assert MinHeap.size(heap) == 4;
                assert MinHeap.peekMin(heap) == ?1;
            },
        );

        test(
            "put - handles duplicates",
            func() {
                let heap = MinHeap.new<Nat>();
                let values = [5, 3, 5, 1, 3, 1];

                for (val in values.vals()) {
                    MinHeap.put(heap, val, Nat.compare);
                };

                assert MinHeap.size(heap) == 6;

                let sorted = Array.tabulate<Nat>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap, Nat.compare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                assert sorted == [1, 1, 3, 3, 5, 5];
            },
        );

        test(
            "put - handles reverse sorted input",
            func() {
                let heap = MinHeap.new<Nat>();
                let values = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1];

                for (val in values.vals()) {
                    MinHeap.put(heap, val, Nat.compare);
                };

                assert MinHeap.peekMin(heap) == ?1;

                let sorted = Array.tabulate<Nat>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap, Nat.compare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                assert sorted == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            },
        );

        test(
            "put - handles already sorted input",
            func() {
                let heap = MinHeap.new<Nat>();
                let values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

                for (val in values.vals()) {
                    MinHeap.put(heap, val, Nat.compare);
                };

                assert MinHeap.peekMin(heap) == ?1;

                let sorted = Array.tabulate<Nat>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap, Nat.compare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                assert sorted == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            },
        );

        test(
            "works with Int type",
            func() {
                let heap = MinHeap.new<Int>();
                let values = [-5, 3, -7, 1, 0, -2, 8];

                for (val in values.vals()) {
                    MinHeap.put(heap, val, Int.compare);
                };

                assert MinHeap.peekMin(heap) == ?(-7);

                let sorted = Array.tabulate<Int>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap, Int.compare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                assert sorted == [-7, -5, -2, 0, 1, 3, 8];
            },
        );

        test(
            "works with custom comparison (max heap)",
            func() {
                let heap = MinHeap.new<Nat>();

                // Reverse comparison to create max heap
                let maxCompare = func(a : Nat, b : Nat) : Order.Order {
                    switch (Nat.compare(a, b)) {
                        case (#less) #greater;
                        case (#greater) #less;
                        case (#equal) #equal;
                    };
                };

                let values = [5, 3, 7, 1, 9, 2, 8];

                for (val in values.vals()) {
                    MinHeap.put(heap, val, maxCompare);
                };

                assert MinHeap.peekMin(heap) == ?9; // Max value for max heap

                let sorted = Array.tabulate<Nat>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap, maxCompare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                assert sorted == [9, 8, 7, 5, 3, 2, 1];
            },
        );

        test(
            "put - triggers resize correctly",
            func() {
                let heap = MinHeap.new<Nat>();

                // Add enough elements to trigger multiple resizes
                // Initial size is 0, first resize to 4, then 8, 16, etc.
                for (i in Iter.range(0, 20)) {
                    MinHeap.put(heap, i, Nat.compare);
                };

                assert MinHeap.size(heap) == 21;
                assert MinHeap.peekMin(heap) == ?0;

                // Verify all elements are present
                for (i in Iter.range(0, 20)) {
                    let min = MinHeap.removeMin(heap, Nat.compare);
                    assert min == ?i;
                };

                assert MinHeap.isEmpty(heap);
            },
        );

        test(
            "mixed operations - put, peek, remove",
            func() {
                let heap = MinHeap.new<Nat>();

                MinHeap.put(heap, 5, Nat.compare);
                assert MinHeap.peekMin(heap) == ?5;

                MinHeap.put(heap, 3, Nat.compare);
                assert MinHeap.peekMin(heap) == ?3;

                MinHeap.put(heap, 7, Nat.compare);
                assert MinHeap.peekMin(heap) == ?3;

                assert MinHeap.removeMin(heap, Nat.compare) == ?3;
                assert MinHeap.peekMin(heap) == ?5;

                MinHeap.put(heap, 1, Nat.compare);
                assert MinHeap.peekMin(heap) == ?1;

                assert MinHeap.removeMin(heap, Nat.compare) == ?1;
                assert MinHeap.removeMin(heap, Nat.compare) == ?5;
                assert MinHeap.removeMin(heap, Nat.compare) == ?7;
                assert MinHeap.isEmpty(heap);
            },
        );

        test(
            "fromArray vs repeated put - same result",
            func() {
                let values = [15, 3, 17, 1, 19, 12, 8, 6, 4, 20];

                // Create heap using fromArray
                let heap1 = MinHeap.fromArray(values, Nat.compare);

                // Create heap using repeated put
                let heap2 = MinHeap.new<Nat>();
                for (val in values.vals()) {
                    MinHeap.put(heap2, val, Nat.compare);
                };

                // Both should produce same sorted output
                let sorted1 = Array.tabulate<Nat>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap1, Nat.compare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                let sorted2 = Array.tabulate<Nat>(
                    values.size(),
                    func(i) {
                        switch (MinHeap.removeMin(heap2, Nat.compare)) {
                            case (?val) val;
                            case null Debug.trap("Expected value");
                        };
                    },
                );

                assert sorted1 == sorted2;
            },
        );
    },
);
