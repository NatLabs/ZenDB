import Array "mo:base@0.16.0/Array";
import Order "mo:base@0.16.0/Order";
import Debug "mo:base@0.16.0/Debug";
import Int "mo:base@0.16.0/Int";
import Iter "mo:base@0.16.0/Iter";
import Option "mo:base@0.16.0/Option";

module {
    type Order = Order.Order;

    /// Min heap state that can be stored as stable
    public type MinHeap<A> = {
        var data : [var ?A];
        var count : Nat;
    };

    /// Create a new empty min heap
    public func new<A>() : MinHeap<A> {
        {
            var data = [var];
            var count = 0;
        };
    };

    /// Create a min heap with initial capacity
    public func newWithCapacity<A>(capacity : Nat) : MinHeap<A> {
        {
            var data = Array.init<?A>(capacity, null);
            var count = 0;
        };
    };

    /// Create a min heap from an array of elements in O(n) time
    public func fromArray<A>(elements : [A], compare : (A, A) -> Order) : MinHeap<A> {
        let heap = newWithCapacity<A>(elements.size());
        heapify(heap, elements, compare);
        heap;
    };

    /// Build a heap from an array of elements in O(n) time
    public func heapify<A>(heap : MinHeap<A>, elements : [A], compare : (A, A) -> Order) {
        let len = elements.size();
        heap.data := Array.init<?A>(len, null);
        heap.count := 0;

        // Add all elements
        for (elem in elements.vals()) {
            heap.data[heap.count] := ?elem;
            heap.count += 1;
        };

        // Heapify from bottom up, starting from last non-leaf node
        if (heap.count > 1) {
            var i : Int = (heap.count / 2) - 1;
            while (i >= 0) {
                siftDown(heap, Int.abs(i), compare);
                i -= 1;
            };
        };
    };

    /// Returns the number of elements in the heap
    public func size<A>(heap : MinHeap<A>) : Nat {
        heap.count;
    };

    /// Returns true if the heap is empty
    public func isEmpty<A>(heap : MinHeap<A>) : Bool {
        heap.count == 0;
    };

    /// Returns the minimum element without removing it
    public func peekMin<A>(heap : MinHeap<A>) : ?A {
        if (heap.count == 0) {
            return null;
        };
        heap.data[0];
    };

    /// Inserts an element into the heap
    public func put<A>(heap : MinHeap<A>, value : A, compare : (A, A) -> Order) {
        // Resize if needed
        if (heap.count >= heap.data.size()) {
            let newSize = if (heap.data.size() == 0) { 4 } else {
                heap.data.size() * 2;
            };

            let newData = Array.tabulateVar<?A>(
                newSize,
                func(i : Nat) : ?A {
                    if (i < heap.data.size()) {
                        heap.data[i];
                    } else {
                        null;
                    };
                },
            );

            heap.data := newData;
        };

        heap.data[heap.count] := ?value;
        siftUp(heap, heap.count, compare);
        heap.count += 1;
    };

    /// Removes and returns the minimum element
    public func removeMin<A>(heap : MinHeap<A>, compare : (A, A) -> Order) : ?A {
        if (heap.count == 0) {
            return null;
        };

        let min = heap.data[0];
        heap.count -= 1;

        if (heap.count > 0) {
            heap.data[0] := heap.data[heap.count];
            heap.data[heap.count] := null;
            siftDown(heap, 0, compare);
        } else {
            heap.data[0] := null;
        };

        min;
    };

    /// Clears all elements from the heap
    public func clear<A>(heap : MinHeap<A>) {
        var i = 0;
        while (i < heap.count) {
            heap.data[i] := null;
            i += 1;
        };
        heap.count := 0;
    };

    public func unsortedVals<A>(heap : MinHeap<A>) : Iter.Iter<A> {
        Iter.map<?A, A>(
            heap.data.vals(),
            func(opt : ?A) : A {
                switch (opt) {
                    case (?val) { val };
                    case (_) {
                        Debug.trap("MinHeap.unsortedVals: Unexpected null value");
                    };
                };
            },
        );
    };

    // Private helper: move element up to maintain heap property
    func siftUp<A>(heap : MinHeap<A>, startIndex : Nat, compare : (A, A) -> Order) {
        var index = startIndex;

        while (index > 0) {
            let parentIndex = (index - 1) / 2;

            switch (heap.data[index], heap.data[parentIndex]) {
                case (?current, ?parent) {
                    switch (compare(current, parent)) {
                        case (#less) {
                            // Swap with parent
                            heap.data[index] := ?parent;
                            heap.data[parentIndex] := ?current;
                            index := parentIndex;
                        };
                        case (_) {
                            return; // Heap property satisfied
                        };
                    };
                };
                case (_) {
                    return;
                };
            };
        };
    };

    // Private helper: move element down to maintain heap property
    func siftDown<A>(heap : MinHeap<A>, startIndex : Nat, compare : (A, A) -> Order) {
        var index = startIndex;

        while (true) {
            let leftChild = 2 * index + 1;
            let rightChild = 2 * index + 2;
            var smallest = index;

            // Check if left child is smaller
            if (leftChild < heap.count) {
                switch (heap.data[leftChild], heap.data[smallest]) {
                    case (?leftVal, ?smallestVal) {
                        switch (compare(leftVal, smallestVal)) {
                            case (#less) {
                                smallest := leftChild;
                            };
                            case (_) {};
                        };
                    };
                    case (_) {};
                };
            };

            // Check if right child is smaller
            if (rightChild < heap.count) {
                switch (heap.data[rightChild], heap.data[smallest]) {
                    case (?rightVal, ?smallestVal) {
                        switch (compare(rightVal, smallestVal)) {
                            case (#less) {
                                smallest := rightChild;
                            };
                            case (_) {};
                        };
                    };
                    case (_) {};
                };
            };

            // If smallest is not the current index, swap and continue
            if (smallest != index) {
                let temp = heap.data[index];
                heap.data[index] := heap.data[smallest];
                heap.data[smallest] := temp;
                index := smallest;
            } else {
                return; // Heap property satisfied
            };
        };
    };
};
