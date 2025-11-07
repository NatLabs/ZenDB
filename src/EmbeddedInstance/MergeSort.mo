import Array "mo:base@0.16.0/Array";
import Int "mo:base@0.16.0/Int";
import Nat "mo:base@0.16.0/Nat";
import Types "Types";

module {
    /// Bottom-Up Merge Sort
    /// Iterative approach that merges progressively larger subarrays
    /// Time Complexity: O(n log n)
    /// Space Complexity: O(n)
    public func sort<T>(arr : [T], compare : Types.CompareFunc<T>) : [T] {
        if (arr.size() <= 1) return arr;

        let n = arr.size();
        var current = arr;
        var buffer = Array.init<T>(n, arr[0]);
        var usingBuffer = false;

        // Start with merge size of 1, double each iteration
        var size = 1;
        while (size < n) {
            var left = 0;

            // Merge adjacent subarrays of current size
            while (left < n) {
                let mid = Nat.min(left + size - 1, n - 1);
                let right = Nat.min(left + 2 * size - 1, n - 1);

                if (mid < right) {
                    if (usingBuffer) {
                        mergeFromBuffer(buffer, current, left, mid, right, compare);
                    } else {
                        mergeToBuffer(current, buffer, left, mid, right, compare);
                    };
                } else if (usingBuffer) {
                    // Copy remaining elements
                    var i = left;
                    while (i <= right) {
                        buffer[i] := current[i];
                        i += 1;
                    };
                };

                left += 2 * size;
            };

            // Swap which array we're using
            usingBuffer := not usingBuffer;
            size *= 2;
        };

        if (usingBuffer) {
            Array.freeze(buffer);
        } else {
            current;
        };
    };

    // Merge from source array to buffer
    private func mergeToBuffer<T>(
        source : [T],
        dest : [var T],
        left : Nat,
        mid : Nat,
        right : Nat,
        compare : Types.CompareFunc<T>,
    ) {
        var i = left; // Index for left subarray
        var j = mid + 1; // Index for right subarray
        var k = left; // Index for merged array

        // Merge while both subarrays have elements
        while (i <= mid and j <= right) {
            switch (compare(source[i], source[j])) {
                case (#less or #equal) {
                    dest[k] := source[i];
                    i += 1;
                };
                case (#greater) {
                    dest[k] := source[j];
                    j += 1;
                };
            };
            k += 1;
        };

        // Copy remaining elements from left subarray
        while (i <= mid) {
            dest[k] := source[i];
            i += 1;
            k += 1;
        };

        // Copy remaining elements from right subarray
        while (j <= right) {
            dest[k] := source[j];
            j += 1;
            k += 1;
        };
    };

    // Merge from buffer back (in-place in the buffer itself)
    private func mergeFromBuffer<T>(
        source : [var T],
        _dest : [T],
        left : Nat,
        mid : Nat,
        right : Nat,
        compare : Types.CompareFunc<T>,
    ) {
        // We need to carefully merge within the buffer itself
        // Create temporary copies of the subarrays
        let leftSize = Int.abs(mid - left + 1);
        let rightSize = Int.abs(right - mid);

        let leftArr = Array.tabulate<T>(leftSize, func(i) { source[left + i] });
        let rightArr = Array.tabulate<T>(rightSize, func(i) { source[mid + 1 + i] });

        var i = 0;
        var j = 0;
        var k = left;

        while (i < leftSize and j < rightSize) {
            switch (compare(leftArr[i], rightArr[j])) {
                case (#less or #equal) {
                    source[k] := leftArr[i];
                    i += 1;
                };
                case (#greater) {
                    source[k] := rightArr[j];
                    j += 1;
                };
            };
            k += 1;
        };

        while (i < leftSize) {
            source[k] := leftArr[i];
            i += 1;
            k += 1;
        };

        while (j < rightSize) {
            source[k] := rightArr[j];
            j += 1;
            k += 1;
        };
    };
};
