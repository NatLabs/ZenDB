import Array "mo:base@0.16.0/Array";
import Int "mo:base@0.16.0/Int";
import Nat "mo:base@0.16.0/Nat";
import Types "Types";

module {
    /// Sorts an array using the Merge Sort algorithm
    /// Time Complexity: O(n log n)
    /// Space Complexity: O(n)
    public func sort<T>(arr : [T], compare : Types.CompareFunc<T>) : [T] {
        if (arr.size() <= 1) return arr;
        mergesortHelper(arr, 0, arr.size(), compare);
    };

    private func mergesortHelper<T>(arr : [T], start : Nat, end : Nat, compare : Types.CompareFunc<T>) : [T] {
        let n = Int.abs(end - start);
        if (n <= 1) return Array.tabulate<T>(n, func(i) { arr[start + i] });

        let mid = start + n / 2;
        let sortedLeft = mergesortHelper(arr, start, mid, compare);
        let sortedRight = mergesortHelper(arr, mid, end, compare);

        merge(sortedLeft, sortedRight, compare);
    };

    private func merge<T>(left : [T], right : [T], compare : Types.CompareFunc<T>) : [T] {
        let leftSize = left.size();
        let rightSize = right.size();
        var i = 0;
        var j = 0;

        Array.tabulate<T>(
            leftSize + rightSize,
            func(_ : Nat) : T {
                if (i >= leftSize) {
                    let val = right[j];
                    j += 1;
                    val;
                } else if (j >= rightSize) {
                    let val = left[i];
                    i += 1;
                    val;
                } else {
                    switch (compare(left[i], right[j])) {
                        case (#less or #equal) {
                            let val = left[i];
                            i += 1;
                            val;
                        };
                        case (#greater) {
                            let val = right[j];
                            j += 1;
                            val;
                        };
                    };
                };
            },
        );
    };
};
