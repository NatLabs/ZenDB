import Debug "mo:base@0.16.0/Debug";
import Array "mo:base@0.16.0/Array";
import Iter "mo:base@0.16.0/Iter";
import Order "mo:base@0.16.0/Order";
import Nat "mo:base@0.16.0/Nat";

import { test; suite } "mo:test";

import Utils "../../src/EmbeddedInstance/Utils";

suite(
    "Utils",
    func() {
        suite(
            "kmerge_or",
            func() {
                test(
                    "Empty iterators should return empty result",
                    func() {
                        let iters : [Iter.Iter<Nat>] = [];
                        let merged = Utils.kmerge_or(iters, Nat.compare);
                        
                        assert merged.next() == null;
                    },
                );

                test(
                    "Single iterator should return its elements",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 3]);
                        let merged = Utils.kmerge_or([iter1], Nat.compare);
                        
                        assert merged.next() == ?1;
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Two sorted iterators should merge correctly",
                    func() {
                        let iter1 = Iter.fromArray([1, 3, 5]);
                        let iter2 = Iter.fromArray([2, 4, 6]);
                        let merged = Utils.kmerge_or([iter1, iter2], Nat.compare);
                        
                        assert merged.next() == ?1;
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == ?4;
                        assert merged.next() == ?5;
                        assert merged.next() == ?6;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Multiple sorted iterators should merge correctly",
                    func() {
                        let iter1 = Iter.fromArray([1, 4, 7]);
                        let iter2 = Iter.fromArray([2, 5, 8]);
                        let iter3 = Iter.fromArray([3, 6, 9]);
                        let merged = Utils.kmerge_or([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?1;
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == ?4;
                        assert merged.next() == ?5;
                        assert merged.next() == ?6;
                        assert merged.next() == ?7;
                        assert merged.next() == ?8;
                        assert merged.next() == ?9;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Overlapping values should be deduplicated",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 3, 5]);
                        let iter2 = Iter.fromArray([2, 3, 4, 6]);
                        let iter3 = Iter.fromArray([1, 3, 5, 7]);
                        let merged = Utils.kmerge_or([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?1;
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == ?4;
                        assert merged.next() == ?5;
                        assert merged.next() == ?6;
                        assert merged.next() == ?7;
                        assert merged.next() == null;
                    },
                );

                test(
                    "All same values should deduplicate to single value",
                    func() {
                        let iter1 = Iter.fromArray([5, 5, 5]);
                        let iter2 = Iter.fromArray([5, 5, 5]);
                        let iter3 = Iter.fromArray([5, 5, 5]);
                        let merged = Utils.kmerge_or([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?5;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Empty iterator in the mix should be handled",
                    func() {
                        let iter1 = Iter.fromArray([1, 3, 5]);
                        let iter2 = Iter.fromArray<Nat>([]);
                        let iter3 = Iter.fromArray([2, 4, 6]);
                        let merged = Utils.kmerge_or([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?1;
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == ?4;
                        assert merged.next() == ?5;
                        assert merged.next() == ?6;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Different length iterators should merge correctly",
                    func() {
                        let iter1 = Iter.fromArray([1, 5, 9, 13, 17]);
                        let iter2 = Iter.fromArray([2, 6]);
                        let iter3 = Iter.fromArray([3, 7, 11, 15]);
                        let merged = Utils.kmerge_or([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?1;
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == ?5;
                        assert merged.next() == ?6;
                        assert merged.next() == ?7;
                        assert merged.next() == ?9;
                        assert merged.next() == ?11;
                        assert merged.next() == ?13;
                        assert merged.next() == ?15;
                        assert merged.next() == ?17;
                        assert merged.next() == null;
                    },
                );
            },
        );

        suite(
            "kmerge_and",
            func() {
                test(
                    "Empty iterators should return empty result",
                    func() {
                        let iters : [Iter.Iter<Nat>] = [];
                        let merged = Utils.kmerge_and(iters, Nat.compare);
                        
                        assert merged.next() == null;
                    },
                );

                test(
                    "Single iterator should return its elements",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 3]);
                        let merged = Utils.kmerge_and([iter1], Nat.compare);
                        
                        assert merged.next() == ?1;
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Two iterators with no overlap should return empty",
                    func() {
                        let iter1 = Iter.fromArray([1, 3, 5]);
                        let iter2 = Iter.fromArray([2, 4, 6]);
                        let merged = Utils.kmerge_and([iter1, iter2], Nat.compare);
                        
                        assert merged.next() == null;
                    },
                );

                test(
                    "Two iterators with complete overlap should return common elements",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 3, 4, 5]);
                        let iter2 = Iter.fromArray([1, 2, 3, 4, 5]);
                        let merged = Utils.kmerge_and([iter1, iter2], Nat.compare);
                        
                        assert merged.next() == ?1;
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == ?4;
                        assert merged.next() == ?5;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Two iterators with partial overlap should return only common elements",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 3, 4, 5, 6]);
                        let iter2 = Iter.fromArray([2, 4, 6, 8]);
                        let merged = Utils.kmerge_and([iter1, iter2], Nat.compare);
                        
                        assert merged.next() == ?2;
                        assert merged.next() == ?4;
                        assert merged.next() == ?6;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Three iterators with common elements",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 3, 4, 5, 6, 7]);
                        let iter2 = Iter.fromArray([2, 3, 4, 5, 6]);
                        let iter3 = Iter.fromArray([3, 4, 5, 6, 8, 9]);
                        let merged = Utils.kmerge_and([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?3;
                        assert merged.next() == ?4;
                        assert merged.next() == ?5;
                        assert merged.next() == ?6;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Multiple iterators with single common element",
                    func() {
                        let iter1 = Iter.fromArray([1, 5, 9]);
                        let iter2 = Iter.fromArray([2, 5, 8]);
                        let iter3 = Iter.fromArray([3, 5, 7]);
                        let merged = Utils.kmerge_and([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?5;
                        assert merged.next() == null;
                    },
                );

                test(
                    "Empty iterator in the mix should return empty",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 3, 4, 5]);
                        let iter2 = Iter.fromArray<Nat>([]);
                        let iter3 = Iter.fromArray([2, 3, 4]);
                        let merged = Utils.kmerge_and([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == null;
                    },
                );

                test(
                    "Different length iterators with overlap",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 3, 4, 5, 10, 15, 20]);
                        let iter2 = Iter.fromArray([2, 4, 10, 20]);
                        let iter3 = Iter.fromArray([2, 3, 4, 9, 10, 20, 25]);
                        let merged = Utils.kmerge_and([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?2;
                        assert merged.next() == ?4;
                        assert merged.next() == ?10;
                        assert merged.next() == ?20;
                        assert merged.next() == null;
                    },
                );

                test(
                    "All iterators with same single element",
                    func() {
                        let iter1 = Iter.fromArray([5]);
                        let iter2 = Iter.fromArray([5]);
                        let iter3 = Iter.fromArray([5]);
                        let merged = Utils.kmerge_and([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?5;
                        assert merged.next() == null;
                    },
                );

                test(
                    "No common elements across all iterators",
                    func() {
                        let iter1 = Iter.fromArray([1, 4, 7, 10]);
                        let iter2 = Iter.fromArray([2, 5, 8, 11]);
                        let iter3 = Iter.fromArray([3, 6, 9, 12]);
                        let merged = Utils.kmerge_and([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == null;
                    },
                );

                test(
                    "Large overlap with many duplicates",
                    func() {
                        let iter1 = Iter.fromArray([1, 2, 2, 3, 3, 3, 4, 4, 4, 4]);
                        let iter2 = Iter.fromArray([2, 2, 3, 3, 3, 4, 4, 4, 4, 5]);
                        let iter3 = Iter.fromArray([1, 2, 2, 3, 3, 3, 4, 4, 4, 4]);
                        let merged = Utils.kmerge_and([iter1, iter2, iter3], Nat.compare);
                        
                        assert merged.next() == ?2;
                        assert merged.next() == ?3;
                        assert merged.next() == ?4;
                        assert merged.next() == null;
                    },
                );
            },
        );
    },
);
