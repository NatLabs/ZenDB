import Heap_Txs_Bench "../bench/heap.txs.bench";
import Heap_Txs_Sorted_Bench "../bench/heap.txs.sorted.bench";
import StableMemory_Txs_Bench "../bench/stable-memory.txs.bench";
import StableMemory_Txs_Sorted_Bench "../bench/stable-memory.txs.sorted.bench";

import { test; suite } "mo:test/async";
import Bench "mo:bench";

import Itertools "mo:itertools@0.2.2/Iter";

persistent actor {

    transient let benchmarks = [Heap_Txs_Bench, Heap_Txs_Sorted_Bench, StableMemory_Txs_Bench, StableMemory_Txs_Sorted_Bench];

    transient var bench = benchmarks[0].init();

    public func runTest(schema : Bench.BenchSchema, i : Nat, j : Nat) : async () {
        await test(
            "Run benchmark: " # schema.rows[i] # " - " # schema.cols[j],
            func() : async () {
                bench.runCell(i, j);
            },
        );
    };

    public func runTests() : async () {
        for (benchmark_config in benchmarks.vals()) {
            bench := benchmark_config.init();
            let schema = bench.getSchema();

            await suite(
                schema.name # " Benchmark Tests",
                func() : async () {

                    for ((i, row) in Itertools.enumerate(schema.rows.vals())) {
                        for ((j, col) in Itertools.enumerate(schema.cols.vals())) {
                            await runTest(schema, i, j);
                        };
                    };
                },
            );
        };
    };

};
