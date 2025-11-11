import Iter "mo:base@0.16.0/Iter";
import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Prelude "mo:base@0.16.0/Prelude";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Option "mo:base@0.16.0/Option";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde@3.4.0/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import BitMap "mo:bit-map@0.1.2";

import ZenDB "../src";
import TxsBenchUtils "txs-bench-utils";

module {
    let { QueryBuilder } = ZenDB;

    type Tx = TxsBenchUtils.Tx;
    public let { TxSchema; candify_tx } = TxsBenchUtils;

    public func init() : Bench.Bench {
        let bench = Bench.Bench();

        bench.name("Benchmarking zenDB with icrc3 txs");
        bench.description("Benchmarking the performance with 10k txs");

        bench.cols([
            "#stableMemory no index",
            "#stableMemory 7 single field indexes",
            "#stableMemory 6 fully covered indexes",
        ]);

        bench.rows([
            "insert with no index",
            "create and populate indexes",
            "create and populate indexes 2",
            "create and populate indexes 3",
            "create and populate indexes 4",
            "create and populate indexes 5",
            "query(): no filter (all txs)",
            "query(): single field (btype = '1mint')",
            "query(): number range (250 < tx.amt <= 400)",
            "query(): #And (btype='1burn' AND tx.amt>=750)",
            "query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)",
            "query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')",
            "query(): #anyOf (btype either of ['1xfer', '2xfer', '1mint'])",
            "query(): #Or (btype == '1xfer' OR tx.amt >= 500)",
            "query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)",
            "query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)",
            "query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))",
            "query() -> principals[0] == tx.to.owner (is recipient)",
            "query() -> principals[0..10] == tx.to.owner (is recipient)",
            // "query() -> all txs involving principals[0]",
            // "query() -> all txs involving principals[0..10]",
            // "update(): single operation -> #add amt += 100",
            // "update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt",
            // "update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt",
            // "update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee)",
            // "replace() -> replace half the tx with new tx",
            // "delete()",

        ]);

        let input_limit = 16_000;
        let limit = 1_000;

        let txs_benchmarks = TxsBenchUtils.TxsBenchmarks(input_limit, limit);

        bench.runner(
            func(col, row) = txs_benchmarks.run_benchmarks(row, col)
        );

        bench;
    };

};
