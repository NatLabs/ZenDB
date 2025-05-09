import Iter "mo:base/Iter";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Prelude "mo:base/Prelude";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import BitMap "mo:bit-map";

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
            // "#heap no index",
            "#stableMemory no index",

            // partially covered indexes
            // "#heap 7 single field indexes",
            "#stableMemory 7 single field indexes",

            // multi-field indexes
            // "#heap 6 fully covered indexes",
            "#stableMemory 6 fully covered indexes",

            // "(skip_limit_pagination limit = 100, -> array"

        ]);

        bench.rows([
            "insert with no index",
            "create and populate indexes",
            "clear collection entries and indexes",
            "insert with indexes",

            "query(): no filter (all txs)",
            "query(): single field (btype = '1mint')",

            // And only 2 queries on the same field -> range query
            "query(): number range (250 < tx.amt <= 400)",

            // And only, 1 query each on 2 fields (btype, amt)
            "query(): #And (btype='1burn' AND tx.amt>=750)",

            // #And only, range query on 2 fields (ts, amt)
            "query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)",

            // #Or only, 3 queries on the same field (btype == '1xfer' or '2xfer' or '1mint')",
            "query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')",
            // "query(): #anyOf (btype either of ['1xfer', '2xfer', '1mint'])",

            // #Or only, 1 query each on 2 different fields (btype, amt)
            "query(): #Or (btype == '1xfer' OR tx.amt >= 500)",

            // #Or only, 1 query each on 3 different fields (btype, amt, ts)
            "query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)",

            // complex queries
            // #Or, range query on 2 fields (ts, amt)
            "query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)",

            // #Or, nested or query on 2 fields (btype, amt)
            "query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))",

            "query() -> principals[0] == tx.to.owner (is recipient)",
            "query() -> principals[0..10] == tx.to.owner (is recipient)",
            "query() -> all txs involving principals[0]",
            "query() -> all txs involving principals[0..10]",
            "update(): single operation -> #add amt += 100",
            "update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt",
            "update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt",
            "update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee)",
            "replaceRecord() -> replace half the tx with new tx",
            "delete()",

        ]);

        let limit = 1_000;

        let txs_benchmarks = TxsBenchUtils.TxsBenchmarks(limit);

        bench.runner(
            func(col, row) = txs_benchmarks.run_benchmarks(row, col)
        );

        bench;
    };

};
