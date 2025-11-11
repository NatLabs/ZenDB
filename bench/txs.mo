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
            "#heap no index",
            "#stableMemory no index",

            // partially covered indexes
            "#heap 7 single field indexes",
            "#stableMemory 7 single field indexes",

            // multi-field indexes
            "#heap 6 fully covered indexes",
            "#stableMemory 6 fully covered indexes",

            // "(skip_limit_pagination limit = 100, -> array"

        ]);

        bench.rows([
            "insert with no index",
            "create and populate indexes",
            "create and populate indexes 2",
            "create and populate indexes 3",
            "create and populate indexes 4",
            "create and populate indexes 5",
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
            // "update(): single operation -> #add amt += 100",
            // "update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt",
            // "update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt",
            // "update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee)",
            // "replace() -> replace half the tx with new tx",
            // "delete()",

        ]);

        let limit = 1_0;
        let fuzz = Fuzz.fromSeed(0x7eadbeef);

        let principals = Array.tabulate(
            50,
            func(i : Nat) : Principal {
                fuzz.principal.randomPrincipal(29);
            },
        );

        let candid_principals = Array.map<Principal, ZenDB.Types.Candid>(
            Iter.toArray(Array.slice<Principal>(principals, 0, 10)),
            func(p : Principal) : ZenDB.Types.Candid = #Principal(p),
        );

        let principals_0_10 = Array.tabulate(
            10,
            func(i : Nat) : Principal {
                principals.get(i);
            },
        );

        let candid_principals_0_10 = Array.map<Principal, ZenDB.Types.Candid>(
            principals_0_10,
            func(p : Principal) : ZenDB.Types.Candid = #Principal(p),
        );

        let predefined_txs = Buffer.Buffer<Tx>(limit);
        let tx_ids = Buffer.Buffer<Nat>(limit);

        for (i in Iter.range(0, limit - 1)) {
            let tx = TxsBenchUtils.new_tx(fuzz, principals);
            predefined_txs.add(tx);
        };

        let canister_id = fuzz.principal.randomPrincipal(29);

        let heap_db_sstore = ZenDB.newStableStore(canister_id, ?{ ZenDB.defaultSettings with memory_type = ?(#heap) });
        let heap_db = ZenDB.launchDefaultDB(heap_db_sstore);
        let #ok(heap_no_index) = heap_db.createCollection<Tx>("heap_no_index", TxSchema, candify_tx, []);
        let #ok(heap_single_field_indexes) = heap_db.createCollection<Tx>("heap_single_field_indexes", TxSchema, candify_tx, []);
        let #ok(heap_fully_covered_indexes) = heap_db.createCollection<Tx>("heap_fully_covered_indexes", TxSchema, candify_tx, []);

        let stable_memory_db_sstore = ZenDB.newStableStore(canister_id, ?{ ZenDB.defaultSettings with memory_type = ?(#stableMemory) });
        let stable_memory_db = ZenDB.launchDefaultDB(stable_memory_db_sstore);
        let #ok(stable_memory_no_index) = stable_memory_db.createCollection<Tx>("stable_memory_no_index", TxSchema, candify_tx, []);
        let #ok(stable_memory_single_field_indexes) = stable_memory_db.createCollection<Tx>("stable_memory_single_field_indexes", TxSchema, candify_tx, []);
        let #ok(stable_memory_fully_covered_indexes) = stable_memory_db.createCollection<Tx>("stable_memory_fully_covered_indexes", TxSchema, candify_tx, []);

        func new_query() : () -> ZenDB.QueryBuilder {
            func() { ZenDB.QueryBuilder() };
        };

        let prev_indexes = [
            [("btype", #Ascending), ("tx.amt", #Ascending)],
            [("btype", #Ascending), ("ts", #Ascending)],
            [("tx.amt", #Ascending)],
            [("ts", #Ascending)],
            [("tx.from.owner", #Ascending), ("tx.from.sub_account", #Ascending)],
            [("tx.to.owner", #Ascending), ("tx.to.sub_account", #Ascending)],
            [("tx.spender.owner", #Ascending), ("tx.spender.sub_account", #Ascending)],
        ];

        let single_field_indexes = [
            [("btype", #Ascending)],
            [("tx.amt", #Ascending)],
            [("ts", #Ascending)],
            [("tx.from.owner", #Ascending)],
            [("tx.to.owner", #Ascending)],
            [("tx.spender.owner", #Ascending)],
            [("fee", #Ascending)],
        ];

        let fully_covered_indexes = [
            [("tx.amt", #Ascending)],
            [("ts", #Ascending)],
            [("btype", #Ascending), ("tx.amt", #Ascending)],
            [("btype", #Ascending), ("ts", #Ascending)],
            [("tx.from.owner", #Ascending), ("ts", #Ascending)],
            [("tx.to.owner", #Ascending), ("ts", #Ascending)],
            [("tx.spender.owner", #Ascending), ("ts", #Ascending)],
        ];

        bench.runner(
            func(col, row) = switch (row) {
                case ("#heap no index") {
                    TxsBenchUtils.run_collection_benchmarks(
                        col,
                        heap_no_index,
                        [],
                        predefined_txs,
                        Buffer.Buffer<Nat>(limit),
                        principals,
                        candid_principals_0_10,
                        new_query(),
                        fuzz,
                        limit,
                    );
                };

                case ("#stableMemory no index") {
                    TxsBenchUtils.run_collection_benchmarks(
                        col,
                        stable_memory_no_index,
                        [],
                        predefined_txs,
                        Buffer.Buffer<Nat>(limit),
                        principals,
                        candid_principals_0_10,
                        new_query(),
                        fuzz,
                        limit,
                    );
                };

                case ("#heap 7 single field indexes") {
                    TxsBenchUtils.run_collection_benchmarks(
                        col,
                        heap_single_field_indexes,
                        single_field_indexes,
                        predefined_txs,
                        Buffer.Buffer<Nat>(limit),
                        principals,
                        candid_principals_0_10,
                        new_query(),
                        fuzz,
                        limit,
                    );
                };

                case ("#stableMemory 7 single field indexes") {
                    TxsBenchUtils.run_collection_benchmarks(
                        col,
                        stable_memory_single_field_indexes,
                        single_field_indexes,
                        predefined_txs,
                        Buffer.Buffer<Nat>(limit),
                        principals,
                        candid_principals_0_10,
                        new_query(),
                        fuzz,
                        limit,
                    );
                };

                case ("#heap 6 fully covered indexes") {
                    TxsBenchUtils.run_collection_benchmarks(
                        col,
                        heap_fully_covered_indexes,
                        fully_covered_indexes,
                        predefined_txs,
                        Buffer.Buffer<Nat>(limit),
                        principals,
                        candid_principals_0_10,
                        new_query(),
                        fuzz,
                        limit,
                    );
                };

                case ("#stableMemory 6 fully covered indexes") {
                    TxsBenchUtils.run_collection_benchmarks(
                        col,
                        stable_memory_fully_covered_indexes,
                        fully_covered_indexes,
                        predefined_txs,
                        Buffer.Buffer<Nat>(limit),
                        principals,
                        candid_principals_0_10,
                        new_query(),
                        fuzz,
                        limit,
                    );
                };

                case (_) {
                    Debug.trap("Should be unreachable:\n row = \"" # debug_show row # "\" and col = \"" # debug_show col # "\"");
                };

            }
        );

        bench;
    };

};
