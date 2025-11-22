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
import SparseBitMap64 "mo:bit-map@0.1.2/SparseBitMap64";

import ZenDB "../src";
import Utils "../src/EmbeddedInstance/Utils";

module TxsBenchUtils {

    public let Cols = [

        // "#heap no index",
        "#heap no index (sorted by ts)",
        "#heap 7 single field indexes (sorted by ts)",
        "#heap 6 fully covered indexes (sorted by ts)",

        "#stableMemory no index (sorted by ts)",
        "#stableMemory no index (sorted by tx.amt)",

        // partially covered indexes sorted by tx.amt
        // "#heap 7 single field indexes (sorted by ts)",
        "#stableMemory 7 single field indexes (sorted by tx.amt)",

        // multi-field indexes sorted by timestamp
        // "#heap 6 fully covered indexes (sorted by ts)",
        "#stableMemory 6 fully covered indexes (sorted by ts)",
        "#stableMemory 6 fully covered indexes (sorted by tx.amt)",

    ];

    public let Rows = [
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
        "replace() -> replace half the tx with new tx",
        "delete()",

    ];

    type Account = {
        owner : Principal;
        // sub_account : ?Blob; // null == [0...0]
    };

    public type Tx = {
        btype : Text;
        phash : Blob;
        ts : Nat;
        tx : {
            amt : Nat;
            from : ?Account;
            to : ?Account;
            spender : ?Account;
            memo : ?Blob;
        };
        fee : ?Nat;
    };

    let AccountSchema = #Record([
        ("owner", #Principal),
        // ("sub_account", #Option(#Blob)),
    ]);

    public let TxSchema : ZenDB.Types.Schema = #Record([
        ("btype", #Text),
        ("phash", #Blob),
        ("ts", #Nat),
        (
            "tx",
            #Record([("amt", #Nat), ("from", #Option(AccountSchema)), ("to", #Option(AccountSchema)), ("spender", #Option(AccountSchema)), ("memo", #Option(#Blob))]),
        ),
        ("fee", #Option(#Nat)),
    ]);

    public let candify_tx = {
        from_blob = func(blob : Blob) : ?Tx {
            from_candid (blob);
        };
        to_blob = func(c : Tx) : Blob { to_candid (c) };
    };

    public func new_tx(fuzz : Fuzz.Fuzzer, principals : [Principal]) : Tx {

        let block_types = [
            "1mint",
            "2approve",
            "1xfer",
            "2xfer",
            "1burn",
        ];

        let btype = fuzz.array.randomEntry(block_types).1;

        let tx : Tx = {
            btype;
            phash = fuzz.blob.randomBlob(32);
            ts = fuzz.nat.randomRange(0, 1000000);
            fee = switch (btype) {
                case ("1mint" or "2approve" or "1burn") { null };
                case ("1xfer" or "2xfer") { ?20 };
                case (_) { null };
            };

            tx = {
                amt = fuzz.nat.randomRange(0, 1000);
                memo = if (fuzz.nat.randomRange(0, 100) % 3 == 0) { null } else {
                    ?fuzz.blob.randomBlob(32);
                };
                to = switch (btype) {
                    case ("1mint" or "1xfer" or "2xfer") {
                        ?{
                            owner = fuzz.array.randomEntry(principals).1;
                            sub_account = if (fuzz.nat.randomRange(0, 100) % 3 != 0) {
                                null;
                            } else {
                                ?fuzz.blob.randomBlob(32);
                            };
                        };
                    };
                    case ("2approve" or "1burn") { null };
                    case (_) { null };
                };

                from = switch (btype) {
                    case ("1mint") { null };
                    case ("1xfer" or "2xfer" or "2approve" or "1burn") {
                        ?{
                            owner = fuzz.array.randomEntry(principals).1;
                            sub_account = if (fuzz.nat.randomRange(0, 100) % 3 != 0) {
                                null;
                            } else {
                                ?fuzz.blob.randomBlob(32);
                            };
                        };
                    };
                    case (_) { null };
                };

                spender = switch (btype) {
                    case ("1mint" or "1xfer" or "2xfer" or "1burn") { null };
                    case ("2approve") {
                        ?{
                            owner = fuzz.array.randomEntry(principals).1;
                            sub_account = if (fuzz.nat.randomRange(0, 100) % 3 != 0) {
                                null;
                            } else {
                                ?fuzz.blob.randomBlob(32);
                            };
                        };
                    };
                    case (_) { null };
                };
            };
        };
    };

    public class TxsBenchmarks(input_limit : Nat, iteration_limit : Nat) {

        let fuzz = Fuzz.fromSeed(0x7eadbeef);

        let principals = Array.tabulate(
            (input_limit / iteration_limit) * 50,
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

        let predefined_txs = Buffer.Buffer<Tx>(input_limit);
        let tx_ids = Buffer.Buffer<Blob>(input_limit);

        for (i in Iter.range(0, input_limit - 1)) {
            let tx = TxsBenchUtils.new_tx(fuzz, principals);
            predefined_txs.add(tx);
        };

        let canister_id = fuzz.principal.randomPrincipal(29);

        let heap_db_sstore = ZenDB.newStableStore(canister_id, ?{ ZenDB.defaultSettings with memory_type = ?(#heap); cache_capacity = ?(iteration_limit / 100) }); // 1% cache
        let heap_db = ZenDB.launchDefaultDB(heap_db_sstore);
        let #ok(heap_no_index) = heap_db.createCollection<Tx>("heap_no_index", TxSchema, candify_tx, null);
        let #ok(heap_single_field_indexes) = heap_db.createCollection<Tx>("heap_single_field_indexes", TxSchema, candify_tx, null);
        let #ok(heap_fully_covered_indexes) = heap_db.createCollection<Tx>("heap_fully_covered_indexes", TxSchema, candify_tx, null);
        let #ok(heap_sorted_no_index) = heap_db.createCollection<Tx>("heap_sorted_no_index", TxSchema, candify_tx, null);
        let #ok(heap_sorted_single_field_indexes) = heap_db.createCollection<Tx>("heap_sorted_single_field_indexes", TxSchema, candify_tx, null);
        let #ok(heap_sorted_fully_covered_indexes) = heap_db.createCollection<Tx>("heap_sorted_fully_covered_indexes", TxSchema, candify_tx, null);
        let #ok(heap_sorted_amt_no_index) = heap_db.createCollection<Tx>("heap_sorted_amt_no_index", TxSchema, candify_tx, null);
        let #ok(heap_sorted_amt_single_field_indexes) = heap_db.createCollection<Tx>("heap_sorted_amt_single_field_indexes", TxSchema, candify_tx, null);
        let #ok(heap_sorted_amt_fully_covered_indexes) = heap_db.createCollection<Tx>("heap_sorted_amt_fully_covered_indexes", TxSchema, candify_tx, null);

        let stable_memory_db_sstore = ZenDB.newStableStore(canister_id, ?{ ZenDB.defaultSettings with memory_type = ?(#stableMemory); cache_capacity = ?(iteration_limit / 100) }); // 1% cache
        let stable_memory_db = ZenDB.launchDefaultDB(stable_memory_db_sstore);
        let #ok(stable_memory_no_index) = stable_memory_db.createCollection<Tx>("stable_memory_no_index", TxSchema, candify_tx, null);
        let #ok(stable_memory_single_field_indexes) = stable_memory_db.createCollection<Tx>("stable_memory_single_field_indexes", TxSchema, candify_tx, null);
        let #ok(stable_memory_fully_covered_indexes) = stable_memory_db.createCollection<Tx>("stable_memory_fully_covered_indexes", TxSchema, candify_tx, null);
        let #ok(stable_memory_sorted_no_index) = stable_memory_db.createCollection<Tx>("stable_memory_sorted_no_index", TxSchema, candify_tx, null);
        let #ok(stable_memory_sorted_single_field_indexes) = stable_memory_db.createCollection<Tx>("stable_memory_sorted_single_field_indexes", TxSchema, candify_tx, null);
        let #ok(stable_memory_sorted_fully_covered_indexes) = stable_memory_db.createCollection<Tx>("stable_memory_sorted_fully_covered_indexes", TxSchema, candify_tx, null);

        let #ok(stable_memory_sorted_amt_no_index) = stable_memory_db.createCollection<Tx>("stable_memory_sorted_amt_no_index", TxSchema, candify_tx, null);
        let #ok(stable_memory_sorted_amt_single_field_indexes) = stable_memory_db.createCollection<Tx>("stable_memory_sorted_amt_single_field_indexes", TxSchema, candify_tx, null);
        let #ok(stable_memory_sorted_amt_fully_covered_indexes) = stable_memory_db.createCollection<Tx>("stable_memory_sorted_amt_fully_covered_indexes", TxSchema, candify_tx, null);

        func new_query() : () -> ZenDB.QueryBuilder {
            func() { ZenDB.QueryBuilder() };
        };

        func new_sorted_query(field : Text, dir : ZenDB.Types.SortDirection) : () -> ZenDB.QueryBuilder {
            func() { ZenDB.QueryBuilder().Sort(field, dir) };
        };

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
            // [("tx.from.owner", #Ascending), ("tx.amt", #Ascending)],
            // [("tx.to.owner", #Ascending), ("tx.amt", #Ascending)],
            // [("tx.spender.owner", #Ascending), ("tx.amt", #Ascending)],
        ];

        public class CollectionBenchmark(
            collection : ZenDB.Collection<Tx>,
            indexes : [[(Text, ZenDB.Types.SortDirection)]],
            inputs : Buffer.Buffer<Tx>,
            tx_ids : Buffer.Buffer<Nat>,
            principals : [Principal],
            candid_principals_0_10 : [ZenDB.Types.Candid],
            new_query : () -> ZenDB.QueryBuilder,
            fuzz : Fuzz.Fuzzer,
            iteration_limit : Nat,
        ) {

            // Batch processing state variables
            var batch_id : ?Nat = null;
            var needs_processing : Bool = false;

            // Helper function to continue processing batch if needed
            func continue_batch_processing() {
                switch (batch_id) {
                    case (?id) {
                        if (needs_processing) {
                            switch (collection.processIndexBatch(id)) {
                                case (#ok(continue_processing)) {
                                    needs_processing := continue_processing;
                                    // If processing is complete, reset batch_id since batch is automatically cleaned up
                                    if (not continue_processing) {
                                        batch_id := null;
                                    };
                                };
                                case (#err(err_msg)) {
                                    Debug.trap(err_msg);
                                };
                            };
                        };
                    };
                    case (null) {};
                };
            };

            public func run_benchmark(benchmark_name : Text) {

                switch (benchmark_name) {
                    case ("insert with no index") {
                        for (tx in inputs.vals()) {
                            let #ok(_) = collection.insert(tx);
                        };

                        // Debug.print("stats: " # debug_show (collection.stats()));
                    };

                    case ("create and populate indexes") {
                        // Create batch for indexes
                        let index_configs = Array.tabulate<ZenDB.Types.CreateIndexParams>(
                            indexes.size(),
                            func(i : Nat) : ZenDB.Types.CreateIndexParams {
                                let index_name = "index_" # debug_show (i);
                                (index_name, indexes[i], null); // name, key_details, ?options
                            },
                        );

                        if (index_configs.size() == 0) return;

                        let res = collection.batchCreateIndexes(index_configs);
                        switch (res) {
                            case (#ok(id)) {
                                batch_id := ?id;
                                needs_processing := true;
                                // Process the first batch
                                switch (collection.processIndexBatch(id)) {
                                    case (#ok(continue_processing)) {
                                        needs_processing := continue_processing;
                                        // If processing is complete, reset batch_id since batch is automatically cleaned up
                                        if (not continue_processing) {
                                            batch_id := null;
                                        };
                                    };
                                    case (#err(err_msg)) {
                                        Debug.trap(err_msg);
                                    };
                                };
                            };
                            case (#err(err)) { Debug.trap(err) };
                        };
                    };

                    case ("create and populate indexes 2") {
                        continue_batch_processing();
                    };

                    case ("create and populate indexes 3") {
                        continue_batch_processing();
                    };

                    case ("create and populate indexes 4") {
                        continue_batch_processing();
                    };

                    case ("create and populate indexes 5") {
                        continue_batch_processing();
                    };

                    case ("clear collection entries and indexes") {
                        collection.clear();
                    };

                    case ("insert with indexes") {
                        for (tx in inputs.vals()) {
                            let #ok(id) = collection.insert(tx);
                            tx_ids.add(Utils.nat_from_12_byte_blob(id));
                        };
                    };

                    case ("query(): no filter (all txs)") {
                        let db_query = new_query();
                        let #ok(matching_txs) = collection.search(db_query);
                        // assert matching_txs.size() == iteration_limit;
                    };

                    case ("query(): single field (btype = '1mint')") {
                        let db_query = new_query().Where(
                            "btype",
                            #eq(#Text("1mint")),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query(): number range (250 < tx.amt <= 400)") {
                        let db_query = new_query().Where(
                            "tx.amt",
                            #gt(#Nat(250)),
                        ).And(
                            "tx.amt",
                            #lte(#Nat(400)),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query(): #And (btype='1burn' AND tx.amt>=750)") {
                        let db_query = new_query().Where(
                            "btype",
                            #eq(#Text("1burn")),
                        ).And(
                            "tx.amt",
                            #gte(#Nat(750)),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)") {
                        let db_query = new_query().Where(
                            "ts",
                            #gt(#Nat(500_000)),
                        ).And(
                            "ts",
                            #lte(#Nat(1_000_000)),
                        ).And(
                            "tx.amt",
                            #gt(#Nat(200)),
                        ).And(
                            "tx.amt",
                            #lte(#Nat(600)),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')") {
                        let db_query = new_query().Where(
                            "btype",
                            #anyOf([#Text("1xfer"), #Text("2xfer"), #Text("1mint")]),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query(): #Or (btype == '1xfer' OR tx.amt >= 500)") {
                        let db_query = new_query().Where(
                            "btype",
                            #eq(#Text("1xfer")),
                        ).Or(
                            "tx.amt",
                            #gte(#Nat(500)),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)") {
                        let db_query = new_query().Where(
                            "btype",
                            #eq(#Text("1xfer")),
                        ).Or(
                            "tx.amt",
                            #gte(#Nat(500)),
                        ).Or(
                            "ts",
                            #gt(#Nat(500_000)),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)") {
                        let db_query = new_query().Where(
                            "ts",
                            #gt(#Nat(500_000)),
                        ).And(
                            "ts",
                            #lte(#Nat(1_000_000)),
                        ).Or(
                            "tx.amt",
                            #gt(#Nat(200)),
                        ).And(
                            "tx.amt",
                            #lte(#Nat(600)),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))") {
                        let db_query = new_query().Where(
                            "btype",
                            #anyOf([#Text("1xfer"), #Text("1burn")]),
                        ).Or(
                            "tx.amt",
                            #lt(#Nat(200)),
                        ).Or(
                            "tx.amt",
                            #gte(#Nat(800)),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query() -> principals[0] == tx.to.owner (is recipient)") {
                        let db_query = new_query().Where(
                            "tx.to.owner",
                            #eq(#Principal(principals.get(0))),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query() -> principals[0..10] == tx.to.owner (is recipient)") {
                        let db_query = new_query().Where(
                            "tx.to.owner",
                            #anyOf(candid_principals_0_10),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query() -> all txs involving principals[0]") {
                        let db_query = new_query().Where(
                            "tx.to.owner",
                            #eq(#Principal(principals.get(0))),
                        ).Or(
                            "tx.from.owner",
                            #eq(#Principal(principals.get(0))),
                        ).Or(
                            "tx.spender.owner",
                            #eq(#Principal(principals.get(0))),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };

                    case ("query() -> all txs involving principals[0..10]") {

                        let db_query = new_query().Where(
                            "tx.to.owner",
                            #anyOf(candid_principals_0_10),
                        ).Or(
                            "tx.from.owner",
                            #anyOf(candid_principals_0_10),
                        ).Or(
                            "tx.spender.owner",
                            #anyOf(candid_principals_0_10),
                        );

                        let #ok(matching_txs) = collection.search(db_query);
                    };
                    case ("update(): single operation -> #add amt += 100") {
                        // Debug.print("tx_ids.size(): " # debug_show (tx_ids.size()));
                        for (i in Itertools.take(tx_ids.vals(), iteration_limit)) {
                            let #ok(_) = collection.updateById(Utils.nat_to_12_byte_blob(i), [("tx.amt", #add(#currValue, #Nat(100)))]);
                        };
                    };

                    case ("update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt") {
                        for (i in Itertools.take(tx_ids.vals(), iteration_limit)) {
                            let #ok(_) = collection.updateById(
                                Utils.nat_to_12_byte_blob(i),
                                [
                                    ("tx.amt", #add(#currValue, #Nat(100))),
                                    ("tx.amt", #sub(#currValue, #Nat(50))),
                                    ("tx.amt", #mul(#currValue, #Nat(2))),
                                    ("tx.amt", #div(#currValue, #Nat(2))),
                                ],
                            );
                        };
                    };

                    case ("update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt") {
                        for (i in Itertools.take(tx_ids.vals(), iteration_limit)) {
                            let #ok(_) = collection.updateById(
                                Utils.nat_to_12_byte_blob(i),
                                [("tx.amt", #div(#mul(#sub(#add(#currValue, #Nat(100)), #Nat(50)), #Nat(2)), #Nat(2)))],
                            );
                        };
                    };

                    case ("update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee)") {
                        for (i in Itertools.take(tx_ids.vals(), iteration_limit)) {
                            let #ok(_) = collection.updateById(
                                Utils.nat_to_12_byte_blob(i),
                                [
                                    ("tx.amt", #add(#currValue, #Nat(100))),
                                    ("ts", #sub(#currValue, #Nat(50))),
                                    ("fee", #mul(#currValue, #Nat(2))),
                                    ("tx.amt", #div(#currValue, #Nat(2))),
                                ],
                            );
                        };
                    };

                    case ("replace() -> replace half the tx with new tx") {
                        for (i in Itertools.take(tx_ids.vals(), iteration_limit)) {
                            let #ok(_) = collection.replace(Utils.nat_to_12_byte_blob(i), new_tx(fuzz, principals));
                        };
                    };

                    case ("delete()") {
                        for (i in Itertools.take(tx_ids.vals(), iteration_limit)) {
                            let #ok(_) = collection.deleteById(Utils.nat_to_12_byte_blob(i));
                        };
                    };

                    case (_) {
                        Debug.trap("Should be unreachable:\n row = zenDB index intersection and col = \"" # debug_show benchmark_name # "\"");
                    };

                };
            };
        };

        let heap_no_index_benchmark = CollectionBenchmark(
            heap_no_index,
            [],
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_query(),
            fuzz,
            iteration_limit,
        );

        let stable_memory_no_index_benchmark = CollectionBenchmark(
            stable_memory_no_index,
            [],
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_query(),
            fuzz,
            iteration_limit,
        );

        let stable_memory_single_field_indexes_benchmark = CollectionBenchmark(
            stable_memory_single_field_indexes,
            single_field_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_query(),
            fuzz,
            iteration_limit,
        );

        let heap_single_field_indexes_benchmark = CollectionBenchmark(
            heap_single_field_indexes,
            single_field_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_query(),
            fuzz,
            iteration_limit,
        );

        let heap_fully_covered_indexes_benchmark = CollectionBenchmark(
            heap_fully_covered_indexes,
            fully_covered_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_query(),
            fuzz,
            iteration_limit,
        );

        let stable_memory_fully_covered_indexes_benchmark = CollectionBenchmark(
            stable_memory_fully_covered_indexes,
            fully_covered_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_query(),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_no_index_benchmark = CollectionBenchmark(
            heap_sorted_no_index,
            [],
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("ts", #Ascending),
            fuzz,
            iteration_limit,
        );

        let stable_memory_sorted_no_index_benchmark = CollectionBenchmark(
            stable_memory_sorted_no_index,
            [],
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("ts", #Ascending),
            fuzz,
            iteration_limit,
        );

        let stable_memory_sorted_single_field_indexes_benchmark = CollectionBenchmark(
            stable_memory_sorted_single_field_indexes,
            single_field_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("ts", #Ascending),
            fuzz,
            iteration_limit,
        );

        let stable_memory_sorted_single_field_indexes_by_amt_benchmark = CollectionBenchmark(
            stable_memory_sorted_single_field_indexes,
            single_field_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_single_field_indexes_benchmark = CollectionBenchmark(
            heap_sorted_single_field_indexes,
            single_field_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("ts", #Ascending),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_fully_covered_indexes_benchmark = CollectionBenchmark(
            heap_sorted_fully_covered_indexes,
            fully_covered_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("ts", #Ascending),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_amt_no_index_benchmark = CollectionBenchmark(
            heap_sorted_amt_no_index,
            [],
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_amt_single_field_indexes_benchmark = CollectionBenchmark(
            heap_sorted_amt_single_field_indexes,
            single_field_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_amt_fully_covered_indexes_benchmark = CollectionBenchmark(
            heap_sorted_amt_fully_covered_indexes,
            fully_covered_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        let stable_memory_sorted_fully_covered_indexes_benchmark = CollectionBenchmark(
            stable_memory_sorted_fully_covered_indexes,
            fully_covered_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("ts", #Ascending),
            fuzz,
            iteration_limit,
        );

        let stable_memory_sorted_no_index_by_amt_benchmark = CollectionBenchmark(
            stable_memory_sorted_no_index,
            [],
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        let stable_memory_sorted_fully_covered_indexes_by_amt_benchmark = CollectionBenchmark(
            stable_memory_sorted_fully_covered_indexes,
            fully_covered_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_no_index_by_amt_benchmark = CollectionBenchmark(
            heap_sorted_no_index,
            [],
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_single_field_indexes_by_amt_benchmark = CollectionBenchmark(
            heap_sorted_single_field_indexes,
            single_field_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        let heap_sorted_fully_covered_indexes_by_amt_benchmark = CollectionBenchmark(
            heap_sorted_fully_covered_indexes,
            fully_covered_indexes,
            predefined_txs,
            Buffer.Buffer<Nat>(iteration_limit),
            principals,
            candid_principals_0_10,
            new_sorted_query("tx.amt", #Ascending),
            fuzz,
            iteration_limit,
        );

        public func run_benchmarks(row : Text, col : Text) {
            switch (row) {
                case ("#heap no index") {
                    heap_no_index_benchmark.run_benchmark(col);
                };

                case ("#stableMemory no index") {
                    stable_memory_no_index_benchmark.run_benchmark(col);
                };

                case ("#heap 7 single field indexes") {
                    heap_single_field_indexes_benchmark.run_benchmark(col);
                };

                case ("#stableMemory 7 single field indexes") {
                    stable_memory_single_field_indexes_benchmark.run_benchmark(col);
                };

                case ("#heap 6 fully covered indexes") {
                    heap_fully_covered_indexes_benchmark.run_benchmark(col);
                };

                case ("#stableMemory 6 fully covered indexes") {
                    stable_memory_fully_covered_indexes_benchmark.run_benchmark(col);
                };

                case ("#heap no index (sorted by ts)") {
                    heap_sorted_no_index_benchmark.run_benchmark(col);
                };

                case ("#heap 7 single field indexes (sorted by ts)") {
                    heap_sorted_single_field_indexes_benchmark.run_benchmark(col);
                };

                case ("#heap 6 fully covered indexes (sorted by ts)") {
                    heap_sorted_fully_covered_indexes_benchmark.run_benchmark(col);
                };

                case ("#heap no index (sorted by tx.amt)") {
                    heap_sorted_no_index_by_amt_benchmark.run_benchmark(col);
                };

                case ("#heap 7 single field indexes (sorted by tx.amt)") {
                    heap_sorted_single_field_indexes_by_amt_benchmark.run_benchmark(col);
                };

                case ("#heap 6 fully covered indexes (sorted by tx.amt)") {
                    heap_sorted_fully_covered_indexes_by_amt_benchmark.run_benchmark(col);
                };

                case ("#stableMemory no index (sorted by ts)") {
                    stable_memory_sorted_no_index_benchmark.run_benchmark(col);
                };

                case ("#stableMemory no index (sorted by tx.amt)") {
                    stable_memory_sorted_no_index_by_amt_benchmark.run_benchmark(col);
                };

                case ("#stableMemory 7 single field indexes (sorted by ts)") {
                    stable_memory_sorted_single_field_indexes_benchmark.run_benchmark(col);
                };

                case ("#stableMemory 7 single field indexes (sorted by tx.amt)") {
                    stable_memory_sorted_single_field_indexes_by_amt_benchmark.run_benchmark(col);
                };

                case ("#stableMemory 6 fully covered indexes (sorted by ts)") {
                    stable_memory_sorted_fully_covered_indexes_benchmark.run_benchmark(col);
                };

                case ("#stableMemory 6 fully covered indexes (sorted by tx.amt)") {
                    stable_memory_sorted_fully_covered_indexes_by_amt_benchmark.run_benchmark(col);
                };

                case (_) {
                    Debug.trap("Should be unreachable:\n row = \"" # debug_show row # "\" and col = \"" # debug_show col # "\"");
                };
            };
        };
    };

    func paginated_queries(
        txs : ZenDB.Collection<Tx>,
        section : Text,
        iteration_limit : Nat,
        predefined_txs : Buffer.Buffer<Tx>,
        principals : [Principal],
        candid_principals : [ZenDB.Types.Candid],
        candid_principals_0_10 : [Principal],
        sort_direction : ZenDB.Types.SortDirection,
        pagination_limit : Nat,
    ) {

        func skip_limit_paginated_query(db_query : ZenDB.QueryBuilder) {
            ignore db_query.Limit(pagination_limit);
            let #ok(result) = txs.search(db_query) else return;
            var documents = result.documents;
            var skip = 0;
            var opt_cursor : ?Nat = null;

            label pagination while (documents.size() > 0) {
                ignore db_query.Skip(skip).Limit(pagination_limit);

                let #ok(result) = txs.search(db_query) else return;

                documents := result.documents;

            };

            skip += documents.size();
        };

        func skip_limit_skip_limit_paginated_query(db_query : ZenDB.QueryBuilder, pagination_limit : Nat) : [(Blob, Tx)] {

            ignore db_query.Limit(pagination_limit);
            let #ok(result) = txs.search(db_query) else Prelude.unreachable();
            let bitmap = SparseBitMap64.fromIter(Iter.map<(Blob, Tx), Nat>(result.documents.vals(), func((id, _) : (Blob, Tx)) : Nat = Utils.nat_from_12_byte_blob(id)));
            let documents = Buffer.fromArray<(Blob, Tx)>(result.documents);
            var batch_size = result.documents.size();

            label skip_limit_pagination while (batch_size > 0) {
                ignore db_query.Skip(documents.size()).Limit(pagination_limit);

                let #ok(result) = txs.search(db_query) else Prelude.unreachable();
                // Debug.print("matching_txs: " # debug_show result.documents);

                assert result.documents.size() <= pagination_limit;
                batch_size := result.documents.size();

                for ((id, tx) in result.documents.vals()) {
                    documents.add((id, tx));

                    if (SparseBitMap64.get(bitmap, Utils.nat_from_12_byte_blob(id))) {
                        Debug.trap("Duplicate entry for id " # debug_show id);
                    } else {
                        SparseBitMap64.set(bitmap, Utils.nat_from_12_byte_blob(id), true);
                    };
                };

            };

            Buffer.toArray(documents);

        };

        switch (section) {
            case ("insert with no index") {};

            case ("clear collection entries and indexes") {};

            case ("insert with 7 indexes") {};

            case ("query(): single field (btype = '1mint')") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #eq(#Text("1mint")),
                );

                skip_limit_paginated_query(db_query);

            };

            case ("query(): #Or (btype == '1xfer' OR '2xfer)'") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #anyOf([#Text("1xfer"), #Text("2xfer")]),
                );

                skip_limit_paginated_query(db_query);
            };

            case ("query() -> principals[0] == tx.to.owner (is recipient)") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #eq(#Principal(principals.get(0))),
                );

                skip_limit_paginated_query(db_query);
            };
            case ("query() -> principals[0..10] == tx.to.owner (is recipient)") {
                let candid_principals = Array.map<Principal, ZenDB.Types.Candid>(
                    Iter.toArray(Array.slice<Principal>(principals, 0, 10)),
                    func(p : Principal) : ZenDB.Types.Candid = #Principal(p),
                );

                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #anyOf(candid_principals),
                );

                skip_limit_paginated_query(db_query);
            };

            case ("query() -> all txs involving principals[0]") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #eq(#Principal(principals.get(0))),
                ).Or(
                    "tx.from.owner",
                    #eq(#Principal(principals.get(0))),
                ).Or(
                    "tx.spender.owner",
                    #eq(#Principal(principals.get(0))),
                );

                skip_limit_paginated_query(db_query);
            };

            case ("query() -> all txs involving principals[0..10]") {
                let candid_principals = Array.map<Principal, ZenDB.Types.Candid>(
                    Iter.toArray(Array.slice(principals, 0, 10)),
                    func(p : Principal) : ZenDB.Types.Candid = #Principal(p),
                );

                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #anyOf(candid_principals),
                ).Or(
                    "tx.from.owner",
                    #anyOf(candid_principals),
                ).Or(
                    "tx.spender.owner",
                    #anyOf(candid_principals),
                );

                skip_limit_paginated_query(db_query);
            };

            case ("query(): number range (250 < tx.amt <= 400)") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.amt",
                    #gt(#Nat(250)),
                ).And(
                    "tx.amt",
                    #lte(#Nat(400)),
                );

                skip_limit_paginated_query(db_query);
            };

            case ("query(): #And (btype='1burn' AND tx.amt>=750)") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #eq(#Text("1burn")),
                ).And(
                    "tx.amt",
                    #gte(#Nat(750)),
                );

                skip_limit_paginated_query(db_query);
            };

            case (_) {
                Debug.trap("Should be unreachable:\n row = zenDB (skip_limit_pagination iteration_limit = 100, -> array) and col = \"" # debug_show section # "\"");
            };

        };

    };

};
