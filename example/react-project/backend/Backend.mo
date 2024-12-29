import Prim "mo:prim";

import Array "mo:base/Array";
import Iter "mo:base/Iter";
import IC "mo:base/ExperimentalInternetComputer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import Cycles "mo:base/ExperimentalCycles";
import Buffer "mo:base/Buffer";
import Option "mo:base/Option";
import Time "mo:base/Time";

import Vector "mo:vector";
import Itertools "mo:itertools/Iter";

import Ledger "ledger";
import ZenDB "../../../src";
import T "Types";
import BlockUtils "BlockUtils";

actor class Backend() {
    type Block = T.Block;
    type Tx = T.Tx;

    stable let db_sstore = ZenDB.new();
    // Debug.print("db_sstore: " # debug_show (db_sstore));
    let db = ZenDB.launch(db_sstore);

    let ledger : Ledger.Service = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai");

    // func blob_to_account(blob : Blob) : Account {
    //     let account = Principal.fromBlob(blob);
    //     { owner = account; sub_account = null };
    // };

    let Billion = 1_000_000_000;
    let Trillion = 1_000_000_000_000;

    // let AccountSchema = #Record([
    //     ("owner", #Principal),
    //     ("sub_account", #Option(#Blob)),
    // ]);

    let BlockSchema : ZenDB.Schema = #Record([
        ("btype", #Text),
        ("phash", #Option(#Blob)),
        ("ts", #Nat),
        ("fee", #Option(#Nat)),
        ("tx_index", #Nat),
        (
            "tx",
            #Record([
                ("amt", #Option(#Nat)),
                ("from", #Option(#Blob)),
                ("to", #Option(#Blob)),
                ("spender", #Option(#Blob)),
                ("memo", #Option(#Blob)),
                ("expires_at", #Option(#Nat)),
                ("expected_allowance", #Option(#Nat)),
            ]),
        ),
    ]);

    let CandifyBlock : ZenDB.Candify<Block> = {
        from_blob = func(blob : Blob) : Block {
            switch (from_candid (blob) : ?Block) {
                case (?block) block;
                case (null) Debug.trap("failed to decode block from blob");
            };
        };
        to_blob = func(block : Block) : Blob {
            to_candid (block);
        };
    };

    let #ok(txs) = db.get_or_create_collection<Block>("_t_x_s", BlockSchema, CandifyBlock);

    // because of the way indexes are selected, the order of the fields in the index matters
    // but also if there are more than one index selected as the best index,
    // you should define them in the order of importance

    // the first two are defined first so if all the indexes are equal for the query,
    // the first two indexes which starting with fields we use for sorting can be used
    // to avoid sorting internally in the db which is really slow and inefficient
    let #ok(_) = txs.create_index(["ts"]);
    let #ok(_) = txs.create_index(["tx.amt"]);

    let #ok(_) = txs.create_index(["btype", "tx.amt"]);
    let #ok(_) = txs.create_index(["btype", "ts"]);

    let #ok(_) = txs.create_index(["tx.from", "btype", "ts"]);
    let #ok(_) = txs.create_index(["tx.from", "btype", "tx.amt"]);

    let #ok(_) = txs.create_index(["tx.to", "btype", "ts"]);
    let #ok(_) = txs.create_index(["tx.to", "btype", "tx.amt"]);

    public func upload_blocks(blocks : [Block]) : async () {
        for (block in blocks.vals()) {
            switch (txs.insert_with_id(block.tx_index, block)) {
                case (#ok(_)) ();
                case (#err(err)) Debug.trap("failed to insert block into txs db: " # debug_show (block) # " \n" # err);
            };
        };
    };

    public func sync_blocks_to_db(length : Nat) : async () {
        await* BlockUtils.sync_blocks_from_ledger_to_db(ledger, txs, length);
    };

    public func pull_blocks(start : Nat, length : Nat) : async [Block] {
        await* BlockUtils.pull_blocks_from_ledger(ledger, start, length);
    };

    public func pull_blocks_into_db(start : Nat, length : Nat) : async () {
        let blocks = await* BlockUtils.pull_blocks_from_ledger(ledger, start, length);

        for (block in blocks.vals()) {
            switch (txs.insert_with_id(block.tx_index, block)) {
                case (#ok(_)) ();
                case (#err(err)) Debug.trap("failed to insert block into txs db: " # debug_show (block) # " \n" # err);
            };
        };
    };

    system func postupgrade() {

    };

    type Options = {
        filter : {
            btype : ?[Text]; // filter based on btype
            to : ?Blob; // filter based on to
            from : ?Blob; // filter based on from
            spender : ?Blob; // filter based on spender
            amt : ?{
                min : ?Nat;
                max : ?Nat;
            };
            ts : ?{
                min : ?Nat;
                max : ?Nat;
            };
        };

        sort : [(Text, { #Ascending; #Descending })];

        pagination : {
            limit : Nat;
            offset : Nat;
        };

        count : Bool;
    };

    type GetTxsResponse = {
        blocks : [Block];
        total : ?Nat;
        instructions : Nat;
    };

    func convert_options_to_db_query(options : Options) : ZenDB.QueryBuilder {

        let Query = ZenDB.QueryBuilder();
        ignore Query.Limit(options.pagination.limit);
        ignore Query.Skip(options.pagination.offset);

        ignore do ? {

            if (options.filter.btype != null) {
                let btypes = options.filter.btype!;
                let values = Array.map<Text, ZenDB.Candid>(btypes, func(btype : Text) : ZenDB.Candid = #Text(btype));

                ignore Query.Where("btype", #In(values));
            };

            if (options.filter.to != null) {
                let to = options.filter.to!;
                ignore Query.Where("tx.to", #eq(#Option(#Blob(to))));
            };

            if (options.filter.from != null) {
                let from = options.filter.from!;
                ignore Query.Where("tx.from", #eq(#Option(#Blob(from))));
            };

            if (options.filter.spender != null) {
                let spender = options.filter.spender!;
                ignore Query.Where("tx.spender", #eq(#Option(#Blob(spender))));
            };

            if (options.filter.amt != null) {
                let amt = options.filter.amt!;
                switch (amt.min) {
                    case (?min) {
                        ignore Query.Where("tx.amt", #gte(#Option(#Nat(min))));
                    };
                    case (null) ();
                };

                switch (amt.max) {
                    case (?max) {
                        ignore Query.Where("tx.amt", #lte(#Option(#Nat(max))));
                    };
                    case (null) ();
                };
            };

            if (options.filter.ts != null) {
                let ts = options.filter.ts!;
                switch (ts.min) {
                    case (?min) {
                        ignore Query.Where("ts", #gte(#Nat(min)));
                    };
                    case (null) ();
                };

                switch (ts.max) {
                    case (?max) {
                        ignore Query.Where("ts", #lte(#Nat(max)));
                    };
                    case (null) ();
                };
            };

            if (options.sort.size() > 0) {
                let (sort_field, sort_direction) = options.sort[0];
                ignore Query.Sort(
                    sort_field,
                    sort_direction,
                );
            };
        };

        Query;

    };

    public query func get_txs(options : Options) : async GetTxsResponse {
        Debug.print("get_txs called with options: " # debug_show options);

        let db_query = convert_options_to_db_query(options);

        var blocks : [Block] = [];
        var total : ?Nat = null;

        let instructions = IC.countInstructions(
            func() {

                let query_res = txs.search(db_query);

                let #ok(matching_txs) = query_res else Debug.trap("get_txs failed: " # debug_show query_res);
                Debug.print("successfully got matching txs: " # debug_show options);

                blocks := Array.map<(Nat, Block), Block>(
                    matching_txs,
                    func(id : Nat, tx : Block) : Block = tx,
                );

                if (options.count) {
                    let #ok(total_matching_txs) = txs.count(db_query) else Debug.trap("txs.count failed");

                    total := ?total_matching_txs;

                    Debug.print("successfully got total matching txs: " # debug_show total);
                };

                Debug.print("get_txs returning " # debug_show { blocks = blocks.size(); total });

            }
        );

        { blocks; total; instructions = Nat64.toNat(instructions) };

    };

    public func get_async_txs(options : Options) : async GetTxsResponse {
        Debug.print("get_async_txs called with options: " # debug_show options);

        let db_query = convert_options_to_db_query(options);

        let performance_start = IC.performanceCounter(0);
        func instructions() : Nat {
            Nat64.toNat(IC.performanceCounter(0) - performance_start);
        };

        let blocks_buffer = Buffer.Buffer<(Nat, Block)>(8);

        let #ok(matching_txs) = txs.search(db_query);

        let total = if (options.count) {
            let #ok(total) = txs.count(db_query) else Debug.trap("txs.count failed");
            Debug.print("successfully got total matching txs: " # debug_show total);
            ?total;
        } else { null };

        Debug.print("successfully got matching txs: " # debug_show options);

        let blocks = Array.map<(Nat, Block), Block>(
            matching_txs,
            func(id : Nat, tx : Block) : Block = tx,
        );

        Debug.print("get_async_txs returning " # debug_show { blocks = blocks.size(); total });

        { blocks; total; instructions = instructions() };

    };

    public func clear() : async () {
        txs.clear();
    };

    type CanisterStats = {
        heap_size : Nat;
        memory_size : Nat;
        stable_memory_size : Nat;
        db_stats : ZenDB.CollectionStats;
    };

    public query func get_time() : async Int {
        Time.now();
    };

    var lock = 0;

    public func read_and_update() : async Nat {
        let val = lock;
        lock += 1;
        val;
    };

    public func reset_lock() : async () {
        lock := 0;
    };

    public query func get_stats() : async CanisterStats {
        let db_stats = txs.stats();

        let main_btree_sm = db_stats.main_btree_index.stable_memory;
        var total_db_stable_memory_size = main_btree_sm.metadata_bytes + main_btree_sm.actual_data_bytes;

        for (index_stats in db_stats.indexes.vals()) {
            total_db_stable_memory_size += index_stats.stable_memory.metadata_bytes + index_stats.stable_memory.actual_data_bytes;
        };

        {
            heap_size = Prim.rts_heap_size();
            memory_size = Prim.rts_memory_size();
            stable_memory_size = total_db_stable_memory_size;
            db_stats;
        };
    };

    let LEDGER_DECIMALS = 8; // decimals will be handled in the frontend before the request is made

    public query func get_txs_from_tx_index_range(start : Nat, length : Nat) : async [Block] {
        let Query = ZenDB.QueryBuilder().Where("tx_index", #gte(#Nat(start))).And("tx_index", #lt(#Nat(start + length)));

        let query_res = txs.search(Query);
        let #ok(matching_txs) = query_res else Debug.trap("get_txs_from_tx_index_range failed: " # debug_show query_res);
        let blocks = Array.map<(Nat, Block), Block>(
            matching_txs,
            func(id : Nat, tx : Block) : Block = tx,
        );

        blocks;

    };

    public query func get_txs_in_range(start : Nat, length : Nat) : async [Block] {
        assert start + length <= txs.size();

        Array.tabulate(
            length,
            func(i : Nat) : Block {
                let #ok(block) = (txs.get(start + i)) else Debug.trap("failed to get block from txs db: " # debug_show (start + i));
                block;
            },
        );
    };

    public query func get_db_size() : async Nat {
        txs.size();
    };

    type DBStats = {
        size : Nat;
        indexes : [Text];

        heap : Nat;
        stable_memory : Nat;
    };

    // public query func get_db_stats() : async {

    // };

    // public query func get_txs_in_range(start : Nat, length : Nat) : async [Block] {
    //     txs.filter(func(block : Block) : Bool = block.tx_index >= start and block.tx_index < start + length);
    // };

};
