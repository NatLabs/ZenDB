import Array "mo:base/Array";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import Cycles "mo:base/ExperimentalCycles";
import Buffer "mo:base/Buffer";

import Vector "mo:vector";
import Itertools "mo:itertools/Iter";

import Ledger "ledger";
import ZenDB "../../../src";

actor class Backend() {
    stable var counter = 0;

    // Get the current count
    public query func get() : async Nat {
        counter;
    };

    // Increment the count by one
    public func inc() : async () {
        counter += 1;
    };

    // Add `n` to the current count
    public func add(n : Nat) : async () {
        counter += n;
    };
    // public type Account = {
    //     owner : Principal;
    //     sub_account : ?Blob; // null == [0...0]
    // };

    public type Tx = {
        amt : ?Nat;
        from : ?Blob;
        to : ?Blob;
        spender : ?Blob;
        memo : ?Blob;
        expires_at : ?Nat;
        expected_allowance : ?Nat;
    };

    public type Block = {
        btype : Text;
        phash : ?Blob;
        ts : Nat;
        fee : ?Nat;
        tx : Tx;
    };

    stable let stored_blocks = Vector.new<Block>();
    stable var blocks_stored_in_db = 0;

    stable let db_sstore = ZenDB.new();
    let db = ZenDB.launch(db_sstore);

    let ledger : Ledger.Service = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai");

    // func blob_to_account(blob : Blob) : Account {
    //     let account = Principal.fromBlob(blob);
    //     { owner = account; sub_account = null };
    // };

    func tokens_to_nat(tokens : Ledger.Tokens) : Nat {
        Nat64.toNat(tokens.e8s);
    };

    func convert_ledger_block(ledger_block : Ledger.CandidBlock) : Block {
        let { parent_hash; timestamp; transaction } = ledger_block;

        let block : Block = switch (transaction.operation) {
            case (? #Approve(approve)) {
                {
                    btype = "2approve";
                    phash = parent_hash;
                    ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                    fee = ?tokens_to_nat(approve.fee);
                    tx = {

                        from = ?(approve.from);
                        to = null;
                        spender = ?(approve.spender);
                        memo = transaction.icrc1_memo;
                        expected_allowance = switch (approve.expected_allowance) {
                            case (null) null;
                            case (?expected_allowance) ?tokens_to_nat(expected_allowance);
                        };
                        amt = ?tokens_to_nat(approve.allowance);
                        expires_at = switch (approve.expires_at) {
                            case (?expires_at) ?Nat64.toNat(expires_at.timestamp_nanos);
                            case (null) null;
                        };
                    };
                };
            };

            case (? #Burn(burn)) {
                switch (burn.spender) {
                    case (?spender) {
                        {
                            btype = "2xfer";
                            phash = parent_hash;
                            ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                            fee = null;
                            tx = {

                                from = ?(burn.from);
                                to = null;
                                spender = ?(spender);
                                memo = transaction.icrc1_memo;
                                amt = ?tokens_to_nat(burn.amount);
                                expected_allowance = null;
                                expires_at = null;
                            };
                        };
                    };

                    case (null) {
                        {
                            btype = "1burn";
                            phash = parent_hash;
                            ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                            fee = null;
                            tx = {

                                from = ?(burn.from);
                                to = null;
                                spender = null;
                                memo = transaction.icrc1_memo;
                                amt = ?tokens_to_nat(burn.amount);
                                expected_allowance = null;
                                expires_at = null;
                            };
                        };
                    };
                };
            };
            case (? #Mint(mint)) {
                {
                    btype = "1mint";
                    phash = parent_hash;
                    fee = null;
                    ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                    tx = {
                        from = null;
                        to = ?(mint.to);
                        spender = null;
                        memo = transaction.icrc1_memo;
                        amt = ?tokens_to_nat(mint.amount);
                        expected_allowance = null;
                        expires_at = null;
                    };
                };
            };
            case (? #Transfer(transfer)) {
                switch (transfer.spender) {
                    case (?spender) {
                        {
                            btype = "2xfer";
                            phash = parent_hash;
                            fee = ?tokens_to_nat(transfer.fee);
                            ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                            tx = {

                                from = ?(transfer.from);
                                to = ?(transfer.to);
                                spender = ?(spender);
                                memo = transaction.icrc1_memo;
                                amt = ?tokens_to_nat(transfer.amount);
                                expected_allowance = null;
                                expires_at = null;
                            };
                        };
                    };
                    case (null) {
                        {
                            btype = "1xfer";
                            phash = parent_hash;
                            fee = ?tokens_to_nat(transfer.fee);
                            ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                            tx = {
                                from = ?(transfer.from);
                                to = ?(transfer.to);
                                spender = null;
                                memo = transaction.icrc1_memo;
                                amt = ?tokens_to_nat(transfer.amount);
                                expected_allowance = null;
                                expires_at = null;
                            };
                        };
                    };
                };
            };
            case (null) Debug.trap("unexpected operation: " # debug_show transaction);
        };

        block;
    };

    let Billion = 1_000_000_000;
    let Trillion = 1_000_000_000_000;

    let MAX_BATCH_TXS_IN_RESPONSE = 2_000;

    public func sync_blocks(length : Nat) : async () {
        Debug.print("Attempting to sync " # debug_show length # " blocks");

        let start = Vector.size(stored_blocks);

        Debug.print("starting from block " # debug_show start);

        let query_blocks_response = await ledger.query_blocks({
            start = Nat64.fromNat(start);
            length = Nat64.fromNat(length);
        });

        Debug.print("retrieved query_blocks_response " # debug_show { query_blocks_response with archived_blocks = [] });

        if (query_blocks_response.archived_blocks.size() > 0) {
            Debug.print("retrieving archived blocks ");

            for (rq in query_blocks_response.archived_blocks.vals()) {

                assert Nat64.toNat(rq.start) == Vector.size(stored_blocks);

                let batches = (Nat64.toNat(rq.length) + (MAX_BATCH_TXS_IN_RESPONSE - 1)) / MAX_BATCH_TXS_IN_RESPONSE;
                Debug.print(
                    debug_show {
                        start = rq.start;
                        length = rq.length;
                        batches = batches;
                    }
                );

                let parallel = Buffer.Buffer<async Ledger.Result_4>(batches);

                for (i in Itertools.range(0, batches)) {
                    parallel.add(
                        rq.callback({
                            start = rq.start + Nat64.fromNat(i * MAX_BATCH_TXS_IN_RESPONSE);
                            length = rq.length - Nat64.fromNat(i * MAX_BATCH_TXS_IN_RESPONSE);
                        })
                    );
                };

                for ((i, async_call) in Itertools.enumerate(parallel.vals())) {
                    Debug.print("retrieving archived blocks batch " # debug_show i);

                    let res = await async_call;
                    let #Ok({ blocks = queried_blocks }) = res else Debug.trap("failed to retrieve archived blocks: " # debug_show res);

                    for (block in queried_blocks.vals()) {
                        let converted_block = convert_ledger_block(block);
                        Vector.add(stored_blocks, converted_block);
                    };

                    Debug.print("Added " # debug_show queried_blocks.size() # " archived blocks to stored_blocks");
                    Debug.print("stored_blocks size: " # debug_show Vector.size(stored_blocks));
                };
            };
        };

        for (block in query_blocks_response.blocks.vals()) {
            let converted_block = convert_ledger_block(block);
            Vector.add(stored_blocks, converted_block);
        };

    };

    // let AccountSchema = #Record([
    //     ("owner", #Principal),
    //     ("sub_account", #Option(#Blob)),
    // ]);

    let BlockSchema : ZenDB.Schema = #Record([
        ("btype", #Text),
        ("phash", #Option(#Blob)),
        ("ts", #Nat),
        ("fee", #Option(#Nat)),
        (
            "tx",
            #Record([("amt", #Option(#Nat)), ("from", #Option(#Blob)), ("to", #Option(#Blob)), ("spender", #Option(#Blob)), ("memo", #Option(#Blob)), ("expires_at", #Option(#Nat)), ("expected_allowance", #Option(#Nat))]),
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

    let #ok(txs) = db.get_or_create_collection<Block>("_t_xs", BlockSchema, CandifyBlock);
    // blocks_stored_in_db := 0;
    let #ok(_) = txs.create_index(["btype", "tx.amt"]);
    let #ok(_) = txs.create_index(["tx.amt"]);
    let #ok(_) = txs.create_index(["tx.from"]);
    let #ok(_) = txs.create_index(["tx.to"]);
    let #ok(_) = txs.create_index(["tx.spender"]);

    system func postupgrade() {

    };

    public func sync_blocks_to_db(length : Nat) : async () {
        Debug.print("Attempting to sync " # debug_show length # " blocks");
        Debug.print("stored_blocks size: " # debug_show Vector.size(stored_blocks));
        Debug.print("blocks_stored_in_db: " # debug_show blocks_stored_in_db);

        if (blocks_stored_in_db >= Vector.size(stored_blocks) or length == 0) return;

        let end = Nat.min(blocks_stored_in_db + length, Vector.size(stored_blocks));

        for (i in Itertools.range(blocks_stored_in_db, end)) {
            let block = Vector.get(stored_blocks, i);

            Debug.print("about to store block index " # debug_show i # ": " # debug_show block);
            let #ok(_) = txs.insert(block);
        };

        blocks_stored_in_db := end;

        Debug.print(debug_show { blocks_stored_in_db });

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
        };
        // pagination : {
        //     limit : Nat;
        //     offset : Nat;
        // };
        // sort : {
        //     amt : ?{
        //         #Ascending;
        //         #Descending;
        //         #None;
        //     };
        // };
    };

    public func get_txs(options : Options) : async [Block] {
        let Query = ZenDB.QueryBuilder();

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

        };

        let query_res = txs.find(Query);
        let #ok(matching_txs) = query_res else Debug.trap("get_txs failed: " # debug_show query_res);

        Array.map<(Nat, Block), Block>(
            matching_txs,
            func(id : Nat, tx : Block) : Block = tx,
        );

    };

    public func get_txs_dummy_values(options : Options) : async [Block] {

        [
            {
                btype = "1mint";
                phash = null;
                ts = 1;
                fee = null;
                tx = {
                    from = null;
                    to : ?Blob = ?"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
                    spender = null;
                    memo = null;
                    amt = ?100;
                    expires_at = null;
                    expected_allowance = null;
                };
            },
            {
                btype = "2xfer";
                phash = null;
                ts = 2;
                fee = ?1;
                tx = {
                    from : ?Blob = ?("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
                    to : ?Blob = ?("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
                    spender : ?Blob = ?("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
                    memo = null;
                    amt = ?100;
                    expires_at = null;
                    expected_allowance = null;
                };
            },
            {
                btype = "1burn";
                phash = null;
                ts = 3;
                fee = null;
                tx = {
                    from : ?Blob = ?("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
                    to = null;
                    spender = null;
                    memo = null;
                    amt = ?100;
                    expires_at = null;
                    expected_allowance = null;
                };
            },
            {
                btype = "2approve";
                phash = null;
                ts = 4;
                fee = ?1;
                tx = {
                    from : ?Blob = ?("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
                    to = null;
                    spender : ?Blob = ?("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
                    memo = null;
                    amt = ?100;
                    expires_at = ?5;
                    expected_allowance = ?100;
                };
            },
        ]

    };

};
