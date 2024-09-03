import Array "mo:base/Array";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Nat64 "mo:base/Nat64";

// import Ledger "mo:ledger-types";
import MemoryBuffer "mo:memory-collection/MemoryBuffer";

import ZenDB "../src";

actor {
    module Ledger {

        public type CandidOperation = {
            #Approve : ({
                fee : Nat;
                from : Blob;
                allowance_e8s : Int;
                allowance : Nat;
                expected_allowance : ?Nat;
                expires_at : ?TimeStamp;
                spender : Blob;
            });
            #Burn : ({
                from : Blob;
                amount : Nat;
                spender : ?Blob;
            });
            #Mint : ({
                to : Blob;
                amount : Nat;
            });
            #Transfer : ({
                to : Blob;
                fee : Nat;
                from : Blob;
                amount : Nat;
                spender : ?Blob;
            });
        };

        public type TimeStamp = { timestamp_nanos : Nat64 };

        public type CandidTransaction = {
            memo : Nat64;
            icrc1_memo : ?Blob;
            operation : ?CandidOperation;
            created_at_time : TimeStamp;
        };

        public type CandidBlock = {
            transaction : CandidTransaction;
            timestamp : TimeStamp;
            parent_hash : ?Blob;
        };

        public type BlockRange = { blocks : [CandidBlock] };

        public type GetBlocksArgs = { start : Nat64; length : Nat64 };

        public type GetBlocksError = {
            #BadFirstBlockIndex : {
                requested_index : Nat64;
                first_valid_index : Nat64;
            };
            #Other : { error_message : Text; error_code : Nat64 };
        };

        public type QueryArchiveResult = {
            #Ok : BlockRange;
            #Err : GetBlocksError;
        };

        public type QueryArchiveFn = shared query GetBlocksArgs -> async QueryArchiveResult;

        public type ArchivedBlocksRange = {
            callback : QueryArchiveFn;
            start : Nat64;
            length : Nat64;
        };

        public type QueryBlocksResponse = {
            certificate : ?Blob;
            blocks : [Block];
            chain_length : Nat64;
            first_block_index : Nat64;
            archived_blocks : [ArchivedBlocksRange]

        };

        public type Service = actor {
            query_blocks : ({ start : Nat64; length : Nat64 }) -> async QueryBlocksResponse;
        };

    };

    stable let db_store = ZenDB.newStableStore();
    let db = ZenDB.launch(db_store);

    let ledger = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : Ledger.Service;

    stable var mbuffer_sstore = MemoryBuffer.newStableStore();
    mbuffer_sstore := MemoryBuffer.upgrade(mbuffer_sstore);

    let candify_block = {
        from_blob = func(blob : Blob) : Block {
            let ?c : ?Block = from_candid (blob);
            c;
        };
        to_blob = func(c : Block) : Blob { to_candid (c) };
    };
    let mbuffer_utils = { blobify = candify_block };
    let mbuffer = MemoryBuffer.MemoryBuffer<Block>(mbuffer_sstore, mbuffer_utils);

    func blob_to_account(blob : Blob) : Account {
        let account = Principal.fromBlob(blob);
        { owner = account; sub_account = null };
    };

    func convert_ledger_block(ledger_block : Ledger.CandidTransaction) : Block {

        let block : Block = switch (ledger_block.operation) {
            case (? #Approve(approve)) {
                {
                    btype = "2approve";
                    phash = null;
                    ts = Nat64.toNat(ledger_block.created_at_time.timestamp_nanos);
                    tx = {
                        fee = approve.fee;
                        from = ?blob_to_account(approve.from);
                        to = null;
                        spender = ?blob_to_account(approve.spender);
                        memo = ledger_block.icrc1_memo;
                        expected_allowance = approve.expected_allowance;
                        amt = ?Nat64.toNat(approve.allowance);
                        expires_at = switch (approve.expires_at) {
                            case (?expires_at) Nat64.toNat(expires_at.timestamp_nanos);
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
                            phash = null;
                            ts = Nat64.toNat(ledger_block.created_at_time.timestamp_nanos);
                            tx = {
                                fee = null;
                                from = burn.from;
                                to = null;
                                spender;
                                memo = ledger_block.icrc1_memo;
                                amt = burn.amount;
                                expected_allowance = null;
                                expires_at = null;
                            };
                        };
                    };

                    case (null) {
                        {
                            btype = "1burn";
                            phash = null;
                            ts = Nat64.toNat(ledger_block.created_at_time.timestamp_nanos);
                            tx = {
                                fee = null;
                                from = burn.from;
                                to = null;
                                spender = null;
                                memo = ledger_block.icrc1_memo;
                                amt = burn.amount;
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
                    phash = null;
                    tx = {
                        fee = null;
                        from = null;
                        to = mint.to;
                        spender = null;
                        memo = ledger_block.icrc1_memo;
                        amt = mint.amount;
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
                            phash = null;
                            tx = {
                                fee = transfer.fee;
                                from = transfer.from;
                                to = transfer.to;
                                spender;
                                memo = ledger_block.icrc1_memo;
                                amt = transfer.amount;
                                expected_allowance = null;
                                expires_at = null;
                            };
                        };
                    };
                    case (null) {
                        {
                            btype = "1xfer";
                            phash = null;
                            tx = {
                                fee = transfer.fee;
                                from = transfer.from;
                                to = transfer.to;
                                spender = null;
                                memo = ledger_block.icrc1_memo;
                                amt = transfer.amount;
                                expected_allowance = null;
                                expires_at = null;
                            };
                        };
                    };
                };
            };
            case (null) Debug.trap("unexpected operation: " # debug_show ledger_block);
        };
    };

    func retrieve_txs(length : Nat) : async* () {
        let query_blocks_response = await ledger.query_blocks({
            start = Nat64.fromNat(mbuffer.size());
            length = Nat64.fromNat(length);
        });

        if (query_blocks_response.archived_blocks.size() > 0) {
            for (rq in query_blocks_response.archived_blocks.vals()) {
                assert rq.start == mbuffer.size();
                let res = await rq.callback({
                    start = rq.start;
                    length = rq.length;
                });
                let #ok(blocks) = res else Debug.trap("failed to retrieve archived blocks: " # debug_show res);

                for (block in blocks.vals()) {
                    let converted_block = convert_ledger_block(block);
                    mbuffer.add(converted_block);
                };
            };

        };

        for (block in query_blocks_response.blocks.vals()) {
            let converted_block = convert_ledger_block(block);
            mbuffer.add(converted_block);
        };

    };

    type Account = {
        owner : Principal;
        sub_account : ?Blob; // null == [0...0]
    };

    type Block = {
        btype : Text;
        phash : ?Blob;
        ts : Nat;
        fee : ?Nat;
        tx : Tx;
    };

    type Tx = {
        amt : ?Nat;
        from : ?Account;
        to : ?Account;
        spender : ?Account;
        memo : ?Blob;
        expires_at : ?Nat;
        expected_allowance : ?Nat;
    };

    let AccountSchema = #Record([
        ("owner", #Principal),
        ("sub_account", #Option(#Blob)),
    ]);

    let TxSchema : ZenDB.Schema = #Record([
        ("btype", #Text),
        ("phash", #Blob),
        ("ts", #Nat),
        ("fee", #Option(#Nat)),
        (
            "tx",
            #Record([("amt", #Option(#Nat)), ("from", #Option(AccountSchema)), ("to", #Option(AccountSchema)), ("spender", #Option(AccountSchema)), ("memo", #Option(#Blob))]),
        ),
    ]);

    let #ok(txs) = db.create_collection<Block>("transactions", TxSchema, candify_block);
    let #ok(_) = txs.create_index(["btype", "tx.amt"]);
    let #ok(_) = txs.create_index(["tx.amt"]);
    let #ok(_) = txs.create_index(["tx.from.owner", "tx.from.sub_account"]);
    let #ok(_) = txs.create_index(["tx.to.owner", "tx.to.sub_account"]);
    let #ok(_) = txs.create_index(["tx.spender.owner", "tx.spender.sub_account"]);

    for (block in mbuffer.vals()) {
        let #ok(_) = txs.insert(block);
    };

    type Options = {
        filter : {
            btype : ?[Text]; // filter based on btype
            to : ?Account; // filter based on to
            from : ?Account; // filter based on from
            spender : ?Account; // filter based on spender
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
                ignore Query.Where("tx.to.owner", #eq(#Principal(to.owner)));

                if (to.sub_account != null) {
                    ignore Query.And("tx.to.sub_account", #eq(#Blob(to.sub_account!)));
                };
            };

            if (options.filter.from != null) {
                let from = options.filter.from!;
                ignore Query.Where("tx.from.owner", #eq(#Principal(from.owner)));

                if (from.sub_account != null) {
                    ignore Query.And("tx.from.sub_account", #eq(#Blob(from.sub_account!)));
                };
            };

            if (options.filter.spender != null) {
                let spender = options.filter.spender!;
                ignore Query.Where("tx.spender.owner", #eq(#Principal(spender.owner)));

                if (spender.sub_account != null) {
                    ignore Query.And("tx.spender.sub_account", #eq(#Blob(spender.sub_account!)));
                };
            };

            if (options.filter.amt != null) {
                let amt = options.filter.amt!;
                switch (amt.min) {
                    case (?min) {
                        ignore Query.Where("tx.amt", #gte(#Nat(min)));
                    };
                    case (null) ();
                };

                switch (amt.max) {
                    case (?max) {
                        ignore Query.Where("tx.amt", #lte(#Nat(max)));
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

};
