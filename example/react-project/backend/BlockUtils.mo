import Array "mo:base@0.16.0/Array";
import Principal "mo:base@0.16.0/Principal";
import Debug "mo:base@0.16.0/Debug";
import Nat64 "mo:base@0.16.0/Nat64";
import Nat "mo:base@0.16.0/Nat";
import Cycles "mo:base@0.16.0/ExperimentalCycles";
import Buffer "mo:base@0.16.0/Buffer";

import Vector "mo:vector@0.4.2";
import Itertools "mo:itertools@0.2.2/Iter";

import Ledger "ledger";
import ZenDB "../../../src";
import T "Types";

module BlockUtils {
    type Block = T.Block;
    type Tx = T.Tx;
    public let MAX_BATCH_TXS_IN_RESPONSE = 2_000;

    public func tokens_to_nat(tokens : Ledger.Tokens) : Nat {
        Nat64.toNat(tokens.e8s);
    };

    public func convert_ledger_block(ledger_block : Ledger.CandidBlock, tx_index : Nat) : Block {
        let { parent_hash; timestamp; transaction } = ledger_block;

        let block : Block = switch (transaction.operation) {
            case (?#Approve(approve)) {
                {
                    btype = "2approve";
                    phash = parent_hash;
                    ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                    fee = ?tokens_to_nat(approve.fee);
                    tx_index;
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

            case (?#Burn(burn)) {
                switch (burn.spender) {
                    case (?spender) {
                        {
                            btype = "2xfer";
                            phash = parent_hash;
                            ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                            fee = null;
                            tx_index;
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
                            tx_index;
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
            case (?#Mint(mint)) {
                {
                    btype = "1mint";
                    phash = parent_hash;
                    fee = null;
                    ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                    tx_index;
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
            case (?#Transfer(transfer)) {
                switch (transfer.spender) {
                    case (?spender) {
                        {
                            btype = "2xfer";
                            phash = parent_hash;
                            fee = ?tokens_to_nat(transfer.fee);
                            ts = Nat64.toNat(transaction.created_at_time.timestamp_nanos);
                            tx_index;
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
                            tx_index;
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

    public func sync_blocks_from_ledger_to_db(ledger : Ledger.Service, txs : ZenDB.Collection<Block>, length : Nat) : async* () {
        Debug.print("Attempting to sync " # debug_show length # " blocks");

        let start = txs.size();

        Debug.print("starting from block " # debug_show start);

        let query_blocks_response = await ledger.query_blocks({
            start = Nat64.fromNat(start);
            length = Nat64.fromNat(length);
        });

        Debug.print("retrieved query_blocks_response " # debug_show { query_blocks_response with archived_blocks = [] });

        if (query_blocks_response.archived_blocks.size() > 0) {
            Debug.print("retrieving archived blocks ");

            for (rq in query_blocks_response.archived_blocks.vals()) {

                assert Nat64.toNat(rq.start) == txs.size();

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
                    assert (Nat64.toNat(rq.start) + (i * MAX_BATCH_TXS_IN_RESPONSE)) == txs.size();
                    Debug.print("retrieving archived blocks batch " # debug_show i);

                    let res = await async_call;
                    let #Ok({ blocks = queried_blocks }) = res else Debug.trap("failed to retrieve archived blocks: " # debug_show res);

                    for ((j, block) in Itertools.enumerate(queried_blocks.vals())) {
                        assert (Nat64.toNat(rq.start) + (i * MAX_BATCH_TXS_IN_RESPONSE) + j) == txs.size();
                        let converted_block = convert_ledger_block(block, txs.size());
                        let document_id = switch (txs.insert(converted_block)) {
                            case (#ok(document_id)) document_id;
                            case (#err(e)) {
                                Debug.trap("failed to insert archived block into txs db: " # debug_show (converted_block, e));
                            };
                        };

                        // let expected = #ok(converted_block);
                        // let actual = txs.get(document_id);

                        // if (actual != expected) {
                        //     Debug.trap(
                        //         "failed to insert archived block into with document_id "
                        //         # debug_show (document_id) # " into txs db: " # debug_show (converted_block, actual)
                        //     );
                        // };

                    };

                    Debug.print("Added " # debug_show queried_blocks.size() # " archived blocks to txs db");
                    Debug.print("txs db size: " # debug_show txs.size());
                };
            };
        };

        for (block in query_blocks_response.blocks.vals()) {
            let converted_block = convert_ledger_block(block, txs.size());
            let #ok(document_id) = txs.insert(converted_block) else Debug.trap("failed to insert block into txs db: " # debug_show (converted_block));
            assert txs.get(document_id) == #ok(converted_block);
        };
    };

    public func pull_blocks_from_ledger(ledger : Ledger.Service, start : Nat, length : Nat) : async* [Block] {
        Debug.print("Attempting to sync " # debug_show length # " blocks");
        let buffer = Buffer.Buffer<Block>(length);

        Debug.print("starting from block " # debug_show start);

        let query_blocks_response = await ledger.query_blocks({
            start = Nat64.fromNat(start);
            length = Nat64.fromNat(length);
        });

        Debug.print("retrieved query_blocks_response " # debug_show { query_blocks_response with archived_blocks = [] });

        if (query_blocks_response.archived_blocks.size() > 0) {
            Debug.print("retrieving archived blocks ");

            for (rq in query_blocks_response.archived_blocks.vals()) {

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

                for (i in Itertools.range(0, batches)) {
                    assert (Nat64.toNat(rq.start) + (i * MAX_BATCH_TXS_IN_RESPONSE)) == start + buffer.size();
                    Debug.print("retrieving archived blocks batch " # debug_show i);

                    let res = await parallel.get(i);
                    let #Ok({ blocks = queried_blocks }) = res else Debug.trap("failed to retrieve archived blocks: " # debug_show res);

                    for ((j, block) in Itertools.enumerate(queried_blocks.vals())) {
                        assert (Nat64.toNat(rq.start) + (i * MAX_BATCH_TXS_IN_RESPONSE) + j) == start + buffer.size();
                        let tx_index = Nat64.toNat(rq.start) + (i * MAX_BATCH_TXS_IN_RESPONSE) + j;
                        let converted_block = convert_ledger_block(block, tx_index);
                        buffer.add(converted_block);
                    };
                };
            };
        };

        for (block in query_blocks_response.blocks.vals()) {
            let converted_block = convert_ledger_block(block, start + buffer.size());
            buffer.add(converted_block);
        };

        Buffer.toArray(buffer);
    };
};
