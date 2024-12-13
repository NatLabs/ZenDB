// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Buffer "mo:base/Buffer";
import Principal "mo:base/Principal";
import Array "mo:base/Array";

import { test; suite } "mo:test";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import BitMap "mo:bit-map";

let fuzz = Fuzz.fromSeed(0x7eadbeef);

type Account = {
    owner : Principal;
    sub_account : ?Blob; // null == [0...0]
};

type Tx = {
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

let principals = Array.tabulate(
    50,
    func(i : Nat) : Principal {
        fuzz.principal.randomPrincipal(29);
    },
);

func new_tx(fuzz : Fuzz.Fuzzer, principals : [Principal]) : Tx {

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
        ts = fuzz.nat.randomRange(0, 1000000000);
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

let limit = 10_000;

let input_txs : [Tx] = Array.tabulate<Tx>(
    limit,
    func(i : Nat) : Tx {
        new_tx(fuzz, principals);
    },
);

type AccountText = {
    owner : Text;
    sub_account : ?Blob;
};

func account_to_text(opt_account : ?Account) : ?AccountText {
    switch (opt_account) {
        case (?account) {
            let { owner; sub_account } = account;
            let owner_as_text = Principal.toText(owner);

            ?{
                owner = "Principal.fromText(\"" # owner_as_text # "\")";
                sub_account;
            }

        };
        case (null) { null };
    };
};

type TxWithAccountText = {
    btype : Text;
    phash : Blob;
    ts : Nat;
    tx : {
        amt : Nat;
        from : ?AccountText;
        to : ?AccountText;
        spender : ?AccountText;
        memo : ?Blob;
    };
    fee : ?Nat;
};

Debug.print(
    debug_show (
        Array.map(
            input_txs,
            func(block : Tx) : TxWithAccountText {
                {
                    block with tx = {
                        block.tx with to = account_to_text(block.tx.to);
                        from = account_to_text(block.tx.from);
                        spender = account_to_text(block.tx.spender);
                    }
                } : TxWithAccountText;
            },
        )
    )
);
