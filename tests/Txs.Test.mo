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
import ZenDB "../src";
import TestUtils "TestUtils";

let fuzz = Fuzz.fromSeed(0x7eadbeef);
let { QueryBuilder } = ZenDB;

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

let AccountSchema = #Record([
    ("owner", #Principal),
    ("sub_account", #Option(#Blob)),
]);

let TxSchema : ZenDB.Schema = #Record([
    ("btype", #Text),
    ("phash", #Blob),
    ("ts", #Nat),
    (
        "tx",
        #Record([("amt", #Nat), ("from", #Option(AccountSchema)), ("to", #Option(AccountSchema)), ("spender", #Option(AccountSchema)), ("memo", #Option(#Blob))]),
    ),
    ("fee", #Option(#Nat)),
]);

let candify_tx = {
    from_blob = func(blob : Blob) : Tx {
        let ?c : ?Tx = from_candid (blob);
        c;
    };
    to_blob = func(c : Tx) : Blob { to_candid (c) };
};

let db_sstore = ZenDB.newStableStore();
let db = ZenDB.launch(db_sstore);

let #ok(txs) = db.create_collection<Tx>("transactions", TxSchema, candify_tx);
let #ok(_) = txs.create_index(["btype", "tx.amt"]);
let #ok(_) = txs.create_index(["btype", "ts"]);
let #ok(_) = txs.create_index(["tx.amt"]);
let #ok(_) = txs.create_index(["ts"]);
let #ok(_) = txs.create_index(["tx.from.owner", "tx.from.sub_account"]);
let #ok(_) = txs.create_index(["tx.to.owner", "tx.to.sub_account"]);
let #ok(_) = txs.create_index(["tx.spender.owner", "tx.spender.sub_account"]);

let input_txs = Buffer.fromArray<Tx>([
    {
        btype = "1mint";
        phash = "abc";
        ts = 1;
        tx = {
            amt = 100;
            to = ?{
                owner = Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                sub_account = null;
            };
            from = null;
            spender = null;
            memo = null;
        };
        fee = null;
    },
    {
        btype = "1burn";
        phash = "abd";
        ts = 2;
        tx = {
            amt = 15;
            from = ?{
                owner = Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                sub_account = null;
            };
            to = null;
            spender = null;
            memo = null;
        };
        fee = null;
    },
    {
        btype = "1xfer";
        phash = "abe";
        ts = 3;
        tx = {
            amt = 5;
            from = ?{
                owner = Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                sub_account = null;
            };
            to = ?{
                owner = Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai");
                sub_account = null;
            };
            spender = null;
            memo = null;
        };
        fee = ?1;
    },
    {
        btype = "1xfer";
        phash = "abf";
        ts = 5;
        tx = {
            amt = 9;
            from = ?{
                owner = Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                sub_account = null;
            };
            to = ?{
                owner = Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai");
                sub_account = null;
            };
            spender = null;
            memo = null;
        };
        fee = ?1;
    },
    {
        btype = "2approve";
        phash = "ac0";
        ts = 5;
        tx = {
            amt = 32;
            from = ?{
                owner = Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                sub_account = null;
            };
            to = null;
            spender = ?{
                owner = Principal.fromText("aboy3-giaaa-aaaar-aaaaq-cai");
                sub_account = null;
            };
            memo = null;
        };
        fee = null;
    },
    {
        btype = "2xfer";
        phash = "abg";
        ts = 5;
        tx = {
            amt = 17;
            from = ?{
                owner = Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                sub_account = null;
            };
            to = ?{
                owner = Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai");
                sub_account = null;
            };
            spender = ?{
                owner = Principal.fromText("aboy3-giaaa-aaaar-aaaaq-cai");
                sub_account = null;
            };
            memo = null;
        };
        fee = ?1;
    },
]);

for (tx in input_txs.vals()) {
    let #ok(_) = txs.insert(tx);
};

type Options = {
    filter : {
        btype : ?[Text]; // filter based on btype
        to : ?Account; // filter based on to
        from : ?Account; // filter based on from
        spender : ?Account; // filter based on spender
        account : ?Account; // filter any transaction involving this account
        amt : ?{
            min : ?Nat;
            max : ?Nat;
        };
    };
    sort : ?(Text, ZenDB.SortDirection);
    // pagination : {
    //     limit : Nat;
    //     offset : Nat;
    // };
};

func options_to_query(options : Options) : ZenDB.QueryBuilder {

    let Query = ZenDB.QueryBuilder();

    ignore do ? {

        if (options.filter.btype != null) {
            let btypes = options.filter.btype!;
            let values = Array.map<Text, ZenDB.Candid>(btypes, func(btype : Text) : ZenDB.Candid = #Text(btype));

            ignore Query.Where("btype", #In(values));
        };

        if (options.filter.account == null) {

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
        };

        if (options.filter.account != null) {
            let account = options.filter.account!;
            ignore Query.Where("tx.to.owner", #eq(#Principal(account.owner)));
            ignore Query.Or("tx.from.owner", #eq(#Principal(account.owner)));
            ignore Query.Or("tx.spender.owner", #eq(#Principal(account.owner)));
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

        if (options.sort != null) {
            ignore Query.Sort(options.sort!);
        };

    };

    Query;

};

func get_txs(options : Options) : [(Nat, Tx)] {
    let Query = options_to_query(options);

    let query_res = txs.find(Query);
    let #ok(matching_txs) = query_res else Debug.trap("get_txs failed: " # debug_show query_res);

    matching_txs;

};

suite(
    "testing txs db",
    func() {
        test(
            "get_txs() with btype = '1mint'",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = ?["1mint"];
                        to = null;
                        from = null;
                        spender = null;
                        account = null;
                        amt = null;
                    };
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        tx.btype == "1mint";
                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },
        );
        test(
            "get_txs() with btype = '1xfer'",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = ?["1xfer"];
                        to = null;
                        from = null;
                        spender = null;
                        account = null;
                        amt = null;
                    };
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        tx.btype == "1xfer";
                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },
        );

        test(
            "get_txs() with btype = '2approve'",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = ?["2approve"];
                        to = null;
                        from = null;
                        spender = null;
                        account = null;
                        amt = null;
                    };
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        tx.btype == "2approve";
                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },
        );

        test(
            "get_txs() with btype  == '1burn' or '1xfer']",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = ?["1burn", "1xfer"];
                        to = null;
                        from = null;
                        spender = null;
                        account = null;
                        amt = null;
                    };
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        tx.btype == "1burn" or tx.btype == "1xfer";
                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },

        );

        test(
            "get_txs() with principal 'rimrc-piaaa-aaaao-aaljq-cai' as the recipient",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = null;
                        to = ?{
                            owner = Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai");
                            sub_account = null;
                        };
                        from = null;
                        spender = null;
                        account = null;
                        amt = null;
                    };
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        ?true == (
                            do ? {

                                tx.tx.to!.owner == Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai");

                            }
                        );

                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },
        );

        test(
            "get_txs() with principal 'suaf3-hqaaa-aaaaf-bfyoa-cai' as the sender",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = null;
                        to = null;
                        from = ?{
                            owner = Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                            sub_account = null;
                        };
                        spender = null;
                        account = null;
                        amt = null;
                    };
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        ?true == (
                            do ? {

                                tx.tx.from!.owner == Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");

                            }
                        );

                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },

        );

        test(
            "get_txs() with principal 'aboy3-giaaa-aaaar-aaaaq-cai' as the spender",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = null;
                        to = null;
                        from = null;
                        spender = ?{
                            owner = Principal.fromText("aboy3-giaaa-aaaar-aaaaq-cai");
                            sub_account = null;
                        };
                        account = null;
                        amt = null;
                    };
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        ?true == (
                            do ? {

                                tx.tx.spender!.owner == Principal.fromText("aboy3-giaaa-aaaar-aaaaq-cai");

                            }
                        );

                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },
        );

        test(
            "get_txs() involving principal 'suaf3-hqaaa-aaaaf-bfyoa-cai'",
            func() {

                let options = {
                    sort = null;
                    filter = {
                        btype = null;
                        to = null;
                        from = null;
                        spender = null;
                        account = ?{
                            owner = Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                            sub_account = null;
                        };
                        amt = null;
                    };

                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {

                        ?true == (
                            do ? {

                                var account_is_included = false;

                                if (tx.tx.to != null) {
                                    account_is_included := account_is_included or tx.tx.to!.owner == Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                                };
                                if (tx.tx.from != null) {
                                    account_is_included := account_is_included or tx.tx.from!.owner == Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                                };

                                if (tx.tx.spender != null) {
                                    account_is_included := account_is_included or tx.tx.spender!.owner == Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai");
                                };

                                account_is_included

                            }
                        );

                    },
                    func(tx : Tx) : Text = debug_show tx,
                );

            },
        );

        test(
            "get_txs() with 'amt' less than 50 and greater than 1",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = null;
                        to = null;
                        from = null;
                        spender = null;
                        account = null;
                        amt = ?{
                            min = ?2;
                            max = ?49;
                        };
                    };
                };

                let db_query = options_to_query(options);

                switch (txs.getBestIndex(db_query)) {
                    case (?index) {
                        Debug.print("query: " # debug_show db_query.build());
                        Debug.print("best index: " # debug_show (index.name, index.key_details));
                    };
                    case (_) ();
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        tx.tx.amt > 1 and tx.tx.amt < 50;
                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },
        );

        test(
            "get_txs() involving two out of three principals",
            func() {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #In([#Principal(Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai")), #Principal(Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai"))]),
                ).Or(
                    "tx.from.owner",
                    #In([#Principal(Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai")), #Principal(Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai"))]),
                ).Or(
                    "tx.spender.owner",
                    #In([#Principal(Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai")), #Principal(Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai"))]),
                );

                let #ok(result) = txs.find(db_query);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {

                        ?true == (
                            do ? {

                                var account_is_included = false;

                                if (tx.tx.to != null) {
                                    account_is_included := account_is_included or tx.tx.to!.owner == Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai") or tx.tx.to!.owner == Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai");
                                };
                                if (tx.tx.from != null) {
                                    account_is_included := account_is_included or tx.tx.from!.owner == Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai") or tx.tx.from!.owner == Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai");
                                };

                                if (tx.tx.spender != null) {
                                    account_is_included := account_is_included or tx.tx.spender!.owner == Principal.fromText("suaf3-hqaaa-aaaaf-bfyoa-cai") or tx.tx.spender!.owner == Principal.fromText("rimrc-piaaa-aaaao-aaljq-cai");
                                };

                                account_is_included

                            }
                        );

                    },
                    func(tx : Tx) : Text = debug_show tx,
                );

            },
        );

        test(
            "get_txs() with 'btype' = '1xfer' and 'amt' > 10",
            func() {
                let options = {
                    sort = null;
                    filter = {
                        btype = ?["1xfer"];
                        to = null;
                        from = null;
                        spender = null;
                        account = null;
                        amt = ?{
                            min = ?11;
                            max = null;
                        };
                    };
                };

                let result = get_txs(options);

                TestUtils.validate_records(
                    input_txs,
                    result,
                    func(id : Nat, tx : Tx) : Bool {
                        tx.btype == "1xfer" and tx.tx.amt > 10;
                    },
                    func(tx : Tx) : Text = debug_show tx,
                );
            },
        );
    },
);
