import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Principal "mo:base/Principal";

import HydraDB "../src";

actor {

    stable let db_store = HydraDB.newStableStore();

    let db = HydraDB.launch(db_store);

    type Account = {
        owner : Principal;
        sub_account : ?Blob; // null == [0...0]
    };

    type Tx = {
        btype : Text;
        phash : Blob;
        ts : Nat;
        tx : {
            amt : ?Nat;
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

    let TxSchema : HydraDB.Schema = #Record([
        ("btype", #Text),
        ("phash", #Blob),
        ("ts", #Nat),
        ("fee", #Option(#Nat)),
        (
            "tx",
            #Record([("amt", #Option(#Nat)), ("from", #Option(AccountSchema)), ("to", #Option(AccountSchema)), ("spender", #Option(AccountSchema)), ("memo", #Option(#Blob))]),
        ),
    ]);

    let candify_tx = {
        from_blob = func(blob : Blob) : Tx {
            let ?c : ?Tx = from_candid (blob);
            c;
        };
        to_blob = func(c : Tx) : Blob { to_candid (c) };
    };

    let #ok(txs) = db.create_collection<Tx>("transactions", TxSchema, candify_tx);
    let #ok(_) = txs.create_index(["btype", "tx.amt"]);
    let #ok(_) = txs.create_index(["tx.amt"]);
    let #ok(_) = txs.create_index(["tx.from.owner", "tx.from.sub_account"]);
    let #ok(_) = txs.create_index(["tx.to.owner", "tx.to.sub_account"]);
    let #ok(_) = txs.create_index(["tx.spender.owner", "tx.spender.sub_account"]);

    let input_txs : [Tx] = [
        {
            btype = "1mint";
            phash = "abc";
            ts = 1;
            tx = {
                amt = ?100;
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
                amt = ?1;
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
                amt = ?1;
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
            ts = 4;
            tx = {
                amt = ?1;
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
                amt = ?10;
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
                amt = ?5;
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
    ];

    for (tx in input_txs.vals()) {
        let #ok(_) = txs.insert(tx);
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

    public func get_txs(options : Options) : async [Tx] {
        let Query = HydraDB.QueryBuilder();

        ignore do ? {

            if (options.filter.btype != null) {
                let btypes = options.filter.btype!;
                let values = Array.map<Text, HydraDB.Candid>(btypes, func(btype : Text) : HydraDB.Candid = #Text(btype));

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

        Array.map<(Nat, Tx), Tx>(
            matching_txs,
            func(id : Nat, tx : Tx) : Tx = tx,
        );

    };

};
