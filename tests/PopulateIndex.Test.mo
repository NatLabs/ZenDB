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
    tx_index : Nat;
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

let TxSchema : ZenDB.Types.Schema = #Record([
    ("btype", #Text),
    ("phash", #Blob),
    ("ts", #Nat),
    ("tx_index", #Nat),
    (
        "tx",
        #Record([("amt", #Nat), ("from", #Option(AccountSchema)), ("to", #Option(AccountSchema)), ("spender", #Option(AccountSchema)), ("memo", #Option(#Blob))]),
    ),
    ("fee", #Option(#Nat)),
]);

let candify_tx = {
    from_blob = func(blob : Blob) : ?Tx {
        from_candid (blob);
    };
    to_blob = func(c : Tx) : Blob { to_candid (c) };
};

let principals = Array.tabulate(
    50,
    func(i : Nat) : Principal {
        fuzz.principal.randomPrincipal(29);
    },
);

func new_tx(fuzz : Fuzz.Fuzzer, principals : [Principal], i : Nat) : Tx {

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
        tx_index = i;
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

let db_sstore = let sstore = ZenDB.newStableStore(
    ?{
        logging = ?{
            log_level = #Debug;
            is_running_locally = true;
        };
        memory_type = ?(#stableMemory);
    }
);
let db = ZenDB.launchDefaultDB(db_sstore);

let #ok(txs) = db.create_collection<Tx>("transactions", TxSchema, candify_tx, []);

let limit = 1000;
let pagination_limit = 3;

let input_txs = Buffer.fromArray<Tx>(
    Array.tabulate<Tx>(
        limit,
        func(i : Nat) : Tx {
            new_tx(fuzz, principals, i);
        },
    )
);

for ((i, tx) in Itertools.enumerate(input_txs.vals())) {
    // the id is assigned incrementally
    // so they should match the tx_index
    let #ok(id) = txs.insert(tx);
};

assert txs.size() == limit;

var size = 0;
let bitmap = BitMap.fromIter(txs.keys());

Debug.print("bitmap: " # debug_show bitmap.size());
assert bitmap.size() == limit;

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
    sort : ?(Text, ZenDB.Types.SortDirection);
    pagination : ?{
        limit : Nat;
        offset : Nat;
        cursor : ?Nat;
    };
};

func options_to_query(options : Options) : ZenDB.QueryBuilder {

    let Query = ZenDB.QueryBuilder();

    ignore do ? {

        if (options.filter.btype != null) {
            let btypes = options.filter.btype!;
            let values = Array.map<Text, ZenDB.Types.Candid>(btypes, func(btype : Text) : ZenDB.Types.Candid = #Text(btype));

            ignore Query.Where("btype", #anyOf(values));
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

        if (options.pagination != null) {
            let pagination = options.pagination!;
            ignore Query.Cursor(pagination.cursor, #Forward);
            ignore Query.Limit(pagination.limit);
        };

    };

    Query;

};

func get_txs(options : Options) : [(Nat, Tx)] {
    let Query = options_to_query(options);

    let query_res = txs.search(Query);
    let #ok(matching_txs) = query_res else Debug.trap("get_txs failed: " # debug_show query_res);

    matching_txs;

};

func get_txs_from_query(db_query : ZenDB.QueryBuilder) : [(Nat, Tx)] {

    let query_res = txs.search(db_query);
    let #ok(matching_txs) = query_res else Debug.trap("get_txs failed: " # debug_show query_res);

    matching_txs;

};

func skip_limit_paginated_query(db_query : ZenDB.QueryBuilder, pagination_limit : Nat) : [(Nat, Tx)] {

    ignore db_query.Limit(pagination_limit);
    let #ok(matching_txs) = txs.search(db_query);
    let bitmap = BitMap.fromIter(Iter.map<(Nat, Tx), Nat>(matching_txs.vals(), func((id, _) : (Nat, Tx)) : Nat = id));
    let records = Buffer.fromArray<(Nat, Tx)>(matching_txs);
    var batch_size = records.size();

    label skip_limit_pagination while (batch_size > 0) {
        ignore db_query.Skip(records.size()).Limit(pagination_limit);

        let #ok(matching_txs) = txs.search(db_query);
        // Debug.print("matching_txs: " # debug_show matching_txs);

        assert matching_txs.size() <= pagination_limit;
        batch_size := matching_txs.size();

        for ((id, tx) in matching_txs.vals()) {
            records.add((id, tx));

            if (bitmap.get(id)) {
                Debug.trap("Duplicate entry for id " # debug_show id);
            } else {
                bitmap.set(id, true);
            };
        };

    };

    ignore db_query.Skip(0).Limit(10000000000000000000000);

    Buffer.toArray(records);

};

func cursor_paginated_query(db_query : ZenDB.QueryBuilder, pagination_limit : Nat) : [(Nat, Tx)] {

    ignore db_query.Limit(pagination_limit);
    let #ok(matching_txs) = txs.search(db_query);
    // Debug.print("matching_txs: " # debug_show matching_txs);
    let records = Buffer.fromArray<(Nat, Tx)>(matching_txs);
    let bitmap = BitMap.fromIter(Iter.map<(Nat, Tx), Nat>(matching_txs.vals(), func((id, _) : (Nat, Tx)) : Nat = id));
    var batch_size = records.size();

    var opt_cursor : ?Nat = null;

    label pagination while (batch_size > 0) {
        switch (opt_cursor, ?(records.get(records.size() - 1).0)) {
            case (?cursor, ?new_cursor) if (cursor == new_cursor) {
                // break pagination;
                Debug.trap("Cursor is not moving");
            } else {
                opt_cursor := ?new_cursor;
            };
            case (null, new_cursor) opt_cursor := new_cursor;
            case (_, null) Debug.trap("Should be unreachable");

        };

        ignore db_query.Cursor(opt_cursor, #Forward).Limit(pagination_limit);

        let #ok(matching_txs) = txs.search(db_query);
        Debug.print("matching_txs: " # debug_show matching_txs);

        assert matching_txs.size() <= pagination_limit;
        batch_size := matching_txs.size();

        for ((id, tx) in matching_txs.vals()) {
            records.add((id, tx));

            if (bitmap.get(id)) {
                Debug.trap("Duplicate entry for id " # debug_show id);
            } else {
                bitmap.set(id, true);
            };
        };

    };

    Buffer.toArray(records);
};

type TestQuery = {
    query_name : Text;
    db_query : ZenDB.QueryBuilder;
    expected_query_resolution : ZenDB.Types.ZenQueryLang;
    check_if_result_matches_query : (Nat, Tx) -> Bool;
    display_record : Tx -> Text;
    sort : [(Text, ZenDB.Types.SortDirection)];
    check_if_results_are_sorted : (Tx, Tx) -> Bool;
};

suite(
    "create indexes and verify they are populated correctly",
    func() {
        let #ok(_) = txs.create_and_populate_index("index_1", [("btype", #Ascending), ("tx.amt", #Ascending)]);
        let #ok(_) = txs.create_and_populate_index("index_2", [("btype", #Ascending), ("ts", #Ascending)]);
        let #ok(_) = txs.create_and_populate_index("index_3", [("tx.amt", #Ascending)]);
        let #ok(_) = txs.create_and_populate_index("index_4", [("ts", #Ascending)]);
        let #ok(_) = txs.create_and_populate_index("index_5", [("tx.from.owner", #Ascending), ("tx.from.sub_account", #Ascending)]);
        let #ok(_) = txs.create_and_populate_index("index_6", [("tx.to.owner", #Ascending), ("tx.to.sub_account", #Ascending)]);
        let #ok(_) = txs.create_and_populate_index("index_7", [("tx.spender.owner", #Ascending), ("tx.spender.sub_account", #Ascending)]);

        // test_suites();
    },
);
