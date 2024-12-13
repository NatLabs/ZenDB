import Iter "mo:base/Iter";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Prelude "mo:base/Prelude";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";


import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import BitMap "mo:bit-map";

import ZenDB "../src";

module {
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

    public func init() : Bench.Bench {
        let bench = Bench.Bench();

        bench.name("Benchmarking zenDB with icrc3 txs");
        bench.description("Benchmarking the performance with 10k txs");

        bench.cols([
            // "(best index)",
            "(full scan, -> array)",
            "(index intersection, -> array)",
            // "(sorted by tx.amt, ↑)",
            // "(sorted by tx.amt, ↓)",
            // "(skip_limit_pagination limit = 100, -> array"
        ]);

        bench.rows([
            "insert",
            "clear",
            "insert with 5 indexes",
            "btype == '1mint'",
            "btype == '1xfer' or '2xfer'",
            "principals[0] == tx.to.owner (is recipient)",
            "principals[0..10] == tx.to.owner (is recipient)",
            // "all txs involving principals[0]",
            // "all txs involving principals[0..10]",
            "250 < tx.amt <= 400",
            "btype == 1burn and tx.amt >= 750",
        ]);

        let limit = 10_000;
        let fuzz = Fuzz.fromSeed(0x7eadbeef);

        let db_sstore = ZenDB.newStableStore();
        let db = ZenDB.launch(db_sstore);

        let #ok(txs) = db.create_collection<Tx>("transactions", TxSchema, candify_tx);

        let principals = Array.tabulate(
            50,
            func(i : Nat) : Principal {
                fuzz.principal.randomPrincipal(29);
            },
        );

        let candid_principals = Array.map<Principal, ZenDB.Candid>(
            Iter.toArray(Array.slice<Principal>(principals, 0, 10)),
            func(p : Principal) : ZenDB.Candid = #Principal(p),
        );

        let principals_0_10 = Array.tabulate(
            10,
            func(i : Nat) : Principal {
                principals.get(i);
            },
        );

        let predefined_txs = Buffer.Buffer<Tx>(limit);

        for (i in Iter.range(0, limit - 1)) {
            let tx = new_tx(fuzz, principals);
            predefined_txs.add(tx);
        };

        bench.runner(
            func(col, row) = switch (row) {
                case ("(best index)") {
                    best_index(txs, col, limit, predefined_txs, principals, candid_principals, principals_0_10);
                };
                case ("(index intersection, -> array)") {
                    index_intersection(txs, col, limit, predefined_txs, principals, candid_principals, principals_0_10);
                };
                case ("(full scan, -> array)") {
                    full_scan(txs, col, limit, predefined_txs, principals, candid_principals, principals_0_10);
                };
                case ("(skip_limit_pagination limit = 100, -> array") {
                    paginated_queries(txs, col, limit, predefined_txs, principals, candid_principals, principals_0_10, #Ascending, 100);
                };
                case (_) {
                    Debug.trap("Should be unreachable:\n row = \"" # debug_show row # "\" and col = \"" # debug_show col # "\"");
                };

            }
        );

        bench;
    };

    func best_index(txs : ZenDB.Collection<Tx>, section : Text, limit : Nat, predefined_txs : Buffer.Buffer<Tx>, principals : [Principal], candid_principals : [ZenDB.Candid], principals_0_10 : [Principal]) {
        switch (section) {

            case ("insert") {
                // re-use the predefined txs
            };

            case ("clear") {
                // re-use the predefined txs
            };

            case ("insert with 5 indexes") {
                // re-use the predefined txs
            };

            case ("btype == '1mint'") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #eq(#Text("1mint")),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("btype == '1xfer' or '2xfer'") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #In([#Text("1xfer"), #Text("2xfer")]),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("principals[0] == tx.to.owner (is recipient)") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #eq(#Principal(principals.get(0))),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("principals[0..10] == tx.to.owner (is recipient)") {

                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #In(candid_principals),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("all txs involving principals[0]") {
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

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("all txs involving principals[0..10]") {
                let candid_principals = Array.map<Principal, ZenDB.Candid>(
                    Iter.toArray(Array.slice(principals, 0, 10)),
                    func(p : Principal) : ZenDB.Candid = #Principal(p),
                );

                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #In(candid_principals),
                ).Or(
                    "tx.from.owner",
                    #In(candid_principals),
                ).Or(
                    "tx.spender.owner",
                    #In(candid_principals),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("250 < tx.amt <= 400") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.amt",
                    #gt(#Nat(250)),
                ).And(
                    "tx.amt",
                    #lte(#Nat(400)),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("btype == 1burn and tx.amt >= 750") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.amt",
                    #gte(#Nat(750)),
                ).And(
                    "btype",
                    #eq(#Text("1burn")),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case (_) {
                Debug.trap("Should be unreachable:\n row = (best index, -> array)  and col = \"" # debug_show section # "\"");
            };

        };

    };

    func index_intersection(txs : ZenDB.Collection<Tx>, section : Text, limit : Nat, predefined_txs : Buffer.Buffer<Tx>, principals : [Principal], candid_principals : [ZenDB.Candid], principals_0_10 : [Principal]) {
        switch (section) {
            case ("insert") {
                // re-use the predefined txs
            };

            case ("clear") {
                // re-use the predefined txs
            };

            case ("insert with 5 indexes") {
                // re-use the predefined txs
            };

            case ("btype == '1mint'") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #eq(#Text("1mint")),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("btype == '1xfer' or '2xfer'") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #In([#Text("1xfer"), #Text("2xfer")]),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("principals[0] == tx.to.owner (is recipient)") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #eq(#Principal(principals.get(0))),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("principals[0..10] == tx.to.owner (is recipient)") {
                let candid_principals = Array.map<Principal, ZenDB.Candid>(
                    Iter.toArray(Array.slice<Principal>(principals, 0, 10)),
                    func(p : Principal) : ZenDB.Candid = #Principal(p),
                );

                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #In(candid_principals),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("all txs involving principals[0]") {
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

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("all txs involving principals[0..10]") {
                let candid_principals = Array.map<Principal, ZenDB.Candid>(
                    Iter.toArray(Array.slice(principals, 0, 10)),
                    func(p : Principal) : ZenDB.Candid = #Principal(p),
                );

                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #In(candid_principals),
                ).Or(
                    "tx.from.owner",
                    #In(candid_principals),
                ).Or(
                    "tx.spender.owner",
                    #In(candid_principals),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("250 < tx.amt <= 400") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.amt",
                    #gt(#Nat(250)),
                ).And(
                    "tx.amt",
                    #lte(#Nat(400)),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case ("btype == 1burn and tx.amt >= 750") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #eq(#Text("1burn")),
                ).And(
                    "tx.amt",
                    #gte(#Nat(750)),
                );

                let #ok(matching_txs) = txs.find(db_query);
            };

            case (_) {
                Debug.trap("Should be unreachable:\n row = zenDB (index intersection, -> array) and col = \"" # debug_show section # "\"");
            };
        };
    };

    func full_scan(txs : ZenDB.Collection<Tx>, section : Text, limit : Nat, predefined_txs : Buffer.Buffer<Tx>, principals : [Principal], candid_principals : [ZenDB.Candid], principals_0_10 : [Principal]) {
        switch (section) {

            case ("insert") {
                for (i in Iter.range(0, limit - 1)) {
                    let tx = predefined_txs.get(i);
                    let #ok(_) = txs.insert(tx);
                };
            };

            case ("clear") {
                txs.clear();
            };

            case ("insert with 5 indexes") {
                let #ok(_) = txs.create_index(["btype", "tx.amt"]);
                let #ok(_) = txs.create_index(["tx.amt"]);
                let #ok(_) = txs.create_index(["btype", "ts"]);
                let #ok(_) = txs.create_index(["ts"]);
                let #ok(_) = txs.create_index(["tx.from.owner", "tx.from.sub_account"]);
                let #ok(_) = txs.create_index(["tx.to.owner", "tx.to.sub_account"]);
                let #ok(_) = txs.create_index(["tx.spender.owner", "tx.spender.sub_account"]);

                for (i in Iter.range(0, limit - 1)) {
                    let tx = predefined_txs.get(i);
                    let #ok(_) = txs.insert(tx);
                };
            };

            case ("btype == '1mint'") {
                let results = txs.filter(
                    func(tx : Tx) : Bool {
                        tx.btype == "1mint";
                    }
                );

            };

            case ("btype == '1xfer' or '2xfer'") {
                let results = txs.filter(
                    func(tx : Tx) : Bool {
                        tx.btype == "1xfer" or tx.btype == "2xfer";
                    }
                );
            };

            case ("principals[0] == tx.to.owner (is recipient)") {
                let results = txs.filter(
                    func(tx : Tx) : Bool {
                        switch (tx.tx.to) {
                            case (?account) {
                                account.owner == principals.get(0);
                            };
                            case (_) { false };
                        };
                    }
                );
            };

            case ("principals[0..10] == tx.to.owner (is recipient)") {
                let principals_0_10 = Array.take(principals, 10);

                let results = txs.filter(
                    func(tx : Tx) : Bool {
                        switch (tx.tx.to) {
                            case (?account) {
                                (Array.find<Principal>(principals_0_10, func(p : Principal) : Bool { p == account.owner }) : ?Principal) != null;
                            };
                            case (_) { false };
                        };
                    }
                );

            };

            case ("all txs involving principals[0]") {
                let results = txs.filter(
                    func(tx : Tx) : Bool {

                        switch (tx.tx.to) {
                            case (?account) {
                                account.owner == principals.get(0);
                            };
                            case (_) {
                                switch (tx.tx.from) {
                                    case (?account) {
                                        account.owner == principals.get(0);
                                    };
                                    case (_) {
                                        switch (tx.tx.spender) {
                                            case (?account) {
                                                account.owner == principals.get(0);
                                            };
                                            case (_) { false };
                                        };
                                    };
                                };
                            };
                        };
                    }
                );
            };

            case ("all txs involving principals[0..10]") {
                let principals_0_10 = Array.take(principals, 10);

                let results = txs.filter(
                    func(tx : Tx) : Bool {

                        switch (tx.tx.to) {
                            case (?account) {
                                (Array.find<Principal>(principals_0_10, func(p : Principal) : Bool { p == account.owner }) : ?Principal) != null;
                            };
                            case (_) {
                                switch (tx.tx.from) {
                                    case (?account) {
                                        (Array.find<Principal>(principals_0_10, func(p : Principal) : Bool { p == account.owner }) : ?Principal) != null;
                                    };
                                    case (_) {
                                        switch (tx.tx.spender) {
                                            case (?account) {
                                                (Array.find<Principal>(principals_0_10, func(p : Principal) : Bool { p == account.owner }) : ?Principal) != null;
                                            };
                                            case (_) { false };
                                        };
                                    };
                                };
                            };
                        };
                    }
                );

            };

            case ("250 < tx.amt <= 400") {
                let results = txs.filter(
                    func(tx : Tx) : Bool {
                        tx.tx.amt > 250 and tx.tx.amt <= 400;
                    }
                );
            };

            case ("btype == 1burn and tx.amt >= 750") {
                let results = txs.filter(
                    func(tx : Tx) : Bool {
                        tx.btype == "1burn" and tx.tx.amt >= 750;
                    }
                );
            };

            case (_) {
                Debug.trap("Should be unreachable:\n row = zenDB (full scan, -> array) and col = \"" # debug_show section # "\"");
            };

        };

    };


  func paginated_queries(txs : ZenDB.Collection<Tx>, section : Text, limit : Nat, predefined_txs : Buffer.Buffer<Tx>, principals : [Principal], candid_principals : [ZenDB.Candid], principals_0_10 : [Principal], sort_direction : ZenDB.SortDirection, pagination_limit : Nat) {

        func skip_limit_paginated_query(db_query : ZenDB.QueryBuilder)  {
            ignore db_query.Limit(pagination_limit);
            let #ok(matching_txs) = txs.find(db_query);
            var records = matching_txs;
            var skip = 0;
            var opt_cursor : ?Nat = null;

            label pagination while (records.size() > 0){
                ignore db_query
                    .Skip(skip)
                    .Limit(pagination_limit);

                let #ok(matching_txs) = txs.find(db_query);

                records := matching_txs;

            };

            skip += records.size();
        };

        func skip_limit_skip_limit_paginated_query(db_query : ZenDB.QueryBuilder, pagination_limit : Nat) : [(Nat, Tx)] {

            ignore db_query.Limit(pagination_limit);
            let #ok(matching_txs) = txs.find(db_query);
            let bitmap = BitMap.fromIter(Iter.map<(Nat, Tx), Nat>(matching_txs.vals(), func((id, _) : (Nat, Tx)) : Nat = id));
            let records = Buffer.fromArray<(Nat, Tx)>(matching_txs);
            var batch_size = records.size();

            label skip_limit_pagination while (batch_size > 0) {
                ignore db_query.Skip(records.size()).Limit(pagination_limit);

                let #ok(matching_txs) = txs.find(db_query);
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

            Buffer.toArray(records);

        };

        switch (section) {
            case ("insert") {};

            case ("clear") {};

            case ("insert with 5 indexes") {};

            case ("btype == '1mint'") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #eq(#Text("1mint")),
                );

                skip_limit_paginated_query(db_query);
                
            };

            case ("btype == '1xfer' or '2xfer'") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "btype",
                    #In([#Text("1xfer"), #Text("2xfer")]),
                );

                skip_limit_paginated_query(db_query);
            };

            case ("principals[0] == tx.to.owner (is recipient)") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #eq(#Principal(principals.get(0))),
                );

                skip_limit_paginated_query(db_query);
            };
            case ("principals[0..10] == tx.to.owner (is recipient)") {
                let candid_principals = Array.map<Principal, ZenDB.Candid>(
                    Iter.toArray(Array.slice<Principal>(principals, 0, 10)),
                    func(p : Principal) : ZenDB.Candid = #Principal(p),
                );

                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #In(candid_principals),
                );

                skip_limit_paginated_query(db_query);
            };


            case ("all txs involving principals[0]") {
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

            case ("all txs involving principals[0..10]") {
                let candid_principals = Array.map<Principal, ZenDB.Candid>(
                    Iter.toArray(Array.slice(principals, 0, 10)),
                    func(p : Principal) : ZenDB.Candid = #Principal(p),
                );

                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.to.owner",
                    #In(candid_principals),
                ).Or(
                    "tx.from.owner",
                    #In(candid_principals),
                ).Or(
                    "tx.spender.owner",
                    #In(candid_principals),
                );

                skip_limit_paginated_query(db_query);
            };

            case ("250 < tx.amt <= 400") {
                let db_query = ZenDB.QueryBuilder().Where(
                    "tx.amt",
                    #gt(#Nat(250)),
                ).And(
                    "tx.amt",
                    #lte(#Nat(400)),
                );

                skip_limit_paginated_query(db_query);
            };

            case ("btype == 1burn and tx.amt >= 750") {
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
                Debug.trap("Should be unreachable:\n row = zenDB (skip_limit_pagination limit = 100, -> array) and col = \"" # debug_show section # "\"");
            };

        };

    };
   
};
