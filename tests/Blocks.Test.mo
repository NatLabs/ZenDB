func get_txs(options : Options) : async GetTxsResponse {
    Debug.print("get_txs called with options: " # debug_show options);
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

    let query_res = txs.find(Query);
    let #ok(matching_txs) = query_res else Debug.trap("get_txs failed: " # debug_show query_res);
    let #ok(total_matching_txs) = txs.count(Query) else Debug.trap("txs.count failed");

    let blocks = Array.map<(Nat, Block), Block>(
        matching_txs,
        func(id : Nat, tx : Block) : Block = tx,
    );

    Debug.print("get_txs returning " # debug_show { blocks = blocks.size(); total = total_matching_txs });

    { blocks; total = total_matching_txs };

};
