// @testmode wasi
/// Memory allocation stats for ZenDB collections
///
/// Run: mops test --testmode wasi MemoryStats

import Runtime "mo:core@2.4/Runtime";
import Iter "mo:core@2.4/Iter";
import Array "mo:core@2.4/Array";
import Nat "mo:core@2.4/Nat";
import Text "mo:core@2.4/Text";
import Debug "mo:core@2.4/Debug";

import { test; suite } "mo:test";
import Fuzz "mo:fuzz";

import ZenDB "../../src/EmbeddedInstance";

// ─── Tx types (inlined from bench/txs-bench-utils.mo) ────────────────────────

type Account = {
    owner : Principal;
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
]);

let TxSchema : ZenDB.Types.Schema = #Record([
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
    from_blob = func(blob : Blob) : ?Tx {
        from_candid(blob);
    };
    to_blob = func(c : Tx) : Blob { to_candid(c) };
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

    {
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

// ─────────────────────────────────────────────────────────────────────────────

let N = 10_000;

let fuzz = Fuzz.fromSeed(0x7eadbeef);

let principals = Array.tabulate(100, func(_ : Nat) : Principal { fuzz.principal.randomPrincipal(29) });

let txs = Array.tabulate<Tx>(N, func(_ : Nat) : Tx {
    new_tx(fuzz, principals);
});

let single_field_indexes : [[(Text, ZenDB.Types.CreateIndexSortDirection)]] = [
    [("btype",               #Ascending)],
    [("tx.amt",              #Ascending)],
    [("ts",                  #Ascending)],
    [("tx.from.owner",       #Ascending)],
    [("tx.to.owner",         #Ascending)],
    [("tx.spender.owner",    #Ascending)],
    [("fee",                 #Ascending)],
];

let fully_covered_indexes : [[(Text, ZenDB.Types.CreateIndexSortDirection)]] = [
    [("tx.amt",                               #Ascending)],
    [("ts",                                   #Ascending)],
    [("btype",         #Ascending), ("tx.amt", #Ascending)],
    [("btype",         #Ascending), ("ts",     #Ascending)],
    [("tx.from.owner", #Ascending), ("btype",         #Ascending), ("ts",     #Ascending)],
    [("tx.to.owner",   #Ascending), ("btype",         #Ascending), ("ts",     #Ascending)],
    [("tx.from.owner", #Ascending), ("tx.to.owner",   #Ascending), ("btype",         #Ascending), ("ts",  #Ascending)],
];

// ─── Formatting helpers ───────────────────────────────────────────────────────

func pad_right(s : Text, width : Nat) : Text {
    let len = s.size();
    if (len >= width) return s;
    var result = s;
    var i = len;
    while (i < width) { result := result # " "; i += 1 };
    result;
};

func pad_left(s : Text, width : Nat) : Text {
    let len = s.size();
    if (len >= width) return s;
    var result = "";
    var i = len;
    while (i < width) { result := result # " "; i += 1 };
    result # s;
};

func repeat_char(c : Text, n : Nat) : Text {
    var result = "";
    var i = 0;
    while (i < n) { result := result # c; i += 1 };
    result;
};

func fmt_kb(bytes : Nat) : Text {
    let kb = bytes / 1024;
    let rem = (bytes % 1024) * 10 / 1024;
    Nat.toText(kb) # "." # Nat.toText(rem) # " KB";
};

func print_divider(label_w : Nat, col_w : Nat, n_cols : Nat) {
    var line = "+" # repeat_char("-", label_w + 2) # "+";
    var i = 0;
    while (i < n_cols) { line := line # repeat_char("-", col_w + 2) # "+"; i += 1 };
    Debug.print(line);
};

func print_header_row(hdr : Text, cols : [Text], label_w : Nat, col_w : Nat) {
    var row = "| " # pad_right(hdr, label_w) # " |";
    for (c in cols.vals()) {
        row := row # " " # pad_left(c, col_w) # " |";
    };
    Debug.print(row);
};

func print_data_row(lbl : Text, values : [Text], label_w : Nat, col_w : Nat) {
    var row = "| " # pad_right(lbl, label_w) # " |";
    for (v in values.vals()) {
        row := row # " " # pad_left(v, col_w) # " |";
    };
    Debug.print(row);
};

// ─── Benchmark helper ────────────────────────────────────────────────────────

type RunStats = {
    no_idx : ZenDB.Types.CollectionStats;
    single_idx : ZenDB.Types.CollectionStats;
    covered_idx : ZenDB.Types.CollectionStats;
};

func collect_stats(memory_type : ZenDB.Types.MemoryType, is_compression_enabled : Bool) : RunStats {
    let canister_id = fuzz.principal.randomPrincipal(29);

    let sstore = ZenDB.newStableStore(canister_id, ?{
        ZenDB.defaultSettings with
        memory_type = ?memory_type;
        is_running_locally = ?true;
        is_compression_enabled = ?is_compression_enabled;
    });
    let db = ZenDB.launchDefaultDB(sstore);

    let #ok(no_idx)      = db.createCollection<Tx>("no_index",      TxSchema, candify_tx, null);
    let #ok(single_idx)  = db.createCollection<Tx>("single_field",  TxSchema, candify_tx, null);
    let #ok(covered_idx) = db.createCollection<Tx>("fully_covered", TxSchema, candify_tx, null);

    let single_params = Array.tabulate<ZenDB.Types.CreateIndexParams>(
        single_field_indexes.size(),
        func(i) { ("sf_idx_" # Nat.toText(i), single_field_indexes[i], null) },
    );
    let covered_params = Array.tabulate<ZenDB.Types.CreateIndexParams>(
        fully_covered_indexes.size(),
        func(i) { ("fc_idx_" # Nat.toText(i), fully_covered_indexes[i], null) },
    );

    let #ok(sf_batch) = single_idx.batchCreateIndexes(single_params);
    var sf_more = true;
    while (sf_more) {
        let #ok(cont) = single_idx.processIndexBatch(sf_batch);
        sf_more := cont;
    };

    let #ok(fc_batch) = covered_idx.batchCreateIndexes(covered_params);
    var fc_more = true;
    while (fc_more) {
        let #ok(cont) = covered_idx.processIndexBatch(fc_batch);
        fc_more := cont;
    };

    for (tx in txs.vals()) {
        ignore no_idx.insert(tx);
        ignore single_idx.insert(tx);
        ignore covered_idx.insert(tx);
    };

    {
        no_idx    = no_idx.stats();
        single_idx  = single_idx.stats();
        covered_idx = covered_idx.stats();
    };
};

// ─── Comparison table ────────────────────────────────────────────────────────

func fmt_pct(before : Nat, after : Nat) : Text {
    if (before == 0) return "    -  ";
    if (after >= before) {
        let g = (after - before) * 100 / before;
        return "-" # Nat.toText(g) # "%";
    };
    let p = (before - after) * 1000 / before;
    Nat.toText(p / 10) # "." # Nat.toText(p % 10) # "%";
};

func print_comparison_table(title : Text, uncompr : RunStats, compr : RunStats) {
    let lw = 50;
    let cw = 24;
    let n_cols = 3;

    Debug.print("\n");
    let box_inner = lw + 2 + (cw + 3) * n_cols;
    Debug.print("╔" # repeat_char("═", box_inner) # "╗");
    Debug.print("║  " # pad_right(title, box_inner - 2) # "║");
    Debug.print("╚" # repeat_char("═", box_inner) # "╝");
    Debug.print("  Each cell shows: allocated (used).");
    Debug.print("  Savings = how much compression reduces allocated bytes (used bytes) relative to uncompressed.");
    Debug.print("");

    print_divider(lw, cw, n_cols);
    print_header_row("Collection / BTree", ["Uncompressed", "Compressed", "Savings"], lw, cw);
    print_divider(lw, cw, n_cols);

    // cmp_row: u_alloc/c_alloc = allocated bytes, u_used/c_used = used bytes
    func cmp_row(lbl : Text, u_alloc : Nat, u_used : Nat, c_alloc : Nat, c_used : Nat) {
        let u_str = fmt_kb(u_alloc) # " (" # fmt_kb(u_used) # ")";
        let c_str = fmt_kb(c_alloc) # " (" # fmt_kb(c_used) # ")";
        let s_str = fmt_pct(u_alloc, c_alloc) # " (" # fmt_pct(u_used, c_used) # ")";
        print_data_row(lbl, [u_str, c_str, s_str], lw, cw);
    };

    func sum_alloc(cs : ZenDB.Types.CollectionStats) : Nat {
        var total = cs.memory.allocatedBytes;
        for (idx in cs.indexes.vals()) { total += idx.memory.allocatedBytes };
        total;
    };

    func sum_idx_alloc(cs : ZenDB.Types.CollectionStats) : Nat {
        var total = 0;
        for (idx in cs.indexes.vals()) { total += idx.memory.allocatedBytes };
        total;
    };

    func cmp_collection(name : Text, u : ZenDB.Types.CollectionStats, c : ZenDB.Types.CollectionStats) {
        cmp_row(name # " (total)", sum_alloc(u), u.total_used_bytes, sum_alloc(c), c.total_used_bytes);

        if (u.indexes.size() == 0) {
            cmp_row("  └─ doc btree", u.memory.allocatedBytes, u.memory.usedBytes, c.memory.allocatedBytes, c.memory.usedBytes);
        };

        if (u.indexes.size() > 0) {
            cmp_row("  └─ indexes (total)", sum_idx_alloc(u), u.total_index_store_bytes, sum_idx_alloc(c), c.total_index_store_bytes);
        };

        var k = 0;
        for (ui in u.indexes.vals()) {
            if (k < c.indexes.size()) {
                let ci = c.indexes[k];
                if (not ui.used_internally) {
                    let fields_str = Text.join(Array.map<(Text, ZenDB.Types.SortDirection), Text>(
                        ui.fields,
                        func((field, dir)) {
                            field # (switch (dir) { case (#Ascending) " ↑"; case (#Descending) " ↓" });
                        },
                    ).vals(), ", ");
                    cmp_row("      └─ " # ui.name # " [" # fields_str # "]", ui.memory.allocatedBytes, ui.memory.usedBytes, ci.memory.allocatedBytes, ci.memory.usedBytes);
                };
            };
            k += 1;
        };

        print_divider(lw, cw, n_cols);
    };

    let doc_count = Nat.toText(N);
    cmp_collection("no_index (" # doc_count # " docs)",      uncompr.no_idx,      compr.no_idx);
    cmp_collection("single_field (" # doc_count # " docs)",  uncompr.single_idx,  compr.single_idx);
    cmp_collection("fully_covered (" # doc_count # " docs)", uncompr.covered_idx, compr.covered_idx);

    Debug.print("");
};

// ─── BTree detail table ───────────────────────────────────────────────────────

func print_btree_detail_table(title : Text, s : RunStats) {
    let lw     = 65;
    let cw     = 12;
    let hdrs   = ["Entries", "Leaves", "Branches", "Allocated", "Used", "Free", "Key Bytes", "Val Bytes", "Metadata"];
    let n_cols = hdrs.size();

    let box_inner = lw + 2 + (cw + 3) * n_cols;
    Debug.print("\n");
    Debug.print("╔" # repeat_char("═", box_inner) # "╗");
    Debug.print("║  " # pad_right(title, box_inner - 2) # "║");
    Debug.print("╚" # repeat_char("═", box_inner) # "╝");
    Debug.print("");

    print_divider(lw, cw, n_cols);
    print_header_row("Config", hdrs, lw, cw);
    print_divider(lw, cw, n_cols);

    func btree_row(row_label : Text, entries : Nat, m : ZenDB.Types.MemoryBTreeStats) {
        print_data_row(
            row_label,
            [
                Nat.toText(entries),
                Nat.toText(m.leafCount),
                Nat.toText(m.branchCount),
                fmt_kb(m.allocatedBytes),
                fmt_kb(m.usedBytes),
                fmt_kb(m.freeBytes),
                fmt_kb(m.keyBytes),
                fmt_kb(m.valueBytes),
                fmt_kb(m.metadataBytes),
            ],
            lw,
            cw,
        );
    };

    func add_btree_stats(a : ZenDB.Types.MemoryBTreeStats, b : ZenDB.Types.MemoryBTreeStats) : ZenDB.Types.MemoryBTreeStats {
        {
            totalNodeCount = a.totalNodeCount + b.totalNodeCount;
            leafCount      = a.leafCount      + b.leafCount;
            branchCount    = a.branchCount    + b.branchCount;
            allocatedBytes = a.allocatedBytes + b.allocatedBytes;
            allocatedPages = a.allocatedPages + b.allocatedPages;
            usedBytes      = a.usedBytes      + b.usedBytes;
            freeBytes      = a.freeBytes      + b.freeBytes;
            keyBytes       = a.keyBytes       + b.keyBytes;
            valueBytes     = a.valueBytes     + b.valueBytes;
            metadataBytes  = a.metadataBytes  + b.metadataBytes;
            branchBytes    = a.branchBytes    + b.branchBytes;
            leafBytes      = a.leafBytes      + b.leafBytes;
            dataBytes      = a.dataBytes      + b.dataBytes;
            bytesPerPage   = a.bytesPerPage;
        };
    };

    func collection_rows(cs : ZenDB.Types.CollectionStats) {
        let user_indexes = Array.filter<ZenDB.Types.IndexStats>(cs.indexes, func(idx) { not idx.used_internally });
        let has_indexes = user_indexes.size() > 0;

        if (has_indexes) {
            // Compute total across doc btree + all user indexes
            var total_entries = cs.entries;
            var total_mem = cs.memory;
            for (idx in user_indexes.vals()) {
                total_entries += idx.entries;
                total_mem := add_btree_stats(total_mem, idx.memory);
            };
            btree_row(
                cs.name # " (total, avg doc " # Nat.toText(cs.avg_document_size) # " B)",
                total_entries,
                total_mem,
            );
        } else {
            btree_row(
                cs.name # " / docs  (avg " # Nat.toText(cs.avg_document_size) # " B)",
                cs.entries,
                cs.memory,
            );
        };

        for (idx in user_indexes.vals()) {
            let fields_str = Text.join(Array.map<(Text, ZenDB.Types.SortDirection), Text>(
                idx.fields,
                func((field, dir)) {
                    field # (switch (dir) { case (#Ascending) " ↑"; case (#Descending) " ↓" });
                },
            ).vals(), ", ");
            btree_row(
                "  └─ " # idx.name # " [" # fields_str # "]",
                idx.entries,
                idx.memory,
            );
        };
        print_divider(lw, cw, n_cols);
    };

    collection_rows(s.no_idx);
    collection_rows(s.single_idx);
    collection_rows(s.covered_idx);
    Debug.print("");
};

// ─────────────────────────────────────────────────────────────────────────────

suite(
    "Memory Allocation Stats",
    func() {
        test(
            "Stable Memory — Compression Comparison (" # Nat.toText(N) # " txs)",
            func() {
                let uncompr = collect_stats(#stableMemory, false);
                let compr   = collect_stats(#stableMemory, true);
                print_comparison_table(
                    "Compression Gains — " # Nat.toText(N) # " icrc3 txs (no idx / 7 single-field / 6 fully-covered)",
                    uncompr,
                    compr,
                );
                print_btree_detail_table("BTree Metadata — Uncompressed (" # Nat.toText(N) # " txs)", uncompr);
                print_btree_detail_table("BTree Metadata — Compressed (" # Nat.toText(N) # " txs)", compr);
            },
        );
    },
);
