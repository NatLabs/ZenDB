import Iter "mo:core@2.4/Iter";
import Array "mo:core@2.4/Array";
import Text "mo:core@2.4/Text";
import Buffer "mo:base@0.16/Buffer";
import Nat "mo:core@2.4/Nat";

import Bench "mo:bench";
import Fuzz "mo:fuzz";

import ZenDB "../src";

module {
    let { QueryBuilder } = ZenDB;

    // -----------------------------------------------------------------------
    // Article corpus
    // -----------------------------------------------------------------------
    //
    // We generate 1 000 articles where each document has a `title` (5–8 tokens)
    // and a `body` (30–50 tokens) drawn from a fixed 40-word pool.
    //
    // The pool is split into frequency tiers so that benchmark queries have
    // predictable and meaningfully different result-set sizes:
    //
    //   RARE    (2 words): injected into ~1 % of docs  → ~10 hits
    //   COMMON  (3 words): injected into ~20 % of docs → ~200 hits
    //   FILLER (35 words): fill the remaining token budget uniformly
    //
    // With 1 000 docs × avg 40 tokens ≈ 40 000 token positions. At ~267
    // positions per filler word on average the gap between the rare and
    // common tiers (10 vs 200 docs) is large enough to stress different
    // execution paths (cursor-only fast path vs bitmap materialisation).

    let RARE_WORDS   = ["quantum", "blockchain"];
    let COMMON_WORDS = ["software", "system", "data"];
    let FILLER_WORDS = [
        "the",   "and",    "for",    "are",    "with",  "this",   "that",  "from",
        "have",  "been",   "not",    "but",    "they",  "more",   "will",  "one",
        "can",   "all",    "its",    "use",    "how",   "new",    "may",   "also",
        "time",  "work",   "into",   "just",   "over",  "two",    "way",   "make",
        "like",  "than",   "first",  "well",   "after", "back",   "other", "still",
        "each",  "even",   "never",  "only",   "since", "place",  "while", "world",
        "point", "large",  "early",  "need",   "later", "single", "real",  "small",
        "study", "given",  "level",  "often",  "human", "power",  "order", "build",
        "value", "clear",  "major",  "public", "local", "right",  "class", "state",
        "model", "group",  "table",  "field",  "range",
    ];

    let CATEGORIES = ["tech", "science", "health", "finance", "culture"];

    type Article = {
        title    : Text;
        body     : Text;
        category : Text;
        published: Bool;
    };

    let ArticleSchema : ZenDB.Types.Schema = #Record([
        ("title",     #Text),
        ("body",      #Text),
        ("category",  #Text),
        ("published", #Bool),
    ]);

    let candify_article = {
        from_blob = func(blob : Blob) : ?Article { from_candid (blob) };
        to_blob   = func(a : Article)  : Blob    { to_candid  (a)    };
    };

    // Build a deterministic article (no randomness in hot path).
    func makeArticle(fuzz : Fuzz.Fuzzer, i : Nat) : Article {
        // Title: 5 filler words + optionally a rare/common word at position 2
        let t0 = fuzz.array.randomEntry(FILLER_WORDS).1;
        let t1 = fuzz.array.randomEntry(FILLER_WORDS).1;
        let t2 = if   (i % 100 == 0) { RARE_WORDS[i % 2]          } // ~1 % rare
                 else if (i % 5  == 0) { COMMON_WORDS[i / 5 % 3]   } // ~20 % common
                 else                  { fuzz.array.randomEntry(FILLER_WORDS).1 };
        let t3 = fuzz.array.randomEntry(FILLER_WORDS).1;
        let t4 = fuzz.array.randomEntry(FILLER_WORDS).1;
        let title = t0 # " " # t1 # " " # t2 # " " # t3 # " " # t4;

        // Body: random length 25–100 tokens, with common words injected at fixed positions
        let body_len = fuzz.nat.randomRange(25, 100);
        let body_buf = Buffer.Buffer<Text>(body_len);
        for (j in Nat.rangeInclusive(0, body_len - 1)) {
            let word =
                if   (j == 10 and i % 5 == 0) { COMMON_WORDS[i / 5 % 3] }
                else if (j == 20 and i % 5 == 0) { COMMON_WORDS[(i / 5 + 1) % 3] }
                else { fuzz.array.randomEntry(FILLER_WORDS).1 };
            body_buf.add(word);
        };
        // Sprinkle a second rare word into 1 % of bodies so phrase test has data
        if (i % 100 == 0) {
            body_buf.add(RARE_WORDS[(i + 1) % 2]);
        };
        let body = Text.join(body_buf.vals(), " ");

        let category = CATEGORIES[i % CATEGORIES.size()];

        { title; body; category; published = (i % 3 != 0) };
    };

    // -----------------------------------------------------------------------
    // Benchmark
    // -----------------------------------------------------------------------

    public func init() : Bench.Bench {
        let bench = Bench.Bench();

        bench.name("Text Index Operations");
        bench.description("Benchmarking createTextIndex, insert, and search operators with 1 000 articles (body: 25–100 tokens, vocab: 80 words)");

        bench.cols([
            // "#heap  with text index",
            "#stableMemory  with text index",
        ]);

        bench.rows([
            // --- Setup cost ---
            "insert 1k articles (no text index yet)",
            "createTextIndex() on populated collection (backfill)",

            // --- Search operators ---
            "search(): #word — rare word  (~10 docs)",
            "search(): #word — common word (~200 docs)",
            "search(): #startsWith — partial prefix",
            "search(): #phrase — 2-word sequence",
            "search(): #anyOf — 3 common words (union)",
            "search(): #allOf — 2 common words (intersect)",
            "search(): #not_(#word) — bracket complement scan",
            "search(): #not_(#phrase) — De Morgan complement scan",
            "search(): #word + .And() category filter",
        ]);

        let limit = 300;

        let fuzz = Fuzz.fromSeed(0xc0ffee);

        let predefined = Array.tabulate<Article>(limit, func(i) = makeArticle(fuzz, i));

        let canister_id = fuzz.principal.randomPrincipal(29);

        // --- Heap collection ---
        let heap_sstore = ZenDB.newStableStore(canister_id, ?{ ZenDB.defaultSettings with memory_type = ?(#heap) });
        let heap_db     = ZenDB.launchDefaultDB(heap_sstore);
        let #ok(heap_col) = heap_db.createCollection<Article>(
            "heap_articles", ArticleSchema, candify_article, null
        );

        // --- Stable-memory collection ---
        let sm_sstore = ZenDB.newStableStore(canister_id, ?{ ZenDB.defaultSettings with memory_type = ?(#stableMemory) });
        let sm_db     = ZenDB.launchDefaultDB(sm_sstore);
        let #ok(sm_col) = sm_db.createCollection<Article>(
            "sm_articles", ArticleSchema, candify_article, null
        );

        bench.runner(func(row, col) {
            let col_obj = switch (col) {
                case ("#heap  with text index")         heap_col;
                case ("#stableMemory  with text index") sm_col;
                case (_)                                heap_col; // unreachable
            };

            let qb = func() : ZenDB.QueryBuilder = QueryBuilder();

            switch row {

                // ---- Setup rows ----------------------------------------

                case ("insert 1k articles (no text index yet)") {
                    for (a in predefined.vals()) {
                        ignore col_obj.insert(a);
                    };
                };

                case ("createTextIndex() on populated collection (backfill)") {
                    ignore col_obj.createTextIndex(
                        "articles_text", ["title", "body"]
                    );
                };

                // ---- Search rows ----------------------------------------

                case ("search(): #word — rare word  (~10 docs)") {
                    ignore col_obj.search(
                        qb().Where("title", #text(#word("quantum")))
                    );
                };

                case ("search(): #word — common word (~200 docs)") {
                    ignore col_obj.search(
                        qb().Where("body", #text(#word("software")))
                    );
                };

                case ("search(): #startsWith — partial prefix") {
                    // "sys" prefix of "system" — medium selectivity
                    ignore col_obj.search(
                        qb().Where("body", #text(#startsWith("sys")))
                    );
                };

                case ("search(): #phrase — 2-word sequence") {
                    // "quantum blockchain" — injected together in ~1 % of docs
                    ignore col_obj.search(
                        qb().Where("body", #text(#phrase("quantum blockchain")))
                    );
                };

                case ("search(): #anyOf — 3 common words (union)") {
                    ignore col_obj.search(
                        qb().Where("body", #text(#anyOf(["software", "system", "data"])))
                    );
                };

                case ("search(): #allOf — 2 common words (intersect)") {
                    ignore col_obj.search(
                        qb().Where("body", #text(#allOf(["software", "data"])))
                    );
                };

                case ("search(): #not_(#word) — bracket complement scan") {
                    ignore col_obj.search(
                        qb().Where("body", #not_(#text(#word("software"))))
                    );
                };

                case ("search(): #not_(#phrase) — De Morgan complement scan") {
                    ignore col_obj.search(
                        qb().Where("body", #not_(#text(#phrase("quantum blockchain"))))
                    );
                };

                case ("search(): #word + .And() category filter") {
                    ignore col_obj.search(
                        qb()
                            .Where("body",     #text(#word("software")))
                            .And("category",   #eq(#Text("tech")))
                    );
                };

                case (_) {};
            };
        });

        bench;
    };
};
