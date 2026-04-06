// @testmode wasi
//
// Text Index Tests
// ================
// A collection has at most ONE text index.  That index covers up to 255 fields.
// Call createTextIndex once, passing an array of field names:
//
//   collection.createTextIndex("people_text", ["name", "description"]) // uses the #basic tokenizer internally
//
// Internally each entry is keyed as:
//
//   (word, field_idx, doc_id, token_pos, char_start, char_end)
//
// where field_idx is the 0-based index of the field in the fields array, stored
// as a single byte.  This lets the engine filter matches by field cheaply.
//
// The `#basic` tokenizer lowercases the raw text and splits on whitespace and
// common punctuation, so "Alice Johnson" yields ["alice", "johnson"].
//
// ===========================================================================
// QUERY OPERATORS FOR TEXT INDEXES
// ===========================================================================
//
// Text queries use the field name as the key and #text(...) as the operator:
//
//   Where("name",        #text( <TextOperator> ))
//   Where("description", #text( <TextOperator> ))
//
// TextOperators variants:
//
//  — #word(text) —
//   Exact single-token match across all indexed fields.  e.g. #word("chen")
//
//  — #phrase(text) —
//   Consecutive token sequence in order.  e.g. #phrase("data scientist")
//   Tokens must appear adjacent and in the same order within the same field.
//
//  — #startsWith(text) —
//   Any indexed token begins with the prefix.  #startsWith("dan") matches docs
//   that contain "daniel", "danny", etc.
//
//  — #anyOf([text]) —
//   At least one of the listed words is present anywhere in the indexed fields
//   (OR semantics).
//
//  — #allOf([text]) —
//   All listed words are present anywhere in the indexed fields, in any order
//   (AND semantics).
//
//  — Positional operators (future) —
//   #wordAt(text, pos)         Token appears at a specific 0-based position.
//   #near(text, text, maxDist) Two words within maxDist tokens of each other.
//
//  — Relevance / fuzzy (future) —
//   #fuzzy(text, maxEdits)     Edit-distance match.
//   #ranked([text])            Results sorted by TF-IDF score.
//
// All text operators are case-insensitive.
//
// ===========================================================================
// RETURN TYPE
// ===========================================================================
//
// search() returns:
//
//   { documents : [(DocumentId, Record, matches : [TextMatch])];
//     instructions; pagination_token; has_more }
//
// TextMatch = { field : Text; word : Text; token_pos : Nat; char_start : Nat; char_end : Nat }
//
// For results that matched only via non-text filters, matches = [].
//
// ===========================================================================
// CONSTRAINTS
// ===========================================================================
//
//   • Only one text index per collection is allowed.  A second call to
//     createTextIndex on the same collection returns #err.
//   • Fields array must be non-empty and have at most 255 entries.
//   • Querying with the text index name of a non-existent index returns #err.
//
// ===========================================================================
// COMPOSITION WITH OTHER QUERY OPERATORS
// ===========================================================================
//
//   .Where("name",   #text(...)).And("is_active", #eq(#Bool(true)))
//   .Where("name",   #text(...)).And("description", #text(...))        // name AND description → AND
//   .Where("name",   #text(...)).Or("description",  #text(...))         // OR
//   .Where("is_active", #eq(#Bool(true))).And("name", #not_(#text(#word("foo"))))
//
// ===========================================================================

import Array "mo:core@2.4/Array";

import ZenDB "../../src/EmbeddedInstance";

import { test } "mo:test";
import ZenDBSuite "../test-utils/TestFramework";

type Person = {
    name : Text;
    description : Text;
    is_active : Bool;
};

// Convenience alias so assertions are less noisy
type TextMatch = ZenDB.Types.TextMatch;

let PersonSchema : ZenDB.Types.Schema = #Record([
    ("name", #Text),
    ("description", #Text),
    ("is_active", #Bool),
]);

let candify_person : ZenDB.Types.Candify<Person> = {
    from_blob = func(blob : Blob) : ?Person { from_candid(blob) };
    to_blob = func(p : Person) : Blob { to_candid(p) };
};

ZenDBSuite.newSuite(
    "Text Index Tests",
    ?{ ZenDBSuite.onlyWithIndex with log_level = #Error },
    func suite_setup(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {

        let #ok(people) = zendb.createCollection<Person>("people", PersonSchema, candify_person, null) else return assert false;

        // One text index covering both fields.
        // Field positions: "name" → byte 0, "description" → byte 1.
        suite_utils.indexOnlyFns(func() {
            let #ok(_) = people.createTextIndex("people_text", ["name", "description"]) else return assert false;
        });

        // -----------------------------------------------------------------
        // Dataset
        // -----------------------------------------------------------------
        // name tokens (basic, lowercased, field byte 0):
        //   alice johnson | daniel carter | charlotte williams | daniel martinez
        //   eve martinez  | chen thompson | isabella chen
        //
        // key words in descriptions (basic, lowercased, field byte 1):
        //   alice:       software engineer, distributed systems, blockchain
        //   daniel c:    hiking, outdoor, adventures, rocky mountains
        //   charlotte:   data scientist, machine learning, predictive analytics
        //   daniel m:    painting, abstract art, creative workshops, community
        //   eve:         cybersecurity, network security, threat detection
        //   chen t:      software engineer, mobile applications, interface design
        //   isabella:    data scientist, natural language processing, artificial intelligence
        //
        // is_active flags:
        //   true:  alice, charlotte, eve, chen
        //   false: daniel_c, daniel_m, isabella
        // -----------------------------------------------------------------

        var alice_id    : ZenDB.Types.DocumentId = "";
        var daniel_c_id : ZenDB.Types.DocumentId = "";
        var charlotte_id : ZenDB.Types.DocumentId = "";
        var daniel_m_id : ZenDB.Types.DocumentId = "";
        var eve_id      : ZenDB.Types.DocumentId = "";
        var chen_id     : ZenDB.Types.DocumentId = "";
        var isabella_id : ZenDB.Types.DocumentId = "";

        test("insert documents", func() {
            let #ok(id) = people.insert({
                name = "Alice Johnson";
                description = "Alice is a software engineer who specializes in distributed systems and blockchain technology.";
                is_active = true;
            }) else return assert false;
            alice_id := id;

            let #ok(id2) = people.insert({
                name = "Daniel Carter";
                description = "Daniel loves hiking and outdoor adventures in the Rocky Mountains and Pacific Northwest.";
                is_active = false;
            }) else return assert false;
            daniel_c_id := id2;

            let #ok(id3) = people.insert({
                name = "Charlotte Williams";
                description = "Charlotte is a data scientist working with machine learning algorithms and predictive analytics.";
                is_active = true;
            }) else return assert false;
            charlotte_id := id3;

            let #ok(id4) = people.insert({
                name = "Daniel Martinez";
                description = "Daniel enjoys painting abstract art and teaching creative workshops at local community centers.";
                is_active = false;
            }) else return assert false;
            daniel_m_id := id4;

            let #ok(id5) = people.insert({
                name = "Eve Martinez";
                description = "Eve is a cybersecurity expert specializing in network security and threat detection systems.";
                is_active = true;
            }) else return assert false;
            eve_id := id5;

            let #ok(id6) = people.insert({
                name = "Chen Thompson";
                description = "Chen is a software engineer working on mobile applications and user interface design.";
                is_active = true;
            }) else return assert false;
            chen_id := id6;

            let #ok(id7) = people.insert({
                name = "Isabella Chen";
                description = "Isabella is a data scientist focusing on natural language processing and artificial intelligence.";
                is_active = false;
            }) else return assert false;
            isabella_id := id7;

            assert people.size() == 7;
        });

        // =================================================================
        // Constraints
        // =================================================================

        test("only one text index allowed per collection", func() {
            // A second call to createTextIndex on the same collection must fail,
            // even when requesting the same fields covered by the existing index.
            let res = people.createTextIndex("other_text", ["name", "description"]);
            assert res == #err("A text index already exists on this collection");
        });

        // =================================================================
        // #word — exact token match (searches all indexed fields)
        // =================================================================
        // Finds all documents where the queried field contains this exact token.
        // The query term is lowercased automatically before matching.
        // Results carry at least one TextMatch entry.

        test("#word - single unique last name", func() {
            // "carter" only in name of "Daniel Carter"
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#word("carter")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id];

            // match captures the token location
            let (_, _, matches) = res.documents[0];
            assert matches[0].field == "name";
            assert matches[0].word == "carter";
        });

        test("#word - shared first name returns both docs", func() {
            // "daniel" in both "Daniel Carter" and "Daniel Martinez"
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#word("daniel")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id, daniel_m_id];
        });

        test("#word - case insensitive (query uppercased, stored lowercased)", func() {
            // "Chen" uppercased; index stores "chen".  Should match both docs.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#word("Chen")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [chen_id, isabella_id];
        });

        test("#word - finds token that only appears in description field", func() {
            // "hiking" is in Daniel Carter's description, not his name.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#word("hiking")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id];

            let (_, _, matches) = res.documents[0];
            assert matches[0].field == "description";
            assert matches[0].word == "hiking";
        });

        test("#word - description-only token matches multiple docs", func() {
            // "scientist" appears in Charlotte's and Isabella's descriptions,
            // not in any name field.  Both should be returned via description matches.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#word("scientist")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [charlotte_id, isabella_id];

            // Every match should come from the description field
            for ((_, _, matches) in res.documents.vals()) {
                for (m in matches.vals()) {
                    assert m.field == "description";
                };
            };
        });

        test("#word - same token hits name field in one doc and description in another", func() {
            // "chen" appears in:
            //   chen_id     → name (token 0: "Chen Thompson") AND description (token 0: "Chen is a …")
            //   isabella_id → name (token 1: "Isabella Chen")
            // Searching name OR description returns both docs; chen_id has matches
            // from both fields while isabella_id has only a name match.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("chen")))
                    .Or("description", #text(#word("chen")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [chen_id, isabella_id];

            // chen_id: matches from both name and description fields
            let (_, _, chen_matches) = res.documents[0];
            let chen_fields = Array.map(chen_matches, func(m : TextMatch) : Text { m.field });
            assert Array.find(chen_fields, func(f : Text) : Bool { f == "name" }) != null;
            assert Array.find(chen_fields, func(f : Text) : Bool { f == "description" }) != null;

            // isabella_id: match only in name
            let (_, _, isabella_matches) = res.documents[1];
            assert isabella_matches[0].field == "name";
        });

        test("#word - same token in both name and description returns matches from both fields", func() {
            // "alice" appears in Alice's name AND her description ("Alice is a …").
            // Searching both fields via OR returns one doc with matches from both fields.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("alice")))
                    .Or("description", #text(#word("alice")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id];

            // both name and description should have a match
            let (_, _, matches) = res.documents[0];
            let fields = Array.map(matches, func(m : TextMatch) : Text { m.field });
            assert Array.find(fields, func(f : Text) : Bool { f == "name" }) != null;
            assert Array.find(fields, func(f : Text) : Bool { f == "description" }) != null;
        });

        test("#word - no match returns empty documents", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#word("nobody")))
            ) else return assert false;

            assert res.documents == [];
        });

        test("non-text query returns empty matches", func() {
            // A plain equality filter on is_active never touches the text index.
            // Every result should have an empty matches list.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("is_active", #eq(#Bool(true)))
            ) else return assert false;

            for ((_, _, matches) in res.documents.vals()) {
                assert matches == [];
            };
        });

        // =================================================================
        // Composition with QueryBuilder operators
        // =================================================================

        test("combining #text with .And() non-text filter", func() {
            // "daniel" in name AND is_active = false → both Daniels (both inactive)
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("daniel")))
                    .And("is_active", #eq(#Bool(false)))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id, daniel_m_id];
        });

        test("combining two #text conditions with .And()", func() {
            // "daniel" in name AND "hiking" in description → only Daniel Carter
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("daniel")))
                    .And("description", #text(#word("hiking")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id];

            // Matches from both the name field (daniel) and description field (hiking)
            let (_, _, matches) = res.documents[0];
            let fields = Array.map(matches, func(m : TextMatch) : Text { m.field });
            assert Array.find(fields, func(f : Text) : Bool { f == "name" }) != null;
            assert Array.find(fields, func(f : Text) : Bool { f == "description" }) != null;
        });

        test("name-word AND description-word narrows to a single doc with cross-field matches", func() {
            // "charlotte" only exists in Charlotte's name field (byte 0).
            // "learning" only exists in Charlotte's description field (byte 1).
            // Requiring both words must match only Charlotte Williams, and the
            // resulting matches should contain entries from both fields.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("charlotte")))
                    .And("description", #text(#word("learning")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [charlotte_id];

            // match list contains one entry from the name field and one from description
            let (_, _, matches) = res.documents[0];
            let fields = Array.map(matches, func(m : TextMatch) : Text { m.field });
            assert Array.find(fields, func(f : Text) : Bool { f == "name" }) != null;
            assert Array.find(fields, func(f : Text) : Bool { f == "description" }) != null;
        });

        test("name-word AND description-word with no common doc returns empty", func() {
            // "eve" is only in Eve Martinez's name.
            // "hiking" is only in Daniel Carter's description.
            // No single document contains both → empty result set.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("eve")))
                    .And("description", #text(#word("hiking")))
            ) else return assert false;

            assert res.documents == [];
        });

        test("combining two #text conditions with .Or()", func() {
            // "alice" in name OR "cybersecurity" in description
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("alice")))
                    .Or("description", #text(#word("cybersecurity")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, eve_id];
        });

        // =================================================================
        // #not_(#text(#word(...))) — negation of a text token match
        // =================================================================
        // Excludes documents where any indexed field contains the given word.

        test("#not_(#text(#word)) combined with non-text .And() filter", func() {
            // Active people: Alice, Charlotte, Eve, Chen
            // Exclude docs where any field contains "johnson" → removes Alice
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("is_active", #eq(#Bool(true)))
                    .And("name", #not_(#text(#word("johnson"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [charlotte_id, eve_id, chen_id];
            // negation-only results carry empty matches
            for ((_, _, matches) in res.documents.vals()) {
                assert matches == [];
            };
        });

        test("#not_(#text(#word)) excludes all docs that contain the word", func() {
            // "martinez" is in names of Daniel M and Eve; negation removes both.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#word("martinez"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, daniel_c_id, charlotte_id, chen_id, isabella_id];
        });

        test("#not_(#text(#word)) word only in description is not excluded from name-NOT", func() {
            // "software" appears only in descriptions of Alice and Chen (field 1),
            // not in any name (field 0). NOT("software") on the name field must return
            // ALL seven docs because no document has "software" as a name token.
            // This also exercises the around-interval logic for a word whose positive
            // bitmap is empty: interval_before and interval_after still collect all docs.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#word("software"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, daniel_c_id, charlotte_id, daniel_m_id, eve_id, chen_id, isabella_id];
        });

        test("#not_(#text(#word)) combined with positive #text condition", func() {
            // "daniel" AND NOT "carter" → keeps Daniel Martinez only
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("daniel")))
                    .And("name", #not_(#text(#word("carter"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_m_id];
        });

        test("#not_(#text(#word)) inactive docs excluding description word", func() {
            // Inactive: Daniel Carter, Daniel Martinez, Isabella.
            // "hiking" only in Daniel Carter's description → remove him.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("is_active", #eq(#Bool(false)))
                    .And("description", #not_(#text(#word("hiking"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_m_id, isabella_id];
        });

        test("two #not_(#text(#word)) conditions with .And()", func() {
            // NOT "johnson" AND NOT "martinez"
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #not_(#text(#word("johnson"))))
                    .And("name", #not_(#text(#word("martinez"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id, charlotte_id, chen_id, isabella_id];
        });

        test("#not_(#text(#word)) doc with word in both name and description is still excluded", func() {
            // "alice" appears in Alice's name (field 0) AND in her description (field 1).
            // NOT("alice") on the name field must still exclude Alice: the around-interval
            // captures her description entry (field 1 > field 0), placing her in around_bitmap,
            // but differenceInPlace then removes her correctly.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#word("alice"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id, charlotte_id, daniel_m_id, eve_id, chen_id, isabella_id];
        });

        // =================================================================
        // #startsWith — token prefix match
        // =================================================================
        // Finds all documents where the queried field contains a token beginning
        // with the given prefix.  Implemented as a range scan over the inverted
        // index between [prefix, prefix∞).

        test("#startsWith - prefix 'dan' matches both Daniels", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#startsWith("dan")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id, daniel_m_id];
        });

        test("#startsWith - prefix 'ch' matches Charlotte, Chen, Isabella (has token 'chen')", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#startsWith("ch")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [charlotte_id, chen_id, isabella_id];
        });

        test("#startsWith - prefix 'mart' matches the two Martinezes", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#startsWith("mart")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_m_id, eve_id];
        });

        test("#startsWith - full-word prefix is equivalent to #word", func() {
            let #ok(res_word) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#word("johnson")))
            ) else return assert false;

            let #ok(res_starts) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#startsWith("johnson")))
            ) else return assert false;

            let ids_word = Array.map(res_word.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            let ids_starts = Array.map(res_starts.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids_word == ids_starts;
        });

        test("#startsWith - prefix in description field matches multiple docs", func() {
            // "soft" is a prefix for "software" which appears in Alice's and Chen's descriptions.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#startsWith("soft")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, chen_id];
        });

        test("#not_(#text(#startsWith)) excludes docs whose name-token starts with prefix", func() {
            // #startsWith("dan") matches daniel_c and daniel_m; NOT excludes them.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#startsWith("dan"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, charlotte_id, eve_id, chen_id, isabella_id];
        });

        test("#not_(#text(#startsWith)) works on description field — excludes 'software' prefix matches", func() {
            // "soft" is a prefix of "software"; Alice and Chen both have "software" in their descriptions.
            // NOT(startsWith("soft")) in description keeps everyone else.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #not_(#text(#startsWith("soft"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id, charlotte_id, daniel_m_id, eve_id, isabella_id];
        });

        test("#not_(#text(#startsWith)) when prefix matches no token — all docs returned", func() {
            // "xyz" is not a prefix of any token in the text index.
            // The positive match set is empty, so NOT returns every document.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#startsWith("xyz"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, daniel_c_id, charlotte_id, daniel_m_id, eve_id, chen_id, isabella_id];
        });

        // =================================================================
        // #phrase — consecutive-word sequence (exact phrase)
        // =================================================================
        // Tokens must be adjacent and in order within the same field.

        test("#phrase - 'data scientist' matches Charlotte and Isabella (description)", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#phrase("data scientist")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [charlotte_id, isabella_id];

            // Matches should show the phrase tokens from the description field
            for ((_, _, matches) in res.documents.vals()) {
                assert Array.find(matches, func(m : TextMatch) : Bool { m.field == "description" }) != null;
            };
        });

        test("#phrase - 'software engineer' matches Alice and Chen", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#phrase("software engineer")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, chen_id];
        });

        test("#phrase - 'machine learning' matches Charlotte only", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#phrase("machine learning")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [charlotte_id];
        });

        test("#phrase - reversed word order does NOT match", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#phrase("scientist data")))
            ) else return assert false;

            assert res.documents == [];
        });

        test("#phrase - words with gap between them do NOT match", func() {
            // "data algorithms" are not adjacent in any description
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#phrase("data algorithms")))
            ) else return assert false;

            assert res.documents == [];
        });

        test("#phrase combined with .And() on a different field", func() {
            // 'data scientist' in description AND is_active = true → Charlotte only (Isabella is inactive)
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("description", #text(#phrase("data scientist")))
                    .And("is_active", #eq(#Bool(true)))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [charlotte_id];
        });

        test("#not_(#text(#phrase)) excludes docs that contain the consecutive phrase", func() {
            // "data scientist" appears in Charlotte and Isabella's descriptions; NOT excludes them.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #not_(#text(#phrase("data scientist"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, daniel_c_id, daniel_m_id, eve_id, chen_id];
        });

        test("#not_(#text(#phrase)) reversed phrase — docs whose words appear in wrong order are included", func() {
            // Charlotte and Isabella have "data" and "scientist" adjacent in their descriptions,
            // but ONLY in the forward order "data scientist".  The reversed phrase "scientist data"
            // never appears adjacent in any document.
            //
            // NOT(phrase("scientist data")) must therefore return ALL 7 docs.
            // By contrast, NOT(phrase("data scientist")) returns only 5, excluding charlotte and
            // isabella.  This comparison verifies that the wrong-order correction is working: those
            // two docs are excluded ONLY when the phrase genuinely matches, not merely because both
            // constituent words are present somewhere in the description.
            let #ok(fwd) = people.search(
                ZenDB.QueryBuilder().Where("description", #not_(#text(#phrase("data scientist"))))
            ) else return assert false;
            let #ok(rev) = people.search(
                ZenDB.QueryBuilder().Where("description", #not_(#text(#phrase("scientist data"))))
            ) else return assert false;

            let fwd_ids = Array.map(fwd.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            let rev_ids = Array.map(rev.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });

            // Forward phrase matches charlotte + isabella → they are excluded from the NOT result.
            assert fwd_ids.size() == 5;
            assert Array.find(fwd_ids, func(id : ZenDB.Types.DocumentId) : Bool { id == charlotte_id }) == null;
            assert Array.find(fwd_ids, func(id : ZenDB.Types.DocumentId) : Bool { id == isabella_id }) == null;

            // Reversed phrase matches nobody → all 7 are included (wrong-order docs are not
            // false-positively excluded just because both words exist in the index).
            assert rev_ids.size() == 7;
            assert Array.find(rev_ids, func(id : ZenDB.Types.DocumentId) : Bool { id == charlotte_id }) != null;
            assert Array.find(rev_ids, func(id : ZenDB.Types.DocumentId) : Bool { id == isabella_id }) != null;
        });

        test("#not_(#text(#phrase)) phrase that never appears — all docs returned", func() {
            // "quantum computing" appears in no document's description.
            // The positive phrase bitmap is empty, so NOT returns every document.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #not_(#text(#phrase("quantum computing"))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, daniel_c_id, charlotte_id, daniel_m_id, eve_id, chen_id, isabella_id];
        });

        // =================================================================
        // #anyOf — union of words (OR semantics) within a single field
        // =================================================================
        // Finds documents whose field contains at least one of the listed words.
        // For cross-field OR, chain multiple .Where/.Or calls instead.

        test("#anyOf - 'carter' OR 'martinez' in name matches three people", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#anyOf(["carter", "martinez"])))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id, daniel_m_id, eve_id];
        });

        test("#anyOf - 'hiking' OR 'painting' in description", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #text(#anyOf(["hiking", "painting"])))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id, daniel_m_id];
        });

        test("#anyOf - single element list is equivalent to #word", func() {
            let #ok(res_word) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#word("alice")))
            ) else return assert false;

            let #ok(res_any) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#anyOf(["alice"])))
            ) else return assert false;

            let ids_word = Array.map(res_word.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            let ids_any = Array.map(res_any.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids_word == ids_any;
        });

        test("#anyOf - matches from each word are reflected individually in the matches list", func() {
            // "daniel" → Daniel Carter, Daniel Martinez.
            // "carter" → Daniel Carter only.
            // Daniel Carter has both → two match entries; Daniel Martinez has one.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#anyOf(["daniel", "carter"])))
            ) else return assert false;

            let ?dc = Array.find(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : Bool { id == daniel_c_id }) else return assert false;
            let (_, _, dc_matches) = dc;
            assert dc_matches.size() == 2;   // "daniel" hit + "carter" hit
            let ?dc_daniel = Array.find(dc_matches, func(m : TextMatch) : Bool { m.word == "daniel" }) else return assert false;
            assert dc_daniel.field == "name";
            assert dc_daniel.token_pos == 0;
            let ?dc_carter = Array.find(dc_matches, func(m : TextMatch) : Bool { m.word == "carter" }) else return assert false;
            assert dc_carter.field == "name";
            assert dc_carter.token_pos == 1;

            let ?dm = Array.find(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : Bool { id == daniel_m_id }) else return assert false;
            let (_, _, dm_matches) = dm;
            assert dm_matches.size() == 1;   // "daniel" hit only
            assert dm_matches[0].word == "daniel";
            assert dm_matches[0].field == "name";
            assert dm_matches[0].token_pos == 0;
        });

        test("#not_(#text(#anyOf)) excludes docs that contain any of the listed words", func() {
            // "hiking" OR "painting" appears in daniel_c and daniel_m; NOT excludes both.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #not_(#text(#anyOf(["hiking", "painting"]))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, charlotte_id, eve_id, chen_id, isabella_id];
        });

        test("#not_(#text(#anyOf)) De Morgan: doc must lack ALL listed words (NOT A AND NOT B)", func() {
            // NOT(anyOf(["carter","martinez"])) ≡ NOT("carter") AND NOT("martinez").
            // "carter"   appears in: daniel_c
            // "martinez" appears in: daniel_m, eve
            // Excluded (has AT LEAST ONE word): daniel_c, daniel_m, eve
            // Kept (lacks BOTH words):          alice, charlotte, chen, isabella
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#anyOf(["carter", "martinez"]))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, charlotte_id, chen_id, isabella_id];
        });

        test("#not_(#text(#anyOf)) word absent from all docs — all docs returned", func() {
            // "quantum" does not appear in any document's description.
            // anyOf(["quantum"]) matches nobody, so NOT includes every document.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("description", #not_(#text(#anyOf(["quantum"]))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, daniel_c_id, charlotte_id, daniel_m_id, eve_id, chen_id, isabella_id];
        });

        // =================================================================
        // #allOf — intersection of words (AND semantics) within a single field
        // =================================================================
        // All listed words must appear in the queried field.
        // For cross-field AND requirements, chain .And() calls across field keys.

        test("#allOf - 'daniel' AND 'carter' in name matches only Daniel Carter", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#allOf(["daniel", "carter"])))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_c_id];

            let (_, _, matches) = res.documents[0];
            assert matches.size() == 2;
            let ?m_daniel = Array.find(matches, func(m : TextMatch) : Bool { m.word == "daniel" }) else return assert false;
            assert m_daniel.field == "name";
            assert m_daniel.token_pos == 0;
            let ?m_carter = Array.find(matches, func(m : TextMatch) : Bool { m.word == "carter" }) else return assert false;
            assert m_carter.field == "name";
            assert m_carter.token_pos == 1;
        });

        test("#allOf - 'daniel' AND 'martinez' in name matches only Daniel Martinez", func() {
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#allOf(["daniel", "martinez"])))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [daniel_m_id];
        });

        test("#allOf cross-field: use .And() to require a word in name AND a word in description", func() {
            // "alice" in name AND "blockchain" in description → only Alice Johnson.
            // Cross-field allOf is expressed via chained .And() with different field keys.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder()
                    .Where("name", #text(#word("alice")))
                    .And("description", #text(#word("blockchain")))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id];

            // matches contain entries from both fields
            let (_, _, matches) = res.documents[0];
            let fields = Array.map(matches, func(m : TextMatch) : Text { m.field });
            assert Array.find(fields, func(f : Text) : Bool { f == "name" }) != null;
            assert Array.find(fields, func(f : Text) : Bool { f == "description" }) != null;
        });

        test("#allOf - requires all words to be present in the field; no match returns empty", func() {
            // No person's name contains both "alice" and "chen"
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#allOf(["alice", "chen"])))
            ) else return assert false;

            assert res.documents == [];
        });

        test("#allOf - single element list is equivalent to #word", func() {
            let #ok(res_word) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#word("johnson")))
            ) else return assert false;

            let #ok(res_all) = people.search(
                ZenDB.QueryBuilder().Where("name", #text(#allOf(["johnson"])))
            ) else return assert false;

            let ids_word = Array.map(res_word.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            let ids_all = Array.map(res_all.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids_word == ids_all;
        });

        test("#not_(#text(#allOf)) excludes docs that contain ALL of the listed words", func() {
            // Only daniel_c has both "daniel" AND "carter" in name; NOT excludes him.
            // daniel_m has "daniel" but NOT "carter", so he is kept.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#allOf(["daniel", "carter"]))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, charlotte_id, daniel_m_id, eve_id, chen_id, isabella_id];
        });

        test("#not_(#text(#allOf)) De Morgan: doc with SOME but not ALL listed words is INCLUDED", func() {
            // NOT(allOf(["daniel","martinez"])) ≡ NOT("daniel") OR NOT("martinez").
            // daniel_m has BOTH "daniel" AND "martinez" → excluded.
            // daniel_c has "daniel" but NOT "martinez" → he is KEPT (he satisfies NOT("martinez")).
            // All others lack both words → also kept.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#allOf(["daniel", "martinez"]))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, daniel_c_id, charlotte_id, eve_id, chen_id, isabella_id];
        });

        test("#not_(#text(#allOf)) word combination that never co-occurs — all docs returned", func() {
            // No person's name contains BOTH "alice" AND "chen" simultaneously.
            // allOf(["alice","chen"]) matches nobody, so NOT returns every document.
            let #ok(res) = people.search(
                ZenDB.QueryBuilder().Where("name", #not_(#text(#allOf(["alice", "chen"]))))
            ) else return assert false;

            let ids = Array.map(res.documents, func((id, _, _) : (ZenDB.Types.DocumentId, Person, [TextMatch])) : ZenDB.Types.DocumentId { id });
            assert ids == [alice_id, daniel_c_id, charlotte_id, daniel_m_id, eve_id, chen_id, isabella_id];
        });

        // =================================================================
        // Future operator sketches (positional / proximity / fuzzy)
        // =================================================================
        //
        // All use the field name as the key.
        //
        // -- #wordAt : word at a specific 0-based token position --
        //
        //   // First token (position 0) of name is "daniel"
        //   people.search(
        //       ZenDB.QueryBuilder().Where("name", #text(#wordAt("daniel", 0)))
        //   )
        //   // → [Daniel Carter, Daniel Martinez]
        //
        //   // Second token (position 1) of name is "chen"  → Isabella Chen, not Chen Thompson
        //   people.search(
        //       ZenDB.QueryBuilder().Where("name", #text(#wordAt("chen", 1)))
        //   )
        //   // → [Isabella Chen]
        //
        // -- #near : two words within N tokens of each other in the same field --
        //
        //   people.search(
        //       ZenDB.QueryBuilder().Where("description", #text(#near("machine", "learning", 1)))
        //   )
        //   // → [Charlotte Williams]
        //
        // -- #fuzzy : typo-tolerant match --
        //
        //   people.search(
        //       ZenDB.QueryBuilder().Where("name", #text(#fuzzy("alce", 1)))
        //   )
        //   // → [Alice Johnson]
        //
        // -- #ranked : return results sorted by token-frequency / TF-IDF score --
        //
        //   people.search(
        //       ZenDB.QueryBuilder().Where("description", #text(#ranked(["data", "scientist"])))
        //   )
        //   // → [Isabella Chen, Charlotte Williams]  (ranked by relevance)
        //

    },
);
