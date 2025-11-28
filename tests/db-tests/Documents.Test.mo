// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Blob "mo:base@0.16.0/Blob";
import Text "mo:base@0.16.0/Text";
import Array "mo:base@0.16.0/Array";
import Principal "mo:base@0.16.0/Principal";
import Option "mo:base@0.16.0/Option";

import ZenDB "../../src/EmbeddedInstance";

import { test; suite } "mo:test";
import Itertools "mo:itertools@0.2.2/Iter";
import Map "mo:map@9.0.1/Map";
import ZenDBSuite "../test-utils/TestFramework";

ZenDBSuite.newSuite(
    "Candid Documents Test",
    ?ZenDBSuite.onlyWithIndex, // unique schema constraints are not supported without index
    func suite_setup(zendb : ZenDB.Database, suite_utils : ZenDBSuite.SuiteUtils) {

        suite(
            "Types to not support:",
            func() {

                test(
                    "Top level Null types",
                    func() {
                        let #err(_) = zendb.createCollection<Null>(
                            "strictly_null",
                            #Null,
                            {
                                from_blob = func(blob : Blob) : ?Null = from_candid (blob);
                                to_blob = func(c : Null) : Blob = to_candid (c);
                            },
                            null,
                        );
                    },
                );

                test(
                    "Top level #Array types",
                    func() {

                        type ArrayType = [Nat];
                        let ArraySchema = #Array(#Nat);
                        let candify : ZenDB.Types.Candify<ArrayType> = {
                            from_blob = func(blob : Blob) : ?ArrayType = from_candid (blob);
                            to_blob = func(c : ArrayType) : Blob = to_candid (c);
                        };

                        let #err(_) = zendb.createCollection<ArrayType>("arrays_0", ArraySchema, candify, null) else return assert false;
                    },
                );

                // test(
                //     "Top level #Empty types",
                //     func() {

                //         // Empty type should be rejected as top-level type (similar to Null)
                //         let #err(_) = zendb.createCollection<Any>(
                //             "empty_type",
                //             #Empty,
                //             {
                //                 from_blob = func(blob : Blob) : ?Any = from_candid (blob);
                //                 to_blob = func(c : Any) : Blob = to_candid (c);
                //             },
                //             null,
                //         );
                //     },
                // );

            },
        );

        suite(
            "Should Support all the other Valid Candid Schema",
            func() {

                test(
                    "Bool Type",
                    func() {
                        let #ok(bools) = zendb.createCollection<Bool>(
                            "bools",
                            #Bool,
                            {
                                from_blob = func(blob : Blob) : ?Bool = from_candid (blob);
                                to_blob = func(c : Bool) : Blob = to_candid (c);
                            },
                            null,
                        ) else return assert false;

                        let #ok(id_true) = bools.insert(true) else return assert false;
                        let #ok(id_false) = bools.insert(false) else return assert false;

                        assert bools.size() == 2;

                        let result1 = bools.search(ZenDB.QueryBuilder().Where("", #eq(#Bool(true))));
                        let #ok(search_result1) = result1 else return assert false;
                        assert search_result1.documents == [(id_true, true)];

                        let result2 = bools.search(ZenDB.QueryBuilder().Where("", #eq(#Bool(false))));
                        let #ok(search_result2) = result2 else return assert false;
                        assert search_result2.documents == [(id_false, false)];
                    },
                );

                test(
                    "Int",
                    func() {
                        let #ok(ints) = zendb.createCollection<Int>(
                            "ints",
                            #Int,
                            {
                                from_blob = func(blob : Blob) : ?Int = from_candid (blob);
                                to_blob = func(c : Int) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("", [#Max(-1)]), #Unique([""])];
                            },
                        ) else return assert false;

                        let #ok(id) = ints.insert(-42) else return assert false;
                        assert ints.size() == 1;

                        let result = ints.search(ZenDB.QueryBuilder().Where("", #eq(#Int(-42))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, -42)];

                        assert ints.get(id) == ?(-42);
                    },
                );

                test(
                    "Principal Type",
                    func() {

                        let testPrincipal = Principal.fromText("2vxsx-fae");

                        let #ok(principals) = zendb.createCollection<Principal>(
                            "principals",
                            #Principal,
                            {
                                from_blob = func(blob : Blob) : ?Principal = from_candid (blob);
                                to_blob = func(c : Principal) : Blob = to_candid (c);
                            },
                            null,
                        ) else return assert false;

                        let #ok(id) = principals.insert(testPrincipal) else return assert false;
                        assert principals.size() == 1;

                        let result = principals.search(ZenDB.QueryBuilder().Where("", #eq(#Principal(testPrincipal))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, testPrincipal)];

                        assert principals.get(id) == ?(testPrincipal);
                    },
                );

                test(
                    "Nat",
                    func() {
                        let #ok(nats) = zendb.createCollection<Nat>(
                            "nats",
                            #Nat,
                            {
                                from_blob = func(blob : Blob) : ?Nat = from_candid (blob);
                                to_blob = func(c : Nat) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("", [#Min(1)]), #Unique([""])];
                            },
                        ) else return assert false;

                        let #ok(id) = nats.insert(42) else return assert false;
                        assert nats.size() == 1;

                        let result = nats.search(ZenDB.QueryBuilder().Where("", #eq(#Nat(42))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, 42)];

                        assert nats.get(id) == ?(42);

                    },
                );

                test(
                    "Float",
                    func() {
                        let #ok(floats) = zendb.createCollection<Float>(
                            "floats",
                            #Float,
                            {
                                from_blob = func(blob : Blob) : ?Float = from_candid (blob);
                                to_blob = func(c : Float) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("", [#Min(1)]), #Unique([""])];
                            },
                        ) else return assert false;

                        let #ok(id) = floats.insert(42.0) else return assert false;
                        assert floats.size() == 1;

                        let result = floats.search(ZenDB.QueryBuilder().Where("", #eq(#Float(42.0))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, 42.0)];

                        assert floats.get(id) == ?(42.0);

                    }

                );

                test(
                    "Text",
                    func() {
                        let #ok(texts) = zendb.createCollection<Text>(
                            "texts",
                            #Text,
                            {
                                from_blob = func(blob : Blob) : ?Text = from_candid (blob);
                                to_blob = func(c : Text) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("", [#MinSize(1)]), #Unique([""])];
                            },
                        ) else return assert false;

                        let #ok(id) = texts.insert("hello") else return assert false;
                        assert texts.size() == 1;

                        let result = texts.search(ZenDB.QueryBuilder().Where("", #eq(#Text("hello"))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, "hello")];

                        assert texts.get(id) == ?("hello");

                    },
                );

                test(
                    "Blob",
                    func() {
                        let #ok(blobs) = zendb.createCollection<Blob>(
                            "blobs",
                            #Blob,
                            {
                                from_blob = func(blob : Blob) : ?Blob = from_candid (blob);
                                to_blob = func(c : Blob) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("", [#MinSize(1)]), #Unique([""])];
                            },
                        ) else return assert false;

                        let #ok(id) = blobs.insert(Blob.fromArray([0, 1, 2, 3])) else return assert false;
                        assert blobs.size() == 1;
                        let #ok(res) = blobs.search(ZenDB.QueryBuilder().Where("", #eq(#Blob(Blob.fromArray([0, 1, 2, 3]))))) else return assert false;
                        assert res.documents == [(id, Blob.fromArray([0, 1, 2, 3]))];
                        assert blobs.get(id) == ?(Blob.fromArray([0, 1, 2, 3]));

                    },
                );

                test(
                    "Option: ?Text",
                    func() {
                        let #ok(options) = zendb.createCollection<?Text>(
                            "options",
                            #Option(#Text),
                            {
                                from_blob = func(blob : Blob) : ??Text = from_candid (blob);
                                to_blob = func(c : ?Text) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("", [#MinSize(1)]), #Unique([""])];
                            },
                        ) else return assert false;

                        let #ok(id) = options.insert(?("hello")) else return assert false;
                        assert options.size() == 1;

                        let result = options.search(ZenDB.QueryBuilder().Where("", #eq(#Option(#Text("hello")))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, ?("hello"))];

                        assert options.get(id) == ?(?("hello"));

                    },
                );

                test(
                    "Nested Option: ???Nat",
                    func() {
                        type NestedOption = ???Nat;
                        let NestedOptionSchema = #Option(#Option(#Option(#Nat)));

                        let #ok(nested_options) = zendb.createCollection<NestedOption>(
                            "nested_options",
                            NestedOptionSchema,
                            {
                                from_blob = func(blob : Blob) : ?NestedOption = from_candid (blob);
                                to_blob = func(c : NestedOption) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("", [#Min(1)]), #Unique([""])];
                            },
                        ) else return assert false;

                        let #ok(id1) = nested_options.insert(???42) else return assert false;
                        let #ok(id2) = nested_options.insert(null) else return assert false;
                        let #ok(id3) = nested_options.insert(?null) else return assert false;
                        let #ok(id4) = nested_options.insert(??null) else return assert false;

                        assert nested_options.size() == 4;

                        let result1 = nested_options.search(
                            ZenDB.QueryBuilder().Where(
                                "",
                                #eq(#Option(#Option(#Option(#Nat(42))))),
                            )
                        );
                        let #ok(search_result1) = result1 else return assert false;
                        assert search_result1.documents == [(id1, ???42)];

                        let result2 = nested_options.search(
                            ZenDB.QueryBuilder().Where(
                                "",
                                #eq(#Null),
                            )
                        );
                        let #ok(search_result2) = result2 else return assert false;
                        assert search_result2.documents == [(id2, null)];

                        let result3 = nested_options.search(
                            ZenDB.QueryBuilder().Where(
                                "",
                                #eq(#Option(#Null)),
                            )
                        );
                        let #ok(search_result3) = result3 else return assert false;
                        assert search_result3.documents == [(id3, ?null)];

                        let result4 = nested_options.search(
                            ZenDB.QueryBuilder().Where(
                                "",
                                #eq(#Option(#Option(#Null))),
                            )
                        );
                        let #ok(search_result4) = result4 else return assert false;
                        assert search_result4.documents == [(id4, ??null)];

                    },
                );

                test(
                    "Record: {a : Nat, b : Text}",
                    func() {
                        type Record = {
                            a : Nat;
                            b : Text;
                        };

                        let RecordSchema = #Record([("a", #Nat), ("b", #Text)]);

                        let #ok(documents) = zendb.createCollection<Record>(
                            "documents",
                            RecordSchema,
                            {
                                from_blob = func(blob : Blob) : ?Record = from_candid (blob);
                                to_blob = func(c : Record) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("a", [#Min(1)]), #Field("b", [#MinSize(1)]), #Unique(["a"]), #Unique(["b"])];
                            },
                        ) else return assert false;

                        let #ok(id) = documents.insert({ a = 42; b = "hello" }) else return assert false;
                        assert documents.size() == 1;

                        let result1 = documents.search(ZenDB.QueryBuilder().Where("a", #eq(#Nat(42))));
                        let #ok(search_result1) = result1 else return assert false;
                        assert search_result1.documents == [(id, { a = 42; b = "hello" })];

                        let result2 = documents.search(ZenDB.QueryBuilder().Where("b", #eq(#Text("hello"))).And("a", #eq(#Nat(42))));
                        let #ok(search_result2) = result2 else return assert false;
                        assert search_result2.documents == [(id, { a = 42; b = "hello" })];

                        assert documents.get(id) == ?({ a = 42; b = "hello" });

                    },
                );

                test(
                    "Record: note {user_id: Principal; title: Text; content: Text}",
                    func() {
                        type Note = {
                            user_id : Principal;
                            title : Text;
                            content : Text;
                        };

                        let NoteSchema : ZenDB.Types.Schema = #Record([
                            ("user_id", #Principal),
                            ("title", #Text),
                            ("content", #Text),
                        ]);

                        let candify : ZenDB.Types.Candify<Note> = {
                            from_blob = func(blob : Blob) : ?Note = from_candid (blob);
                            to_blob = func(c : Note) : Blob = to_candid (c);
                        };

                        let schema_constraints : [ZenDB.Types.SchemaConstraint] = [
                            #Unique(["user_id", "title"]), // a user cannot have two notes with the same title
                            #Field("title", [#MaxSize(100)]), // title must be <= 100 characters
                            #Field("content", [#MaxSize(100_000)]), // content must be <= 100_000 characters
                        ];

                        let #ok(notes) = zendb.createCollection<Note>(
                            "notes",
                            NoteSchema,
                            candify,
                            ?{
                                schema_constraints = schema_constraints;
                            },
                        ) else return assert false;

                        let #ok(id) = notes.insert({
                            user_id = Principal.fromText("2vxsx-fae");
                            title = "hello.mo";
                            content = "This is a test note";
                        }) else return assert false;

                        let #err(_) = notes.insert({
                            user_id = Principal.fromText("2vxsx-fae");
                            title = "hello.mo";
                            content = "Replaced content";
                        }) else return assert false;

                        assert notes.size() == 1;

                        let result1 = notes.search(
                            ZenDB.QueryBuilder().Where(
                                "user_id",
                                #eq(#Principal(Principal.fromText("2vxsx-fae"))),
                            ).And(
                                "title",
                                #eq(#Text("hello.mo")),
                            )
                        );
                        let #ok(search_result1) = result1 else return assert false;
                        assert search_result1.documents == [(id, { user_id = Principal.fromText("2vxsx-fae"); title = "hello.mo"; content = "This is a test note" })];

                        let #ok(update_result) = notes.update(
                            ZenDB.QueryBuilder().Where(
                                "user_id",
                                #eq(#Principal(Principal.fromText("2vxsx-fae"))),
                            ).And(
                                "title",
                                #eq(#Text("hello.mo")),
                            ),
                            [("content", #Text("This is version 2 of the note"))],
                        );

                        assert update_result.updated_count == 1;

                        let result2 = notes.search(
                            ZenDB.QueryBuilder().Where(
                                "user_id",
                                #eq(#Principal(Principal.fromText("2vxsx-fae"))),
                            ).And(
                                "title",
                                #eq(#Text("hello.mo")),
                            )
                        );
                        let #ok(search_result2) = result2 else return assert false;
                        assert search_result2.documents == [(id, { user_id = Principal.fromText("2vxsx-fae"); title = "hello.mo"; content = "This is version 2 of the note" })];

                    },
                );

                test(
                    "Variant:  { #active; #inactive }",
                    func() {
                        type Variant = {
                            #active;
                            #inactive;
                        };

                        let VariantSchema = #Variant([("active", #Null), ("inactive", #Null)]);

                        let candify : ZenDB.Types.Candify<Variant> = {
                            from_blob = func(blob : Blob) : ?Variant = from_candid (blob);
                            to_blob = func(c : Variant) : Blob = to_candid (c);
                        };

                        // Should fail on indexes created on variant fields with #Null type
                        let #err(_) = zendb.createCollection<Variant>("variants_0", VariantSchema, candify, ?{ schema_constraints = [#Unique(["active"])] }) else return assert false;
                        let #err(_) = zendb.createCollection<Variant>("variants_0", VariantSchema, candify, ?{ schema_constraints = [#Unique(["inactive"])] }) else return assert false;
                        let #err(_) = zendb.createCollection<Variant>("variants_0", VariantSchema, candify, ?{ schema_constraints = [#Field("active", [#Min(1)])] }) else return assert false;
                        let #err(_) = zendb.createCollection<Variant>("variants_0", VariantSchema, candify, ?{ schema_constraints = [#Field("inactive", [#Min(1)])] }) else return assert false;

                        let #ok(variants) = zendb.createCollection<Variant>(
                            "variants_0",
                            VariantSchema,
                            candify,
                            null,
                        ) else return assert false;

                        let #ok(id) = variants.insert(#active) else return assert false;
                        let #ok(id2) = variants.insert(#inactive) else return assert false;

                        assert variants.size() == 2;
                        Debug.print(debug_show (variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("active"))))));
                        Debug.print(debug_show (variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("inactive"))))));
                        Debug.print(debug_show (variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("unknown"))))));

                        let r1 = variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("active"))));
                        let #ok(sr1) = r1 else return assert false;
                        assert sr1.documents == [(id, #active)];

                        let r2 = variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("inactive"))));
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id2, #inactive)];

                        let r3 = variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("unknown"))));
                        let #ok(sr3) = r3 else return assert false;
                        assert sr3.documents == [];

                        let r4 = variants.search(ZenDB.QueryBuilder().Where("active", #exists));
                        let #ok(sr4) = r4 else return assert false;
                        assert sr4.documents == [(id, #active)];

                        let r5 = variants.search(ZenDB.QueryBuilder().Where("inactive", #exists));
                        let #ok(sr5) = r5 else return assert false;
                        assert sr5.documents == [(id2, #inactive)];

                        assert variants.get(id) == ?(#active);

                    },
                );

                test(
                    "Variant:  { #name: Text; #id: Nat }",
                    func() {
                        type Variant = {
                            #name : Text;
                            #id : Nat;
                        };

                        let VariantSchema = #Variant([("name", #Text), ("id", #Nat)]);

                        let #ok(variants) = zendb.createCollection<Variant>(
                            "variants_1",
                            VariantSchema,
                            {
                                from_blob = func(blob : Blob) : ?Variant = from_candid (blob);
                                to_blob = func(c : Variant) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("name", [#MinSize(1)]), #Field("id", [#Min(1)]), #Unique(["id"]), #Unique(["name"])];
                            },
                        ) else return assert false;

                        let #ok(id) = variants.insert(#name("hello")) else return assert false;
                        let #ok(id2) = variants.insert(#id(42)) else return assert false;

                        assert variants.size() == 2;

                        let result1 = variants.search(ZenDB.QueryBuilder().Where("name", #eq(#Text("hello"))));
                        let #ok(search_result1) = result1 else return assert false;
                        assert search_result1.documents == [(id, #name("hello"))];

                        let result2 = variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Nat(42))));
                        let #ok(search_result2) = result2 else return assert false;
                        assert search_result2.documents == [(id2, #id(42))];

                        let result3 = variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("name"))));
                        let #ok(search_result3) = result3 else return assert false;
                        assert search_result3.documents == [(id, #name("hello"))];

                        let result4 = variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("id"))));
                        let #ok(search_result4) = result4 else return assert false;
                        assert search_result4.documents == [(id2, #id(42))];

                        let result5 = variants.search(ZenDB.QueryBuilder().Where("name", #exists));
                        let #ok(search_result5) = result5 else return assert false;
                        assert search_result5.documents == [(id, #name("hello"))];

                        let result6 = variants.search(ZenDB.QueryBuilder().Where("id", #exists));
                        let #ok(search_result6) = result6 else return assert false;
                        assert search_result6.documents == [(id2, #id(42))];

                        assert variants.get(id) == ?(#name("hello"));

                    },
                );

                test(
                    "Tuples: (Nat, Text)",
                    func() {
                        // Tuples are converted to documents in Candid
                        // They become documents with numbered fields, that can be accessed by their index
                        // e.g. (Nat, Text) becomes { _0_ : Nat; _1_ : Text }
                        //
                        // ZenDB provides helpers for the most common tuple types

                        type Tuple = ZenDB.Tuple<Nat, Text>;

                        let TupleSchema = ZenDB.Schema.Tuple(#Nat, #Text);

                        let candify : ZenDB.Types.Candify<Tuple> = {
                            from_blob = func(blob : Blob) : ?Tuple = from_candid (blob);
                            to_blob = func(c : Tuple) : Blob = to_candid (c);
                        };

                        let #ok(tuples) = zendb.createCollection<Tuple>(
                            "tuples",
                            TupleSchema,
                            candify,
                            ?{
                                schema_constraints = [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Unique(["0"]), #Unique(["1"])];
                            },
                        ) else return assert false;

                        let #ok(id) = tuples.insert(ZenDB.Tuple(42, "hello")) else return assert false;
                        assert tuples.size() == 1;

                        let result = tuples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, ZenDB.Tuple(42, "hello"))];

                        assert tuples.get(id) == ?(ZenDB.Tuple(42, "hello"));
                        assert tuples.get(id) == ?({ _0_ = 42; _1_ = "hello" });
                        assert switch (tuples.get(id)) {
                            case (?t) ZenDB.fromTuple(t) == (42, "hello");
                            case (_) false;
                        };

                    },
                );

                test(
                    "Triples: (Nat, Text, Nat)",
                    func() {
                        type Triple = ZenDB.Triple<Nat, Text, Nat>;

                        let TripleSchema = ZenDB.Schema.Triple(#Nat, #Text, #Nat);

                        let #ok(triples) = zendb.createCollection<Triple>(
                            "triples",
                            TripleSchema,
                            {
                                from_blob = func(blob : Blob) : ?Triple = from_candid (blob);
                                to_blob = func(c : Triple) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Field("2", [#Min(1)]), #Unique(["0"]), #Unique(["1"]), #Unique(["2"])];
                            },
                        ) else return assert false;

                        let #ok(id) = triples.insert(ZenDB.Triple(42, "hello", 100)) else return assert false;
                        assert triples.size() == 1;

                        let r1 = triples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42))));
                        let #ok(search_result) = r1 else return assert false;
                        assert search_result.documents == [(id, ZenDB.Triple(42, "hello", 100))];

                        let r2 = triples.search(ZenDB.QueryBuilder().Where("1", #eq(#Text("hello"))));
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id, ZenDB.Triple(42, "hello", 100))];

                        let r3 = triples.search(ZenDB.QueryBuilder().Where("2", #eq(#Nat(100))));
                        let #ok(sr3) = r3 else return assert false;
                        assert sr3.documents == [(id, ZenDB.Triple(42, "hello", 100))];
                        assert triples.get(id) == ?(ZenDB.Triple(42, "hello", 100));
                        assert triples.get(id) == ?({
                            _0_ = 42;
                            _1_ = "hello";
                            _2_ = 100;
                        });

                        assert switch (triples.get(id)) {
                            case (?t) ZenDB.fromTriple(t) == (42, "hello", 100);
                            case (_) false;
                        };

                    },

                );

                test(
                    "Quadruples: (Nat, Text, Nat, Blob)",
                    func() {
                        type MotokoQuadruple = (Nat, Text, Nat, Blob);
                        type Quadruple = ZenDB.Quadruple<Nat, Text, Nat, Blob>;

                        let QuadrupleSchema = #Tuple([#Nat, #Text, #Nat, #Blob]);

                        let #ok(quadruples) = zendb.createCollection<Quadruple>(
                            "quadruples",
                            QuadrupleSchema,
                            {
                                from_blob = func(blob : Blob) : ?Quadruple = from_candid (blob);
                                to_blob = func(c : Quadruple) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Field("2", [#Min(1)]), #Field("3", [#MinSize(1)]), #Unique(["0"]), #Unique(["1"]), #Unique(["2"]), #Unique(["3"])];
                            },
                        ) else return assert false;

                        let #ok(id) = quadruples.insert(ZenDB.Quadruple(42, "hello", 100, Blob.fromArray([0, 1, 2]))) else return assert false;
                        assert quadruples.size() == 1;
                        let #ok(search_result) = quadruples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42)))) else return assert false;
                        assert search_result.documents == [(id, ZenDB.Quadruple(42, "hello", 100, Blob.fromArray([0, 1, 2])))];
                        assert quadruples.get(id) == ?(ZenDB.Quadruple(42, "hello", 100, Blob.fromArray([0, 1, 2])));
                        assert quadruples.get(id) == ?({
                            _0_ = 42;
                            _1_ = "hello";
                            _2_ = 100;
                            _3_ = Blob.fromArray([0, 1, 2]);
                        });

                        assert switch (quadruples.get(id)) {
                            case (?q) ZenDB.fromQuadruple(q) == (42, "hello", 100, "\00\01\02");
                            case (_) false;
                        };

                    },

                );

                test(
                    "Nested Records: {a : {b : Nat; c : Text}; d : Nat}",
                    func() {
                        type NestedRecord = {
                            a : { b : Nat; c : Text };
                            d : Nat;
                        };

                        let NestedRecordSchema = #Record([("a", #Record([("b", #Nat), ("c", #Text)])), ("d", #Nat)]);

                        let #ok(nested_records) = zendb.createCollection<NestedRecord>(
                            "nested_records",
                            NestedRecordSchema,
                            {
                                from_blob = func(blob : Blob) : ?NestedRecord = from_candid (blob);
                                to_blob = func(c : NestedRecord) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("a.b", [#Min(1)]), #Field("a.c", [#MinSize(1)]), #Field("d", [#Min(1)]), #Unique(["a.b"]), #Unique(["a.c"]), #Unique(["d"])];
                            },
                        ) else return assert false;

                        let #ok(id) = nested_records.insert({
                            a = { b = 42; c = "hello" };
                            d = 100;
                        }) else return assert false;
                        assert nested_records.size() == 1;

                        let result = nested_records.search(ZenDB.QueryBuilder().Where("a.b", #eq(#Nat(42))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, { a = { b = 42; c = "hello" }; d = 100 })];
                        assert nested_records.get(id) == ?({
                            a = { b = 42; c = "hello" };
                            d = 100;
                        });

                    },
                );

                test(
                    "Nested Variants: { #name: Text; #id: { #active: Nat; #inactive } }",
                    func() {
                        type NestedVariant = {
                            #name : Text;
                            #id : { #active : Nat; #inactive };
                        };

                        let NestedVariantSchema = #Variant([("name", #Text), ("id", #Variant([("active", #Nat), ("inactive", #Null)]))]);

                        let #ok(nested_variants) = zendb.createCollection<NestedVariant>(
                            "nested_variants",
                            NestedVariantSchema,
                            {
                                from_blob = func(blob : Blob) : ?NestedVariant = from_candid (blob);
                                to_blob = func(c : NestedVariant) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("name", [#MinSize(1)]), #Field("id.active", [#Min(1)]), #Unique(["name"]), #Unique(["id.active"])];
                            },
                        );

                        let #ok(id) = nested_variants.insert(#name("hello")) else return assert false;
                        let #ok(id2) = nested_variants.insert(#id(#active(42))) else return assert false;
                        let #ok(id3) = nested_variants.insert(#id(#inactive)) else return assert false;

                        assert nested_variants.size() == 3;

                        let r1 = nested_variants.search(ZenDB.QueryBuilder().Where("name", #eq(#Text("hello"))));
                        let #ok(search_result) = r1 else return assert false;
                        assert search_result.documents == [(id, #name("hello"))];

                        let r2 = nested_variants.search(ZenDB.QueryBuilder().Where("id.active", #eq(#Nat(42))));
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id2, #id(#active(42)))];

                        let r3 = nested_variants.search(ZenDB.QueryBuilder().Where("id.inactive", #eq(#Null)));
                        let #ok(sr3) = r3 else return assert false;
                        assert sr3.documents == [(id3, #id(#inactive))];

                        let r4 = nested_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("name"))));
                        let #ok(sr4) = r4 else return assert false;
                        assert sr4.documents == [(id, #name("hello"))];

                        let r5 = nested_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("id"))));
                        let #ok(sr5) = r5 else return assert false;
                        assert sr5.documents == [(id2, #id(#active(42))), (id3, #id(#inactive))];

                        let r6 = nested_variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Text("active"))));
                        let #ok(sr6) = r6 else return assert false;
                        assert sr6.documents == [(id2, #id(#active(42)))];

                        let r7 = nested_variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Text("inactive"))));
                        let #ok(sr7) = r7 else return assert false;
                        assert sr7.documents == [(id3, #id(#inactive))];

                        let r8 = nested_variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Text("unknown"))));
                        let #ok(sr8) = r8 else return assert false;
                        assert sr8.documents == [];

                        let r9 = nested_variants.search(ZenDB.QueryBuilder().Where("name", #exists));
                        let #ok(sr9) = r9 else return assert false;
                        assert sr9.documents == [(id, #name("hello"))];

                        Debug.print(debug_show (nested_variants.search(ZenDB.QueryBuilder().Where("id", #exists))));
                        let r10 = nested_variants.search(ZenDB.QueryBuilder().Where("id", #exists));
                        let #ok(sr10) = r10 else return assert false;
                        assert sr10.documents == [(id2, #id(#active(42))), (id3, #id(#inactive))];

                        let r11 = nested_variants.search(ZenDB.QueryBuilder().Where("id.active", #exists));
                        let #ok(sr11) = r11 else return assert false;
                        assert sr11.documents == [(id2, #id(#active(42)))];

                        let r12 = nested_variants.search(ZenDB.QueryBuilder().Where("id.inactive", #exists));
                        let #ok(sr12) = r12 else return assert false;
                        assert sr12.documents == [(id3, #id(#inactive))];

                        assert nested_variants.get(id) == ?(#name("hello"));
                        assert nested_variants.get(id2) == ?(#id(#active(42)));
                        assert nested_variants.get(id3) == ?(#id(#inactive));

                    },
                );

                test(
                    "Nested Tuples: (Nat, (Text, Nat))",
                    func() {
                        type NestedTuple = ZenDB.Tuple<Nat, ZenDB.Tuple<Text, Nat>>;

                        let NestedTupleSchema = ZenDB.Schema.Tuple(#Nat, ZenDB.Schema.Tuple(#Text, #Nat));

                        let #ok(nested_tuples) = zendb.createCollection<NestedTuple>(
                            "nested_tuples",
                            NestedTupleSchema,
                            {
                                from_blob = func(blob : Blob) : ?NestedTuple = from_candid (blob);
                                to_blob = func(c : NestedTuple) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("0", [#Min(1)]), #Field("1.0", [#MinSize(1)]), #Field("1.1", [#Min(1)]), #Unique(["0"]), #Unique(["1.0"]), #Unique(["1.1"])];
                            },
                        ) else return assert false;

                        let #ok(id) = nested_tuples.insert(ZenDB.Tuple(42, ZenDB.Tuple("hello", 100))) else return assert false;
                        assert nested_tuples.size() == 1;

                        let result = nested_tuples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, ZenDB.Tuple(42, ZenDB.Tuple("hello", 100)))];

                        assert nested_tuples.get(id) == ?(ZenDB.Tuple(42, ZenDB.Tuple("hello", 100)));

                    },
                );

                test(
                    "Optional Records: ?{a : Nat; b : Text}",
                    func() {
                        type OptionalRecord = ?{ a : Nat; b : Text };

                        let OptionalRecordSchema = #Option(#Record([("a", #Nat), ("b", #Text)]));

                        let #ok(optional_records) = zendb.createCollection<OptionalRecord>(
                            "optional_records",
                            OptionalRecordSchema,
                            {
                                from_blob = func(blob : Blob) : ?OptionalRecord = from_candid (blob);
                                to_blob = func(c : OptionalRecord) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("a", [#Min(1)]), #Field("b", [#MinSize(1)]), #Unique(["a"]), #Unique(["b"])];
                            },
                        ) else return assert false;

                        let #ok(id) = optional_records.insert(?({ a = 42; b = "hello" })) else return assert false;
                        let #ok(id2) = optional_records.insert(null) else return assert false;

                        assert optional_records.size() == 2;

                        let r1 = optional_records.search(ZenDB.QueryBuilder().Where("a", #eq(#Nat(42))));
                        let #ok(search_result) = r1 else return assert false;
                        assert search_result.documents == [(id, ?({ a = 42; b = "hello" }))];

                        let r2 = optional_records.search(ZenDB.QueryBuilder().Where("b", #eq(#Text("hello"))));
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id, ?({ a = 42; b = "hello" }))];

                        let r3 = optional_records.search(ZenDB.QueryBuilder().Where("", #eq(#Null)));
                        let #ok(sr3) = r3 else return assert false;
                        assert sr3.documents == [(id2, null)];

                        let r4 = optional_records.search(ZenDB.QueryBuilder().Where("a", #exists));
                        let #ok(sr4) = r4 else return assert false;
                        assert sr4.documents == [(id, ?({ a = 42; b = "hello" }))];

                        let r5 = optional_records.search(ZenDB.QueryBuilder().Where("b", #exists));
                        let #ok(sr5) = r5 else return assert false;
                        assert sr5.documents == [(id, ?({ a = 42; b = "hello" }))];

                        assert optional_records.get(id) == ?(?({ a = 42; b = "hello" }));
                        assert optional_records.get(id2) == ?(null);

                    },
                );

                test(
                    "Optional Variants: ?{ #name: Text; #id: Nat }",
                    func() {
                        type OptionalVariant = ?{ #name : Text; #id : Nat };

                        let OptionalVariantSchema = #Option(#Variant([("name", #Text), ("id", #Nat)]));
                        let candify = {
                            from_blob = func(blob : Blob) : ?OptionalVariant = from_candid (blob);
                            to_blob = func(c : OptionalVariant) : Blob = to_candid (c);
                        };

                        let #err(_) = zendb.createCollection<OptionalVariant>("optional_variants", OptionalVariantSchema, candify, ?{ schema_constraints = [#Unique([""])] }) else return assert false;

                        let #ok(optional_variants) = zendb.createCollection<OptionalVariant>(
                            "optional_variants",
                            OptionalVariantSchema,
                            candify,
                            ?{
                                schema_constraints = [#Field("name", [#MinSize(1)]), #Field("id", [#Min(1)]), #Unique(["id"]), #Unique(["name"])];
                            },
                        );

                        let #ok(id) = optional_variants.insert(?(#name("hello"))) else return assert false;
                        let #ok(id2) = optional_variants.insert(?(#id(42))) else return assert false;
                        let #ok(id3) = optional_variants.insert(null) else return assert false;

                        assert optional_variants.size() == 3;

                        let r1 = optional_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("name"))));
                        let #ok(search_result) = r1 else return assert false;
                        assert search_result.documents == [(id, ?(#name("hello")))];

                        let r2 = optional_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("id"))));
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id2, ?(#id(42)))];

                        let r3 = optional_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("unknown"))));
                        let #ok(sr3) = r3 else return assert false;
                        assert sr3.documents == [];

                        let r4 = optional_variants.search(ZenDB.QueryBuilder().Where("name", #exists));
                        let #ok(sr4) = r4 else return assert false;
                        assert sr4.documents == [(id, ?(#name("hello")))];

                        let r5 = optional_variants.search(ZenDB.QueryBuilder().Where("id", #exists));
                        let #ok(sr5) = r5 else return assert false;
                        assert sr5.documents == [(id2, ?(#id(42)))];

                        let r6 = optional_variants.search(ZenDB.QueryBuilder().Where("name", #eq(#Text("hello"))));
                        let #ok(sr6) = r6 else return assert false;
                        assert sr6.documents == [(id, ?(#name("hello")))];

                        let r7 = optional_variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Nat(42))));
                        let #ok(sr7) = r7 else return assert false;
                        assert sr7.documents == [(id2, ?(#id(42)))];

                        let r8 = optional_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Null)));
                        let #ok(sr8) = r8 else return assert false;
                        assert sr8.documents == [(id3, null)];

                        assert optional_variants.get(id) == ?(?(#name("hello")));
                        assert optional_variants.get(id2) == ?(?(#id(42)));
                        assert optional_variants.get(id3) == ?(null);

                    },
                );

                test(
                    "Optional Tuples: ?(Nat, Text)",
                    func() {
                        type OptionalTuple = ?(Nat, Text);

                        let OptionalTupleSchema = #Option(#Tuple([#Nat, #Text]));

                        let #ok(optional_tuples) = zendb.createCollection<OptionalTuple>(
                            "optional_tuples",
                            OptionalTupleSchema,
                            {
                                from_blob = func(blob : Blob) : ?OptionalTuple = from_candid (blob);
                                to_blob = func(c : OptionalTuple) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Unique(["0"]), #Unique(["1"])];
                            },
                        ) else return assert false;

                        let #ok(id) = optional_tuples.insert(?((42, "hello"))) else return assert false;
                        let #ok(id2) = optional_tuples.insert(null) else return assert false;

                        assert optional_tuples.size() == 2;

                        let r1 = optional_tuples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42))));
                        let #ok(search_result) = r1 else return assert false;
                        assert search_result.documents == [(id, ?((42, "hello")))];

                        let r2 = optional_tuples.search(ZenDB.QueryBuilder().Where("1", #eq(#Text("hello"))));
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id, ?((42, "hello")))];

                        let r3 = optional_tuples.search(ZenDB.QueryBuilder().Where("", #eq(#Null)));
                        let #ok(sr3) = r3 else return assert false;
                        assert sr3.documents == [(id2, null)];

                        assert optional_tuples.get(id) == ?(?((42, "hello")));
                        assert optional_tuples.get(id2) == ?(null);

                    },
                );

                test(
                    "Deeply Nested Optional Records",
                    func() {
                        type DeepOptional = ?{
                            name : Text;
                            details : ?{
                                id : Nat;
                                metadata : ?{
                                    active : Bool;
                                    tags : ?[Text];
                                };
                            };
                        };

                        let DeepOptionalSchema = #Option(#Record([("name", #Text), ("details", #Option(#Record([("id", #Nat), ("metadata", #Option(#Record([("active", #Bool), ("tags", #Option(#Array(#Text)))])))])))]));

                        let #ok(deep_optionals) = zendb.createCollection<DeepOptional>(
                            "deep_optionals",
                            DeepOptionalSchema,
                            {
                                from_blob = func(blob : Blob) : ?DeepOptional = from_candid (blob);
                                to_blob = func(c : DeepOptional) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("name", [#MinSize(1)]), #Field("details.id", [#Min(1)]), #Unique(["name"]), #Unique(["details.id"])];
                            },
                        ) else return assert false;

                        // Full object with all fields
                        let complete : DeepOptional = ?{
                            name = "complete";
                            details = ?{
                                id = 1;
                                metadata = ?{
                                    active = true;
                                    tags = ?["tag1", "tag2"];
                                };
                            };
                        };

                        // Various levels of null fields
                        let noTags : DeepOptional = ?{
                            name = "noTags";
                            details = ?{
                                id = 2;
                                metadata = ?{
                                    active = false;
                                    tags = null;
                                };
                            };
                        };

                        let noMetadata : DeepOptional = ?{
                            name = "noMetadata";
                            details = ?{
                                id = 3;
                                metadata = null;
                            };
                        };

                        let noDetails : DeepOptional = ?{
                            name = "noDetails";
                            details = null;
                        };

                        let completelyNull : DeepOptional = null;

                        let #ok(id1) = deep_optionals.insert(complete) else return assert false;
                        let #ok(id2) = deep_optionals.insert(noTags) else return assert false;
                        let #ok(id3) = deep_optionals.insert(noMetadata) else return assert false;
                        let #ok(id4) = deep_optionals.insert(noDetails) else return assert false;
                        let #ok(id5) = deep_optionals.insert(completelyNull) else return assert false;

                        assert deep_optionals.size() == 5;

                        // Test nested field queries
                        let r1 = deep_optionals.search(ZenDB.QueryBuilder().Where("name", #eq(#Text("complete"))));
                        let #ok(search_result) = r1 else return assert false;
                        assert search_result.documents == [(id1, complete)];

                        let r2 = deep_optionals.search(ZenDB.QueryBuilder().Where("details.id", #eq(#Nat(1))));
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id1, complete)];

                        let r3 = deep_optionals.search(ZenDB.QueryBuilder().Where("details.metadata.active", #eq(#Bool(true))));
                        let #ok(sr3) = r3 else return assert false;
                        assert sr3.documents == [(id1, complete)];

                        // Test null field handling
                        let r4 = deep_optionals.search(ZenDB.QueryBuilder().Where("", #eq(#Null)));
                        let #ok(sr4) = r4 else return assert false;
                        assert sr4.documents == [(id5, completelyNull)];

                        // Retrieving documents
                        assert deep_optionals.get(id1) == ?(complete);
                        assert deep_optionals.get(id5) == ?(completelyNull);
                    },
                );

                test(
                    "Mixed Numeric Types",
                    func() {
                        // Test all numeric types in a single structure
                        type NumericRecord = {
                            int_val : Int;
                            int8_val : Int8;
                            int16_val : Int16;
                            int32_val : Int32;
                            int64_val : Int64;
                            nat_val : Nat;
                            nat8_val : Nat8;
                            nat16_val : Nat16;
                            nat32_val : Nat32;
                            nat64_val : Nat64;
                        };

                        let NumericRecordSchema = #Record([
                            ("int_val", #Int),
                            ("int8_val", #Int8),
                            ("int16_val", #Int16),
                            ("int32_val", #Int32),
                            ("int64_val", #Int64),
                            ("nat_val", #Nat),
                            ("nat8_val", #Nat8),
                            ("nat16_val", #Nat16),
                            ("nat32_val", #Nat32),
                            ("nat64_val", #Nat64),
                        ]);

                        let #ok(numeric_records) = zendb.createCollection<NumericRecord>(
                            "numeric_records",
                            NumericRecordSchema,
                            {
                                from_blob = func(blob : Blob) : ?NumericRecord = from_candid (blob);
                                to_blob = func(c : NumericRecord) : Blob = to_candid (c);
                            },
                            null,
                        ) else return assert false;

                        let testNumericRecord : NumericRecord = {
                            int_val = -100;
                            int8_val = -8;
                            int16_val = -16;
                            int32_val = -32;
                            int64_val = -64;
                            nat_val = 100;
                            nat8_val = 8;
                            nat16_val = 16;
                            nat32_val = 32;
                            nat64_val = 64;
                        };

                        let #ok(id) = numeric_records.insert(testNumericRecord) else return assert false;
                        assert numeric_records.size() == 1;

                        // Test searching for different numeric types
                        let r1 = numeric_records.search(ZenDB.QueryBuilder().Where("int_val", #eq(#Int(-100))));
                        let #ok(search_result) = r1 else return assert false;
                        assert search_result.documents == [(id, testNumericRecord)];

                        let r2 = numeric_records.search(ZenDB.QueryBuilder().Where("nat8_val", #eq(#Nat8(8))));
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id, testNumericRecord)];

                        let r3 = numeric_records.search(ZenDB.QueryBuilder().Where("int16_val", #eq(#Int16(-16))));
                        let #ok(sr3) = r3 else return assert false;
                        assert sr3.documents == [(id, testNumericRecord)];

                        let r4 = numeric_records.search(ZenDB.QueryBuilder().Where("nat64_val", #eq(#Nat64(64))));
                        let #ok(sr4) = r4 else return assert false;
                        assert sr4.documents == [(id, testNumericRecord)];

                        assert numeric_records.get(id) == ?(testNumericRecord);
                    },
                );

                test(
                    "Record with Array Fields",
                    func() {
                        type RecordWithArrays = {
                            id : Nat;
                            numbers : [Nat];
                            texts : [Text];
                        };

                        let RecordWithArraysSchema = #Record([
                            ("id", #Nat),
                            ("numbers", #Array(#Nat)),
                            ("texts", #Array(#Text)),
                        ]);

                        let #ok(records_with_arrays) = zendb.createCollection<RecordWithArrays>(
                            "records_with_arrays",
                            RecordWithArraysSchema,
                            {
                                from_blob = func(blob : Blob) : ?RecordWithArrays = from_candid (blob);
                                to_blob = func(c : RecordWithArrays) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("id", [#Min(1)]), #Unique(["id"])];
                                // Note: Cannot add constraints on array fields since they're not queryable
                            },
                        ) else return assert false;

                        let #ok(id) = records_with_arrays.insert({
                            id = 1;
                            numbers = [1, 2, 3];
                            texts = ["a", "b", "c"];
                        }) else return assert false;

                        assert records_with_arrays.size() == 1;

                        let result = records_with_arrays.search(ZenDB.QueryBuilder().Where("id", #eq(#Nat(1))));
                        let #ok(search_result) = result else return assert false;
                        assert search_result.documents == [(id, { id = 1; numbers = [1, 2, 3]; texts = ["a", "b", "c"] })];

                        assert records_with_arrays.get(id) == ?({
                            id = 1;
                            numbers = [1, 2, 3];
                            texts = ["a", "b", "c"];
                        });
                    },
                );

                test(
                    "Variant with Array Fields",
                    func() {
                        type VariantWithArrays = {
                            #single : Nat;
                            #list : [Text];
                            #matrix : [[Nat]];
                        };

                        let VariantWithArraysSchema = #Variant([
                            ("single", #Nat),
                            ("list", #Array(#Text)),
                            ("matrix", #Array(#Array(#Nat))),
                        ]);

                        let #ok(variants_with_arrays) = zendb.createCollection<VariantWithArrays>(
                            "variants_with_arrays",
                            VariantWithArraysSchema,
                            {
                                from_blob = func(blob : Blob) : ?VariantWithArrays = from_candid (blob);
                                to_blob = func(c : VariantWithArrays) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [#Field("single", [#Min(1)]), #Unique(["single"])];
                                // Note: Cannot add constraints on array fields
                            },
                        ) else return assert false;

                        let #ok(id1) = variants_with_arrays.insert(#single(42)) else return assert false;
                        let #ok(id2) = variants_with_arrays.insert(#list(["a", "b", "c"])) else return assert false;
                        let #ok(id3) = variants_with_arrays.insert(#matrix([[1, 2], [3, 4]])) else return assert false;

                        assert variants_with_arrays.size() == 3;
                        let #ok(result1) = variants_with_arrays.search(ZenDB.QueryBuilder().Where("single", #eq(#Nat(42)))) else return assert false;
                        assert result1.documents == [(id1, #single(42))];

                        let #ok(result2) = variants_with_arrays.search(ZenDB.QueryBuilder().Where("", #eq(#Text("list")))) else return assert false;
                        assert result2.documents == [(id2, #list(["a", "b", "c"]))];

                        let #ok(result3) = variants_with_arrays.search(ZenDB.QueryBuilder().Where("", #eq(#Text("matrix")))) else return assert false;
                        assert result3.documents == [(id3, #matrix([[1, 2], [3, 4]]))];
                    },
                );

                test(
                    "Extreme Nesting with All Types",
                    func() {

                        // Creating an extremely complex nested structure with most Candid types
                        type ExtremeNesting = {
                            id : Nat;
                            metadata : {
                                name : Text;
                                description : ?Text;
                                owner : Principal;
                                created : Int;
                                tags : [Text];
                            };
                            status : {
                                #active : {
                                    level : Nat8;
                                    features : [{
                                        name : Text;
                                        enabled : Bool;
                                        config : [{
                                            key : Text;
                                            value : {
                                                #number : Nat;
                                                #text : Text;
                                                #flag : Bool;
                                            };
                                        }];
                                    }];
                                };
                                #inactive : {
                                    reason : ?Text;
                                    until : ?Int;
                                };
                                #pending;
                            };
                            data : ?[{
                                key : Text;
                                values : [{
                                    timestamp : Int;
                                    measurement : {
                                        #simple : Nat;
                                        #complex : {
                                            primary : Nat;
                                            secondary : [Nat];
                                            metadata : ?{
                                                source : Text;
                                                reliability : Nat8;
                                                raw : Blob;
                                            };
                                        };
                                    };
                                }];
                            }];
                        };

                        // Constructing the schema for this extreme nesting
                        let ValueVariantSchema = #Variant([
                            ("number", #Nat),
                            ("text", #Text),
                            ("flag", #Bool),
                        ]);

                        let ConfigItemSchema = #Record([
                            ("key", #Text),
                            ("value", ValueVariantSchema),
                        ]);

                        let FeatureSchema = #Record([
                            ("name", #Text),
                            ("enabled", #Bool),
                            ("config", #Array(ConfigItemSchema)),
                        ]);

                        let MeasurementMetadataSchema = #Record([
                            ("source", #Text),
                            ("reliability", #Nat8),
                            ("raw", #Blob),
                        ]);

                        let ComplexMeasurementSchema = #Record([
                            ("primary", #Nat),
                            ("secondary", #Array(#Nat)),
                            ("metadata", #Option(MeasurementMetadataSchema)),
                        ]);

                        let MeasurementSchema = #Variant([
                            ("simple", #Nat),
                            ("complex", ComplexMeasurementSchema),
                        ]);

                        let ValueSchema = #Record([
                            ("timestamp", #Int),
                            ("measurement", MeasurementSchema),
                        ]);

                        let DataItemSchema = #Record([
                            ("key", #Text),
                            ("values", #Array(ValueSchema)),
                        ]);

                        let StatusActiveSchema = #Record([
                            ("level", #Nat8),
                            ("features", #Array(FeatureSchema)),
                        ]);

                        let StatusInactiveSchema = #Record([
                            ("reason", #Option(#Text)),
                            ("until", #Option(#Int)),
                        ]);

                        let StatusSchema = #Variant([
                            ("active", StatusActiveSchema),
                            ("inactive", StatusInactiveSchema),
                            ("pending", #Null),
                        ]);

                        let MetadataSchema = #Record([
                            ("name", #Text),
                            ("description", #Option(#Text)),
                            ("owner", #Principal),
                            ("created", #Int),
                            ("tags", #Array(#Text)),
                        ]);

                        let ExtremeNestingSchema = #Record([
                            ("id", #Nat),
                            ("metadata", MetadataSchema),
                            ("status", StatusSchema),
                            ("data", #Option(#Array(DataItemSchema))),
                        ]);

                        let #ok(extreme_nesting) = zendb.createCollection<ExtremeNesting>(
                            "extreme_nesting",
                            ExtremeNestingSchema,
                            {
                                from_blob = func(blob : Blob) : ?ExtremeNesting = from_candid (blob);
                                to_blob = func(c : ExtremeNesting) : Blob = to_candid (c);
                            },
                            ?{
                                schema_constraints = [
                                    #Field("id", [#Min(1)]),
                                    #Field("metadata.name", [#MinSize(1)]),
                                    #Field("status.active.level", [#Min(1)]),
                                    #Field("data.key", [#MinSize(1)]),
                                    #Unique(["id"]),
                                    #Unique(["metadata.name"]),
                                    #Unique(["status.active.level"]),
                                    //#Unique(["data.key"]), // -> hit the memory limit for creating regions
                                    // this should not happen, because we have access to 65536 by default.
                                    // And this test only creates 31 collections +  43 unique constraints which results in 74 btrees
                                    // Considering there are 4 regions per btree, we should have 296 regions in total for each test type.
                                    // With the index and no index tests, we should have 592 regions in total.
                                    // By default, we allocate 1 page per region to store metadata, so the total memory used is 592 pages.
                                    // Which is well below the 65536 pages limit.
                                ];
                            }

                        ) else return assert false;

                        let testPrincipal = Principal.fromText("2vxsx-fae");

                        // Create a deeply nested test instance
                        let testInstance : ExtremeNesting = {
                            id = 1;
                            metadata = {
                                name = "Extreme Test";
                                description = ?"A test of extreme nesting";
                                owner = testPrincipal;
                                created = 1683721600;
                                tags = ["test", "complex", "nested"];
                            };
                            status = #active({
                                level = 5;
                                features = [{
                                    name = "feature1";
                                    enabled = true;
                                    config = [
                                        {
                                            key = "setting1";
                                            value = #number(42);
                                        },
                                        {
                                            key = "setting2";
                                            value = #text("value");
                                        },
                                        {
                                            key = "setting3";
                                            value = #flag(true);
                                        },
                                    ];
                                }];
                            });
                            data = ?[{
                                key = "sensor1";
                                values = [
                                    {
                                        timestamp = 1683721600;
                                        measurement = #complex({
                                            primary = 100;
                                            secondary = [101, 102, 103];
                                            metadata = ?{
                                                source = "device1";
                                                reliability = 95;
                                                raw = Blob.fromArray([0, 1, 2, 3]);
                                            };
                                        });
                                    },
                                    {
                                        timestamp = 1683721601;
                                        measurement = #simple(50);
                                    },
                                ];
                            }];
                        };

                        let #ok(id) = extreme_nesting.insert(testInstance) else return assert false;
                        assert extreme_nesting.size() == 1;

                        // Deep path queries to test indexing and search capabilities
                        let r1 = extreme_nesting.search(
                            ZenDB.QueryBuilder().Where("metadata.name", #eq(#Text("Extreme Test")))
                        );
                        let #ok(search_result) = r1 else return assert false;
                        assert search_result.documents == [(id, testInstance)];

                        let r2 = extreme_nesting.search(
                            ZenDB.QueryBuilder().Where("status", #eq(#Text("active")))
                        );
                        let #ok(sr2) = r2 else return assert false;
                        assert sr2.documents == [(id, testInstance)];

                        //! accessing elements nested in arrays not supported yet
                        // assert extreme_nesting.search(
                        //     ZenDB.QueryBuilder().Where("data.key", #eq(#Text("sensor1")))
                        // ) == #ok({ documents = [(0, testInstance)]; instructions = _ });

                        // assert extreme_nesting.search(
                        //     ZenDB.QueryBuilder().Where("data.values.measurement", #eq(#Text("simple")))
                        // ) == #ok({ documents = [(0, testInstance)]; instructions = _ });

                        assert extreme_nesting.get(id) == ?(testInstance);
                    },
                );

            },

        );

    },
);
