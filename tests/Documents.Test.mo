// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Principal "mo:base/Principal";
import Option "mo:base/Option";

import ZenDB "../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Map "mo:map/Map";
import Record "mo:serde/Candid/Text/Parser/Record";
import ZenDBSuite "TestFramework";

ZenDBSuite.newNoIndexSetup(
    "Candid Documents Test",
    func collection_setup(zendb : ZenDB.Database) {},
    func suite_setup(zendb : ZenDB.Database) {

        suite(
            "Types to not support:",
            func() {

                test(
                    "Top level Null types",
                    func() {
                        let #err(_) = zendb.create_collection<Null>(
                            "strictly_null",
                            #Null,
                            {
                                from_blob = func(blob : Blob) : ?Null = from_candid (blob);
                                to_blob = func(c : Null) : Blob = to_candid (c);
                            },
                            [],
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

                        let #err(_) = zendb.create_collection<ArrayType>("arrays_0", ArraySchema, candify, []) else return assert false;
                    },
                );

            },
        );

        suite(
            "Should Support all the other Valid Candid Schema",
            func() {

                test(
                    "Bool Type",
                    func() {
                        let #ok(bools) = zendb.create_collection<Bool>(
                            "bools",
                            #Bool,
                            {
                                from_blob = func(blob : Blob) : ?Bool = from_candid (blob);
                                to_blob = func(c : Bool) : Blob = to_candid (c);
                            },
                            [],
                        ) else return assert false;

                        let #ok(id_true) = bools.insert(true) else return assert false;
                        let #ok(id_false) = bools.insert(false) else return assert false;

                        assert bools.size() == 2;
                        assert bools.search(ZenDB.QueryBuilder().Where("", #eq(#Bool(true)))) == #ok([(0, true)]);
                        assert bools.search(ZenDB.QueryBuilder().Where("", #eq(#Bool(false)))) == #ok([(1, false)]);
                    },
                );

                test(
                    "Principal Type",
                    func() {

                        let testPrincipal = Principal.fromText("2vxsx-fae");

                        let #ok(principals) = zendb.create_collection<Principal>(
                            "principals",
                            #Principal,
                            {
                                from_blob = func(blob : Blob) : ?Principal = from_candid (blob);
                                to_blob = func(c : Principal) : Blob = to_candid (c);
                            },
                            [],
                        ) else return assert false;

                        let #ok(id) = principals.insert(testPrincipal) else return assert false;
                        assert principals.size() == 1;
                        assert principals.search(ZenDB.QueryBuilder().Where("", #eq(#Principal(testPrincipal)))) == #ok([(0, testPrincipal)]);
                        assert principals.get(id) == ?(testPrincipal);
                    },
                );

                test(
                    "Nat",
                    func() {
                        let #ok(nats) = zendb.create_collection<Nat>(
                            "nats",
                            #Nat,
                            {
                                from_blob = func(blob : Blob) : ?Nat = from_candid (blob);
                                to_blob = func(c : Nat) : Blob = to_candid (c);
                            },
                            [#Field("", [#Min(1)]), #Unique([""])],
                        ) else return assert false;

                        let #ok(id) = nats.insert(42) else return assert false;
                        assert nats.size() == 1;
                        assert nats.search(ZenDB.QueryBuilder().Where("", #eq(#Nat(42)))) == #ok([(0, 42)]);
                        assert nats.get(id) == ?(42);

                    },
                );

                test(
                    "Float",
                    func() {
                        let #ok(floats) = zendb.create_collection<Float>(
                            "floats",
                            #Float,
                            {
                                from_blob = func(blob : Blob) : ?Float = from_candid (blob);
                                to_blob = func(c : Float) : Blob = to_candid (c);
                            },
                            [#Field("", [#Min(1)]), #Unique([""])],
                        ) else return assert false;

                        let #ok(id) = floats.insert(42.0) else return assert false;
                        assert floats.size() == 1;
                        assert floats.search(ZenDB.QueryBuilder().Where("", #eq(#Float(42.0)))) == #ok([(0, 42.0)]);
                        assert floats.get(id) == ?(42.0);

                    }

                );

                test(
                    "Text",
                    func() {
                        let #ok(texts) = zendb.create_collection<Text>(
                            "texts",
                            #Text,
                            {
                                from_blob = func(blob : Blob) : ?Text = from_candid (blob);
                                to_blob = func(c : Text) : Blob = to_candid (c);
                            },
                            [#Field("", [#MinSize(1)]), #Unique([""])],
                        ) else return assert false;

                        let #ok(id) = texts.insert("hello") else return assert false;
                        assert texts.size() == 1;
                        assert texts.search(ZenDB.QueryBuilder().Where("", #eq(#Text("hello")))) == #ok([(0, "hello")]);
                        assert texts.get(id) == ?("hello");

                    },
                );

                test(
                    "Blob",
                    func() {
                        let #ok(blobs) = zendb.create_collection<Blob>(
                            "blobs",
                            #Blob,
                            {
                                from_blob = func(blob : Blob) : ?Blob = from_candid (blob);
                                to_blob = func(c : Blob) : Blob = to_candid (c);
                            },
                            [#Field("", [#MinSize(1)]), #Unique([""])],
                        ) else return assert false;

                        let #ok(id) = blobs.insert(Blob.fromArray([0, 1, 2, 3])) else return assert false;
                        assert blobs.size() == 1;
                        assert blobs.search(ZenDB.QueryBuilder().Where("", #eq(#Blob(Blob.fromArray([0, 1, 2, 3]))))) == #ok([(0, Blob.fromArray([0, 1, 2, 3]))]);
                        assert blobs.get(id) == ?(Blob.fromArray([0, 1, 2, 3]));

                    },
                );

                test(
                    "Option: ?Text",
                    func() {
                        let #ok(options) = zendb.create_collection<?Text>(
                            "options",
                            #Option(#Text),
                            {
                                from_blob = func(blob : Blob) : ??Text = from_candid (blob);
                                to_blob = func(c : ?Text) : Blob = to_candid (c);
                            },
                            [#Field("", [#MinSize(1)]), #Unique([""])],
                        ) else return assert false;

                        let #ok(id) = options.insert(?("hello")) else return assert false;
                        assert options.size() == 1;
                        assert options.search(ZenDB.QueryBuilder().Where("", #eq(#Option(#Text("hello"))))) == #ok([(0, ?("hello"))]);
                        assert options.get(id) == ?(?("hello"));

                    },
                );

                test(
                    "Nested Option: ???Nat",
                    func() {
                        type NestedOption = ???Nat;
                        let NestedOptionSchema = #Option(#Option(#Option(#Nat)));

                        let #ok(nested_options) = zendb.create_collection<NestedOption>(
                            "nested_options",
                            NestedOptionSchema,
                            {
                                from_blob = func(blob : Blob) : ?NestedOption = from_candid (blob);
                                to_blob = func(c : NestedOption) : Blob = to_candid (c);
                            },
                            [#Field("", [#Min(1)]), #Unique([""])],
                        ) else return assert false;

                        let #ok(id1) = nested_options.insert(???42) else return assert false;
                        let #ok(id2) = nested_options.insert(null) else return assert false;
                        let #ok(id3) = nested_options.insert(?null) else return assert false;
                        let #ok(id4) = nested_options.insert(??null) else return assert false;

                        assert nested_options.size() == 4;
                        assert nested_options.search(
                            ZenDB.QueryBuilder().Where(
                                "",
                                #eq(#Option(#Option(#Option(#Nat(42))))),
                            )
                        ) == #ok([(0, ???42)]);

                        assert nested_options.search(
                            ZenDB.QueryBuilder().Where(
                                "",
                                #eq(#Null),
                            )
                        ) == #ok([(1, null)]);

                        assert nested_options.search(
                            ZenDB.QueryBuilder().Where(
                                "",
                                #eq(#Option(#Null)),
                            )
                        ) == #ok([(2, ?null)]);

                        assert nested_options.search(
                            ZenDB.QueryBuilder().Where(
                                "",
                                #eq(#Option(#Option(#Null))),
                            )
                        ) == #ok([(3, ??null)]);

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

                        let #ok(records) = zendb.create_collection<Record>(
                            "records",
                            RecordSchema,
                            {
                                from_blob = func(blob : Blob) : ?Record = from_candid (blob);
                                to_blob = func(c : Record) : Blob = to_candid (c);
                            },
                            [#Field("a", [#Min(1)]), #Field("b", [#MinSize(1)]), #Unique(["a"]), #Unique(["b"])],
                        ) else return assert false;

                        let #ok(id) = records.insert({ a = 42; b = "hello" }) else return assert false;
                        assert records.size() == 1;
                        assert records.search(ZenDB.QueryBuilder().Where("a", #eq(#Nat(42)))) == #ok([(0, { a = 42; b = "hello" })]);
                        assert records.search(ZenDB.QueryBuilder().Where("b", #eq(#Text("hello"))).And("a", #eq(#Nat(42)))) == #ok([(0, { a = 42; b = "hello" })]);
                        assert records.get(id) == ?({ a = 42; b = "hello" });

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

                        let #ok(notes) = zendb.create_collection<Note>(
                            "notes",
                            NoteSchema,
                            candify,
                            schema_constraints,
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

                        assert notes.search(
                            ZenDB.QueryBuilder().Where(
                                "user_id",
                                #eq(#Principal(Principal.fromText("2vxsx-fae"))),
                            ).And(
                                "title",
                                #eq(#Text("hello.mo")),
                            )
                        ) == #ok([(0, { user_id = Principal.fromText("2vxsx-fae"); title = "hello.mo"; content = "This is a test note" })]);

                        let #ok(total_updated) = notes.update(
                            ZenDB.QueryBuilder().Where(
                                "user_id",
                                #eq(#Principal(Principal.fromText("2vxsx-fae"))),
                            ).And(
                                "title",
                                #eq(#Text("hello.mo")),
                            ),
                            [("content", #Text("This is version 2 of the note"))],
                        );

                        assert total_updated == 1;

                        assert notes.search(
                            ZenDB.QueryBuilder().Where(
                                "user_id",
                                #eq(#Principal(Principal.fromText("2vxsx-fae"))),
                            ).And(
                                "title",
                                #eq(#Text("hello.mo")),
                            )
                        ) == #ok([(0, { user_id = Principal.fromText("2vxsx-fae"); title = "hello.mo"; content = "This is version 2 of the note" })]);

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
                        let #err(_) = zendb.create_collection<Variant>("variants_0", VariantSchema, candify, [#Unique(["active"])]) else return assert false;
                        let #err(_) = zendb.create_collection<Variant>("variants_0", VariantSchema, candify, [#Unique(["inactive"])]) else return assert false;
                        let #err(_) = zendb.create_collection<Variant>("variants_0", VariantSchema, candify, [#Field("active", [#Min(1)])]) else return assert false;
                        let #err(_) = zendb.create_collection<Variant>("variants_0", VariantSchema, candify, [#Field("inactive", [#Min(1)])]) else return assert false;

                        let #ok(variants) = zendb.create_collection<Variant>(
                            "variants_0",
                            VariantSchema,
                            candify,
                            [],
                        ) else return assert false;

                        let #ok(id) = variants.insert(#active) else return assert false;
                        let #ok(id2) = variants.insert(#inactive) else return assert false;

                        assert variants.size() == 2;
                        Debug.print(debug_show (variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("active"))))));
                        Debug.print(debug_show (variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("inactive"))))));
                        Debug.print(debug_show (variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("unknown"))))));

                        assert variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("active")))) == #ok([(0, #active)]);
                        assert variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("inactive")))) == #ok([(1, #inactive)]);
                        assert variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("unknown")))) == #ok([]);

                        assert variants.search(ZenDB.QueryBuilder().Where("active", #exists)) == #ok([(0, #active)]);
                        assert variants.search(ZenDB.QueryBuilder().Where("inactive", #exists)) == #ok([(1, #inactive)]);

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

                        let #ok(variants) = zendb.create_collection<Variant>(
                            "variants_1",
                            VariantSchema,
                            {
                                from_blob = func(blob : Blob) : ?Variant = from_candid (blob);
                                to_blob = func(c : Variant) : Blob = to_candid (c);
                            },
                            [#Field("name", [#MinSize(1)]), #Field("id", [#Min(1)]), #Unique(["id"]), #Unique(["name"])],
                        ) else return assert false;

                        let #ok(id) = variants.insert(#name("hello")) else return assert false;
                        let #ok(id2) = variants.insert(#id(42)) else return assert false;

                        assert variants.size() == 2;
                        assert variants.search(ZenDB.QueryBuilder().Where("name", #eq(#Text("hello")))) == #ok([(0, #name("hello"))]);
                        assert variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Nat(42)))) == #ok([(1, #id(42))]);

                        assert variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("name")))) == #ok([(0, #name("hello"))]);
                        assert variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("id")))) == #ok([(1, #id(42))]);

                        assert variants.search(ZenDB.QueryBuilder().Where("name", #exists)) == #ok([(0, #name("hello"))]);
                        assert variants.search(ZenDB.QueryBuilder().Where("id", #exists)) == #ok([(1, #id(42))]);

                        assert variants.get(id) == ?(#name("hello"));

                    },
                );

                test(
                    "Tuples: (Nat, Text)",
                    func() {
                        // Tuples are converted to records in Candid
                        // They become records with numbered fields, that can be accessed by their index
                        // e.g. (Nat, Text) becomes { _0_ : Nat; _1_ : Text }
                        //
                        // ZenDB provides helpers for the most common tuple types

                        type Tuple = ZenDB.Tuple<Nat, Text>;

                        let TupleSchema = ZenDB.Schema.Tuple(#Nat, #Text);

                        let candify : ZenDB.Types.Candify<Tuple> = {
                            from_blob = func(blob : Blob) : ?Tuple = from_candid (blob);
                            to_blob = func(c : Tuple) : Blob = to_candid (c);
                        };

                        let #ok(tuples) = zendb.create_collection<Tuple>(
                            "tuples",
                            TupleSchema,
                            candify,
                            [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Unique(["0"]), #Unique(["1"])],
                        ) else return assert false;

                        let #ok(id) = tuples.insert(ZenDB.Tuple(42, "hello")) else return assert false;
                        assert tuples.size() == 1;
                        assert tuples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42)))) == #ok([(0, ZenDB.Tuple(42, "hello"))]);
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

                        let #ok(triples) = zendb.create_collection<Triple>(
                            "triples",
                            TripleSchema,
                            {
                                from_blob = func(blob : Blob) : ?Triple = from_candid (blob);
                                to_blob = func(c : Triple) : Blob = to_candid (c);
                            },
                            [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Field("2", [#Min(1)]), #Unique(["0"]), #Unique(["1"]), #Unique(["2"])],
                        ) else return assert false;

                        let #ok(id) = triples.insert(ZenDB.Triple(42, "hello", 100)) else return assert false;
                        assert triples.size() == 1;
                        assert triples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42)))) == #ok([(0, ZenDB.Triple(42, "hello", 100))]);
                        assert triples.search(ZenDB.QueryBuilder().Where("1", #eq(#Text("hello")))) == #ok([(0, ZenDB.Triple(42, "hello", 100))]);
                        assert triples.search(ZenDB.QueryBuilder().Where("2", #eq(#Nat(100)))) == #ok([(0, ZenDB.Triple(42, "hello", 100))]);

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

                        let #ok(quadruples) = zendb.create_collection<Quadruple>(
                            "quadruples",
                            QuadrupleSchema,
                            {
                                from_blob = func(blob : Blob) : ?Quadruple = from_candid (blob);
                                to_blob = func(c : Quadruple) : Blob = to_candid (c);
                            },
                            [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Field("2", [#Min(1)]), #Field("3", [#MinSize(1)]), #Unique(["0"]), #Unique(["1"]), #Unique(["2"]), #Unique(["3"])],
                        ) else return assert false;

                        let #ok(id) = quadruples.insert(ZenDB.Quadruple(42, "hello", 100, Blob.fromArray([0, 1, 2]))) else return assert false;
                        assert quadruples.size() == 1;
                        assert quadruples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42)))) == #ok([(0, ZenDB.Quadruple(42, "hello", 100, Blob.fromArray([0, 1, 2])))]);
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

                        let #ok(nested_records) = zendb.create_collection<NestedRecord>(
                            "nested_records",
                            NestedRecordSchema,
                            {
                                from_blob = func(blob : Blob) : ?NestedRecord = from_candid (blob);
                                to_blob = func(c : NestedRecord) : Blob = to_candid (c);
                            },
                            [#Field("a.b", [#Min(1)]), #Field("a.c", [#MinSize(1)]), #Field("d", [#Min(1)]), #Unique(["a.b"]), #Unique(["a.c"]), #Unique(["d"])],
                        ) else return assert false;

                        let #ok(id) = nested_records.insert({
                            a = { b = 42; c = "hello" };
                            d = 100;
                        }) else return assert false;
                        assert nested_records.size() == 1;
                        assert nested_records.search(ZenDB.QueryBuilder().Where("a.b", #eq(#Nat(42)))) == #ok([(0, { a = { b = 42; c = "hello" }; d = 100 })]);
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

                        let #ok(nested_variants) = zendb.create_collection<NestedVariant>(
                            "nested_variants",
                            NestedVariantSchema,
                            {
                                from_blob = func(blob : Blob) : ?NestedVariant = from_candid (blob);
                                to_blob = func(c : NestedVariant) : Blob = to_candid (c);
                            },
                            [#Field("name", [#MinSize(1)]), #Field("id.active", [#Min(1)]), #Field("id.inactive", [#MinSize(1)]), #Unique(["name"]), #Unique(["id.active"]), #Unique(["id.inactive"])],
                        );

                        let #ok(id) = nested_variants.insert(#name("hello")) else return assert false;
                        let #ok(id2) = nested_variants.insert(#id(#active(42))) else return assert false;
                        let #ok(id3) = nested_variants.insert(#id(#inactive)) else return assert false;

                        assert nested_variants.size() == 3;
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("name", #eq(#Text("hello")))) == #ok([(0, #name("hello"))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("id.active", #eq(#Nat(42)))) == #ok([(1, #id(#active(42)))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("id.inactive", #eq(#Null))) == #ok([(2, #id(#inactive))]);

                        assert nested_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("name")))) == #ok([(0, #name("hello"))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("id")))) == #ok([(1, #id(#active(42))), (2, #id(#inactive))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Text("active")))) == #ok([(1, #id(#active(42)))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Text("inactive")))) == #ok([(2, #id(#inactive))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Text("unknown")))) == #ok([]);

                        assert nested_variants.search(ZenDB.QueryBuilder().Where("name", #exists)) == #ok([(0, #name("hello"))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("id", #exists)) == #ok([(1, #id(#active(42))), (2, #id(#inactive))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("id.active", #exists)) == #ok([(1, #id(#active(42)))]);
                        assert nested_variants.search(ZenDB.QueryBuilder().Where("id.inactive", #exists)) == #ok([(2, #id(#inactive))]);

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

                        let #ok(nested_tuples) = zendb.create_collection<NestedTuple>(
                            "nested_tuples",
                            NestedTupleSchema,
                            {
                                from_blob = func(blob : Blob) : ?NestedTuple = from_candid (blob);
                                to_blob = func(c : NestedTuple) : Blob = to_candid (c);
                            },
                            [#Field("0", [#Min(1)]), #Field("1.0", [#MinSize(1)]), #Field("1.1", [#Min(1)]), #Unique(["0"]), #Unique(["1.0"]), #Unique(["1.1"])],
                        ) else return assert false;

                        let #ok(id) = nested_tuples.insert(ZenDB.Tuple(42, ZenDB.Tuple("hello", 100))) else return assert false;
                        assert nested_tuples.size() == 1;
                        assert nested_tuples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42)))) == #ok([(0, ZenDB.Tuple(42, ZenDB.Tuple("hello", 100)))]);
                        assert nested_tuples.get(id) == ?(ZenDB.Tuple(42, ZenDB.Tuple("hello", 100)));

                    },
                );

                test(
                    "Optional Records: ?{a : Nat; b : Text}",
                    func() {
                        type OptionalRecord = ?{ a : Nat; b : Text };

                        let OptionalRecordSchema = #Option(#Record([("a", #Nat), ("b", #Text)]));

                        let #ok(optional_records) = zendb.create_collection<OptionalRecord>(
                            "optional_records",
                            OptionalRecordSchema,
                            {
                                from_blob = func(blob : Blob) : ?OptionalRecord = from_candid (blob);
                                to_blob = func(c : OptionalRecord) : Blob = to_candid (c);
                            },
                            [#Field("a", [#Min(1)]), #Field("b", [#MinSize(1)]), #Unique(["a"]), #Unique(["b"])],
                        ) else return assert false;

                        let #ok(id) = optional_records.insert(?({ a = 42; b = "hello" })) else return assert false;
                        let #ok(id2) = optional_records.insert(null) else return assert false;

                        assert optional_records.size() == 2;
                        assert optional_records.search(ZenDB.QueryBuilder().Where("a", #eq(#Nat(42)))) == #ok([(0, ?({ a = 42; b = "hello" }))]);
                        assert optional_records.search(ZenDB.QueryBuilder().Where("b", #eq(#Text("hello")))) == #ok([(0, ?({ a = 42; b = "hello" }))]);
                        assert optional_records.search(ZenDB.QueryBuilder().Where("", #eq(#Null))) == #ok([(1, null)]);

                        assert optional_records.search(ZenDB.QueryBuilder().Where("a", #exists)) == #ok([(0, ?({ a = 42; b = "hello" }))]);
                        assert optional_records.search(ZenDB.QueryBuilder().Where("b", #exists)) == #ok([(0, ?({ a = 42; b = "hello" }))]);

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

                        let #err(_) = zendb.create_collection<OptionalVariant>("optional_variants", OptionalVariantSchema, candify, [#Unique([""])]) else return assert false;

                        let #ok(optional_variants) = zendb.create_collection<OptionalVariant>(
                            "optional_variants",
                            OptionalVariantSchema,
                            candify,
                            [#Field("name", [#MinSize(1)]), #Field("id", [#Min(1)]), #Unique(["id"]), #Unique(["name"])],
                        );

                        let #ok(id) = optional_variants.insert(?(#name("hello"))) else return assert false;
                        let #ok(id2) = optional_variants.insert(?(#id(42))) else return assert false;
                        let #ok(id3) = optional_variants.insert(null) else return assert false;

                        assert optional_variants.size() == 3;
                        assert optional_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("name")))) == #ok([(0, ?(#name("hello")))]);
                        assert optional_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("id")))) == #ok([(1, ?(#id(42)))]);
                        assert optional_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Text("unknown")))) == #ok([]);

                        assert optional_variants.search(ZenDB.QueryBuilder().Where("name", #exists)) == #ok([(0, ?(#name("hello")))]);
                        assert optional_variants.search(ZenDB.QueryBuilder().Where("id", #exists)) == #ok([(1, ?(#id(42)))]);

                        assert optional_variants.search(ZenDB.QueryBuilder().Where("name", #eq(#Text("hello")))) == #ok([(0, ?(#name("hello")))]);
                        assert optional_variants.search(ZenDB.QueryBuilder().Where("id", #eq(#Nat(42)))) == #ok([(1, ?(#id(42)))]);
                        assert optional_variants.search(ZenDB.QueryBuilder().Where("", #eq(#Null))) == #ok([(2, null)]);

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

                        let #ok(optional_tuples) = zendb.create_collection<OptionalTuple>(
                            "optional_tuples",
                            OptionalTupleSchema,
                            {
                                from_blob = func(blob : Blob) : ?OptionalTuple = from_candid (blob);
                                to_blob = func(c : OptionalTuple) : Blob = to_candid (c);
                            },
                            [#Field("0", [#Min(1)]), #Field("1", [#MinSize(1)]), #Unique(["0"]), #Unique(["1"])],
                        ) else return assert false;

                        let #ok(id) = optional_tuples.insert(?((42, "hello"))) else return assert false;
                        let #ok(id2) = optional_tuples.insert(null) else return assert false;

                        assert optional_tuples.size() == 2;
                        assert optional_tuples.search(ZenDB.QueryBuilder().Where("0", #eq(#Nat(42)))) == #ok([(0, ?((42, "hello")))]);
                        assert optional_tuples.search(ZenDB.QueryBuilder().Where("1", #eq(#Text("hello")))) == #ok([(0, ?((42, "hello")))]);
                        assert optional_tuples.search(ZenDB.QueryBuilder().Where("", #eq(#Null))) == #ok([(1, null)]);

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

                        let #ok(deep_optionals) = zendb.create_collection<DeepOptional>(
                            "deep_optionals",
                            DeepOptionalSchema,
                            {
                                from_blob = func(blob : Blob) : ?DeepOptional = from_candid (blob);
                                to_blob = func(c : DeepOptional) : Blob = to_candid (c);
                            },
                            [#Field("name", [#MinSize(1)]), #Field("details.id", [#Min(1)]), #Unique(["name"]), #Unique(["details.id"])],
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
                        assert deep_optionals.search(ZenDB.QueryBuilder().Where("name", #eq(#Text("complete")))) == #ok([(0, complete)]);
                        assert deep_optionals.search(ZenDB.QueryBuilder().Where("details.id", #eq(#Nat(1)))) == #ok([(0, complete)]);
                        assert deep_optionals.search(ZenDB.QueryBuilder().Where("details.metadata.active", #eq(#Bool(true)))) == #ok([(0, complete)]);

                        // Test null field handling
                        assert deep_optionals.search(ZenDB.QueryBuilder().Where("", #eq(#Null))) == #ok([(4, completelyNull)]);

                        // Retrieving records
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

                        let #ok(numeric_records) = zendb.create_collection<NumericRecord>(
                            "numeric_records",
                            NumericRecordSchema,
                            {
                                from_blob = func(blob : Blob) : ?NumericRecord = from_candid (blob);
                                to_blob = func(c : NumericRecord) : Blob = to_candid (c);
                            },
                            [],
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
                        assert numeric_records.search(ZenDB.QueryBuilder().Where("int_val", #eq(#Int(-100)))) == #ok([(0, testNumericRecord)]);
                        assert numeric_records.search(ZenDB.QueryBuilder().Where("nat8_val", #eq(#Nat8(8)))) == #ok([(0, testNumericRecord)]);
                        assert numeric_records.search(ZenDB.QueryBuilder().Where("int16_val", #eq(#Int16(-16)))) == #ok([(0, testNumericRecord)]);
                        assert numeric_records.search(ZenDB.QueryBuilder().Where("nat64_val", #eq(#Nat64(64)))) == #ok([(0, testNumericRecord)]);

                        assert numeric_records.get(id) == ?(testNumericRecord);
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

                        let #ok(extreme_nesting) = zendb.create_collection<ExtremeNesting>(
                            "extreme_nesting",
                            ExtremeNestingSchema,
                            {
                                from_blob = func(blob : Blob) : ?ExtremeNesting = from_candid (blob);
                                to_blob = func(c : ExtremeNesting) : Blob = to_candid (c);
                            },
                            [#Field("id", [#Min(1)]), #Field("metadata.name", [#MinSize(1)]), #Field("status.active.level", [#Min(1)]), #Field("data.key", [#MinSize(1)]), #Unique(["id"]), #Unique(["metadata.name"]), #Unique(["status.active.level"]), #Unique(["data.key"])],
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
                        assert extreme_nesting.search(
                            ZenDB.QueryBuilder().Where("metadata.name", #eq(#Text("Extreme Test")))
                        ) == #ok([(0, testInstance)]);

                        assert extreme_nesting.search(
                            ZenDB.QueryBuilder().Where("status", #eq(#Text("active")))
                        ) == #ok([(0, testInstance)]);

                        //! accessing elements nested in arrays not supported yet
                        // assert extreme_nesting.search(
                        //     ZenDB.QueryBuilder().Where("data.key", #eq(#Text("sensor1")))
                        // ) == #ok([(0, testInstance)]);

                        // assert extreme_nesting.search(
                        //     ZenDB.QueryBuilder().Where("data.values.measurement", #eq(#Text("simple")))
                        // ) == #ok([(0, testInstance)]);

                        assert extreme_nesting.get(id) == ?(testInstance);
                    },
                );

            },

        );

    },
);
