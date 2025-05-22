import Option "mo:base/Option";
import Array "mo:base/Array";

import Itertools "mo:itertools/Iter";

import ZenDB "../../src";

shared ({ caller = owner }) actor class Notes() {
    stable let zendb = ZenDB.newStableStore(null);
    let db = ZenDB.launchDefaultDB(zendb);

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

    let candify_notes : ZenDB.Types.Candify<Note> = {
        from_blob = func(blob : Blob) : ?Note { from_candid (blob) };
        to_blob = func(c : Note) : Blob { to_candid (c) };
    };

    let schema_constraints : [ZenDB.Types.SchemaConstraint] = [
        #Unique(["user_id", "title"]), // a user cannot have two notes with the same title
        #Field("title", [#MaxSize(100)]), // title must be <= 100 characters
        #Field("content", [#MaxSize(100_000)]), // content must be <= 100_000 characters
    ];

    let #ok(notes_collection) = db.createCollection<Note>("notes", NoteSchema, candify_notes, schema_constraints);

    public shared ({ caller = user_id }) func createNote(title : Text, content : Text) : async ZenDB.Types.Result<Nat, Text> {
        let note : Note = { user_id; title; content };
        notes_collection.insert(note);
    };

    public shared ({ caller = user_id }) func getNote(title : Text) : async ZenDB.Types.Result<Note, Text> {
        let response = notes_collection.search(
            ZenDB.QueryBuilder().Where(
                "user_id",
                #eq(#Principal(user_id)),
            ).And(
                "title",
                #eq(#Text(title)),
            )
        );

        let notes = switch (response) {
            case (#ok(notes)) notes;
            case (#err(msg)) return #err(msg);
        };

        if (notes.size() == 0) {
            return #err("Note not found");
        };

        let (note_id, note) = notes.get(0);

        #ok(note);

    };

    public shared ({ caller = user_id }) func updateNote(title : Text, content : Text) : async ZenDB.Types.Result<(), Text> {

        let notes_to_update_query = ZenDB.QueryBuilder().Where(
            "user_id",
            #eq(#Principal(user_id)),
        ).And(
            "title",
            #eq(#Text(title)),
        );

        let response = notes_collection.update(
            notes_to_update_query,
            [
                ("content", #Text(content)),
            ],
        );

        switch (response) {
            case (#ok(documents_updated)) assert documents_updated == 1;
            case (#err(msg)) return #err(msg);
        };

        #ok();

    };

    public shared ({ caller = user_id }) func deleteNote(title : Text) : async ZenDB.Types.Result<(), Text> {
        let response = notes_collection.delete(
            ZenDB.QueryBuilder().Where(
                "user_id",
                #eq(#Principal(user_id)),
            ).And(
                "title",
                #eq(#Text(title)),
            )
        );

        let deleted_notes = switch (response) {
            case (#ok(deleted_notes)) deleted_notes;
            case (#err(msg)) return #err(msg);
        };

        assert Itertools.all(
            deleted_notes.vals(),
            func((note_id, note) : (Nat, Note)) : Bool {
                note.user_id == user_id and note.title == title;
            },
        );

        #ok();

    };

    public shared ({ caller = user_id }) func getAllNotes(curr_page : Nat, opt_page_size : ?Nat) : async ZenDB.Types.Result<[Note], Text> {
        let page_size = Option.get(opt_page_size, 10);

        let response = notes_collection.search(
            ZenDB.QueryBuilder().Where(
                "user_id",
                #eq(#Principal(user_id)),
            ).Limit(
                page_size
            ).Skip(
                page_size * curr_page
            )
        );

        let notes = switch (response) {
            case (#ok(notes)) notes;
            case (#err(msg)) return #err(msg);
        };

        #ok(
            Array.map(
                notes,
                func((note_id, note) : (Nat, Note)) : Note {
                    note;
                },
            )
        );
    };

    public shared ({ caller = user_id }) func getNoteCount() : async ZenDB.Types.Result<Nat, Text> {
        let response = notes_collection.count(
            ZenDB.QueryBuilder().Where(
                "user_id",
                #eq(#Principal(user_id)),
            )
        );

        let count = switch (response) {
            case (#ok(count)) count;
            case (#err(msg)) return #err(msg);
        };

        #ok(count);
    };

    public shared ({ caller }) func getAllNotesCount() : async ZenDB.Types.Result<Nat, Text> {
        if (caller != owner) {
            return #err("Only the owner can call this function");
        };

        notes_collection.count(
            ZenDB.QueryBuilder()
        );

    };

};
