import Principal "mo:core@2.4/Principal";
import Array "mo:core@2.4/Array";
import Debug "mo:core@2.4/Debug";
import Text "mo:core@2.4/Text";
import Char "mo:core@2.4/Char";
import Nat8 "mo:core@2.4/Nat8";
import Nat32 "mo:core@2.4/Nat32";
import Result "mo:core@2.4/Result";
import Order "mo:core@2.4/Order";
import Iter "mo:core@2.4/Iter";
import Buffer "mo:base@0.16/Buffer";
import Nat "mo:core@2.4/Nat";
import Option "mo:core@2.4/Option";
import Hash "mo:base@0.16/Hash";
import Float "mo:core@2.4/Float";
import Int "mo:core@2.4/Int";

import Map "mo:map@9.0/Map";

import T "../../Types";
import Logger "../../Logger";

import CollectionUtils "../CollectionUtils";

import Tokenizer "Tokenizers";
import CompositeIndex "CompositeIndex";
import Runtime "mo:core@2.4/Runtime";
import BTree "../../BTree";

module TextIndex {



    public func new(
        collection : T.StableCollection,
        name : Text,
        fields : [Text],
        tokenizer : Tokenizer.Tokenizer,
    ) : T.TextIndex {

        let internal_index = CompositeIndex.new(
            collection,
            name,
            [] : [(Text, T.SortDirection)], // not all the fields stored in the internal composite index are field values, some are metadata for the text index implementation such as token position and character offsets.
            false, // if true, the index is unique and the document ids are not concatenated with the index key values to make duplicate values appear unique
            false, // cannot be deleted
        );

        {
            internal_index;
            fields;
            tokenizer;
        };

    };

    public func deallocate(
        collection : T.StableCollection,
        text_index : T.TextIndex,
    ) {
        CompositeIndex.deallocate(collection, text_index.internal_index);
    };

    public func insert(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        field_idx : Nat,
        id : T.DocumentId,
        text : Text,
    ) : T.Result<(), Text> {

        let tokens = Tokenizer.tokenize(text_index.tokenizer, text);

        var token_index : Nat = 0;
        for ((word, positions) in tokens.vals()) {
            for ((start, end) in positions.vals()) {
                let index_key_values : [T.CandidQuery] = [
                    #Text(word), // word comes first for better prefix compression
                    #Nat8(Nat8.fromNat(field_idx)), // field index within the text_index.fields array (max 255 fields)
                    #Blob(id), // document_id for clustering by document
                    #Nat32(Nat32.fromNat(token_index)), // token sequence position in document
                    #Nat32(Nat32.fromNat(start)), // character start position for highlighting
                    #Nat32(Nat32.fromNat(end)), // character end position for precise boundaries
                ];
                token_index += 1;

                switch (CompositeIndex.insert(collection, text_index.internal_index, id, index_key_values)) {
                    case (#ok(())) {
                        // Insertion successful, continue
                    };
                    case (#err(msg)) {
                        let error_msg = "TextIndex.insert(): Failed to insert word '" # word # "' at position " # debug_show (start) # " for document id " # debug_show (id) # ": " # msg;
                        return #err(error_msg);
                    };
                };
            };
        };

        #ok();

    };

    func get_text_for_field(
        collection : T.StableCollection,
        field : Text,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : ?Text {

        switch (CollectionUtils.getIndexColumns(collection, [(field, #Ascending)], document_id, candid_map)) {
            case (?index_key_values) switch (index_key_values[0]) {
                case (#Text(text)) { ?text };
                case (_) { null };
            };
            case (null) { null };
        };

    };

    public func insertWithCandidMap(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : T.Result<(), Text> {

        var field_idx : Nat = 0;
        for (field in text_index.fields.vals()) {
            switch (get_text_for_field(collection, field, document_id, candid_map)) {
                case (?existing_text) {
                    switch (insert(collection, text_index, field_idx, document_id, existing_text)) {
                        case (#err(msg)) { return #err(msg) };
                        case (#ok(())) {};
                    };
                };
                case (null) {}; // field not present in document, skip
            };
            field_idx += 1;
        };

        #ok();

    };

    public func remove(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        field_idx : Nat,
        id : T.DocumentId,
        text : Text,
    ) : T.Result<(), Text> {

        let tokens = Tokenizer.tokenize(text_index.tokenizer, text);

        var token_index : Nat = 0;
        for ((word, positions) in tokens.vals()) {
            for ((start, end) in positions.vals()) {
                let index_key_values : [T.CandidQuery] = [
                    #Text(word), // word comes first for better prefix compression
                    #Nat8(Nat8.fromNat(field_idx)), // field index within the text_index.fields array (max 255 fields)
                    #Blob(id), // document_id for clustering by document
                    #Nat32(Nat32.fromNat(token_index)), // token sequence position in document
                    #Nat32(Nat32.fromNat(start)), // character start position for highlighting
                    #Nat32(Nat32.fromNat(end)), // character end position for precise boundaries
                ];
                token_index += 1;

                switch (CompositeIndex.remove(collection, text_index.internal_index, id, index_key_values)) {
                    case (#ok(())) {
                        // Removal successful, continue
                    };
                    case (#err(msg)) {
                        let error_msg = "TextIndex.remove(): Failed to remove word '" # word # "' at position " # debug_show (start) # " for document id " # debug_show (id) # ": " # msg;
                        return #err(error_msg);
                    };
                };
            };
        };

        #ok();

    };

    public func removeWithCandidMap(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : T.Result<(), Text> {

        var field_idx : Nat = 0;
        for (field in text_index.fields.vals()) {
            switch (get_text_for_field(collection, field, document_id, candid_map)) {
                case (?existing_text) {
                    switch (remove(collection, text_index, field_idx, document_id, existing_text)) {
                        case (#err(msg)) { return #err(msg) };
                        case (#ok(())) {};
                    };
                };
                case (null) {}; // field not present in document, skip
            };
            field_idx += 1;
        };

        #ok();

    };

    public func scan(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        field_idx : Nat,
        start_query : (Text, ?Nat, ?Nat, ?Nat, ?Nat),
        end_query : (Text, ?Nat, ?Nat, ?Nat, ?Nat),
    ) : (Nat, Nat) {

        let lower_bound : [T.CandidQuery] = [
            #Text(start_query.0),
            #Nat8(Nat8.fromNat(field_idx)),
            if (start_query.1 == null) #Minimum else #Nat(Option.get(start_query.1, 0)),
            if (start_query.2 == null) #Minimum else #Nat32(Nat32.fromNat(Option.get(start_query.2, 0))),
            if (start_query.3 == null) #Minimum else #Nat32(Nat32.fromNat(Option.get(start_query.3, 0))),
            if (start_query.4 == null) #Minimum else #Nat32(Nat32.fromNat(Option.get(start_query.4, 0))),
        ];

        let upper_bound : [T.CandidQuery] = [
            #Text(end_query.0),
            #Nat8(Nat8.fromNat(field_idx)),
            if (end_query.1 == null) #Maximum else #Nat(Option.get(end_query.1, 0)),
            if (end_query.2 == null) #Maximum else #Nat32(Nat32.fromNat(Option.get(end_query.2, 0))),
            if (end_query.3 == null) #Maximum else #Nat32(Nat32.fromNat(Option.get(end_query.3, 0))),
            if (end_query.4 == null) #Maximum else #Nat32(Nat32.fromNat(Option.get(end_query.4, 0))),
        ];

        CompositeIndex.scan_with_bounds(
            collection,
            text_index.internal_index,
            lower_bound,
            upper_bound,
        );

    };

    public func clear(
        collection : T.StableCollection,
        text_index : T.TextIndex,
    ) {
        CompositeIndex.clear(collection, text_index.internal_index);
    };

    public func get_matches_for_doc(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        field_idx : Nat,
        word : Text,
        doc_id : T.DocumentId,
    ) : [T.TextMatch] {
        let field_idx_n8 = Nat8.fromNat(field_idx);
        let lower_bound : [T.CandidQuery] = [#Text(word), #Nat8(field_idx_n8), #Blob(doc_id), #Minimum, #Minimum, #Minimum];
        let upper_bound : [T.CandidQuery] = [#Text(word), #Nat8(field_idx_n8), #Blob(doc_id), #Maximum, #Maximum, #Maximum];
        let interval = CompositeIndex.scan_with_bounds(collection, text_index.internal_index, lower_bound, upper_bound);
        let entries = CompositeIndex.from_interval_to_array(collection, text_index.internal_index, interval);
        let field = if (field_idx < text_index.fields.size()) text_index.fields[field_idx] else "";
        Array.map(
            entries,
            func((key_values, _) : ([T.CandidQuery], T.DocumentId)) : T.TextMatch {
                let stored_word = switch (key_values[0]) { case (#Text(w)) w; case (_) Runtime.trap("TextIndex: expected Text key") };
                let token_pos = switch (key_values[3]) { case (#Nat32(n)) Nat32.toNat(n); case (_) Runtime.trap("TextIndex: expected Nat32 token_pos") };
                let char_start = switch (key_values[4]) { case (#Nat32(n)) Nat32.toNat(n); case (_) Runtime.trap("TextIndex: expected Nat32 char_start") };
                let char_end = switch (key_values[5]) { case (#Nat32(n)) Nat32.toNat(n); case (_) Runtime.trap("TextIndex: expected Nat32 char_end") };
                { field; word = stored_word; token_pos; char_start; char_end };
            },
        );
    };

    /// Returns the tokenized words from `phrase_text` using the index's own tokenizer,
    /// in text-position order.  Used by phrase-search to enumerate constituent words.
    public func phrase_words(text_index : T.TextIndex, phrase_text : Text) : [Text] {
        let tokens = Tokenizer.tokenize(text_index.tokenizer, phrase_text);
        Array.map(tokens, func((w, _) : (Text, [(Nat, Nat)])) : Text { w });
    };

    /// Returns `?(index_name, field_idx)` if `field` is covered by this text index, else `null`.
    public func verifyIndexedField(text_index : T.TextIndex, field : Text) : ?(Text, Nat) {
        var i : Nat = 0;
        for (f in text_index.fields.vals()) {
            if (f == field) return ?(text_index.internal_index.name, i);
            i += 1;
        };
        null
    };

    /// Returns `IndexStats` for a text index.  The `fields` entry lists the
    /// user-visible field names (mapped to `#Ascending` as a placeholder
    /// direction, since sort order is not meaningful for text indexes).
    public func stats(
        text_index : T.TextIndex,
        collection_entries : Nat,
        hidden : Bool,
    ) : T.IndexStats {
        let internal = text_index.internal_index;
        let memory = BTree.getMemoryStats(internal.data);
        let index_entries = CompositeIndex.size(internal);
        {
            name = internal.name;
            fields = Array.map<Text, (Text, T.SortDirection)>(
                text_index.fields,
                func(f : Text) : (Text, T.SortDirection) { (f, #Ascending) },
            );
            entries = index_entries;
            memory;
            index_type = #text_index;
            is_unique = internal.is_unique;
            used_internally = internal.used_internally;
            hidden;
            avg_index_key_size = if (collection_entries == 0) 0 else (memory.keyBytes / collection_entries);
            total_index_key_size = memory.keyBytes;
            total_index_data_bytes = memory.dataBytes;
        };
    };

};
