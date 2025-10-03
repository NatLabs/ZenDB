import Principal "mo:base@0.16.0/Principal";
import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Nat32 "mo:base@0.16.0/Nat32";
import Result "mo:base@0.16.0/Result";
import Order "mo:base@0.16.0/Order";
import Iter "mo:base@0.16.0/Iter";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Option "mo:base@0.16.0/Option";
import Hash "mo:base@0.16.0/Hash";
import Float "mo:base@0.16.0/Float";
import Int "mo:base@0.16.0/Int";

import T "../../Types";
import Logger "../../Logger";

import CollectionUtils "../CollectionUtils";

import Tokenizer "Tokenizers";
import CompositeIndex "CompositeIndex";

module TextIndex {

    public func new(
        collection : T.StableCollection,
        name : Text,
        field : Text,
        tokenizer : Tokenizer.Tokenizer,
    ) : T.TextIndex {

        let internal_index = CompositeIndex.new(
            collection,
            name,
            [] : [(Text, T.SortDirection)],
            false, // if true, the index is unique and the document ids are not concatenated with the index key values to make duplicate values appear unique
            false, // cannot be deleted
        );

        {
            internal_index;
            field;
            tokenizer;
        };

    };

    public func insert(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        id : T.DocumentId,
        text : Text,
    ) : T.Result<(), Text> {

        let tokens = Tokenizer.tokenize(text_index.tokenizer, text);

        var token_index : Nat = 0;
        for ((word, positions) in tokens.vals()) {
            for ((start, end) in positions.vals()) {
                let index_key_values : [T.CandidQuery] = [
                    #Text(word), // word comes first for better prefix compression
                    #Blob(id), // document_id second for better clustering by document
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
                        let error_msg = "InvertedTextIndex.insert(): Failed to insert word '" # word # "' at position " # debug_show (start) # " for document id " # debug_show (id) # ": " # msg;
                        return #err(error_msg);
                    };
                };
            };
        };

        #ok();

    };

    func get_text_from_candid_map(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        document_id : T.DocumentId,
        candid_map : T.CandidMap,
    ) : ?Text {

        switch (CollectionUtils.getIndexColumns(collection, [(text_index.field, #Ascending)], document_id, candid_map)) {
            case (?index_key_values) switch (index_key_values[0]) {
                case (#Text(text)) { ?text };
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

        switch (get_text_from_candid_map(collection, text_index, document_id, candid_map)) {
            case (?existing_text) {
                insert(collection, text_index, document_id, existing_text);
            };
            case (null) {
                return #err("InvertedTextIndex.insertWithCandidMap(): Failed to find existing text for document id " # debug_show (document_id) # " in the provided candid map");
            };
        };

    };

    public func remove(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        id : T.DocumentId,
        text : Text,
    ) : T.Result<(), Text> {

        let tokens = Tokenizer.tokenize(text_index.tokenizer, text);

        var token_index : Nat = 0;
        for ((word, positions) in tokens.vals()) {
            for ((start, end) in positions.vals()) {
                let index_key_values : [T.CandidQuery] = [
                    #Text(word), // word comes first for better prefix compression
                    #Blob(id), // document_id second for better clustering by document
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
                        let error_msg = "InvertedTextIndex.remove(): Failed to remove word '" # word # "' at position " # debug_show (start) # " for document id " # debug_show (id) # ": " # msg;
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

        switch (get_text_from_candid_map(collection, text_index, document_id, candid_map)) {
            case (?existing_text) {
                return remove(collection, text_index, document_id, existing_text);
            };
            case (null) {
                return #err("InvertedTextIndex.removeWithCandidMap(): Failed to find existing text for document id " # debug_show (document_id) # " in the provided candid map");
            };
        };

    };

    public func scan(
        collection : T.StableCollection,
        text_index : T.TextIndex,
        start_query : (Text, ?Nat, ?Nat, ?Nat, ?Nat),
        end_query : (Text, ?Nat, ?Nat, ?Nat, ?Nat),
    ) : (Nat, Nat) {

        let lower_bound : [T.CandidQuery] = [
            #Text(start_query.0),
            if (start_query.1 == null) #Minimum else #Nat(Option.get(start_query.1, 0)),
            if (start_query.2 == null) #Minimum else #Nat32(Nat32.fromNat(Option.get(start_query.2, 0))),
            if (start_query.3 == null) #Minimum else #Nat32(Nat32.fromNat(Option.get(start_query.3, 0))),
            if (start_query.4 == null) #Minimum else #Nat32(Nat32.fromNat(Option.get(start_query.4, 0))),
        ];

        let upper_bound : [T.CandidQuery] = [
            #Text(end_query.0),
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

};
