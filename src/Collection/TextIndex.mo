import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Option "mo:base/Option";

import T "../Types";
import Logger "../Logger";

import CollectionUtils "Utils";
import Index "Index";

import Tokenizer "../Tokenizers";

module TextIndex {

    public type TextIndex = {
        index : T.Index;
        field : Text; // the field this index is on
        tokenizer : Tokenizer.Tokenizer; // the tokenizer used for this index
    };

    public func new(
        collection : T.StableCollection,
        name : Text,
        field : Text,
        tokenizer : Tokenizer.Tokenizer,
    ) : TextIndex {

        let index = Index.new(
            collection,
            name,
            [] : [(Text, T.SortDirection)],
            false, // if true, the index is unique and the document ids are not concatenated with the index key values to make duplicate values appear unique
            false, // cannot be deleted
        );

        {
            index;
            field;
            tokenizer;
        };

    };

    public func insert(
        collection : T.StableCollection,
        text_index : TextIndex,
        id : Nat,
        text : Text,
    ) : T.Result<(), Text> {

        let tokens = Tokenizer.tokenize(text_index.tokenizer, text);

        var token_index : Nat = 0;
        for ((word, positions) in tokens.vals()) {
            for ((start, end) in positions.vals()) {
                let index_key_values : [T.CandidQuery] = [
                    #Text(word), // word comes first for better prefix compression
                    #Nat(id), // document_id second for better clustering by document
                    #Nat32(Nat32.fromNat(token_index)), // token sequence position in document
                    #Nat32(Nat32.fromNat(start)), // character start position for highlighting
                    #Nat32(Nat32.fromNat(end)), // character end position for precise boundaries
                ];
                token_index += 1;

                switch (Index.insert(collection, text_index.index, id, index_key_values)) {
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

    public func remove(
        collection : T.StableCollection,
        text_index : TextIndex,
        id : Nat,
        text : Text,
    ) : T.Result<(), Text> {

        let tokens = Tokenizer.tokenize(text_index.tokenizer, text);

        var token_index : Nat = 0;
        for ((word, positions) in tokens.vals()) {
            for ((start, end) in positions.vals()) {
                let index_key_values : [T.CandidQuery] = [
                    #Text(word), // word comes first for better prefix compression
                    #Nat(id), // document_id second for better clustering by document
                    #Nat32(Nat32.fromNat(token_index)), // token sequence position in document
                    #Nat32(Nat32.fromNat(start)), // character start position for highlighting
                    #Nat32(Nat32.fromNat(end)), // character end position for precise boundaries
                ];
                token_index += 1;

                switch (Index.remove(collection, text_index.index, id, index_key_values)) {
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

    public func scan(
        collection : T.StableCollection,
        text_index : TextIndex,
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

        Index.scan_with_bounds(
            collection,
            text_index.index,
            lower_bound,
            upper_bound,
        );

    };

};
