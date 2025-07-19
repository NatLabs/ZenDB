import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";

import T "../Types";
import Logger "../Logger";

import CollectionUtils "Utils";
import Index "Index";

module {

    public type InvertedTextIndex = T.Index and {
        field : Text; // the field this index is on
    };

    public func new(
        collection : T.StableCollection,
        name : Text,
        field : Text,
        used_internally : Bool, // cannot be deleted by user if true
    ) : InvertedTextIndex {

        let index : InvertedTextIndex = {
            name;
            field;
            key_details = [];
            data = CollectionUtils.newBtree(collection);
            used_internally;
            is_unique = false; // inverted text indexes are never unique on a single text value
        };

        index;

    };

    func split_into_words_and_pos(text : Text) : T.Iter<(Text, pos : Nat32)> {
        // Split the text into words based on whitespace and punctuation

        var pos : Nat32 = 0;

        Iter.map<Text, (Text, Nat32)>(
            Text.split(
                text,
                #predicate(
                    func(c : Char) : Bool {
                        pos += 1;
                        Char.isAlphabetic(c) or Char.isDigit(c) or c == '_' or c == '-';
                    }
                ),
            ),
            func(word : Text) : (Text, Nat32) {
                (word, pos - Nat32.fromNat(word.size()));
            },
        );

    };

    public func insert(
        collection : T.StableCollection,
        index : InvertedTextIndex,
        id : Nat,
        text : Text,
    ) : T.Result<(), Text> {

        let words_and_pos = split_into_words_and_pos(text);

        for ((word, pos) in words_and_pos) {

            let index_key_values = [
                #Text(Text.toLowercase(word)), // normalize to lowercase
                #Nat32(pos), // store the position of the word in the text
                #Nat(id), // store the document id
            ];

            switch (Index.insert(collection, index, id, index_key_values)) {
                case (#ok(())) {
                    // Insertion successful, continue
                };
                case (#err(msg)) {
                    let error_msg = "InvertedTextIndex.insert(): Failed to insert word '" # word # "' at position " # debug_show (pos) # " for document id " # debug_show (id) # ": " # msg;
                    return #err(error_msg);
                };
            };

        };

        #ok();

    };

    public func remove(
        collection : T.StableCollection,
        index : InvertedTextIndex,
        id : Nat,
        text : Text,
    ) : T.Result<(), Text> {

        let words_and_pos = split_into_words_and_pos(text);

        for ((word, pos) in words_and_pos) {

            let index_key_values = [
                #Text(Text.toLowercase(word)), // normalize to lowercase
                #Nat32(pos), // store the position of the word in the text
                #Nat(id), // store the document id
            ];

            switch (Index.remove(collection, index, id, index_key_values)) {
                case (#ok(())) {
                    // Removal successful, continue
                };
                case (#err(msg)) {
                    let error_msg = "InvertedTextIndex.remove(): Failed to remove word '" # word # "' at position " # debug_show (pos) # " for document id " # debug_show (id) # ": " # msg;
                    return #err(error_msg);
                };
            };

        };

        #ok();

    };

};
