import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Char "mo:base/Char";
import Debug "mo:base/Debug";

import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Int "mo:base/Int";

import Map "mo:map/Map";
import Itertools "mo:itertools/Iter";
import BufferDeque "mo:buffer-deque/BufferDeque";

module BasicTokenizer {

    let delimeters = " \t\n\r,;:!?.\"'()[]{}<>/\\|`~@#$%^&*+=-_";

    let delimeters_not_registered = "\u{000C}\u{000B}"; // "\f" and "\v"

    func is_delimeter(c : Char) : Bool {
        Text.contains(delimeters, #char(c)) or Text.contains(delimeters_not_registered, #char(c));
    };

    func map_entry<K, V>(map : Map.Map<K, V>, key_hash : Map.HashUtils<K>, key : K, default_value : V) : V {
        switch (Map.get(map, key_hash, key)) {
            case (?value) value;
            case (null) {
                ignore Map.put(map, key_hash, key, default_value);
                default_value;
            };
        };
    };

    public type Token = (Text, [(start : Nat, end : Nat)]);

    public func tokenize(raw_text : Text) : [Token] {
        let lowercase_text = Text.toLowercase(raw_text);

        var counter : Int = -1;
        let token_positions = BufferDeque.BufferDeque<(Nat, Nat)>((lowercase_text.size() / 5) + 8);

        var token_start_pos : Int = 0;
        var prev_char_is_delimeter = true;

        let delimeter_pattern = func(c : Char) : Bool {
            counter += 1;
            let curr_char_is_delimeter = is_delimeter(c);

            if (prev_char_is_delimeter and not curr_char_is_delimeter) {
                token_start_pos := counter;
            } else if (not prev_char_is_delimeter and curr_char_is_delimeter) {
                token_positions.addBack(Int.abs(token_start_pos), Int.abs(counter));
            };

            if (not curr_char_is_delimeter and counter == (lowercase_text.size() - 1)) {
                token_positions.addBack(Int.abs(token_start_pos), lowercase_text.size());
            };

            prev_char_is_delimeter := curr_char_is_delimeter;

            curr_char_is_delimeter;
        };

        let token_map = Map.new<Text, Buffer.Buffer<(Nat, Nat)>>();
        let tokens_iterator = Text.split(lowercase_text, #predicate(delimeter_pattern));

        for (token in tokens_iterator) {
            // Debug.print("tokens_iterator: " # token);

            if (token != "") {
                let positions = map_entry(token_map, Map.thash, token, Buffer.Buffer<(Nat, Nat)>(8));
                let ?token_position = token_positions.popFront() else {
                    Debug.trap("Unexpected end of token positions while tokenizing");
                };

                // Debug.print(debug_show (token_position));
                positions.add(token_position);

            };
        };

        Array.tabulate<Token>(
            Map.size(token_map),
            func(i : Nat) : Token {

                let entry = switch (Map.popFront(token_map, Map.thash)) {
                    case (?entry) entry;
                    case (null) Debug.trap("Unexpected end of map while tokenizing");
                };

                (entry.0, Buffer.toArray(entry.1));
            },
        );

    };
};
