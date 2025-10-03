import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Char "mo:base/Char";

import { test; suite } "mo:test";
import Fuzz "mo:fuzz";

import BasicTokenizer "../../../src/Collection/Index/Tokenizers/BasicTokenizer";

let fuzz = Fuzz.fromSeed(0x12345678);

let lorem_ipsum = fuzz.text.randomText(10_000);
let random_text = fuzz.text._random(10_000, func() : Char { Char.fromNat32(fuzz.nat32.randomRange(0, 1000)) });

suite(
    "BasicTokenizer",
    func() {

        test(
            "tokenize simple text",
            func() {
                let text = "Hello, world!";
                let tokens = BasicTokenizer.tokenize(text);

                assert (tokens.size() == 2);

                assert tokens[0] == ("hello", [(0, 5)]);
                assert tokens[1] == ("world", [(7, 12)]);
            },
        );

        test(
            "tokenize text with multiple occurrences",
            func() {
                let text = "Test., test?, TEST!";
                let tokens = BasicTokenizer.tokenize(text);

                assert (tokens.size() == 1);

                assert tokens[0] == ("test", [(0, 4), (7, 11), (14, 18)]);
            },
        );

        test(
            "tokenize text with various delimiters",
            func() {
                let text = "Hello\tworld\nThis is a test.";
                let tokens = BasicTokenizer.tokenize(text);

                assert (tokens.size() == 6);

                assert tokens[0] == ("hello", [(0, 5)]);
                assert tokens[1] == ("world", [(6, 11)]);
                assert tokens[2] == ("this", [(12, 16)]);
                assert tokens[3] == ("is", [(17, 19)]);
                assert tokens[4] == ("a", [(20, 21)]);
                assert tokens[5] == ("test", [(22, 26)]);
            },
        );

        test(
            "tokenize empty text",
            func() {
                let text = "";
                let tokens = BasicTokenizer.tokenize(text);

                assert (tokens.size() == 0);
            },
        );

        test(
            "tokenize text with no delimiters",
            func() {
                let text = "Nodelimitershere";
                let tokens = BasicTokenizer.tokenize(text);

                assert (tokens.size() == 1);

                assert tokens[0] == ("nodelimitershere", [(0, 16)]);
            },
        );

        test(
            "tokenize text with a single character",
            func() {
                let text = "A";
                let tokens = BasicTokenizer.tokenize(text);

                assert (tokens.size() == 1);
                assert tokens[0] == ("a", [(0, 1)]);
            },
        );

        test(
            "tokenize text with special characters",
            func() {
                let text = "Hello! @world #test $%^&*()";
                let tokens = BasicTokenizer.tokenize(text);

                assert (tokens.size() == 3);

                assert tokens[0] == ("hello", [(0, 5)]);
                assert tokens[1] == ("world", [(8, 13)]);
                assert tokens[2] == ("test", [(15, 19)]);
            },
        );

        test(
            "tokenize text with numbers",
            func() {
                let text = "Test123, test456! $45.78";
                let tokens = BasicTokenizer.tokenize(text);

                assert tokens[0] == ("test123", [(0, 7)]);
                assert tokens[1] == ("test456", [(9, 16)]);
                assert tokens[2] == ("45", [(19, 21)]);
                assert tokens[3] == ("78", [(22, 24)]);

            },
        );

        test(
            "tokenize lorem ipsum",
            func() {
                let tokens = BasicTokenizer.tokenize(lorem_ipsum);
                Debug.print("Tokens from lorem ipsum: " # debug_show tokens);
            },
        );

        test(
            "tokenize random text",
            func() {
                let tokens = BasicTokenizer.tokenize(random_text);
                Debug.print("Tokens from random text: " # debug_show tokens);
            },
        );

    },
);
