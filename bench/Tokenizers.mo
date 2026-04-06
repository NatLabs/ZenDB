import Iter "mo:core@2.4/Iter";
import Debug "mo:core@2.4/Debug";
import Buffer "mo:base/Buffer";
import Text "mo:core@2.4/Text";
import Nat "mo:core@2.4/Nat";
import Nat64 "mo:core@2.4/Nat64";
import Char "mo:core@2.4/Char";

import Bench "mo:bench";
import Fuzz "mo:fuzz";

import BasicTokenizer "../src/EmbeddedInstance/Collection/Index/Tokenizers/BasicTokenizer";
import Runtime "mo:core@2.4/Runtime";

module {

    public func init() : Bench.Bench {
        let fuzz = Fuzz.fromSeed(0xdeadbeef);

        let bench = Bench.Bench();
        bench.name("Comparing Text Tokenizers");
        bench.description("Benchmarking the performance with 10k entries");

        bench.rows([
            "BasicTokenizer"
        ]);

        bench.cols([
            "tokenize() lorem ipsum",
            "tokenize() random text",
        ]);

        let limit = 10_000;

        let lorem_ipsum = fuzz.text.randomText(limit);
        let random_text = fuzz.text._random(10_000, func() : Char { Char.fromNat32(fuzz.nat32.randomRange(0, 1000)) });

        bench.runner(
            func(row, col) = switch (row, col) {
                case ("BasicTokenizer", "tokenize() lorem ipsum") {
                    ignore BasicTokenizer.tokenize(lorem_ipsum);
                };
                case ("BasicTokenizer", "tokenize() random text") {
                    ignore BasicTokenizer.tokenize(random_text);
                };
                case (_) {
                    Runtime.trap("Should not reach with row = " # debug_show row # " and col = " # debug_show col);
                };
            }
        );

        bench;
    };
};
