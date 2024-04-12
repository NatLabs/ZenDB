import Iter "mo:base/Iter";
import Debug "mo:base/Debug";
import Prelude "mo:base/Prelude";

import Bench "mo:bench";
import Fuzz "mo:fuzz";

import Lib  "../src";

module {
    public func init() : Bench.Bench {
        let bench = Bench.Bench();

        bench.name("Benchmarking the Lib.greet() function");
        bench.description("Benchmarking the performance with 10k calls");

        bench.rows(["Lib"]);
        bench.cols(["greet()"]);

        let fuzz = Fuzz.Fuzz();

        bench.runner(
            func(row, col) = switch (row, col) {

                case ("Lib", "greet()") {

                    for (i in Iter.range(0, 9_999)){
                        let text : Text = fuzz.text.randomText(10);

                        ignore Lib.greet(text);
                    }
                };

                case (_) {
                    Debug.trap("Should be unreachable:\n row = \"" # debug_show row # "\" and col = \"" # debug_show col # "\"");
                };
            }
        );

        bench;
    };
};
