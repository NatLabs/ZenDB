// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import { test; suite } "mo:test";

import Lib "../src";

suite(
    "Lib Test",
    func() {
        test(
            "greet() fn works",
            func() {
                assert Lib.greet("world") == "Hello, world!"
            },
        );
    },
);
