import { test; suite } "mo:test";
import Array "mo:core@2.4/Array";
import Migration "../../src/EmbeddedInstance/Migration";

suite(
    "Migration",
    func() {
        test(
            "runs bounded, retry-safe copy, verify, cutover, cleanup, and seal steps",
            func() {
                let state = Migration.newState<Nat>(1);
                let controller = Migration.Controller<Nat, Nat>(state);
                ignore controller.begin("users-v2", 2);

                let source = [10, 20, 30];
                var copied : [Nat] = [];
                let next = func(cursor : ?Nat, limit : Nat) : [(Nat, Nat)] {
                    let start = switch (cursor) { case null 0; case (?value) value + 1 };
                    var result : [(Nat, Nat)] = [];
                    var index = start;
                    while (index < source.size() and result.size() < limit) {
                        result := Array.concat(result, [(index, source[index])]);
                        index += 1;
                    };
                    result;
                };
                let copy = func(value : Nat) : Migration.Result<()> {
                    copied := Array.concat(copied, [value]);
                    #ok();
                };

                assert controller.copyStep(1, 2, next, copy).copied == 2;
                assert controller.copyStep(1, 2, next, copy).copied == 2;
                assert controller.copyStep(2, 2, next, copy).copied == 3;
                assert controller.copyStep(3, 2, next, copy).phase == #verifying;
                assert copied == [10, 20, 30];

                let verify = func(value : Nat) : Bool { value == copied[value / 10 - 1] };
                ignore controller.verifyStep(4, 2, next, verify);
                ignore controller.verifyStep(5, 2, next, verify);
                assert controller.verifyStep(6, 2, next, verify).phase == #readyToCutover;

                var cutOver = false;
                assert controller.commit(7, func() { cutOver := true }).activeGeneration == 2;
                assert cutOver;

                var removed : [Nat] = [];
                let remove = func(value : Nat) : Migration.Result<()> {
                    removed := Array.concat(removed, [value]);
                    #ok();
                };
                ignore controller.cleanupStep(8, 2, next, remove);
                ignore controller.cleanupStep(9, 2, next, remove);
                assert controller.cleanupStep(10, 2, next, remove).phase == #readyToSeal;
                assert removed == [10, 20, 30];
                assert controller.seal(11, func() : Bool { true }).phase == #sealed;
                controller.requireSealed();
            },
        );
    },
);
