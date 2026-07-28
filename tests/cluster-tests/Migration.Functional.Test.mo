// @testmode replica
//
// This test runs on PocketIC and creates MigrationExample as a separate local
// canister.  It is deliberately written as an end-to-end bridge-release
// example: copy, verify, cut over, clean up, then seal.

import { test; suite } "mo:test/async";

import MigrationExample "MigrationExample";

persistent actor {
    transient let TRILLION = 1_000_000_000_000;

    public func runTests() : async () {
        // This is a real canister-to-canister call, not an in-process unit test.
        let app = await (with cycles = 2 * TRILLION) MigrationExample.MigrationExample();
        await app.installLegacyExampleData();

        await suite(
            "Migration functional example",
            func() : async () {
                test(
                    "moves a stable generation through copy, verify, cutover, cleanup, and seal",
                    func() : async () {
                        let started = await app.beginMigration();
                        assert started.phase == #copying;
                        assert await app.writesAreFrozen();

                        // Copy in bounded batches.  Repeating step 1 models an
                        // uncertain client response and must not copy twice.
                        assert (await app.copyNext(1, 2)).copied == 2;
                        assert (await app.copyNext(1, 2)).copied == 2;
                        assert (await app.copyNext(2, 2)).copied == 3;
                        assert (await app.copyNext(3, 2)).phase == #verifying;

                        // Verify the target before it becomes authoritative.
                        ignore await app.verifyNext(4, 2);
                        ignore await app.verifyNext(5, 2);
                        assert (await app.verifyNext(6, 2)).phase == #readyToCutover;

                        // Commit changes the public read path atomically.
                        let committed = await app.commitMigration(7);
                        assert committed.activeGeneration == 2;
                        assert await app.activeGenerationSize() == 3;
                        assert await app.hasActiveUser("Ada Lovelace");
                        assert await app.hasActiveUser("Grace Hopper");
                        assert await app.hasActiveUser("Edsger Dijkstra");

                        // Cleanup happens after cutover and is bounded too.
                        ignore await app.cleanUpNext(8, 2);
                        ignore await app.cleanUpNext(9, 2);
                        assert (await app.cleanUpNext(10, 2)).phase == #readyToSeal;
                        assert await app.oldGenerationSize() == 0;

                        // Only now may a final Wasm drop the legacy stable field.
                        assert (await app.sealMigration(11)).phase == #sealed;
                        await app.requireSealedForFinalUpgrade();
                    },
                );
            },
        );
    };
};
