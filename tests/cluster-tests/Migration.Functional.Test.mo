// @testmode replica
//
// This test runs on PocketIC and creates MigrationExample as a separate local
// canister.  It is deliberately written as an end-to-end bridge-release
// example: copy, verify, cut over, clean up, then seal.

import { test; suite } "mo:test/async";
import Principal "mo:core@2.4/Principal";

import MigrationExample "MigrationExample";
import MigrationRunner "MigrationRunner";

persistent actor {
    transient let TRILLION = 1_000_000_000_000;

    public func runTests() : async () {
        // The application being migrated and the migrator are distinct local
        // canisters. The runner only orchestrates; data operations remain in the
        // application canister so every callback stays non-awaiting and atomic.
        let app = await (with cycles = 2 * TRILLION) MigrationExample.MigrationExample();
        await app.installLegacyExampleData();
        let migrator = await (with cycles = 2 * TRILLION) MigrationRunner.MigrationRunner(Principal.fromActor(app));

        await suite(
            "Migration functional example",
            func() : async () {
                test(
                    "moves a stable generation through copy, verify, cutover, cleanup, and seal",
                    func() : async () {
                        await migrator.migrateUsers();
                        let finished = await app.progress();
                        assert finished.phase == #sealed;
                        assert await app.writesAreFrozen();
                        assert finished.activeGeneration == 2;
                        assert await app.activeGenerationSize() == 3;
                        assert await app.hasActiveUser("Ada Lovelace");
                        assert await app.hasActiveUser("Grace Hopper");
                        assert await app.hasActiveUser("Edsger Dijkstra");
                        assert await app.oldGenerationSize() == 0;
                    },
                );
            },
        );
    };
};
