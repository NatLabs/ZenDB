// A separate orchestration canister. It never touches ZenDB tables directly:
// every bounded step executes inside MigrationExample, where its stable state
// and the table writes can commit atomically in one update message.

import MigrationExample "MigrationExample";
import Principal "mo:core@2.4/Principal";

persistent actor class MigrationRunner(appId : Principal) {
    // Persisting a principal keeps the migrator's target available after its
    // own upgrade; the actor reference can then be reconstructed cheaply.
    var appId_ = appId;
    transient let app : MigrationExample.MigrationExample = actor (Principal.toText(appId_));

    /// A production migrator would persist its own job queue and call one of
    /// these steps per timer/heartbeat. Keeping the calls explicit here makes
    /// the complete bridge-release protocol easy to follow.
    public func migrateUsers() : async () {
        ignore await app.beginMigration();

        // Retrying this update is intentional: Migration.Controller makes the
        // already completed step a no-op after an uncertain response.
        ignore await app.copyNext(1, 2);
        ignore await app.copyNext(1, 2);
        ignore await app.copyNext(2, 2);
        ignore await app.copyNext(3, 2);

        ignore await app.verifyNext(4, 2);
        ignore await app.verifyNext(5, 2);
        ignore await app.verifyNext(6, 2);
        ignore await app.commitMigration(7);

        ignore await app.cleanUpNext(8, 2);
        ignore await app.cleanUpNext(9, 2);
        ignore await app.cleanUpNext(10, 2);
        ignore await app.sealMigration(11);
        await app.requireSealedForFinalUpgrade();
    };
};
