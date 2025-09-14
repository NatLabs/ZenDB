import Array "mo:base@0.16.0/Array";
import Int "mo:base@0.16.0/Int";
import Random "mo:base@0.16.0/Random";

import ZenDB "mo:zendb";

persistent actor FlyingNinja {
    type LeaderboardEntry = {
        name : Text;
        score : Nat;
    };

    let LeaderboardEntrySchema : ZenDB.Schema = #Record([
        ("name", #Text),
        ("score", #Nat),
    ]);

    let candify : ZenDB.Candify<LeaderboardEntry> = {
        to_blob = func(data : LeaderboardEntry) : Blob = to_candid (data);
        from_blob = func(blob : Blob) : ?LeaderboardEntry = from_candid (blob);
    };

    stable let zendb = ZenDB.newStableState(null);
    let db = ZenDB.launchDefaultDB();

    let #ok(leaderboard) = db.createCollection(
        "leaderboard",
        LeaderboardEntrySchema,
        candify,
        ?{
            schemaConstraints = [#Unique("name")];
        },
    );

    let #ok(_) = leaderboard.createIndex("score_idx", [("score", #Descending)], null);

    // Returns if a certain score is good enough to warrant an entry on the leaderboard.
    public query func isHighScore(score : Nat) : async Bool {
        if (leaderboard.size() < 10) {
            return true;
        };
        // Whenever a new entry is added, the leaderboard is sorted.
        // We can safely assume that the last entry has the lowest score.

        let #ok(lowestScores) = leaderboard.search(
            ZenDB.QueryBuilder().Sort("score", #Ascending).Limit(1)
        );

        return score > lowestScores[0].score;
    };

    // Adds a new entry to the leaderboard if the score is good enough.
    public func addLeaderboardEntry(name : Text, score : Nat) : async [LeaderboardEntry] {
        let newEntry : LeaderboardEntry = { name; score };

        let #ok(_) = leaderboard.insert(newEntry);

        // Keep only the top 10 scores
        // delete the smallest score immediately, if the leaderboard is larger than 10
        if (leaderboard.size() > 10) {
            let #ok(_) = leaderboard.delete(
                ZenDB.QueryBuilder().Sort("score", #Ascending).Limit(1)
            );
        };

        return leaderboard;
    };

    // Returns the current leaderboard.
    public query func getLeaderboard() : async [LeaderboardEntry] {
        let #ok(entries) = leaderboard.search(
            ZenDB.QueryBuilder().Sort("score", #Descending)
        );

        return entries;
    };

    // Produces secure randomness as a seed to the game.
    public func getRandomness() : async Blob {
        await Random.blob();
    };
};
