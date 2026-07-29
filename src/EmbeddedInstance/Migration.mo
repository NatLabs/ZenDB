import Runtime "mo:core@2.4/Runtime";

/// A resumable, two-generation migration state machine.
///
/// Keep a `State` in stable actor state and use this controller from *update*
/// methods only.  None of the callbacks supplied to a step may `await`: an
/// error is converted to a trap so the cursor and every write in that message
/// roll back together.
module {
    public type Phase = {
        #idle;
        #copying;
        #verifying;
        #readyToCutover;
        #cleaning;
        #readyToSeal;
        #sealed;
    };

    /// `lastCompletedStep` makes externally-driven calls retry-safe.  Invoke a
    /// new operation with the next number; retry the same number after an
    /// uncertain response and it becomes a no-op.
    public type State<Cursor> = {
        var migrationId : Text;
        var phase : Phase;
        var activeGeneration : Nat;
        var targetGeneration : Nat;
        var copyCursor : ?Cursor;
        var verifyCursor : ?Cursor;
        var cleanupCursor : ?Cursor;
        var copied : Nat;
        var verified : Nat;
        var cleaned : Nat;
        var lastCompletedStep : Nat;
    };

    public type Progress = {
        migrationId : Text;
        phase : Phase;
        activeGeneration : Nat;
        targetGeneration : Nat;
        copied : Nat;
        verified : Nat;
        cleaned : Nat;
        lastCompletedStep : Nat;
    };

    public type Result<T> = { #ok : T; #err : Text };

    public func newState<Cursor>(activeGeneration : Nat) : State<Cursor> = {
        var migrationId = "";
        var phase = #idle;
        var activeGeneration;
        var targetGeneration = activeGeneration;
        var copyCursor = null;
        var verifyCursor = null;
        var cleanupCursor = null;
        var copied = 0;
        var verified = 0;
        var cleaned = 0;
        var lastCompletedStep = 0;
    };

    /// A cursor belongs to the item just processed. `next` must return items
    /// strictly after the supplied cursor, in a deterministic order.
    public class Controller<Cursor, Item>(state : State<Cursor>) {
        func progress() : Progress = {
            migrationId = state.migrationId;
            phase = state.phase;
            activeGeneration = state.activeGeneration;
            targetGeneration = state.targetGeneration;
            copied = state.copied;
            verified = state.verified;
            cleaned = state.cleaned;
            lastCompletedStep = state.lastCompletedStep;
        };

        func checkStep(step : Nat) : Bool {
            if (step <= state.lastCompletedStep) return false;
            if (step != state.lastCompletedStep + 1) {
                Runtime.trap("Migration steps must be consecutive; expected " # debug_show (state.lastCompletedStep + 1) # ", got " # debug_show step);
            };
            true;
        };

        func finishStep(step : Nat) { state.lastCompletedStep := step };

        func requirePhase(phase : Phase) {
            if (state.phase != phase) {
                Runtime.trap("Migration step is invalid in phase " # debug_show state.phase);
            };
        };

        func requirePositiveLimit(limit : Nat) {
            if (limit == 0) Runtime.trap("Migration batch limit must be positive");
        };

        func requireBoundedBatch(batchSize : Nat, limit : Nat) {
            if (batchSize > limit) Runtime.trap("Migration next callback returned more items than the requested batch limit");
        };

        /// Starts a new migration. Repeating a successfully started migration
        /// with the same id is safe; a different plan is rejected.
        public func begin(migrationId : Text, targetGeneration : Nat) : Progress {
            if (state.phase != #idle) {
                if (state.migrationId == migrationId and state.targetGeneration == targetGeneration) return progress();
                Runtime.trap("A migration is already active or sealed");
            };
            if (migrationId == "") Runtime.trap("Migration id must not be empty");
            if (targetGeneration <= state.activeGeneration) Runtime.trap("Target generation must be newer than the active generation");

            state.migrationId := migrationId;
            state.targetGeneration := targetGeneration;
            state.copyCursor := null;
            state.verifyCursor := null;
            state.cleanupCursor := null;
            state.copied := 0;
            state.verified := 0;
            state.cleaned := 0;
            state.lastCompletedStep := 0;
            state.phase := #copying;
            progress();
        };

        public func getProgress() : Progress { progress() };

        /// Allows cleanup callbacks to prove that cutover has completed.
        ///
        /// Keep this check in the controller rather than duplicating phase
        /// checks in application endpoints: an endpoint that accidentally
        /// exposes a source-removal callback must not be able to delete old
        /// data while the migration is still copying or verifying it.
        public func requireCleaning() {
            requirePhase(#cleaning);
        };

        /// Copies at most `limit` source items. `copy` must be idempotent
        /// (normally an upsert with the source document id).
        public func copyStep(
            step : Nat,
            limit : Nat,
            next : (?Cursor, Nat) -> [(Cursor, Item)],
            copy : Item -> Result<()>,
        ) : Progress {
            if (not checkStep(step)) return progress();
            requirePhase(#copying);
            requirePositiveLimit(limit);

            let batch = next(state.copyCursor, limit);
            requireBoundedBatch(batch.size(), limit);
            if (batch.size() == 0) {
                state.phase := #verifying;
            } else {
                for ((_, item) in batch.vals()) {
                    switch (copy(item)) {
                        case (#ok(())) {};
                        case (#err(message)) Runtime.trap("Migration copy failed: " # message);
                    };
                };
                state.copyCursor := ?batch[batch.size() - 1].0;
                state.copied += batch.size();
            };
            finishStep(step);
            progress();
        };

        /// Verifies the copied representation in bounded batches. A mismatch
        /// traps, preserving the existing bridge and leaving its cursor at the
        /// previously verified item.
        public func verifyStep(
            step : Nat,
            limit : Nat,
            next : (?Cursor, Nat) -> [(Cursor, Item)],
            verify : Item -> Bool,
        ) : Progress {
            if (not checkStep(step)) return progress();
            requirePhase(#verifying);
            requirePositiveLimit(limit);

            let batch = next(state.verifyCursor, limit);
            requireBoundedBatch(batch.size(), limit);
            if (batch.size() == 0) {
                state.phase := #readyToCutover;
            } else {
                for ((_, item) in batch.vals()) {
                    if (not verify(item)) Runtime.trap("Migration verification failed");
                };
                state.verifyCursor := ?batch[batch.size() - 1].0;
                state.verified += batch.size();
            };
            finishStep(step);
            progress();
        };

        /// Atomically makes the target generation authoritative. Application
        /// writes must be frozen before copying, or dual-written by `commit`.
        public func commit(step : Nat, cutover : () -> ()) : Progress {
            if (not checkStep(step)) return progress();
            requirePhase(#readyToCutover);
            cutover();
            state.activeGeneration := state.targetGeneration;
            state.phase := #cleaning;
            finishStep(step);
            progress();
        };

        /// Deletes old-generation data incrementally after cutover. `remove`
        /// must be idempotent, so retrying an interrupted execution is safe.
        public func cleanupStep(
            step : Nat,
            limit : Nat,
            next : (?Cursor, Nat) -> [(Cursor, Item)],
            remove : Item -> Result<()>,
        ) : Progress {
            if (not checkStep(step)) return progress();
            requirePhase(#cleaning);
            requirePositiveLimit(limit);

            let batch = next(state.cleanupCursor, limit);
            requireBoundedBatch(batch.size(), limit);
            if (batch.size() == 0) {
                state.phase := #readyToSeal;
            } else {
                for ((_, item) in batch.vals()) {
                    switch (remove(item)) {
                        case (#ok(())) {};
                        case (#err(message)) Runtime.trap("Migration cleanup failed: " # message);
                    };
                };
                state.cleanupCursor := ?batch[batch.size() - 1].0;
                state.cleaned += batch.size();
            };
            finishStep(step);
            progress();
        };

        /// Logical removal is deliberately separate from physical cleanup.
        /// Call this only when the old generation is empty. A final Wasm may
        /// then remove the old stable field without iterating tables.
        public func seal(step : Nat, oldGenerationIsEmpty : () -> Bool) : Progress {
            if (not checkStep(step)) return progress();
            requirePhase(#readyToSeal);
            if (not oldGenerationIsEmpty()) Runtime.trap("Cannot seal migration while old generation is not empty");
            state.phase := #sealed;
            finishStep(step);
            progress();
        };

        /// Use this in the final upgrade migration before dropping old fields.
        public func requireSealed() {
            if (state.phase != #sealed) Runtime.trap("Final upgrade requires a sealed migration");
        };
    };
};
