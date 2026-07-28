/// Reusable callbacks for migrating one ZenDB collection into another.
///
/// These helpers deliberately leave the record conversion and target's source-id
/// field to the application. They cover the mechanical parts that must be
/// consistent in every bridge release: stable source snapshots, bounded reads,
/// idempotent target insertion, verification, and source cleanup.

import Collection "Collection";
import Database "Database";
import Migration "Migration";
import Query "Query";
import T "Types";

module {
    public type Result<A> = { #ok : A; #err : Text };
    public type Item<Record> = (T.DocumentId, Record);

    /// Capture the source ids before copying. Keep this array in stable actor
    /// state; it is the deterministic source for the Nat cursor used below.
    public func snapshotIds<Record>(source : Collection.Collection<Record>) : [T.DocumentId] {
        var ids : [T.DocumentId] = [];
        for ((id, _) in source.entries()) ids := ids # [id];
        ids;
    };

    /// Returns at most `limit` source documents after `cursor`. Missing ids are
    /// skipped, which makes the same callback suitable for cleanup retries.
    public func nextFromSnapshot<Record>(
        source : Collection.Collection<Record>,
        sourceIds : [T.DocumentId],
        cursor : ?Nat,
        limit : Nat,
    ) : [(Nat, Item<Record>)] {
        let start = switch (cursor) { case null 0; case (?index) index + 1 };
        var batch : [(Nat, Item<Record>)] = [];
        var index = start;
        while (index < sourceIds.size() and batch.size() < limit) {
            let id = sourceIds[index];
            switch (source.get(id)) {
                case (?record) batch := batch # [(index, (id, record))];
                case null {};
            };
            index += 1;
        };
        batch;
    };

    /// Creates the target table if needed and begins the controller. On the
    /// first call it snapshots source ids; retries retain `existingSourceIds`.
    /// Store that array in stable state with the controller.
    public func begin<Source, Target>(
        controller : Migration.Controller<Nat, Item<Source>>,
        source : Collection.Collection<Source>,
        existingSourceIds : [T.DocumentId],
        database : Database.Database,
        targetName : Text,
        targetSchema : T.Schema,
        targetCandify : T.Candify<Target>,
        targetOptions : ?T.CreateCollectionOptions,
        migrationId : Text,
        targetGeneration : Nat,
    ) : Result<{ progress : Migration.Progress; sourceIds : [T.DocumentId] }> {
        switch (database.getCollection<Target>(targetName, targetCandify)) {
            case (#ok(_)) {};
            case (#err(_)) {
                switch (database.createCollection<Target>(targetName, targetSchema, targetCandify, targetOptions)) {
                    case (#ok(_)) {};
                    case (#err(message)) return #err(message);
                };
            };
        };

        let sourceIds = switch (controller.getProgress().phase) {
            case (#idle) snapshotIds(source);
            case (_) existingSourceIds;
        };
        #ok({ progress = controller.begin(migrationId, targetGeneration); sourceIds });
    };

    /// Inserts a transformed source item once. The target must have a field that
    /// stores its old ZenDB document id, preferably protected by `#Unique`.
    public func copyBySourceId<Source, Target>(
        target : Collection.Collection<Target>,
        sourceIdField : Text,
        item : Item<Source>,
        transform : Item<Source> -> Target,
    ) : Result<()> {
        let sourceId = item.0;
        let #ok(matches) = target.search(
            Query.QueryBuilder().Where(sourceIdField, #eq(#Blob(sourceId)))
        ) else return #err("could not look up the copied source id");

        if (matches.documents.size() == 0) {
            switch (target.insert(transform(item))) {
                case (#ok(_)) #ok();
                case (#err(message)) #err(message);
            };
        } else if (matches.documents.size() == 1) {
            #ok();
        } else {
            #err("target has conflicting rows for a source id");
        };
    };

    /// Confirms that exactly one target row exists and satisfies `matches`.
    public func verifyBySourceId<Source, Target>(
        target : Collection.Collection<Target>,
        sourceIdField : Text,
        item : Item<Source>,
        matches : (Item<Source>, Target) -> Bool,
    ) : Bool {
        switch (target.search(Query.QueryBuilder().Where(sourceIdField, #eq(#Blob(item.0))))) {
            case (#ok(result)) result.documents.size() == 1 and matches(item, result.documents[0].1);
            case (#err(_)) false;
        };
    };

    /// Deleting an absent source row is considered successful, so this callback
    /// remains idempotent when cleanup is retried after an uncertain response.
    public func removeSource<Record>(
        source : Collection.Collection<Record>,
        item : Item<Record>,
    ) : Result<()> {
        switch (source.deleteById(item.0)) {
            case (#ok(_)) #ok();
            case (#err(_)) #ok();
        };
    };
};
