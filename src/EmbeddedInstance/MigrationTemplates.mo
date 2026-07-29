/// Reusable callbacks for migrating one ZenDB collection into another.
///
/// These helpers deliberately leave the record conversion and target's source-id
/// field to the application. They cover the mechanical parts that must be
/// consistent in every bridge release: bounded stable scans, idempotent target
/// insertion, verification, and source cleanup.

import Collection "Collection";
import Buffer "mo:base@0.16/Buffer";
import Database "Database";
import Migration "Migration";
import Query "Query";
import T "Types";

module {
    public type Result<A> = { #ok : A; #err : Text };
    public type Item<Record> = (T.DocumentId, Record);

    /// Returns at most `limit` source documents strictly after `cursor` in
    /// document-id order. The scan seeks through ZenDB's B-tree; it neither
    /// materializes the source ids nor rescans an already migrated prefix.
    ///
    /// Freeze source writes before the first call. During cleanup, the prior
    /// cursor has been deleted, so the inclusive B-tree scan naturally resumes
    /// at the following id.
    public func nextFromCursor<Record>(
        source : Collection.Collection<Record>,
        cursor : ?T.DocumentId,
        limit : Nat,
    ) : [(T.DocumentId, Item<Record>)] {
        if (limit == 0) return [];
        let batch = Buffer.Buffer<(T.DocumentId, Item<Record>)>(0);
        label collect for ((id, record) in source.scan(cursor, null)) {
            // B-tree scans include the lower bound. Skip it while the prior
            // row still exists (copy and verification); it no longer exists
            // after cleanup, so the first result is already strictly newer.
            if (switch (cursor) { case (?last) id == last; case null false }) {
                continue collect;
            };
            batch.add((id, (id, record)));
            if (batch.size() == limit) break collect;
        };
        Buffer.toArray(batch);
    };

    /// Creates the target table if needed and begins the controller. This does
    /// no source scan, so starting a migration stays bounded even for very
    /// large collections. Freeze writes in the same update that calls `begin`.
    public func begin<Source, Target>(
        controller : Migration.Controller<T.DocumentId, Item<Source>>,
        database : Database.Database,
        targetName : Text,
        targetSchema : T.Schema,
        targetCandify : T.Candify<Target>,
        targetOptions : ?T.CreateCollectionOptions,
        migrationId : Text,
        targetGeneration : Nat,
    ) : Result<Migration.Progress> {
        switch (database.getCollection<Target>(targetName, targetCandify)) {
            case (#ok(_)) {};
            case (#err(_)) {
                switch (database.createCollection<Target>(targetName, targetSchema, targetCandify, targetOptions)) {
                    case (#ok(_)) {};
                    case (#err(message)) return #err(message);
                };
            };
        };

        #ok(controller.begin(migrationId, targetGeneration));
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

    /// Deletes an absent source row successfully, making cleanup retries
    /// idempotent. The controller check is required even when this function is
    /// called from an application-facing endpoint: source data must not be
    /// removed before copy, verification, and cutover have completed.
    public func removeSource<Cursor, Record>(
        controller : Migration.Controller<Cursor, Item<Record>>,
        source : Collection.Collection<Record>,
        item : Item<Record>,
    ) : Result<()> {
        controller.requireCleaning();
        // A missing row means a previously committed cleanup request is being
        // retried. Do not swallow errors from an attempted delete: it may have
        // failed while maintaining an index, and accepting it would let the
        // migration commit a corrupted source collection.
        switch (source.get(item.0)) {
            case null return #ok();
            case (?_) {};
        };
        switch (source.deleteById(item.0)) {
            case (#ok(_)) #ok();
            case (#err(message)) #err(message);
        };
    };
};
