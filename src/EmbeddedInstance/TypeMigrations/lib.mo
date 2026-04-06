import Blob "mo:core@2.4/Blob";
import Debug "mo:core@2.4/Debug";

import V0_ "v0*";
import V1_ "v1*";
import V2_ "v2*";
import Runtime "mo:core@2.4/Runtime";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    public let V0 = V0_;
    public let V1 = V1_;
    public let V2 = V2_;

    public type StableStore = V2_.StableStore;

    public type VersionedStableStore = {
        #v0 : V0_.VersionedStableStore;
        #v1 : V1_.VersionedStableStore;
        #v2 : V2_.VersionedStableStore;
    };

    /// Required for actor migrations if the type is no longer compatible with future versions
    public type PrevVersionedStableStore = {
        #v0 : V0_.PrevVersionedStableStore;
        #v1 : V1_.PrevVersionedStableStore;
        #v2 : V2_.PrevVersionedStableStore;
    };

    public func upgrade(versions : PrevVersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v0(v0)) {
                Runtime.trap("Cannot upgrade from " # V0_.to_text(v0) # ". This version requires manual data migration to v1.0.0 due to breaking changes (DocumentId: Nat → Blob, BitMap changes). No automatic upgrade path available.");
            };
            case (#v1(v1)) {
                Runtime.trap("Cannot upgrade from " # V1_.to_text(v1) # ". This version requires manual data migration to v2.0.0 due to breaking changes in index encoding (Orchid: prefix-based → lexicographic escape-based encoding). Indexes must be rebuilt. No automatic upgrade path available.");
            };
            case (#v2(v2)) { #v2(V2_.upgrade(v2)) };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V2_.StableStore {
        switch (asset_versions) {
            case (#v0(stable_store)) {
                Runtime.trap("Cannot upgrade from " # V0_.to_text(stable_store) # ". This version requires manual data migration to v1.0.0 due to breaking changes (DocumentId: Nat → Blob, BitMap changes). No automatic upgrade path available.");
            };
            case (#v1(stable_store)) {
                Runtime.trap("Cannot upgrade from " # V1_.to_text(stable_store) # ". This version requires manual data migration to v2.0.0 due to breaking changes in index encoding (Orchid: prefix-based → lexicographic escape-based encoding). Indexes must be rebuilt. No automatic upgrade path available.");
            };
            case (#v2(stable_store)) { V2_.get_current_state(stable_store) };
        };
    };

    public func share_version(sstore : V2_.StableStore) : VersionedStableStore {
        #v2(V2_.share_version(sstore));
    };

    public func to_text(versions : PrevVersionedStableStore) : Text {
        switch (versions) {
            case (#v0(v)) { V0_.to_text(v) };
            case (#v1(v)) { V1_.to_text(v) };
            case (#v2(v)) { V2_.to_text(v) };
        };
    };

};
