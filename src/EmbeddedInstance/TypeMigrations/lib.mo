import Blob "mo:base@0.16.0/Blob";
import Debug "mo:base@0.16.0/Debug";

import V0_ "v0*";
import V1_ "v1*";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    public let V0 = V0_;
    public let V1 = V1_;

    public type StableStore = V1_.StableStore;

    public type VersionedStableStore = {
        #v0 : V0_.VersionedStableStore;
        #v1 : V1_.VersionedStableStore;
    };

    /// Required for actor migrations if the type is no longer compatible with future versions
    public type PrevVersionedStableStore = {
        #v0 : V0_.PrevVersionedStableStore;
        #v1 : V1_.PrevVersionedStableStore;
    };

    public func upgrade(versions : PrevVersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v0(v0)) {
                Debug.trap("Cannot upgrade from " # V0_.to_text(v0) # ". This version requires manual data migration to v1.0.0 due to breaking changes (DocumentId: Nat → Blob, BitMap changes). No automatic upgrade path available.");
            };
            case (#v1(v1)) { #v1(V1_.upgrade(v1)) };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V1_.StableStore {
        switch (asset_versions) {
            case (#v0(stable_store)) {
                Debug.trap("Cannot upgrade from " # V0_.to_text(stable_store) # ". This version requires manual data migration to v1.0.0 due to breaking changes (DocumentId: Nat → Blob, BitMap changes). No automatic upgrade path available.");
            };
            case (#v1(stable_store)) { V1_.get_current_state(stable_store) };
        };
    };

    public func share_version(sstore : V1_.StableStore) : VersionedStableStore {
        #v1(V1_.share_version(sstore));
    };

    public func to_text(versions : VersionedStableStore) : Text {
        switch (versions) {
            case (#v0(v)) { V0_.to_text(v) };
            case (#v1(v)) { V1_.to_text(v) };
        };
    };

};
