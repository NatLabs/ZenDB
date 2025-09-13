import Blob "mo:base@0.16.0/Blob";
import Debug "mo:base@0.16.0/Debug";

import V0_ "v0*";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    public let V0 = V0_;

    public type StableStore = V0.StableStore;

    public type VersionedStableStore = {
        #v0 : V0.VersionedStableStore;
    };

    public func upgrade(versions : VersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v0(v0)) { #v0(v0) };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V0.StableStore {
        switch (asset_versions) {
            case (#v0(stable_store)) { V0.get_current_state(stable_store) };
            case (_) Debug.trap(
                "
                Invalid version of stable store. Please call upgrade() on the stable store.
                "
            );
        };
    };

    public func share_version(sstore : V0.StableStore) : VersionedStableStore {
        #v0(V0.share_version(sstore));
    };

    public func to_text(versions : VersionedStableStore) : Text {
        switch (versions) {
            case (#v0(v)) { V0.to_text(v) };
        };
    };

};
