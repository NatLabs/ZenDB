import Blob "mo:base@0.16.0/Blob";
import Debug "mo:base@0.16.0/Debug";

import V0_2_0_types "v0.2.0/types";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    public let V0_2_0 = { Types = V0_2_0_types };

    public type StableStore = V0_2_0_types.StableStore;

    public type VersionedStableStore = {
        #v0_2_0 : V0_2_0_types.StableStore;
    };

    public func upgrade(versions : VersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v0_2_0(v0_2_0)) { #v0_2_0(v0_2_0) };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V0_2_0_types.StableStore {
        switch (asset_versions) {
            case (#v0_2_0(stable_store)) { stable_store };
            case (_) Debug.trap(
                "
                Invalid version of stable store. Please call upgrade() on the stable store.
                "
            );
        };
    };

    public func share_version(sstore : V0_2_0_types.StableStore) : VersionedStableStore {
        #v0_2_0(sstore);
    };

    public func to_text(versions : VersionedStableStore) : Text {
        switch (versions) {
            case (#v0_2_0(_)) { "v0.2.0" };
        };
    };

};
