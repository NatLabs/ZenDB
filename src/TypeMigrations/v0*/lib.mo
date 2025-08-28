import Blob "mo:base/Blob";
import Debug "mo:base/Debug";

import V0_0_1_types "v0.0.1/types";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    public let V0_0_1 = { Types = V0_0_1_types };

    public type StableStore = V0_0_1_types.StableStore;

    public type VersionedStableStore = {
        #v0_0_1 : V0_0_1_types.StableStore;
    };

    public func upgrade(versions : VersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v0_0_1(v0_0_1)) { #v0_0_1(v0_0_1) };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V0_0_1_types.StableStore {
        switch (asset_versions) {
            case (#v0_0_1(stable_store)) { stable_store };
            case (_) Debug.trap(
                "
                Invalid version of stable store. Please call upgrade() on the stable store.
                "
            );
        };
    };

    public func share_version(sstore : V0_0_1_types.StableStore) : VersionedStableStore {
        #v0_0_1(sstore);
    };

};
