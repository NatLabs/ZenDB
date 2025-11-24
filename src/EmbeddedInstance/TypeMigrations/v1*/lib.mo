import Debug "mo:base@0.16.0/Debug";

import V1_0_0_types "v1.0.0/types";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    public let V1_0_0 = { Types = V1_0_0_types };

    public type StableStore = V1_0_0_types.StableStore;
    public type VersionedStableStore = {
        #v1_0_0 : V1_0_0_types.StableStore;
    };

    public func upgrade(versions : VersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v1_0_0(v1_0_0)) {
                #v1_0_0(v1_0_0);
            };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V1_0_0_types.StableStore {
        switch (asset_versions) {
            case (#v1_0_0(stable_store)) { stable_store };
            case (_) Debug.trap(
                "
                Invalid version of stable store " # debug_show (to_text(asset_versions)) # ". Expected v1.0.0. Please call upgrade() on the stable store.
                "
            );
        };
    };

    public func share_version(sstore : V1_0_0_types.StableStore) : VersionedStableStore {
        #v1_0_0(sstore);
    };

    public func to_text(versions : VersionedStableStore) : Text {
        switch (versions) {
            case (#v1_0_0(_)) { "v1.0.0" };
        };
    };

};
