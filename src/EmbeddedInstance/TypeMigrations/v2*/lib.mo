import Debug "mo:core@2.4/Debug";

import V2_0_0_types "v2.0.0/types";
import Runtime "mo:core@2.4/Runtime";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    public let V2_0_0 = { Types = V2_0_0_types };

    public type StableStore = V2_0_0_types.StableStore;
    public type VersionedStableStore = {
        #v2_0_0 : V2_0_0_types.StableStore;
    };

    public type PrevVersionedStableStore = {
        #v2_0_0 : V2_0_0_types.StableStore;
    };

    public func upgrade(versions : PrevVersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v2_0_0(v2_0_0)) {
                #v2_0_0(v2_0_0);
            };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V2_0_0_types.StableStore {
        switch (asset_versions) {
            case (#v2_0_0(stable_store)) { stable_store };
            case (_) Runtime.trap(
                "
                Invalid version of stable store " # debug_show (to_text(asset_versions)) # ". Expected v2.0.0. Please call upgrade() on the stable store.
                "
            );
        };
    };

    public func share_version(sstore : V2_0_0_types.StableStore) : VersionedStableStore {
        #v2_0_0(sstore);
    };

    public func to_text(versions : PrevVersionedStableStore) : Text {
        switch (versions) {
            case (#v2_0_0(_)) { "v2.0.0" };
        };
    };

};
