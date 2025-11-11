import Blob "mo:base@0.16.0/Blob";
import Debug "mo:base@0.16.0/Debug";

import V0_2_0_types "v0.2.0/types";

import V0_2_1_types "v0.2.1/types";
import V0_2_1_migrate "v0.2.1/migrate";

import V0_2_2_types "v0.2.2/types";
import V0_2_2_migrate "v0.2.2/migrate";

import V0_2_3_types "v0.2.3/types";
import V0_2_3_migrate "v0.2.3/migrate";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    public let V0_2_0 = { Types = V0_2_0_types };
    public let V0_2_1 = { Types = V0_2_1_types };
    public let V0_2_2 = { Types = V0_2_2_types };
    public let V0_2_3 = { Types = V0_2_3_types };

    public type StableStore = V0_2_3_types.StableStore;
    public type VersionedStableStore = {
        #v0_2_0 : V0_2_0_types.StableStore;
        #v0_2_1 : V0_2_1_types.StableStore;
        #v0_2_2 : V0_2_2_types.StableStore;
        #v0_2_3 : V0_2_3_types.StableStore;
    };

    public func upgrade(versions : VersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v0_2_0(v0_2_0)) {
                let v0_2_1 = V0_2_1_migrate.upgrade(v0_2_0);
                let v0_2_2 = V0_2_2_migrate.upgrade(v0_2_1);
                let v0_2_3 = V0_2_3_migrate.upgrade(v0_2_2);
                #v0_2_3(v0_2_3);
            };
            case (#v0_2_1(v0_2_1)) {
                let v0_2_2 = V0_2_2_migrate.upgrade(v0_2_1);
                let v0_2_3 = V0_2_3_migrate.upgrade(v0_2_2);
                #v0_2_3(v0_2_3);
            };
            case (#v0_2_2(v0_2_2)) {
                let v0_2_3 = V0_2_3_migrate.upgrade(v0_2_2);
                #v0_2_3(v0_2_3);
            };
            case (#v0_2_3(v0_2_3)) {
                #v0_2_3(v0_2_3);
            };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V0_2_3_types.StableStore {
        switch (asset_versions) {
            case (#v0_2_3(stable_store)) { stable_store };
            case (_) Debug.trap(
                "
                Invalid version of stable store" # debug_show (to_text(asset_versions)) # ". Expected v0.2.3.  Please call upgrade() on the stable store.
                "
            );
        };
    };

    public func share_version(sstore : V0_2_3_types.StableStore) : VersionedStableStore {
        #v0_2_3(sstore);
    };

    public func to_text(versions : VersionedStableStore) : Text {
        switch (versions) {
            case (#v0_2_0(_)) { "v0.2.0" };
            case (#v0_2_1(_)) { "v0.2.1" };
            case (#v0_2_2(_)) { "v0.2.2" };
            case (#v0_2_3(_)) { "v0.2.3" };
        };
    };

};
