import Debug "mo:base@0.16.0/Debug";

import Types_v0_1_0 "v0.1.0/types";

module {

    public type VersionedStableStore = {
        #v0_1_0 : Types_v0_1_0.StableStore;
    };

    public type PrevVersionedStableStore = {
        #v0_1_0 : Types_v0_1_0.StableStore;
    };

    public func upgrade(prev_store : PrevVersionedStableStore) : VersionedStableStore {
        switch (prev_store) {
            case (#v0_1_0(state)) {
                #v0_1_0(state);
            };
            case (_) {
                Debug.trap("Invalid version " # to_text(prev_store) # " of stable store. Expected v0.2.4. Please call upgrade() on the stable store.");
            };
        };
    };

    public func share(store : VersionedStableStore) : VersionedStableStore {
        store;
    };

    public func get_current_state(store : VersionedStableStore) : Types_v0_1_0.StableStore {
        let upgraded = upgrade(store);
        switch (upgraded) {
            case (#v0_1_0(state)) state;
            case (_) {
                Debug.trap("Invalid version of stable store " # to_text(store) # ". Expected v0.2.4. Please call upgrade() on the stable store.");
            };

        };
    };

    public func to_text(store : PrevVersionedStableStore) : Text {
        switch (store) {
            case (#v0_1_0(_)) { "v0.1.0" };

        };
    };
};
