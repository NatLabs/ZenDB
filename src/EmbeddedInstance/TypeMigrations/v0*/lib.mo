import V0_1_0 "./v0.1.0/types";

module {
    public type VersionedStableStore = {
        #v0_1_0 : V0_1_0.StableStore;
    };

    public func upgrade(prev_store : VersionedStableStore) : VersionedStableStore {
        prev_store;
    };

    public func share(store : VersionedStableStore) : VersionedStableStore {
        store;
    };

    public func get_current_state(store : VersionedStableStore) : V0_1_0.StableStore {
        switch (store) {
            case (#v0_1_0(state)) state;
        };
    };

    public func to_text(store : VersionedStableStore) : Text {
        switch (store) {
            case (#v0_1_0(_)) { "v0.1.0" };
        };
    };
};
