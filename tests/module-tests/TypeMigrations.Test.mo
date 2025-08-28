import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Map "mo:map/Map";

import { test; suite } "mo:test";
import Candid "mo:serde/Candid";
import T "../../src/TypeMigrations";
import ZenDB "../../src";

import ZenDB_V0_0_1 "mo:zendb@0.0.1";

suite(
    " Type Migrations Tests ",
    func() {
        test(
            " Test stable store version initialization : mops v0.0.1 -> v0.0.2 ",
            func() {
                let stable_store = ZenDB_V0_0_1.newStableStore(null);
                let versioned_store = T.share_version(stable_store);

                // Debug.print("Versioned store : " # debug_show (versioned_store));
                // Debug.print("Current state : " # debug_show (T.get_current_state(versioned_store)));
            },
        );
    },
);
