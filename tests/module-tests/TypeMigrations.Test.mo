import Debug "mo:base@0.16.0/Debug";
import Iter "mo:base@0.16.0/Iter";
import Text "mo:base@0.16.0/Text";
import Nat "mo:base@0.16.0/Nat";
import Map "mo:map@9.0.1/Map";

import { test; suite } "mo:test";
import Candid "mo:serde@3.3.2/Candid";
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
