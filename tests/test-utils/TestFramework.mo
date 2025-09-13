// @testmode wasi
import Debug "mo:base@0.16.0/Debug";
import Buffer "mo:base@0.16.0/Buffer";
import Blob "mo:base@0.16.0/Blob";
import Text "mo:base@0.16.0/Text";
import Array "mo:base@0.16.0/Array";
import Option "mo:base@0.16.0/Option";
import Principal "mo:base@0.16.0/Principal";

import ZenDB "../../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools@0.2.2/Iter";
import Map "mo:map@9.0.1/Map";
import Fuzz "mo:fuzz";

module TestFramework {

    public type Settings = {
        log_level : ZenDB.Types.LogLevel;
        compare_with_index : Bool;
        compare_with_no_index : Bool;
    };

    /// default setting runs tests with no index
    public let defaultSettings : Settings = {
        log_level = #Error;
        compare_with_index = false;
        compare_with_no_index = true;
    };

    /// This setting runs tests with both index and no index.
    public let withAndWithoutIndex : Settings = {
        log_level = #Error;
        compare_with_index = true;
        compare_with_no_index = true;
    };

    /// This setting runs tests only with index.
    public let onlyWithIndex : Settings = {
        log_level = #Error;
        compare_with_index = true;
        compare_with_no_index = false;
    };

    public type CreateIndexOnCollection = (
        collection_name : Text,
        index_name : Text,
        index_key_details : [(Text, ZenDB.Types.SortDirection)],
        options : ?ZenDB.Types.CreateIndexOptions,
    ) -> ZenDB.Types.Result<(), Text>;

    public type SuiteUtils = {
        createIndex : CreateIndexOnCollection;
    };

    public func newSuite(
        test_name : Text,
        options : ?Settings,
        zendb_suite : (zendb : ZenDB.Database, suite_utils : SuiteUtils) -> (),
    ) {
        let fuzz = Fuzz.fromSeed(0x23123abc);
        let settings = Option.get(options, defaultSettings);

        func run_suite_with_or_without_indexes(memory_type_suite_name : Text, zendb_sstore : ZenDB.Types.VersionedStableStore) {

            if (settings.compare_with_no_index) {
                suite(
                    memory_type_suite_name # " - with no index",
                    func() {
                        let #ok(zendb) = ZenDB.createDB(zendb_sstore, "no_index");
                        let suite_utils : SuiteUtils = {
                            createIndex : CreateIndexOnCollection = func(
                                collection_name : Text,
                                index_name : Text,
                                index_key_details : [(Text, ZenDB.Types.SortDirection)],
                                options : ?ZenDB.Types.CreateIndexOptions,
                            ) {
                                // no-op
                                return #ok(());
                            };
                        };

                        zendb_suite(zendb, suite_utils);
                    },
                );

            };

            if (settings.compare_with_index) {
                suite(
                    memory_type_suite_name # " - with indexes",
                    func() {
                        let #ok(zendb) = ZenDB.createDB(zendb_sstore, "with_index");

                        let suite_utils : SuiteUtils = {
                            createIndex : CreateIndexOnCollection = func(
                                collection_name : Text,
                                index_name : Text,
                                index_key_details : [(Text, ZenDB.Types.SortDirection)],
                                options : ?ZenDB.Types.CreateIndexOptions,
                            ) {

                                zendb._create_index_on_collection(
                                    collection_name,
                                    index_name,
                                    index_key_details,
                                    switch (options) {
                                        case (?{ is_unique }) is_unique;
                                        case (_) false;
                                    },
                                );

                            };
                        };

                        zendb_suite(zendb, suite_utils);
                    },
                );
            };

        };

        func run_suite_for_all_memory_types() {
            suite(
                test_name,
                func() {
                    suite(
                        "Stable Memory",
                        func() {

                            let zendb_sstore = let sstore = ZenDB.newStableStore(
                                fuzz.principal.randomPrincipal(29),
                                ?{
                                    logging = ?{
                                        log_level = settings.log_level;
                                        is_running_locally = true;
                                    };
                                    memory_type = ?(#stableMemory);
                                },
                            );

                            run_suite_with_or_without_indexes("Stable Memory", zendb_sstore);

                        },
                    );

                    suite(
                        "Heap Memory",
                        func() {
                            let zendb_sstore = let sstore = ZenDB.newStableStore(
                                fuzz.principal.randomPrincipal(29),
                                ?{
                                    logging = ?{
                                        log_level = settings.log_level;
                                        is_running_locally = true;
                                    };
                                    memory_type = ?(#heap);
                                },
                            );
                            run_suite_with_or_without_indexes("Heap Memory", zendb_sstore);

                        },
                    );
                },
            );
        };

        run_suite_for_all_memory_types();

    };

};
