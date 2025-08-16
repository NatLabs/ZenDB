// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Option "mo:base/Option";

import ZenDB "../../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Map "mo:map/Map";

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

    type Function<A, B> = A -> B;

    public type SuiteUtils = {
        createIndex : CreateIndexOnCollection;
        indexOnlyFns : Function<Function<(), ()>, ()>;
    };

    public func newSuite(
        test_name : Text,
        options : ?Settings,
        zendb_suite : (zendb : ZenDB.Database, suite_utils : SuiteUtils) -> (),
    ) {
        let settings = Option.get(options, defaultSettings);

        func run_suite_with_or_without_indexes(memory_type_suite_name : Text, zendb_sstore : ZenDB.Types.StableStore) {

            if (settings.compare_with_no_index) {
                suite(
                    memory_type_suite_name # " - with no index",
                    func() {
                        let #ok(zendb) = ZenDB.createDB(zendb_sstore, "no_index");
                        let suite_utils : SuiteUtils = {
                            indexOnlyFns = func(callback_fns : Function<(), ()>) {
                                // no-op
                            };
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
                            indexOnlyFns = func(callback_fns : Function<(), ()>) {
                                callback_fns();
                            };
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
                                        case (?{ isUnique }) isUnique;
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

        suite(
            test_name,
            func() {
                suite(
                    "Stable Memory",
                    func() {

                        let zendb_sstore = let sstore = ZenDB.newStableStore(
                            ?{
                                logging = ?{
                                    log_level = settings.log_level;
                                    is_running_locally = true;
                                };
                                memory_type = ?(#stableMemory);
                            }
                        );

                        run_suite_with_or_without_indexes("Stable Memory", zendb_sstore);

                    },
                );

                suite(
                    "Heap Memory",
                    func() {
                        let zendb_sstore = let sstore = ZenDB.newStableStore(
                            ?{
                                logging = ?{
                                    log_level = settings.log_level;
                                    is_running_locally = true;
                                };
                                memory_type = ?(#heap);
                            }
                        );
                        run_suite_with_or_without_indexes("Heap Memory", zendb_sstore);

                    },
                );
            },
        );

    };

};
