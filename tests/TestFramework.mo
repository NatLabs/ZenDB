// @testmode wasi
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Option "mo:base/Option";

import ZenDB "../src";

import { test; suite } "mo:test";
import Itertools "mo:itertools/Iter";
import Map "mo:map/Map";

module TestFramework {

    public type IndexSetup = (db_name : Text, indexes : [(index_name : Text, index_key_details : [(Text, ZenDB.Types.SortDirection)], is_unique : Bool)]);

    public type Settings = {
        log_level : ZenDB.Types.LogLevel;
        compare_with_index : Bool;
        compare_with_no_index : Bool;
    };

    public let defaultSettings : Settings = {
        log_level = #Error;
        compare_with_index = false;
        compare_with_no_index = false;
    };

    public let withAndWithoutIndex : Settings = {
        log_level = #Error;
        compare_with_index = true;
        compare_with_no_index = true;
    };

    public let onlyWithIndex : Settings = {
        log_level = #Error;
        compare_with_index = true;
        compare_with_no_index = false;
    };

    public func newZenDBSuite(
        test_name : Text,
        options : ?Settings,
        zendb_collection_setup : (zendb : ZenDB.Database) -> (),
        zendb_index_setup : (zendb : ZenDB.Database) -> (),
        zendb_suite : (zendb : ZenDB.Database) -> (),
    ) {
        let settings = Option.get(options, defaultSettings);

        func run_suite_on_index_granularity(zendb_sstore : ZenDB.Types.StableStore) {
            if (settings.compare_with_index or settings.compare_with_no_index) {
                if (settings.compare_with_no_index) {
                    suite(
                        "with no index",
                        func() {
                            let #ok(zendb) = ZenDB.createDB(zendb_sstore, "no_index");

                            zendb_collection_setup(zendb);
                            zendb_suite(zendb);
                        },
                    );

                };

                if (settings.compare_with_index) {
                    suite(
                        "with indexes",
                        func() {
                            let #ok(zendb) = ZenDB.createDB(zendb_sstore, "with_index");

                            zendb_collection_setup(zendb);
                            zendb_index_setup(zendb);
                            zendb_suite(zendb);
                        },
                    );
                };

            } else {

                let #ok(zendb) = ZenDB.createDB(zendb_sstore, "no_index");

                zendb_collection_setup(zendb);
                zendb_suite(zendb);

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

                        run_suite_on_index_granularity(zendb_sstore);

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
                        run_suite_on_index_granularity(zendb_sstore);

                    },
                );
            },
        );

    };

    public func newNoIndexSetup(
        test_name : Text,
        zendb_collection_setup : (zendb : ZenDB.Database) -> (),
        zendb_suite : (zendb : ZenDB.Database) -> (),
    ) {
        func run_suite_on_index_granularity(zendb_sstore : ZenDB.Types.StableStore) {
            suite(
                "with no index",
                func() {
                    let #ok(zendb) = ZenDB.createDB(zendb_sstore, "no_index");

                    zendb_collection_setup(zendb);
                    zendb_suite(zendb);
                },
            );
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
                                    log_level = #Error;
                                    is_running_locally = true;
                                };
                                memory_type = ?(#stableMemory);
                            }
                        );

                        run_suite_on_index_granularity(zendb_sstore);

                    },
                );

                suite(
                    "Heap Memory",
                    func() {
                        let zendb_sstore = let sstore = ZenDB.newStableStore(
                            ?{
                                logging = ?{
                                    log_level = #Error;
                                    is_running_locally = true;
                                };
                                memory_type = ?(#heap);
                            }
                        );

                        run_suite_on_index_granularity(zendb_sstore);

                    },
                );
            },
        );
    };

    public func newIndexOnly(
        test_name : Text,
        zendb_collection_setup : (zendb : ZenDB.Database) -> (),
        zendb_index_setup : (zendb : ZenDB.Database) -> (),
        zendb_suite : (zendb : ZenDB.Database) -> (),
    ) {
        func run_suite_on_index_granularity(zendb_sstore : ZenDB.Types.StableStore) {
            suite(
                "with indexes",
                func() {
                    let #ok(zendb) = ZenDB.createDB(zendb_sstore, "with_index");

                    zendb_collection_setup(zendb);
                    zendb_index_setup(zendb);
                    zendb_suite(zendb);
                },
            );
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
                                    log_level = #Error;
                                    is_running_locally = true;
                                };
                                memory_type = ?(#stableMemory);
                            }
                        );

                        run_suite_on_index_granularity(zendb_sstore);

                    },
                );

                suite(
                    "Heap Memory",
                    func() {
                        let zendb_sstore = let sstore = ZenDB.newStableStore(
                            ?{
                                logging = ?{
                                    log_level = #Error;
                                    is_running_locally = true;
                                };
                                memory_type = ?(#heap);
                            }
                        );
                        run_suite_on_index_granularity(zendb_sstore);

                    },
                );
            },
        );
    };

    public type CreateIndexOnCollection = (
        collection_name : Text,
        index_name : Text,
        index_key_details : [(Text, ZenDB.Types.SortDirection)],
        is_unique : Bool,
    ) -> ZenDB.Types.Result<(), Text>;

    public type SuiteUtils = {
        create_index : CreateIndexOnCollection;
    };

    public func newSuite(
        test_name : Text,
        options : ?Settings,
        zendb_suite : (zendb : ZenDB.Database, suite_utils : SuiteUtils) -> (),
    ) {
        let settings = Option.get(options, defaultSettings);

        func run_suite_on_index_granularity(zendb_sstore : ZenDB.Types.StableStore) {

            if (settings.compare_with_index or settings.compare_with_no_index) {
                if (settings.compare_with_no_index) {
                    suite(
                        "with no index",
                        func() {
                            let #ok(zendb) = ZenDB.createDB(zendb_sstore, "no_index");
                            let suite_utils : SuiteUtils = {
                                create_index : CreateIndexOnCollection = func(
                                    collection_name : Text,
                                    index_name : Text,
                                    index_key_details : [(Text, ZenDB.Types.SortDirection)],
                                    is_unique : Bool,
                                ) {
                                    // no-op
                                    return #ok(());
                                };
                            };

                            zendb_suite(
                                zendb,
                                suite_utils,
                            );
                        },
                    );

                };

                if (settings.compare_with_index) {
                    suite(
                        "with indexes",
                        func() {
                            let #ok(zendb) = ZenDB.createDB(zendb_sstore, "with_index");

                            let suite_utils : SuiteUtils = {
                                create_index : CreateIndexOnCollection = func(
                                    collection_name : Text,
                                    index_name : Text,
                                    index_key_details : [(Text, ZenDB.Types.SortDirection)],
                                    is_unique : Bool,
                                ) {

                                    zendb.create_index_on_collection(
                                        collection_name,
                                        index_name,
                                        index_key_details,
                                        is_unique,
                                    );

                                };
                            };
                            zendb_suite(
                                zendb,
                                suite_utils,
                            );
                        },
                    );
                };

            } else {

                let #ok(zendb) = ZenDB.createDB(zendb_sstore, "no_index");

                zendb_suite(
                    zendb,
                    {
                        create_index : CreateIndexOnCollection = func(
                            collection_name : Text,
                            index_name : Text,
                            index_key_details : [(Text, ZenDB.Types.SortDirection)],
                            is_unique : Bool,
                        ) {
                            // no-op
                            return #ok(());
                        };
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

                        run_suite_on_index_granularity(zendb_sstore);

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
                        run_suite_on_index_granularity(zendb_sstore);

                    },
                );
            },
        );

    };

};
