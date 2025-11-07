import Prim "mo:prim";
import Debug "mo:base@0.16.0/Debug";
import Nat "mo:base@0.16.0/Nat";
import Array "mo:base@0.16.0/Array";
import Order "mo:base@0.16.0/Order";
import Option "mo:base@0.16.0/Option";
import ExperimentalInternetComputer "mo:base@0.16.0/ExperimentalInternetComputer";

import T "Types";

module Logger {

    public type LogLevel = T.LogLevel;

    public module LogLevel {
        let log_level_order = [
            #Debug,
            #Info,
            #Warn,
            #Error,
            #Trap,
        ];

        func getLogLevelPriority(log_level : LogLevel) : Nat {
            let ?index = Array.indexOf(
                log_level,
                log_level_order,
                func(l1 : LogLevel, l2 : LogLevel) : Bool {
                    return debug_show (l1) == debug_show (l2);
                },
            ) else Debug.trap("Logger error: logLevel " # debug_show (log_level) # " not found");

            index;
        };

        public func compare(l1 : LogLevel, l2 : LogLevel) : Order.Order {
            Nat.compare(
                getLogLevelPriority(l1),
                getLogLevelPriority(l2),
            );
        };
    };

    public type Logger = T.Logger;

    public func init(log_level : LogLevel, is_running_locally : Bool) : Logger {
        {
            var log_level = log_level;
            var next_thread_id = 0;
            var is_running_locally = is_running_locally;
        };
    };

    public func setLogLevel(logger : Logger, log_level : LogLevel) {
        logger.log_level := log_level;
    };

    public func getLogLevel(logger : Logger) : LogLevel {
        return logger.log_level;
    };

    public func setIsRunLocally(logger : Logger, is_running_locally : Bool) {
        logger.is_running_locally := is_running_locally;
    };

    public func logAtLevel(logger : Logger, log_level : LogLevel, msg : Text) {
        if (LogLevel.compare(log_level, logger.log_level) == #less) {
            return;
        };

        switch (log_level) {
            case (#Debug) {
                Debug.print("[DEBUG]: " # msg);
            };
            case (#Info) {
                Debug.print("[INFO]: " # msg);
            };
            case (#Warn) {
                Debug.print("[WARN]: " # msg);
            };
            case (#Error) {
                Debug.print("[ERROR]: " # msg);
            };
            case (#Trap) {
                Debug.trap(msg);
            };
        };

    };

    public func log(logger : Logger, msg : Text) {
        logAtLevel(logger, #Debug, msg);
    };

    public func debugMsg(logger : Logger, msg : Text) {
        logAtLevel(logger, #Debug, msg);
    };

    public func info(logger : Logger, msg : Text) {
        logAtLevel(logger, #Info, msg);
    };

    public func warn(logger : Logger, msg : Text) {
        logAtLevel(logger : Logger, #Warn, msg);
    };

    public func error(logger : Logger, msg : Text) {
        logAtLevel(logger, #Error, msg);
    };

    public func trap(msg : Text) : None {
        Debug.trap(msg);
    };

    public func print(msg : Text) {
        Debug.print(msg);
    };

    public func lazyLogAtLevel(logger : Logger, log_level : LogLevel, msgFn : () -> Text) {
        if (LogLevel.compare(log_level, logger.log_level) == #less) {
            return;
        };

        switch (log_level) {
            case (#Debug) {
                Debug.print("[DEBUG]: " # msgFn());
            };
            case (#Info) {
                Debug.print("[INFO]: " # msgFn());
            };
            case (#Warn) {
                Debug.print("[WARN]: " # msgFn());
            };
            case (#Error) {
                Debug.print("[ERROR]: " # msgFn());
            };
            case (#Trap) {
                Debug.trap(msgFn());
            };
        };

    };

    public func lazyLog(logger : Logger, msgFn : () -> Text) {
        lazyLogAtLevel(logger, #Debug, msgFn);
    };

    public func lazyDebug(logger : Logger, msgFn : () -> Text) {
        lazyLogAtLevel(logger, #Debug, msgFn);
    };

    public func lazyInfo(logger : Logger, msgFn : () -> Text) {
        lazyLogAtLevel(logger, #Info, msgFn);
    };

    public func lazyWarn(logger : Logger, msgFn : () -> Text) {
        lazyLogAtLevel(logger, #Warn, msgFn);
    };

    public func lazyError(logger : Logger, msgFn : () -> Text) {
        lazyLogAtLevel(logger, #Error, msgFn);
    };

    /// A convenience class that wraps a Logger and namespace to reduce verbosity
    /// in logging calls. Instead of repeatedly passing the logger and formatting
    /// the namespace, you can create a NamespacedLogger once and use it throughout
    /// a module or function.
    ///
    /// Example usage:
    /// ```motoko
    /// let log = Logger.NamespacedLogger(collection.logger, "MyModule.myFunction");
    /// log.logDebug("Processing started");
    /// log.logInfo("Found " # Nat.toText(count) # " items");
    /// log.logError("Operation failed");
    /// ```
    public class NamespacedLogger(logger : Logger, namespace : Text) {

        /// Log a debug message
        public func logDebug(msg : Text) {
            debugMsg(logger, namespace # ": " # msg);
        };

        /// Log a debug message with lazy evaluation
        public func lazyDebug(msgFn : () -> Text) {
            lazyLogAtLevel(logger, #Debug, func() { namespace # ": " # msgFn() });
        };

        /// Log an info message
        public func logInfo(msg : Text) {
            Logger.info(logger, namespace # ": " # msg);
        };

        /// Log an info message with lazy evaluation
        public func lazyInfo(msgFn : () -> Text) {
            lazyLogAtLevel(logger, #Info, func() { namespace # ": " # msgFn() });
        };

        /// Log a warning message
        public func logWarn(msg : Text) {
            Logger.warn(logger, namespace # ": " # msg);
        };

        /// Log a warning message with lazy evaluation
        public func lazyWarn(msgFn : () -> Text) {
            lazyLogAtLevel(logger, #Warn, func() { namespace # ": " # msgFn() });
        };

        /// Log an error message
        public func logError(msg : Text) {
            Logger.error(logger, namespace # ": " # msg);
        };

        /// Log an error message with lazy evaluation
        public func lazyError(msgFn : () -> Text) {
            lazyLogAtLevel(logger, #Error, func() { namespace # ": " # msgFn() });
        };

        /// Log at a specific level
        public func log(log_level : LogLevel, msg : Text) {
            Logger.logAtLevel(logger, log_level, namespace # ": " # msg);
        };

        /// Log at a specific level with lazy evaluation
        public func lazyLog(log_level : LogLevel, msgFn : () -> Text) {
            Logger.lazyLogAtLevel(logger, log_level, func() { namespace # ": " # msgFn() });
        };

        public func trap(msg : Text) : None {
            Debug.trap(namespace # ": " # msg);
        };

        /// Create a sub-namespaced logger by appending to the current namespace
        /// Example: if current namespace is "Module", calling subnamespace("function")
        /// creates a logger with namespace "Module.function"
        public func subnamespace(name : Text) : NamespacedLogger {
            NamespacedLogger(logger, namespace # "." # name);
        };

        /// Get the current namespace
        public func getNamespace() : Text {
            namespace;
        };

        /// Get the underlying logger
        public func getLogger() : Logger {
            logger;
        };
    };

    public class Thread(logger : Logger, name : Text, parent_thread_id : ?Nat) {
        let thread_id = logger.next_thread_id;
        logger.next_thread_id += 1;

        public func getId() : Nat { thread_id };
        public func getName() : Text { name };
        public func getParentId() : ?Nat { parent_thread_id };

        func get_instructions() : Nat64 {
            if (logger.is_running_locally) {
                return 0;
            };

            return ExperimentalInternetComputer.performanceCounter(1);
        };

        let instructions_start = get_instructions();

        Logger.logAtLevel(
            logger,
            #Info,
            "[Thread: START] ID: " #debug_show (thread_id) # "; NAME: " # name # "; PARENT: " # debug_show (Option.get(parent_thread_id, -1)),
        );

        // Threads are executed at the #Info level
        // If the current viewable log level is set above #Info,
        // the thread should be logged as individual messages not linked to the thread
        // Each of these logs will include the name of the thread at the start

        public func formatMsg(msg : Text) : Text {
            if (LogLevel.compare(#Info, logger.log_level) == #less) {
                return name # ": " # msg;
            } else {
                return "[Thread: " # debug_show (thread_id) # "] " # msg;
            };
        };

        public func logAtLevel(log_level : LogLevel, msg : Text) {
            Logger.logAtLevel(logger, log_level, formatMsg(msg));
        };

        public func log(msg : Text) {
            logAtLevel(#Debug, msg);
        };

        public func debugMsg(msg : Text) {
            logAtLevel(#Debug, msg);
        };

        public func info(msg : Text) {
            logAtLevel(#Info, msg);
        };

        public func warn(msg : Text) {
            logAtLevel(#Warn, msg);
        };

        public func error(msg : Text) {
            logAtLevel(#Error, msg);
        };

        public func trap(msg : Text) : None {
            Debug.trap(formatMsg(msg));
        };

        public func end() {
            let instructions = get_instructions() - instructions_start;
            logAtLevel(#Info, "[Thread: END] ID: " # debug_show (thread_id) # "; INSTRUCTIONS: " # debug_show (instructions));
        };

        public func createSubThread(name : Text) : Thread {
            return Thread(logger, name, ?thread_id);
        };

        public func lazyLogAtLevel(log_level : LogLevel, msgFn : () -> Text) {
            Logger.lazyLogAtLevel(
                logger,
                log_level,
                func() {
                    formatMsg(msgFn());
                },
            );
        };

        public func lazyLog(msgFn : () -> Text) {
            lazyLogAtLevel(#Debug, msgFn);
        };

        public func lazyDebug(msgFn : () -> Text) {
            lazyLogAtLevel(#Debug, msgFn);
        };

        public func lazyInfo(msgFn : () -> Text) {
            lazyLogAtLevel(#Info, msgFn);
        };

        public func lazyWarn(msgFn : () -> Text) {
            lazyLogAtLevel(#Warn, msgFn);
        };

        public func lazyError(msgFn : () -> Text) {
            lazyLogAtLevel(#Error, msgFn);
        };

    };

};
