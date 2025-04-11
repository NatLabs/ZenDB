import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Array "mo:base/Array";
import Order "mo:base/Order";
import Option "mo:base/Option";
import ExperimentalInternetComputer "mo:base/ExperimentalInternetComputer";
import NewInternetComputer "mo:new-base/InternetComputer";

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

    public func info(logger : Logger, msg : Text) {
        logAtLevel(logger, #Info, msg);
    };

    public func warn(logger : Logger, msg : Text) {
        logAtLevel(logger : Logger, #Warn, msg);
    };

    public func error(logger : Logger, msg : Text) {
        logAtLevel(logger, #Error, msg);
    };

    public func trap(msg : Text) {
        Debug.trap(msg);
    };

    public func print(msg : Text) {
        Debug.print(msg);
    };

    public class Thread(logger : Logger, name : Text, parent_thread_id : ?Nat) {
        let thread_id = logger.next_thread_id;
        logger.next_thread_id += 1;

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

        public func logAtLevel(log_level : LogLevel, msg : Text) {
            let text_format = if (LogLevel.compare(#Info, logger.log_level) == #less) {
                name # ": " # msg;
            } else {
                "[Thread: " # debug_show (thread_id) # "] " # msg;
            };

            Logger.logAtLevel(logger, log_level, text_format);
        };

        public func log(msg : Text) {
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

        public func trap(msg : Text) {
            logAtLevel(#Trap, msg);
        };

        public func end() {
            let instructions = get_instructions() - instructions_start;
            logAtLevel(#Info, "[Thread: END] ID: " # debug_show (thread_id) # "; INSTRUCTIONS: " # debug_show (instructions));
        };

        public func createSubThread(name : Text) : Thread {
            return Thread(logger, name, ?thread_id);
        };

    };

};
