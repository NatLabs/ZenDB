import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Result "mo:base/Result";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Hash "mo:base/Hash";
import Float "mo:base/Float";
import Int "mo:base/Int";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";
import Tag "mo:candid/Tag";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import T "Types";

module {

    public type HqlOperators = T.HqlOperators;
    public type HydraQueryLang = T.HydraQueryLang;
    public type Operator = T.Operator;
    let { thash; bhash } = Map;

    public class QueryBuilder() = self {

        var _query : HydraQueryLang = #Operation("dummy_node", #eq(#Null));
        var is_and : Bool = true;
        var buffer = Buffer.Buffer<HydraQueryLang>(8);

        public func Where(key : Text, op : HqlOperators) : QueryBuilder {
            return And(key, op);
        };

        func update_query(new_is_and : Bool) {

            if (buffer.size() > 0) {
                if (is_and) {
                    _query := #And(Buffer.toArray(buffer));
                } else {
                    _query := #Or(Buffer.toArray(buffer));
                };

                buffer.clear();
                buffer.add(_query);
            };

            is_and := new_is_and;
        };

        func handle_not(key : Text, not_op : HqlOperators) {
            switch (not_op) {
                case (#eq(value)) {
                    // #eq(x) -> #Or([#lt(x), #gt(x)])

                    if (not is_and) {
                        buffer.add(#Operation(key, #lt(value)));
                        buffer.add(#Operation(key, #gt(value)));
                    } else {
                        buffer.add(#Or([#Operation(key, #lt(value)), #Operation(key, #gt(value))]));
                    };

                };
                case (#lt(value)) {
                    // #Not(#lt(x)) -> #gte(x)
                    buffer.add(#Operation(key, #gte(value)));
                };
                case (#gt(value)) {
                    // #Not(#gt(x) )-> #lte(x)
                    buffer.add(#Operation(key, #lte(value)));
                };
                case (#lte(value)) {
                    // #Note(#lte(x)) -> #gt(x)
                    buffer.add(#Operation(key, #gt(value)));
                };
                case (#gte(value)) {
                    // #Not(#gte(x)) -> #lt(x)
                    buffer.add(#Operation(key, #lt(value)));
                };
                case (#In(values)) {
                    // #Not(#In([x, y, z])) -> #And([#Not(x), #Not(y), #Not(z)])
                    // -> #And([#Or([#lt(x), #gt(x)]), #Or([#lt(y), #gt(y)]), #Or([#lt(z), #gt(z)])])

                    if (is_and) {
                        for (value in values.vals()) {
                            buffer.add(#Or([#Operation(key, #lt(value)), #Operation(key, #gt(value))]));
                        };
                    } else {
                        buffer.add(
                            #And(
                                Array.tabulate(
                                    values.size(),
                                    func(i : Nat) : HydraQueryLang {
                                        #Or([#Operation(key, #lt(values.get(i))), #Operation(key, #gt(values.get(i)))]);
                                    },
                                )
                            )
                        );
                    };

                };
                case (#Not(nested_op)) {
                    // #Not(#Not(x)) -> x
                    buffer.add(#Operation(key, nested_op));
                };
            };
        };

        func handle_op(key : Text, op : HqlOperators) {
            switch (op) {
                case (#In(values)) {
                    if (not is_and) {
                        for (value in values.vals()) {
                            buffer.add(#Operation(key, #eq(value : T.Candid)));
                        };
                    } else if (buffer.size() == 0) {

                        // replace the and_buffer with an or_buffer
                        is_and := false;
                        for (value in values.vals()) {
                            buffer.add(#Operation(key, #eq(value : T.Candid)));
                        };

                    } else {
                        buffer.add(
                            #Or(
                                Array.tabulate(
                                    values.size(),
                                    func(i : Nat) : HydraQueryLang {
                                        #Operation(
                                            key,
                                            #eq(values.get(i) : T.Candid),
                                        );
                                    },
                                )
                            )
                        );
                    };

                };
                case (#Not(not_op)) {
                    handle_not(key, not_op);
                };
                case (_) {
                    buffer.add(#Operation(key, op));
                };

            };

        };

        public func And(key : Text, op : HqlOperators) : QueryBuilder {

            let and_buffer = if (is_and) {
                buffer

            } else {
                update_query(true);
                buffer;
            };

            handle_op(key, op);

            self;
        };

        public func Or(key : Text, op : HqlOperators) : QueryBuilder {

            let or_buffer = if (not is_and) {
                buffer;
            } else {
                update_query(false);
                buffer;
            };

            handle_op(key, op);
            self;
        };

        // public func Or_Query(new_query : QueryBuilder) : QueryBuilder {
        //     let or_buffer = if (not is_and) {
        //         buffer;
        //     } else {
        //         update_query(false);
        //         buffer;
        //     };

        //     let op = new_query.build();

        //     handle_op(op);

        //     self;
        // };

        // public func And_Query(new_query : QueryBuilder) : QueryBuilder {

        //     let and_buffer = if (is_and) {
        //         buffer;
        //     } else {
        //         update_query(true);
        //         buffer;
        //     };

        //     let op = new_query.build();

        //     handle_op(key, p);

        //     self;
        // };

        public func build() : HydraQueryLang {
            update_query(true); // input params is no longer relevant
            // Debug.print("Query: " # debug_show _query);
            _query;
        };

    };

    // validate that all the query fields are defined in the schema
    public func validate_query(collection : T.StableCollection, hydra_query : T.HydraQueryLang) : T.Result<(), Text> {

        switch (hydra_query) {
            case (#Operation(field, op)) {

                if (not Set.has(collection.schema_keys_set, thash, field)) {

                    if (Text.contains(field, #text("."))) {
                        for (key in Text.split(field, #text("."))) {
                            if (not Set.has(collection.schema_keys_set, thash, key)) {
                                return #err("Field " # key # " not found in schema");
                            };
                        };
                    } else {
                        return #err("Field " # field # " not found in schema");
                    };
                };
            };
            case (#And(buffer)) {
                for (expr in buffer.vals()) {
                    switch (validate_query(collection, expr)) {
                        case (#err(err)) return #err(err);
                        case (#ok(_)) ();
                    };
                };

            };
            case (#Or(buffer)) {
                for (expr in buffer.vals()) {
                    switch (validate_query(collection, expr)) {
                        case (#err(err)) return #err(err);
                        case (#ok(_)) ();
                    };
                };
            };
        };

        #ok();
    };

};
