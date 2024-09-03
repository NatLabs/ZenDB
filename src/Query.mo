import Array "mo:base/Array";

import Text "mo:base/Text";
import Debug "mo:base/Debug";

import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";

import Map "mo:map/Map";
import Set "mo:map/Set";

import T "Types";

module {

    public type HqlOperators = T.HqlOperators;
    public type HydraQueryLang = T.HydraQueryLang;
    public type Operator = T.Operator;
    let { thash; bhash } = Map;

    type StableQuery = T.StableQuery;
    type Cursor = T.Cursor;
    type PaginationDirection = T.PaginationDirection;

    public class QueryBuilder() = self {

        var _query : HydraQueryLang = #Operation("dummy_node", #eq(#Null));
        var is_and : Bool = true;
        var buffer = Buffer.Buffer<HydraQueryLang>(8);
        var pagination_cursor : ?Cursor = null;
        var pagination_limit : ?Nat = null;
        var pagination_skip : ?Nat = null; // skip from beginning of the query
        var _cursor_offset = 0;
        var _direction : PaginationDirection = #Forward;
        var sort_by : ?(Text, T.SortDirection) = null; // only support sorting by one field for now

        public func Where(key : Text, op : HqlOperators) : QueryBuilder {
            return And(key, op);
        };

        func update_query(new_is_and : Bool) {

            if (buffer.size() > 1 and is_and != new_is_and) {
                switch (_query) {
                    case (#And(_)) if (not new_is_and) _query := #And(Buffer.toArray(buffer));
                    case (#Or(_)) if (new_is_and) _query := #Or(Buffer.toArray(buffer));
                    case (_) _query := #And(Buffer.toArray(buffer));
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
            // Debug.print("is_and: " # debug_show is_and);
            // Debug.print("query: " # debug_show _query);
            // Debug.print("buffer: " # debug_show Buffer.toArray(buffer));
            update_query(true);
            // Debug.print("is_and: " # debug_show is_and);
            // Debug.print("query: " # debug_show _query);
            // Debug.print("buffer: " # debug_show Buffer.toArray(buffer));
            handle_op(key, op);
            // Debug.print("is_and: " # debug_show is_and);
            // Debug.print("query: " # debug_show _query);
            // Debug.print("buffer: " # debug_show Buffer.toArray(buffer));

            self;
        };

        public func Or(key : Text, op : HqlOperators) : QueryBuilder {
            // Debug.print("is_or: " # debug_show (not is_and));
            // Debug.print("query: " # debug_show _query);
            // Debug.print("buffer: " # debug_show Buffer.toArray(buffer));
            update_query(false);
            // Debug.print("is_or: " # debug_show (not is_and));
            // Debug.print("query: " # debug_show _query);
            // Debug.print("buffer: " # debug_show Buffer.toArray(buffer));
            handle_op(key, op);
            // Debug.print("is_or: " # debug_show (not is_and));
            // Debug.print("query: " # debug_show _query);
            // Debug.print("buffer: " # debug_show Buffer.toArray(buffer));

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

        public func Sort(key : Text, direction : T.SortDirection) : QueryBuilder {
            sort_by := ?(key, direction);
            self;
        };

        public func Pagination(cursor : ?Cursor, limit : Nat) : QueryBuilder {
            pagination_cursor := cursor;
            pagination_limit := ?limit;
            self;
        };

        public func Cursor(cursor : ?Cursor, direction : PaginationDirection) : QueryBuilder {
            pagination_cursor := cursor;
            // _cursor_offset := cursor_offset;
            _direction := direction;
            self;
        };

        public func Limit(limit : Nat) : QueryBuilder {
            pagination_limit := ?limit;
            self;
        };

        public func Skip(skip : Nat) : QueryBuilder {
            pagination_skip := ?skip;
            self;
        };

        public func build() : StableQuery {
            // update_query(true); // input params is no longer relevant
            let resolved_query = if (buffer.size() == 0) {
                _query;
            } else if (is_and) {
                #And(Buffer.toArray(buffer));
            } else {
                #Or(Buffer.toArray(buffer));
            };

            // Debug.print("Query: " # debug_show resolved_query);

            {
                query_operations = resolved_query;
                sort_by;
                pagination = {
                    cursor = switch (pagination_cursor) {
                        case (?cursor) ?(cursor, _direction);
                        case (_) null;
                    };
                    limit = pagination_limit;
                    skip = pagination_skip;
                };
            };
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
