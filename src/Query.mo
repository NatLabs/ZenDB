import Array "mo:base/Array";

import Text "mo:base/Text";
import Debug "mo:base/Debug";

import Buffer "mo:base/Buffer";
import Option "mo:base/Option";
import Nat "mo:base/Nat";

import Map "mo:map/Map";
import Set "mo:map/Set";

import T "Types";

module {

    public type ZqlOperators = T.ZqlOperators;
    public type ZenQueryLang = T.ZenQueryLang;
    public type Operator = T.Operator;
    let { thash; bhash } = Map;

    type StableQuery = T.StableQuery;
    type Cursor = T.Cursor;
    type PaginationDirection = T.PaginationDirection;

    public class QueryBuilder() = self {

        var opt_nested_query : ?ZenQueryLang = null;
        var is_and : Bool = true;
        var buffer = Buffer.Buffer<ZenQueryLang>(8);
        var pagination_cursor : ?Cursor = null;
        var pagination_limit : ?Nat = null;
        var pagination_skip : ?Nat = null; // skip from beginning of the query
        var _cursor_offset = 0;
        var _direction : PaginationDirection = #Forward;
        var sort_by : ?(Text, T.SortDirection) = null; // only support sorting by one field for now

        public func Where(key : Text, op : ZqlOperators) : QueryBuilder {
            return And(key, op);
        };

        func update_query(new_is_and : Bool) {
            let old_is_and = is_and;

            if (buffer.size() > 0 and old_is_and != new_is_and) {
                // Debug.print("(old_is_and, new_is_and): " # debug_show (old_is_and, new_is_and));
                // Debug.print("old nested query: " # debug_show opt_nested_query);
                switch (opt_nested_query) {
                    case (null) {
                        if (buffer.size() == 1) {
                            opt_nested_query := ?(buffer.get(0));
                        } else if (old_is_and) {
                            opt_nested_query := ?(#And(Buffer.toArray(buffer)));
                        } else {
                            opt_nested_query := ?(#Or(Buffer.toArray(buffer)));
                        };
                    };
                    case (?nested_query) {
                        buffer.insert(0, nested_query);

                        if (old_is_and) {
                            opt_nested_query := ?(#And(Buffer.toArray(buffer)));
                        } else {
                            opt_nested_query := ?(#Or(Buffer.toArray(buffer)));
                        };
                    };
                };

                // Debug.print("new nested query: " # debug_show opt_nested_query);

                buffer.clear();
            };

            is_and := new_is_and;
        };

        func handle_not(key : Text, not_op : ZqlOperators) {
            switch (not_op) {
                case (#eq(value)) {
                    // #Not(#eq(x)) -> #Or([#lt(x), #gt(x)])

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
                    // #Not(#lte(x)) -> #gt(x)
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
                                    func(i : Nat) : ZenQueryLang {
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

        func handle_op(key : Text, op : ZqlOperators) {
            switch (op) {
                case (#In(values)) {
                    update_query(false);
                    for (value in values.vals()) {
                        buffer.add(#Operation(key, #eq(value : T.Candid)));
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

        public func And(key : Text, op : ZqlOperators) : QueryBuilder {

            update_query(true);
            handle_op(key, op);

            self;
        };

        public func Or(key : Text, op : ZqlOperators) : QueryBuilder {
            update_query(false);
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
            update_query(not is_and); // flushes buffer because the state is switched

            let resolved_query = switch (opt_nested_query) {
                case (null) #And([]);
                case (?#Operation(op)) #And([#Operation(op)]);
                case (?nested_query) nested_query;
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
    public func validate_query(collection : T.StableCollection, hydra_query : T.ZenQueryLang) : T.Result<(), Text> {

        switch (hydra_query) {
            case (#Operation(field, op)) {
                // Debug.print(debug_show (Set.toArray(collection.schema_keys_set)));

                if (not Set.has(collection.schema_keys_set, thash, field)) {

                    if (Text.contains(field, #text("."))) {
                        for (key in Text.split(field, #text("."))) {
                            if (not Set.has(collection.schema_keys_set, thash, key)) {
                                return #err("Field '" # key # "' not found in schema");
                            };
                        };
                    } else {
                        return #err("Field '" # field # "' not found in schema");
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
