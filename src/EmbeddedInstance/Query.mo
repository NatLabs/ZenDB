import Array "mo:base@0.16.0/Array";

import Text "mo:base@0.16.0/Text";
import Debug "mo:base@0.16.0/Debug";

import Buffer "mo:base@0.16.0/Buffer";
import Option "mo:base@0.16.0/Option";
import Nat "mo:base@0.16.0/Nat";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";

import T "Types";
import C "Constants";
import Logger "Logger";
import Schema "Collection/Schema";
import SchemaMap "Collection/SchemaMap";
import CandidUtils "CandidUtils";

module {
    let LOGGER_NAMESPACE = "Query";

    public type ZqlOperators = T.ZqlOperators;
    public type ZenQueryLang = T.ZenQueryLang;
    public type Operator = T.Operator;
    let { thash; bhash } = Map;

    type StableQuery = T.StableQuery;
    type PaginationToken = T.PaginationToken;
    type PaginationDirection = T.PaginationDirection;

    public class QueryBuilder() = self {

        public var _opt_nested_query : ?ZenQueryLang = null;
        public var _is_and : Bool = true;
        public var _buffer = Buffer.Buffer<ZenQueryLang>(8);
        public var _pagination_token : ?PaginationToken = null;
        public var _pagination_limit : ?Nat = null;
        public var _pagination_skip : ?Nat = null; // skip from beginning of the query
        public var _cursor_offset = 0;
        public var _direction : PaginationDirection = #Forward;
        public var _sort_by : ?(Text, T.SortDirection) = null; // only support sorting by one field for now

        func update_query(new_is_and : Bool) {
            let old_is_and = _is_and;

            if (_buffer.size() > 0 and old_is_and != new_is_and) {
                // Debug.print("(old_is_and, new_is_and): " # debug_show (old_is_and, new_is_and));
                // Debug.print("old nested query: " # debug_show opt_nested_query);
                switch (_opt_nested_query) {
                    case (null) {
                        if (_buffer.size() == 1) {
                            _opt_nested_query := ?(_buffer.get(0));
                        } else if (old_is_and) {
                            _opt_nested_query := ?(#And(Buffer.toArray(_buffer)));
                        } else {
                            _opt_nested_query := ?(#Or(Buffer.toArray(_buffer)));
                        };
                    };
                    case (?nested_query) {
                        _buffer.insert(0, nested_query);

                        if (old_is_and) {
                            _opt_nested_query := ?(#And(Buffer.toArray(_buffer)));
                        } else {
                            _opt_nested_query := ?(#Or(Buffer.toArray(_buffer)));
                        };
                    };
                };

                // Debug.print("new nested query: " # debug_show opt_nested_query);

                _buffer.clear();
            };

            _is_and := new_is_and;
        };

        func handle_not(key : Text, not_op : ZqlOperators) : ZenQueryLang {
            switch (not_op) {
                case (#eq(value)) {
                    // #not_(#eq(x)) -> #Or([#lt(x), #gt(x)])
                    #Or([#Operation(key, #lt(value)), #Operation(key, #gt(value))]);
                };
                case (#lt(value)) {
                    // #not_(#lt(x)) -> #gte(x)
                    #Operation(key, #gte(value));
                };
                case (#gt(value)) {
                    // #not_(#gt(x) )-> #lte(x)
                    #Operation(key, #lte(value));
                };
                case (#lte(value)) {
                    // #not_(#lte(x)) -> #gt(x)
                    #Operation(key, #gt(value));
                };
                case (#gte(value)) {
                    // #not_(#gte(x)) -> #lt(x)
                    #Operation(key, #lt(value));
                };
                case (#between(min, max)) {
                    // #not_(#between(min, max))
                    // -> #not_(#And([#gte(min), #lte(max)]))
                    // -> #Or([#lt(min), #gt(max)])
                    #Or([#Operation(key, #lt(min)), #Operation(key, #gt(max))]);
                };
                case (#betweenExclusive(min, max)) {
                    // #not_(#betweenExclusive(min, max))
                    // -> #not_(#And([#gt(min), #lt(max)]))
                    // -> #Or([#lte(min), #gte(max)])
                    #Or([#Operation(key, #lte(min)), #Operation(key, #gte(max))]);
                };
                case (#betweenLeftOpen(min, max)) {
                    // #not_(#betweenLeftOpen(min, max))
                    // -> #not_(#And([#gt(min), #lte(max)]))
                    // -> #Or([#lte(min), #gt(max)])
                    #Or([#Operation(key, #lte(min)), #Operation(key, #gt(max))]);
                };
                case (#betweenRightOpen(min, max)) {
                    // #not_(#betweenRightOpen(min, max))
                    // -> #not_(#And([#gte(min), #lt(max)]))
                    // -> #Or([#lt(min), #gte(max)])
                    #Or([#Operation(key, #lt(min)), #Operation(key, #gte(max))]);
                };
                case (#exists) {
                    // #not_(#exists)
                    // buffer.add(#Operation(key, #eq(#Null)));
                    Debug.trap("QueryBuilder: #not_(#exists) is not supported");
                };
                case (#startsWith(prefix)) {
                    let prefix_lower_bound = prefix;
                    let res = CandidUtils.Ops.concatBytes(prefix, "\FF");
                    switch (res) {
                        case (#err(msg)) {
                            Debug.trap("QueryBuilder: Failed to create upper bound for #startsWith. " # msg);
                        };
                        case (#ok(prefix_upper_bound)) {
                            // #not_(#startsWith(prefix))
                            // -> #not_(#between(prefix_lower_bound, prefix_upper_bound))
                            // -> #Or([#lt(prefix_lower_bound), #gt(prefix_upper_bound)])
                            #Or([#Operation(key, #lt(prefix_lower_bound)), #Operation(key, #gt(prefix_upper_bound))]);
                        };
                    };

                };
                case (#anyOf(values)) {
                    // #not_(#anyOf([y, z, x]))
                    //
                    // If the values were sorted, we would need only find the gaps between them
                    // Sort the values: [y, z, x] -> [x, y, z]
                    // Then express as: #Or([
                    //  #lt(x),                           // values before first
                    //  #betweenExclusive(x, y),          // values between first and second
                    //  #betweenExclusive(y, z),          // values between second and third
                    //  #gt(z)                            // values after last
                    // ])

                    let sorted_values = Array.sort(
                        values,
                        func(a : T.Candid, b : T.Candid) : T.Order {
                            CandidUtils.compare(#Empty, a, b);
                        },
                    );

                    let conditions = Array.tabulate(
                        sorted_values.size() + 1,
                        func(i : Nat) : ZenQueryLang {
                            if (i == 0) {
                                #Operation(key, #lt(sorted_values[0]));
                            } else if (i == sorted_values.size()) {
                                #Operation(key, #gt(sorted_values[sorted_values.size() - 1]));
                            } else {
                                reduce_op(key, #betweenExclusive(sorted_values[i - 1], sorted_values[i]));
                            };
                        },
                    );

                    #Or(conditions);

                };
                case (#not_(nested_op)) {
                    // #not_(#not_(x)) -> x
                    #Operation(key, nested_op);
                };
            };
        };

        func reduce_op(key : Text, op : ZqlOperators) : ZenQueryLang {
            switch (op) {
                // aliases
                case (#anyOf(values)) {
                    // #anyOf([x, y, z]) -> #Or([#eq(x), #eq(y), #eq(z)])
                    #Or(
                        Array.map(
                            values,
                            func(value : T.Candid) : ZenQueryLang {
                                #Operation(key, #eq(value));
                            },
                        )
                    );

                };
                case (#not_(not_op)) {
                    handle_not(key, not_op);
                };
                case (#between(min, max)) {
                    // #between(min, max) -> #And([#gte(min), #lte(max)])
                    #And([#Operation(key, #gte(min)), #Operation(key, #lte(max))]);
                };
                case (#betweenExclusive(min, max)) {
                    // #betweenExclusive(min, max) -> #And([#gt(min), #lt(max)])
                    #And([#Operation(key, #gt(min)), #Operation(key, #lt(max))]);
                };
                case (#betweenLeftOpen(min, max)) {
                    // #betweenLeftOpen(min, max) -> #And([#gt(min), #lte(max)])
                    #And([#Operation(key, #gt(min)), #Operation(key, #lte(max))]);
                };
                case (#betweenRightOpen(min, max)) {
                    // #betweenRightOpen(min, max) -> #And([#gte(min), #lt(max)])
                    #And([#Operation(key, #gte(min)), #Operation(key, #lt(max))]);
                };
                case (#startsWith(prefix)) {
                    let prefix_lower_bound = prefix;
                    let #ok(prefix_upper_bound) = CandidUtils.Ops.concatBytes(prefix, "\FF") else {
                        Debug.trap("QueryBuilder: Failed to create upper bound for #startsWith");
                    };

                    reduce_op(key, #between(prefix_lower_bound, prefix_upper_bound));

                };
                // core operations
                case (_) {
                    #Operation(key, op);
                };
            };
        };

        func handle_op(key : Text, op : ZqlOperators) {
            _buffer.add(reduce_op(key, op));
        };

        public func RawQuery(query_lang : T.ZenQueryLang) : QueryBuilder {
            update_query(true);
            _buffer.add(query_lang);

            self;

        };

        public func Where(key : Text, op : ZqlOperators) : QueryBuilder {
            return And(key, op);
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

        public func OrQuery(new_query : QueryBuilder) : QueryBuilder {
            update_query(false);

            let nested_query = new_query.build();
            _buffer.add(nested_query.query_operations);

            self;
        };

        public func AndQuery(new_query : QueryBuilder) : QueryBuilder {
            update_query(true);

            let nested_query = new_query.build();
            _buffer.add(nested_query.query_operations);

            self;
        };

        public func Sort(key : Text, direction : T.SortDirection) : QueryBuilder {
            _sort_by := ?(key, direction);
            self;
        };

        public func SortDirection(direction : T.SortDirection) : QueryBuilder {
            switch (_sort_by) {
                case (null) {
                    Debug.trap("QueryBuilder: SortDirection() called before SortField()");
                };
                case (?(key, _)) {
                    _sort_by := ?(key, direction);
                };
            };

            self;
        };

        public func SortField(key : Text) : QueryBuilder {
            switch (_sort_by) {
                case (null) {
                    _sort_by := ?(key, #Ascending);
                };
                case (?(_, direction)) {
                    _sort_by := ?(key, direction);
                };
            };

            self;
        };

        public func PaginationToken(cursor : PaginationToken) : QueryBuilder {
            _pagination_token := ?cursor;
            _pagination_skip := null; // reset skip when using cursor
            self;
        };

        public func Limit(limit : Nat) : QueryBuilder {
            _pagination_limit := ?limit;
            self;
        };

        public func Skip(skip : Nat) : QueryBuilder {
            switch (_pagination_token) {
                case (?_) {
                    Debug.trap("QueryBuilder: Cannot use Skip() when PaginationToken() is set");
                };
                case (null) {};
            };

            _pagination_skip := ?skip;
            self;
        };

        public func build() : StableQuery {
            update_query(not _is_and); // flushes _buffer because the state is switched

            let resolved_query = switch (_opt_nested_query) {
                case (null) #And([]);
                case (?#Operation(op)) #And([#Operation(op)]);
                case (?nested_query) nested_query;
            };

            // Debug.print("Query: " # debug_show resolved_query);

            {
                query_operations = flattenQuery(resolved_query);
                sort_by = _sort_by;
                pagination = {
                    cursor = _pagination_token;
                    limit = _pagination_limit;
                    skip = _pagination_skip;
                };
            };
        };

        public func clone() : QueryBuilder {
            let new_builder = QueryBuilder();

            new_builder._opt_nested_query := _opt_nested_query;
            new_builder._is_and := _is_and;
            for (item in _buffer.vals()) {
                new_builder._buffer.add(item);
            };
            new_builder._pagination_token := _pagination_token;
            new_builder._pagination_limit := _pagination_limit;
            new_builder._pagination_skip := _pagination_skip;
            new_builder._cursor_offset := _cursor_offset;
            new_builder._direction := _direction;
            new_builder._sort_by := _sort_by;

            new_builder;
        };

    };

    // validate that all the query fields are defined in the schema
    public func validateQuery(collection : T.StableCollection, zendb_query : T.ZenQueryLang) : T.Result<(), Text> {

        switch (zendb_query) {
            case (#Operation(field, op)) {
                // Debug.print(debug_show (Set.toArray(collection.schema_keys_set)));

                if (
                    field != "" and
                    Option.isNull(Nat.fromText(field)) and
                    not Set.has(collection.schema_keys_set, Map.thash, field) and
                    field != C.DOCUMENT_ID
                ) {

                    if (Text.contains(field, #text("."))) {
                        for (key in Text.split(field, #text("."))) {
                            if (Option.isNull(Nat.fromText(key)) and not Set.has(collection.schema_keys_set, Map.thash, key)) {
                                return #err("Field '" # key # "' not found in schema when validating query");
                            };
                        };
                    } else {
                        return #err("Field '" # field # "' not found in schema when validating query");
                    };
                };
            };
            case (#And(buffer)) {
                for (expr in buffer.vals()) {
                    switch (validateQuery(collection, expr)) {
                        case (#err(err)) return #err(err);
                        case (#ok(_)) ();
                    };
                };

            };
            case (#Or(buffer)) {
                for (expr in buffer.vals()) {
                    switch (validateQuery(collection, expr)) {
                        case (#err(err)) return #err(err);
                        case (#ok(_)) ();
                    };
                };
            };
        };

        #ok();
    };

    // Process the query and convert it to a format most suitable for the collection
    public func processQuery(collection : T.StableCollection, zendb_query : T.ZenQueryLang) : T.Result<T.ZenQueryLang, Text> {
        let log = Logger.NamespacedLogger(collection.logger, LOGGER_NAMESPACE).subnamespace("processQuery");

        func handle_operation(field : Text, op : T.ZqlOperators) : T.Result<T.ZenQueryLang, Text> {
            // Debug.print(debug_show (Set.toArray(collection.schema_keys_set)));
            let ?candid_type = SchemaMap.get(collection.schema_map, field) else {
                log.lazyDebug(func() = "Field '" # field # "' not found in schema");
                return #err("Field '" # field # "' not found in schema");
            };

            func handle_operator_value(operator_type : T.CandidType, operator_value : T.Candid) : T.Candid {
                switch (operator_type, operator_value) {
                    case (#Option(_), #Option(_) or #Null) operator_value;
                    case (#Option(_), _) {
                        // wrap with #Option only if the type is an #Option
                        // and the value is neither #Option nor #Null
                        CandidUtils.inheritOptionsFromType(operator_type, CandidUtils.unwrapOption(operator_value));
                    };
                    case (_) operator_value;
                };

            };

            func handle_operator(op : ZqlOperators, operator_value_type : T.CandidType) : T.ZqlOperators {

                switch (op) {
                    case (#eq(value)) #eq(handle_operator_value(operator_value_type, value));
                    case (#gte(value)) #gte(handle_operator_value(operator_value_type, value));
                    case (#lte(value)) #lte(handle_operator_value(operator_value_type, value));
                    case (#lt(value)) #lt(handle_operator_value(operator_value_type, value));
                    case (#gt(value)) #gt(handle_operator_value(operator_value_type, value));
                    case (#between(min, max)) #between(handle_operator_value(operator_value_type, min), handle_operator_value(operator_value_type, max));
                    case (#betweenExclusive(min, max)) #betweenExclusive(handle_operator_value(operator_value_type, min), handle_operator_value(operator_value_type, max));
                    case (#betweenLeftOpen(min, max)) #betweenLeftOpen(handle_operator_value(operator_value_type, min), handle_operator_value(operator_value_type, max));
                    case (#betweenRightOpen(min, max)) #betweenRightOpen(handle_operator_value(operator_value_type, min), handle_operator_value(operator_value_type, max));
                    case (#exists) #exists;
                    case (#startsWith(prefix)) #startsWith(handle_operator_value(operator_value_type, prefix));
                    case (#anyOf(values)) #anyOf(Array.map(values, func(value : T.Candid) : T.Candid = handle_operator_value(operator_value_type, value)));
                    case (#not_(op)) #not_(handle_operator(op, operator_value_type));
                };

            };

            let res = switch (candid_type) {
                case (#Option(_)) {
                    #Operation(field, handle_operator(op, candid_type));
                };
                case (_) #Operation(field, op);
            };

            #ok(res);
        };

        switch (zendb_query) {
            case (#Operation(field, op)) {
                (handle_operation(field, op));
            };
            case (#And(buffer)) {
                let new_buffer = Buffer.Buffer<ZenQueryLang>(buffer.size());
                for (expr in buffer.vals()) {
                    switch (processQuery(collection, expr)) {
                        case (#err(err)) return #err(err);
                        case (#ok(new_expr)) new_buffer.add(new_expr);
                    };
                };
                #ok(#And(Buffer.toArray(new_buffer)));
            };
            case (#Or(buffer)) {
                let new_buffer = Buffer.Buffer<ZenQueryLang>(buffer.size());
                for (expr in buffer.vals()) {
                    switch (processQuery(collection, expr)) {
                        case (#err(err)) return #err(err);
                        case (#ok(new_expr)) new_buffer.add(new_expr);
                    };
                };
                #ok(#Or(Buffer.toArray(new_buffer)));
            };
        };

    };

    // Flatten nested #And or #Or operations in a query
    // Example: #And([#Operation(...), #And([...])]) -> #And([#Operation(...), ...])
    public func flattenQuery(zendb_query : T.ZenQueryLang) : T.ZenQueryLang {

        func flattenAnd(queries : [ZenQueryLang]) : [ZenQueryLang] {
            let buffer = Buffer.Buffer<ZenQueryLang>(queries.size());

            for (q in queries.vals()) {
                switch (flattenQuery(q)) {
                    case (#And(nested)) {
                        // Flatten nested #And by merging its contents
                        for (nested_q in nested.vals()) {
                            buffer.add(nested_q);
                        };
                    };
                    case (#Or(nested)) {
                        if (nested.size() > 0) {
                            buffer.add(#Or(nested));
                        };
                    };
                    case (other) {
                        buffer.add(other);
                    };
                };
            };

            Buffer.toArray(buffer);
        };

        func flattenOr(queries : [ZenQueryLang]) : [ZenQueryLang] {
            let buffer = Buffer.Buffer<ZenQueryLang>(queries.size());

            for (q in queries.vals()) {
                switch (flattenQuery(q)) {
                    case (#Or(nested)) {
                        // Flatten nested #Or by merging its contents
                        for (nested_q in nested.vals()) {
                            buffer.add(nested_q);
                        };
                    };
                    case (#And(nested)) {
                        if (nested.size() > 0) {
                            buffer.add(#And(nested));
                        };
                    };
                    case (other) {
                        buffer.add(other);
                    };
                };
            };

            Buffer.toArray(buffer);
        };

        switch (zendb_query) {
            case (#Operation(field, op)) {
                #Operation(field, op);
            };
            case (#And(queries)) {
                #And(flattenAnd(queries));
            };
            case (#Or(queries)) {
                #Or(flattenOr(queries));
            };
        };
    };

    // public func optimizeQuery()

};
