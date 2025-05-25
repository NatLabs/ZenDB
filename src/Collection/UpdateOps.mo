import Debug "mo:base/Debug";
import Array "mo:base/Array";
import Nat "mo:base/Nat";

import Itertools "mo:itertools/Iter";

import T "../Types";
import CandidUtils "../CandidUtils";
import CandidMap "../CandidMap";
import Utils "../Utils";
import Logger "../Logger";

module {

    func handle_multi_field_update_operations(
        collection : T.StableCollection,
        candid_map : T.CandidMap,
        field_type : T.CandidType,
        field_value : T.Candid,
        op : T.FieldUpdateOperations,
    ) : T.Result<T.Candid, Text> {

        func handle_nested_operations(
            candid_map : T.CandidMap,
            nested_operations : [T.FieldUpdateOperations],
            operation_handler : (T.Iter<T.Candid>) -> T.Result<T.Candid, Text>,
        ) : T.Result<T.Candid, Text> {
            let candid_values = Array.init<T.Candid>(nested_operations.size(), #Null);

            for ((i, nested_op) in Itertools.enumerate(nested_operations.vals())) {

                switch (handle_field_update_operation_helper(collection, candid_map, field_type, field_value, nested_op)) {
                    case (#ok(candid_value)) candid_values[i] := candid_value;
                    case (#err(msg)) return #err(debug_show (nested_operations) # " failed: " # msg);
                };

            };

            operation_handler(candid_values.vals());
        };

        let res = switch (op) {
            case (#addAll(nested_operations)) {
                handle_nested_operations(candid_map, nested_operations, CandidUtils.MultiOps.add);
            };
            case (#subAll(nested_operations)) {
                handle_nested_operations(candid_map, nested_operations, CandidUtils.MultiOps.sub);
            };
            case (#mulAll(nested_operations)) {
                handle_nested_operations(candid_map, nested_operations, CandidUtils.MultiOps.mul);
            };
            case (#divAll(nested_operations)) {
                handle_nested_operations(candid_map, nested_operations, CandidUtils.MultiOps.div);
            };
            case (_) {
                #err("Invalid FieldUpdateOperations in handle_multi_field_update_operations(): " # debug_show op);
            };
        };

        res;
    };

    func handle_single_field_update_operation(
        collection : T.StableCollection,
        candid_map : T.CandidMap,
        field_type : T.CandidType,
        field_value : T.Candid,
        op : T.FieldUpdateOperations,
    ) : T.Result<T.Candid, Text> {

        func extract_candid_and_apply_fn_to_one_parameter(
            op : T.FieldUpdateOperations,
            fn : (T.Candid) -> T.Result<T.Candid, Text>,
        ) : T.Result<T.Candid, Text> {
            let candid = switch (handle_field_update_operation_helper(collection, candid_map, field_type, field_value, op)) {
                case (#ok(candid)) candid;
                case (#err(msg)) return #err("Failed to handle single field update operation: " # msg);
            };

            fn(candid);
        };

        func extract_candid_and_apply_fn_to_two_parameters(
            op1 : T.FieldUpdateOperations,
            op2 : T.FieldUpdateOperations,
            fn : (T.Candid, T.Candid) -> T.Result<T.Candid, Text>,
        ) : T.Result<T.Candid, Text> {
            let candid1 = switch (handle_field_update_operation_helper(collection, candid_map, field_type, field_value, op1)) {
                case (#ok(candid)) candid;
                case (#err(msg)) return #err("Failed to handle first parameter: " # msg);
            };

            let candid2 = switch (handle_field_update_operation_helper(collection, candid_map, field_type, field_value, op2)) {
                case (#ok(candid)) candid;
                case (#err(msg)) return #err("Failed to handle second parameter: " # msg);
            };

            fn(candid1, candid2);
        };

        let res : T.Result<T.Candid, Text> = switch (op) {
            case (#currValue) { return #ok(field_value) };
            case (#get(requested_field_name)) {
                let ?value = CandidMap.get(candid_map, collection.schema_map, requested_field_name) else return #err("Field '" # requested_field_name # "' not found in record");
                return #ok(value);
            };

            // number operations

            case (#abs(inner_op)) {
                extract_candid_and_apply_fn_to_one_parameter(inner_op, CandidUtils.Ops.abs);
            };
            case (#neg(inner_op)) {
                extract_candid_and_apply_fn_to_one_parameter(inner_op, CandidUtils.Ops.neg);
            };
            case (#floor(inner_op)) {
                extract_candid_and_apply_fn_to_one_parameter(inner_op, CandidUtils.Ops.floor);
            };
            case (#ceil(inner_op)) {
                extract_candid_and_apply_fn_to_one_parameter(inner_op, CandidUtils.Ops.ceil);
            };
            case (#sqrt(inner_op)) {
                extract_candid_and_apply_fn_to_one_parameter(inner_op, CandidUtils.Ops.sqrt);
            };
            case (#pow(base, exponent)) {
                extract_candid_and_apply_fn_to_two_parameters(base, exponent, CandidUtils.Ops.pow);
            };
            case (#min(a, b)) {
                extract_candid_and_apply_fn_to_two_parameters(a, b, CandidUtils.Ops.min);
            };
            case (#max(a, b)) {
                extract_candid_and_apply_fn_to_two_parameters(a, b, CandidUtils.Ops.max);
            };
            case (#mod(a, b)) {
                extract_candid_and_apply_fn_to_two_parameters(a, b, CandidUtils.Ops.mod);
            };
            case (#add(a, b)) {
                extract_candid_and_apply_fn_to_two_parameters(a, b, CandidUtils.Ops.add);
            };
            case (#sub(a, b)) {
                extract_candid_and_apply_fn_to_two_parameters(a, b, CandidUtils.Ops.sub);
            };
            case (#mul(a, b)) {
                extract_candid_and_apply_fn_to_two_parameters(a, b, CandidUtils.Ops.mul);
            };
            case (#div(a, b)) {
                extract_candid_and_apply_fn_to_two_parameters(a, b, CandidUtils.Ops.div);
            };

            // text operations
            case (#trim(inner_op, toTrim)) {
                extract_candid_and_apply_fn_to_one_parameter(
                    inner_op,
                    func(candid : T.Candid) : T.Result<T.Candid, Text> {
                        CandidUtils.Ops.trim(candid, toTrim);
                    },
                );
            };
            case (#lowercase(text)) {
                extract_candid_and_apply_fn_to_one_parameter(text, CandidUtils.Ops.lowercase);
            };
            case (#uppercase(text)) {
                extract_candid_and_apply_fn_to_one_parameter(text, CandidUtils.Ops.uppercase);
            };
            case (#replaceSubText(inner_op, search, replacement)) {
                extract_candid_and_apply_fn_to_one_parameter(
                    inner_op,
                    func(candid : T.Candid) : T.Result<T.Candid, Text> {
                        CandidUtils.Ops.replaceSubText(candid, search, replacement);
                    },
                );
            };
            case (#slice(inner_op, start, end)) {
                extract_candid_and_apply_fn_to_one_parameter(
                    inner_op,
                    func(candid : T.Candid) : T.Result<T.Candid, Text> {
                        CandidUtils.Ops.slice(candid, start, end);
                    },
                );
            };
            case (#concat(a, b)) {
                extract_candid_and_apply_fn_to_two_parameters(a, b, CandidUtils.Ops.concat);
            };

            case (_) {
                return #err("Invalid FieldUpdateOperations in handle_single_field_update_operation(): " # debug_show op);
            };
        };

        res;
    };

    func is_candid(op : T.FieldUpdateOperations) : Bool {
        switch (op) {
            case (
                #Text(_) or #Nat(_) or #Nat8(_) or #Nat16(_) or
                #Nat32(_) or #Nat64(_) or #Int(_) or #Int8(_) or
                #Int16(_) or #Int32(_) or #Int64(_) or #Float(_) or
                #Blob(_) or #Principal(_) or #Null(_) or #Option(_) or
                #Record(_) or #Map(_) or #Variant(_) or #Array(_)
            ) true;
            case (_) false;
        };
    };

    func to_candid_value(op : T.FieldUpdateOperations) : T.Result<T.Candid, Text> {
        let candid = switch (op) {
            case (#Text(value)) #Text(value);
            case (#Nat(value)) #Nat(value);
            case (#Nat8(value)) #Nat8(value);
            case (#Nat16(value)) #Nat16(value);
            case (#Nat32(value)) #Nat32(value);
            case (#Nat64(value)) #Nat64(value);
            case (#Int(value)) #Int(value);
            case (#Int8(value)) #Int8(value);
            case (#Int16(value)) #Int16(value);
            case (#Int32(value)) #Int32(value);
            case (#Int64(value)) #Int64(value);
            case (#Float(value)) #Float(value);
            case (#Blob(value)) #Blob(value);
            case (#Principal(value)) #Principal(value);
            case (#Null(_)) #Null;
            case (#Option(inner_value)) inner_value;
            case (#Record(value)) #Record(value);
            case (#Variant(value)) #Variant(value);
            case (#Array(value)) #Array(value);
            case (rest) return #err("Invalid candid type: " # debug_show (rest));
        };

        return #ok(candid);
    };

    func is_single_operation(op : T.FieldUpdateOperations) : Bool {
        switch (op) {
            case (
                #concat(_, _) or #get(_) or
                #abs(_) or #neg(_) or #floor(_) or #ceil(_) or #sqrt(_) or
                #pow(_, _) or #min(_, _) or #max(_, _) or #mod(_, _) or #trim(_, _) or
                #lowercase(_) or #uppercase(_) or #replaceSubText(_, _, _) or #slice(_, _, _) or
                #add(_, _) or #sub(_, _) or #mul(_, _) or #div(_, _) or #currValue(_)
            ) true;
            case (_) false;
        };
    };

    func handle_field_update_operation_helper(
        collection : T.StableCollection,
        candid_map : T.CandidMap,
        field_type : T.CandidType,
        field_value : T.Candid,
        op : T.FieldUpdateOperations,
    ) : T.Result<T.Candid, Text> {

        if (is_candid(op)) {
            Logger.lazyDebug(collection.logger, func() = "handle_field_update_operation_helper: op is candid: " # debug_show (op));
            return to_candid_value(op);
        } else if (is_single_operation(op)) {
            Logger.lazyDebug(collection.logger, func() = "handle_field_update_operation_helper: op is single operation: " # debug_show (op));
            let res = handle_single_field_update_operation(collection, candid_map, field_type, field_value, op);
            Logger.lazyDebug(
                collection.logger,
                func() = "res: " # debug_show (res),
            );
            return res;
        };

        Logger.lazyDebug(collection.logger, func() = "handle_field_update_operation_helper: op is multi operation: " # debug_show (op));
        handle_multi_field_update_operations(collection, candid_map, field_type, field_value, op)

    };

    public func handle_field_update_operation(
        collection : T.StableCollection,
        candid_map : T.CandidMap,
        field_type : T.CandidType,
        field_value : T.Candid,
        op : T.FieldUpdateOperations,
    ) : T.Result<T.Candid, Text> {

        let new_value = switch (handle_field_update_operation_helper(collection, candid_map, field_type, field_value, op)) {
            case (#ok(new_value)) new_value;
            case (#err(msg)) return #err("Failed to handle field update operation: " # msg);
        };

        switch (CandidUtils.Cast.cast(field_type, new_value)) {
            case (#ok(new_value_cast_to_type)) return #ok(new_value_cast_to_type);
            case (#err(err)) {
                let err_msg = "Failed to cast field result (" # debug_show (new_value) # ") to type '" # debug_show field_type # "': " # err;
                return Utils.log_error_msg(collection.logger, err_msg);
            };
        };

    };
};
