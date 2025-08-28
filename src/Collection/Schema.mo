import Result "mo:base/Result";
import Order "mo:base/Order";
import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Iter "mo:base/Iter";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Hash "mo:base/Hash";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Int32 "mo:base/Int32";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Int16 "mo:base/Int16";
import Int64 "mo:base/Int64";
import Int8 "mo:base/Int8";
import Nat16 "mo:base/Nat16";
import Nat8 "mo:base/Nat8";

import Int8Cmp "mo:memory-collection@0.3.2/TypeUtils/Int8Cmp";

import Itertools "mo:itertools/Iter";
import { sort_candid_type } "mo:serde/Candid/Blob/CandidUtils";

import T "../Types";
import Utils "../Utils";

module {

    type Order = Order.Order;
    type Schema = T.Schema;
    type Candid = T.Candid;
    type CandidQuery = T.CandidQuery;
    type Result<A, B> = Result.Result<A, B>;

    func send_error<A, B, C>(res : Result<A, B>) : Result<C, B> {
        switch (res) {
            case (#ok(_)) Debug.trap("send_error: unexpected error type");
            case (#err(err)) return #err(err);
        };
    };

    public func processSchema(schema : Schema) : Schema {
        sort_candid_type(schema);
    };

    // Interchangeable types
    // - #Array(#Nat8) <-> #Blob
    //
    // Forward compatible types
    // These types can be updated in the following ways but not the other way around:
    // - A document field can be updated to an optional type
    //     #Record ([("field", #Nat)]) -> #Record ([("field", #Option(#Nat))])
    // - A variant type can be extended to include new variant options, but can't remove existing ones
    //     #Variant ([("id", #Nat)]) -> #Variant ([("id", #Nat), ("name", #Text)])

    public func isSchemaBackwardCompatible(curr : Schema, new : Schema) : Bool {
        switch (curr, new) {
            case (#Empty, #Empty) true;
            case (#Null, #Null) true;
            case (#Text, #Text) true;
            case (#Nat, #Nat) true;
            case (#Int, #Int) true;
            case (#Float, #Float) true;
            case (#Bool, #Bool) true;
            case (#Principal, #Principal) true;
            case (#Option(inner_curr), #Option(inner_new)) isSchemaBackwardCompatible(inner_curr, inner_new);
            // types can be updated to become optional but not the other way around
            case (curr, #Option(inner_new)) isSchemaBackwardCompatible(curr, inner_new);
            case (#Blob, #Array(#Nat8)) true;
            case (#Array(#Nat8), #Blob) true;
            case (#Array(inner_curr), #Array(inner_new)) isSchemaBackwardCompatible(inner_curr, inner_new);
            case (#Tuple(curr), #Tuple(new)) {
                if (curr.size() != new.size()) return false;
                for ((a, b) in Itertools.zip(curr.vals(), new.vals())) {
                    if (not isSchemaBackwardCompatible(a, b)) return false;
                };
                true;
            };
            case (#Record(fields_curr), #Record(fields_new)) {
                let sorted_fields_new = Array.sort(
                    fields_new,
                    func(a : (Text, Schema), b : (Text, Schema)) : Order {
                        let ?i = Array.indexOf<(Text, Schema)>(a, fields_curr, func(a : (Text, Schema), b : (Text, Schema)) : Bool { a.0 == b.0 }) else return #greater;
                        let ?j = Array.indexOf(b, fields_curr, func(a : (Text, Schema), b : (Text, Schema)) : Bool { a.0 == b.0 }) else return #less;

                        Nat.compare(i, j);
                    },
                );

                for (i in Itertools.range(0, fields_curr.size())) {
                    let (name_curr, schema_curr) = fields_curr[i];
                    let (name_new, schema_new) = sorted_fields_new[i];
                    if (name_curr != name_new) return false;
                    if (not isSchemaBackwardCompatible(schema_curr, schema_new)) return false;
                };

                for (i in Itertools.range(fields_curr.size(), sorted_fields_new.size())) {
                    let (_, schema_new) = sorted_fields_new[i];

                    // new fields must be optional so they are backward compatible
                    let #Option(_) = schema_new else return false;
                };

                true;
            };
            case (#Variant(variants_curr), #Variant(variants_new)) {

                let sorted_variants_new = Array.sort(
                    variants_new,
                    func(a : (Text, Schema), b : (Text, Schema)) : Order {
                        let ?i = Array.indexOf(a, variants_curr, func(a : (Text, Schema), b : (Text, Schema)) : Bool { a.0 == b.0 }) else return #greater;
                        let ?j = Array.indexOf(b, variants_curr, func(a : (Text, Schema), b : (Text, Schema)) : Bool { a.0 == b.0 }) else return #less;

                        Nat.compare(i, j);
                    },
                );

                for (i in Itertools.range(0, variants_curr.size())) {
                    let (name_curr, schema_curr) = variants_curr[i];
                    let (name_new, schema_new) = variants_new[i];
                    if (name_curr != name_new) return false;
                    if (not isSchemaBackwardCompatible(schema_curr, schema_new)) return false;
                };

                // no need to validate new variants
                true;
            };
            case (_) false;
        };
    };

    public func validateSchema(schema : Schema) : Result<(), Text> {
        switch (schema) {
            case (#Array(_)) #err("Top level #Array is not supported");
            case (#Empty) #err("A schema with a single #Empty type is not valid, translated to () in motoko");
            case (#Null) #err("A schema with a single #Null type is not valid");
            case (others) #ok;
        };
    };

    public func validate(schema : Schema, document : Candid) : Result<(), Text> {

        switch (schema, document) {
            case (#Empty, #Empty) #ok;
            case (#Null, #Null) #ok;
            case (#Text, #Text(_)) #ok;
            case (#Nat, #Nat(_)) #ok;
            case (#Nat8, #Nat8(_)) #ok;
            case (#Nat16, #Nat16(_)) #ok;
            case (#Nat32, #Nat32(_)) #ok;
            case (#Nat64, #Nat64(_)) #ok;
            case (#Int, #Int(_)) #ok;
            case (#Int8, #Int8(_)) #ok;
            case (#Int16, #Int16(_)) #ok;
            case (#Int32, #Int32(_)) #ok;
            case (#Int64, #Int64(_)) #ok;
            case (#Float, #Float(_)) #ok;
            case (#Bool, #Bool(_)) #ok;
            case (#Principal, #Principal(_)) #ok;
            case (#Blob, #Blob(_)) #ok;
            case (#Option(inner), #Null) #ok;
            case (#Option(inner), document) {

                // it should pass in
                // the case where you update a schema type to be optional
                return validate(inner, document);
            };
            case (schema, #Option(inner)) {
                if (inner == #Null) return #ok;

                validate(schema, inner);
            };
            case (#Tuple(tuple_types), #Tuple(tuple_values)) {
                if (tuple_types.size() != tuple_values.size()) return #err("Tuple size mismatch: expected " # debug_show (tuple_types.size()) # ", got " # debug_show (tuple_values.size()));

                for ((i, value) in Itertools.enumerate(tuple_values.vals())) {
                    let res = validate(tuple_types[i], value);
                    let #ok(_) = res else return send_error(res);
                };

                #ok;

            };
            case (#Tuple(tuples), #Record(documents)) {
                if (documents.size() != tuples.size()) return #err("Tuple size mismatch: expected " # debug_show (tuples.size()) # ", got " # debug_show (documents.size()));

                for ((i, (key, _)) in Itertools.enumerate(documents.vals())) {
                    if (key != Nat.toText(i)) return #err("Tuple key mismatch: expected " # Nat.toText(i) # ", got " # debug_show (key));
                };

                for ((i, (key, value)) in Itertools.enumerate(documents.vals())) {
                    let res = validate(tuples[i], value);
                    let #ok(_) = res else return send_error(res);
                };

                #ok;

            };
            case (#Record(fields), #Record(documents)) {
                if (fields.size() != documents.size()) {
                    return #err("Record size mismatch: " # debug_show (("schema", fields.size()), ("document", documents.size())));
                };

                let sorted_fields = Array.sort(
                    fields,
                    func(a : (Text, Schema), b : (Text, Schema)) : Order {
                        Text.compare(a.0, b.0);
                    },
                );

                let sorted_records = Array.sort(
                    documents,
                    func(a : (Text, Candid), b : (Text, Candid)) : Order {
                        Text.compare(a.0, b.0);
                    },
                );

                // should sort fields and documents
                var i = 0;
                while (i < fields.size()) {
                    let field = sorted_fields[i];
                    let document = sorted_records[i];

                    if (field.0 != document.0) return #err("Record field mismatch: " # debug_show (("field", field.0), ("document", document.0)) # debug_show (fields, documents));

                    let res = validate(field.1, document.1);
                    let #ok(_) = res else return send_error(res);

                    i += 1;
                };

                #ok;
            };
            case (#Array(inner), #Array(documents)) {
                var i = 0;
                while (i < documents.size()) {
                    let res = validate(inner, documents[i]);
                    let #ok(_) = res else return send_error(res);
                    i += 1;
                };
                #ok;
            };
            case (#Variant(variants), #Variant((record_key, nested_record))) {

                let result = Array.find<(Text, Schema)>(
                    variants,
                    func((variant_name, _) : (Text, Schema)) : Bool {
                        variant_name == record_key;
                    },
                );

                // Debug.print("schema: " # debug_show (schema));
                // Debug.print("document: " # debug_show (document));

                switch (result) {
                    case (null) return #err("Variant not found in schema");
                    case (?(name, variant)) return validate(variant, nested_record);
                };
            };

            case (a, b) return #err("validate(): schema and document mismatch: " # debug_show (a, b) # " in " # debug_show (schema, document));
        };
    };

    // schema is added here to get the order of the #Variant type
    public func cmpCandid(schema : Schema, a : CandidQuery, b : CandidQuery) : Int8 {

        switch (schema, a, b) {
            // The #Minimum variant is used in queries to represent the minimum value
            case (_, #Minimum, _) -1;
            case (_, _, #Minimum) 1;

            // The #Maximum variant is used in queries to represent the maximum value
            case (_, #Maximum, _) 1;
            case (_, _, #Maximum) -1;

            case (_, #Null, #Null) 0;
            case (_, #Empty, #Empty) 0;

            case (_, _, #Null) 1;
            case (_, #Null, _) -1;

            case (_, #Text(a), #Text(b)) Int8Cmp.Text(a, b);
            case (_, #Blob(a), #Blob(b)) Int8Cmp.Blob(a, b);
            case (_, #Nat(a), #Nat(b)) Int8Cmp.Nat(a, b);
            case (_, #Nat8(a), #Nat8(b)) Int8Cmp.Nat8(a, b);
            case (_, #Nat16(a), #Nat16(b)) Int8Cmp.Nat16(a, b);
            case (_, #Nat32(a), #Nat32(b)) Int8Cmp.Nat32(a, b);
            case (_, #Nat64(a), #Nat64(b)) Int8Cmp.Nat64(a, b);
            case (_, #Principal(a), #Principal(b)) Int8Cmp.Principal(a, b);
            case (_, #Float(a), #Float(b)) Int8Cmp.Float(a, b);
            case (_, #Bool(a), #Bool(b)) Int8Cmp.Bool(a, b);
            case (_, #Int(a), #Int(b)) Int8Cmp.Int(a, b);
            case (_, #Int8(a), #Int8(b)) Int8Cmp.Int8(a, b);
            case (_, #Int16(a), #Int16(b)) Int8Cmp.Int16(a, b);
            case (_, #Int32(a), #Int32(b)) Int8Cmp.Int32(a, b);
            case (_, #Int64(a), #Int64(b)) Int8Cmp.Int64(a, b);

            case (_, #Option(a), #Option(b)) {
                switch (a, b) {
                    case (#Null, #Null) 0;
                    case (#Null, _) -1;
                    case (_, #Null) 1;
                    case (_, _) cmpCandid(schema, a, b);
                };
            };
            case (#Variant(schema), #Variant(a), #Variant(b)) {

                let ?i = Array.indexOf<(Text, Any)>(
                    a,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("cmpCandid: variant not found in schema");

                let ?j = Array.indexOf<(Text, Any)>(
                    b,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("cmpCandid: variant not found in schema");

                let res = Int8Cmp.Nat(i, j);

                if (res == 0) {
                    cmpCandid(schema[i].1, a.1, b.1);
                } else {
                    res;
                };

            };

            // case (_, #Array(a), #Array(b)) {
            //     // compare the length of the arrays
            //     let len_cmp = Int8Cmp.Nat(a.size(), b.size());
            //     if (len_cmp != 0) return len_cmp;

            //     let min_len = Nat.min(a.size(), b.size());
            //     for (i in Iter.range(0, min_len - 1)) {
            //         let cmp_result = cmpCandid(a[i], b[i]);
            //         if (cmp_result != 0) return cmp_result;
            //     };
            //     Int8Cmp.Nat(a.size(), b.size());
            // };

            case (schema, a, b) {
                // Debug.print(debug_show (a, b));
                Debug.trap("cmpCandid: unexpected candid type " # debug_show { schema; a; b });
            };
        };
    };

    public func cmpCandidIgnoreOption(schema : Schema, a : CandidQuery, b : CandidQuery) : Int8 {

        switch (schema, a, b) {
            // The #Minimum variant is used in queries to represent the minimum value
            case (_, #Minimum, _) -1;
            case (_, _, #Minimum) 1;

            // The #Maximum variant is used in queries to represent the maximum value
            case (_, #Maximum, _) 1;
            case (_, _, #Maximum) -1;

            case (_, #Null, #Null) 0;
            case (_, #Empty, #Empty) 0;

            case (_, _, #Null) 1;
            case (_, #Null, _) -1;

            case (_, #Text(a), #Text(b)) Int8Cmp.Text(a, b);
            case (_, #Blob(a), #Blob(b)) Int8Cmp.Blob(a, b);
            case (_, #Nat(a), #Nat(b)) Int8Cmp.Nat(a, b);
            case (_, #Nat8(a), #Nat8(b)) Int8Cmp.Nat8(a, b);
            case (_, #Nat16(a), #Nat16(b)) Int8Cmp.Nat16(a, b);
            case (_, #Nat32(a), #Nat32(b)) Int8Cmp.Nat32(a, b);
            case (_, #Nat64(a), #Nat64(b)) Int8Cmp.Nat64(a, b);
            case (_, #Principal(a), #Principal(b)) Int8Cmp.Principal(a, b);
            case (_, #Float(a), #Float(b)) Int8Cmp.Float(a, b);
            case (_, #Bool(a), #Bool(b)) Int8Cmp.Bool(a, b);
            case (_, #Int(a), #Int(b)) Int8Cmp.Int(a, b);
            case (_, #Int8(a), #Int8(b)) Int8Cmp.Int8(a, b);
            case (_, #Int16(a), #Int16(b)) Int8Cmp.Int16(a, b);
            case (_, #Int32(a), #Int32(b)) Int8Cmp.Int32(a, b);
            case (_, #Int64(a), #Int64(b)) Int8Cmp.Int64(a, b);

            case (_, #Option(a), #Option(b)) {
                switch (a, b) {
                    case (#Null, #Null) 0;
                    case (#Null, _) -1;
                    case (_, #Null) 1;
                    case (_, _) cmpCandid(schema, a, b);
                };
            };
            case (_, #Option(a), b) cmpCandidIgnoreOption(schema, a, b);
            case (_, a, #Option(b)) cmpCandidIgnoreOption(schema, a, b);
            case (#Variant(schema), #Variant(a), #Variant(b)) {

                let ?i = Array.indexOf<(Text, Any)>(
                    a,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("cmpCandid: variant not found in schema");

                let ?j = Array.indexOf<(Text, Any)>(
                    b,
                    schema,
                    func((name, _) : (Text, Any), (name2, _) : (Text, Any)) : Bool {
                        name == name2;
                    },
                ) else Debug.trap("cmpCandid: variant not found in schema");

                let res = Int8Cmp.Nat(i, j);

                if (res == 0) {
                    cmpCandid(schema[i].1, a.1, b.1);
                } else {
                    res;
                };

            };

            // case (_, #Array(a), #Array(b)) {
            //     // compare the length of the arrays
            //     let len_cmp = Int8Cmp.Nat(a.size(), b.size());
            //     if (len_cmp != 0) return len_cmp;

            //     let min_len = Nat.min(a.size(), b.size());
            //     for (i in Iter.range(0, min_len - 1)) {
            //         let cmp_result = cmpCandid(a[i], b[i]);
            //         if (cmp_result != 0) return cmp_result;
            //     };
            //     Int8Cmp.Nat(a.size(), b.size());
            // };

            case (schema, a, b) {
                // Debug.print(debug_show (a, b));
                Debug.trap("cmpCandid: unexpected candid type " # debug_show { schema; a; b });
            };
        };
    };

    public func generateDefaultValue(schema : Schema) : Result<Candid, Text> {
        let candid : Candid = switch (schema) {
            case (#Empty) #Empty;
            case (#Null) #Null;
            case (#Text) #Text("");
            case (#Nat) #Nat(0);
            case (#Nat8) #Nat8(0);
            case (#Nat16) #Nat16(0);
            case (#Nat32) #Nat32(0);
            case (#Nat64) #Nat64(0);
            case (#Int) #Int(0);
            case (#Int8) #Int8(0);
            case (#Int16) #Int16(0);
            case (#Int32) #Int32(0);
            case (#Int64) #Int64(0);
            case (#Float) #Float(0.0);
            case (#Bool) #Bool(false);
            case (#Principal) #Principal(Principal.fromBlob("\04")); // anonymous principal
            case (#Blob) #Blob("");
            case (#Option(inner)) switch (generateDefaultValue(inner)) {
                case (#ok(value)) #Option(value);
                case (#err(err)) return #err(err);
            };
            case (#Array(inner)) switch (generateDefaultValue(inner)) {
                case (#ok(val)) #Array([val]);
                case (#err(err)) return #err(err);
            };
            case (#Tuple(tuples)) {

                let buffer = Buffer.Buffer<Candid>(tuples.size());

                for (i in Itertools.range(0, tuples.size())) {
                    let tuple_type = tuples[i];
                    let value = switch (generateDefaultValue(tuple_type)) {
                        case (#ok(value)) value;
                        case (#err(err)) return #err(err);
                    };

                    buffer.add(value);
                };

                #Tuple(Buffer.toArray(buffer))

            };
            case (#Record(fields) or #Map(fields)) {

                let buffer = Buffer.Buffer<(Text, Candid)>(fields.size());
                for (i in Itertools.range(0, fields.size())) {
                    let (name, record_type) = fields[i];
                    let value = switch (generateDefaultValue(record_type)) {
                        case (#ok(value)) value;
                        case (#err(err)) return #err(err);
                    };

                    buffer.add((name, value));
                };

                #Record(Buffer.toArray(buffer));

            };

            case (#Variant(variants)) {
                let (name, variant_type) = variants[0];

                switch (generateDefaultValue(variant_type)) {
                    case (#ok(value)) #Variant((name, value));
                    case (#err(err)) return #err(err);
                };
            };

            case (_) return #err("generateDefaultValue: unexpected schema type " # debug_show (schema));
        };

        #ok(candid);
    };

    // public func validate_constraints()
};
