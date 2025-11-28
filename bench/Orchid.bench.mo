import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Array "mo:base/Array";

import Bench "mo:bench";
import Fuzz "mo:fuzz";

import Orchid "../src/EmbeddedInstance/Collection/Orchid";
import T "../src/EmbeddedInstance/Types";

module {
    type CandidQuery = T.CandidQuery;

    public func init() : Bench.Bench {
        let fuzz = Fuzz.fromSeed(0xdeadbeef);

        let bench = Bench.Bench();
        bench.name("Benchmarking Orchid Encoder/Decoder");
        bench.description("Benchmarking the performance with 1k random values per type");

        bench.cols([
            "encode()",
            "decode()",
        ]);

        bench.rows([
            "Null",
            "Empty",
            "Bool",
            "Nat8",
            "Nat16",
            "Nat32",
            "Nat64",
            "Nat",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int",
            "Float",
            "Principal",
            "Text",
            "Blob",
            "Option(Nat)",
        ]);

        let limit = 1_000;

        // Generate random test data for each type
        let null_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Null);
        let empty_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Empty);
        let bool_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Bool(fuzz.bool.random()));

        let nat8_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Nat8(fuzz.nat8.random()));
        let nat16_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Nat16(fuzz.nat16.random()));
        let nat32_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Nat32(fuzz.nat32.random()));
        let nat64_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Nat64(fuzz.nat64.random()));
        let nat_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Nat(fuzz.nat.randomRange(0, 1_000_000)));

        let int8_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Int8(fuzz.int8.random()));
        let int16_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Int16(fuzz.int16.random()));
        let int32_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Int32(fuzz.int32.random()));
        let int64_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Int64(fuzz.int64.random()));
        let int_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Int(fuzz.int.randomRange(-1_000_000, 1_000_000)));

        let float_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Float(fuzz.float.random()));

        let principal_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Principal(fuzz.principal.randomPrincipal(29)));
        let text_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Text(fuzz.text.randomAlphanumeric(fuzz.nat.randomRange(10, 100))));
        let blob_values : [CandidQuery] = Array.tabulate<CandidQuery>(limit, func(_ : Nat) : CandidQuery = #Blob(fuzz.blob.randomBlob(fuzz.nat.randomRange(10, 100))));

        let option_values : [CandidQuery] = Array.tabulate<CandidQuery>(
            limit,
            func(_ : Nat) : CandidQuery {
                // 20% chance of being null
                if (fuzz.nat.randomRange(0, 100) < 20) {
                    #Null;
                } else {
                    #Option(#Nat(fuzz.nat.randomRange(0, 1_000_000)));
                };
            },
        );

        // Pre-encode all values for decode benchmarks
        let null_encoded = Array.map<CandidQuery, Blob>(null_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let empty_encoded = Array.map<CandidQuery, Blob>(empty_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let bool_encoded = Array.map<CandidQuery, Blob>(bool_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let nat8_encoded = Array.map<CandidQuery, Blob>(nat8_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let nat16_encoded = Array.map<CandidQuery, Blob>(nat16_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let nat32_encoded = Array.map<CandidQuery, Blob>(nat32_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let nat64_encoded = Array.map<CandidQuery, Blob>(nat64_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let nat_encoded = Array.map<CandidQuery, Blob>(nat_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let int8_encoded = Array.map<CandidQuery, Blob>(int8_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let int16_encoded = Array.map<CandidQuery, Blob>(int16_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let int32_encoded = Array.map<CandidQuery, Blob>(int32_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let int64_encoded = Array.map<CandidQuery, Blob>(int64_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let int_encoded = Array.map<CandidQuery, Blob>(int_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let float_encoded = Array.map<CandidQuery, Blob>(float_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let principal_encoded = Array.map<CandidQuery, Blob>(principal_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let text_encoded = Array.map<CandidQuery, Blob>(text_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let blob_encoded = Array.map<CandidQuery, Blob>(blob_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));
        let option_encoded = Array.map<CandidQuery, Blob>(option_values, func(v : CandidQuery) : Blob = Orchid.Orchid.blobify.to_blob([v]));

        bench.runner(
            func(row, col) = switch (row, col) {
                case ("Null", "encode()") {
                    for (i in null_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([null_values[i]]);
                    };
                };
                case ("Null", "decode()") {
                    for (i in null_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(null_encoded[i]);
                    };
                };

                case ("Empty", "encode()") {
                    for (i in empty_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([empty_values[i]]);
                    };
                };
                case ("Empty", "decode()") {
                    for (i in empty_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(empty_encoded[i]);
                    };
                };

                case ("Bool", "encode()") {
                    for (i in bool_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([bool_values[i]]);
                    };
                };
                case ("Bool", "decode()") {
                    for (i in bool_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(bool_encoded[i]);
                    };
                };

                case ("Nat8", "encode()") {
                    for (i in nat8_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([nat8_values[i]]);
                    };
                };
                case ("Nat8", "decode()") {
                    for (i in nat8_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(nat8_encoded[i]);
                    };
                };

                case ("Nat16", "encode()") {
                    for (i in nat16_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([nat16_values[i]]);
                    };
                };
                case ("Nat16", "decode()") {
                    for (i in nat16_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(nat16_encoded[i]);
                    };
                };

                case ("Nat32", "encode()") {
                    for (i in nat32_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([nat32_values[i]]);
                    };
                };
                case ("Nat32", "decode()") {
                    for (i in nat32_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(nat32_encoded[i]);
                    };
                };

                case ("Nat64", "encode()") {
                    for (i in nat64_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([nat64_values[i]]);
                    };
                };
                case ("Nat64", "decode()") {
                    for (i in nat64_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(nat64_encoded[i]);
                    };
                };

                case ("Nat", "encode()") {
                    for (i in nat_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([nat_values[i]]);
                    };
                };
                case ("Nat", "decode()") {
                    for (i in nat_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(nat_encoded[i]);
                    };
                };

                case ("Int8", "encode()") {
                    for (i in int8_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([int8_values[i]]);
                    };
                };
                case ("Int8", "decode()") {
                    for (i in int8_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(int8_encoded[i]);
                    };
                };

                case ("Int16", "encode()") {
                    for (i in int16_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([int16_values[i]]);
                    };
                };
                case ("Int16", "decode()") {
                    for (i in int16_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(int16_encoded[i]);
                    };
                };

                case ("Int32", "encode()") {
                    for (i in int32_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([int32_values[i]]);
                    };
                };
                case ("Int32", "decode()") {
                    for (i in int32_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(int32_encoded[i]);
                    };
                };

                case ("Int64", "encode()") {
                    for (i in int64_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([int64_values[i]]);
                    };
                };
                case ("Int64", "decode()") {
                    for (i in int64_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(int64_encoded[i]);
                    };
                };

                case ("Int", "encode()") {
                    for (i in int_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([int_values[i]]);
                    };
                };
                case ("Int", "decode()") {
                    for (i in int_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(int_encoded[i]);
                    };
                };

                case ("Float", "encode()") {
                    for (i in float_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([float_values[i]]);
                    };
                };
                case ("Float", "decode()") {
                    for (i in float_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(float_encoded[i]);
                    };
                };

                case ("Principal", "encode()") {
                    for (i in principal_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([principal_values[i]]);
                    };
                };
                case ("Principal", "decode()") {
                    for (i in principal_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(principal_encoded[i]);
                    };
                };

                case ("Text", "encode()") {
                    for (i in text_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([text_values[i]]);
                    };
                };
                case ("Text", "decode()") {
                    for (i in text_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(text_encoded[i]);
                    };
                };

                case ("Blob", "encode()") {
                    for (i in blob_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([blob_values[i]]);
                    };
                };
                case ("Blob", "decode()") {
                    for (i in blob_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(blob_encoded[i]);
                    };
                };

                case ("Option(Nat)", "encode()") {
                    for (i in option_values.keys()) {
                        ignore Orchid.Orchid.blobify.to_blob([option_values[i]]);
                    };
                };
                case ("Option(Nat)", "decode()") {
                    for (i in option_encoded.keys()) {
                        ignore Orchid.Orchid.blobify.from_blob(option_encoded[i]);
                    };
                };

                case (_) {
                    Debug.trap("Should not reach with row = " # debug_show row # " and col = " # debug_show col);
                };
            }
        );

        bench;
    };
};
