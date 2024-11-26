import Buffer "mo:base/Buffer";

import Itertools "mo:itertools/Iter";
import TypeUtils "mo:memory-collection/TypeUtils";

import T "Types";

module {
    type Candid = T.Candid;

    public let Orchid : TypeUtils.TypeUtils<[Candid]> = {
        blobify = {
            to_blob = func(candid_values : [Candid]) : Blob {
                let buffer = Buffer.Buffer<Nat8>(100);
                buffer.add(candid_values.size() |> Nat8.fromNat(_));

                func encode(buffer : Buffer.Buffer<Nat8>, candid : Candid) {

                    switch (candid) {
                        case (#Minimum) buffer.add(0);
                        case (#Array(_) or #Record(_) or #Map(_) or #Variant(_) or #Tuple(_)) Debug.trap("Orchid does not support compound types: " # debug_show (candid));
                        case (#Option(option_type)) {
                            buffer.add(CandidTypeCode.Option);
                            encode(buffer, option_type);
                        };

                        case (#Principal(p)) {
                            buffer.add(CandidTypeCode.Principal);

                            let blob = Principal.toBlob(p);
                            let bytes = Blob.toArray(blob);

                            let size = Nat8.fromNat(blob.size()); // -> Are principals only limited to 29 bytes? or just Principals for user and canister ids?

                            buffer.add(size);

                            var i = 0;
                            while (i < bytes.size()) {
                                buffer.add(bytes[i]);
                                i += 1;
                            };

                        };
                        case (#Text(t)) {
                            let utf8 = Text.encodeUtf8(t);
                            let bytes = Blob.toArray(utf8);
                            // let size = Nat8.fromNat(utf8.size()); -> the size will throw of the comparison, because text should be compared in lexicographic order and not by size
                            buffer.add(CandidTypeCode.Text);

                            var i = 0;
                            while (i < bytes.size()) {
                                buffer.add(bytes[i]);
                                i += 1;
                            };

                            buffer.add(0); // null terminator, helps with lexicographic comparison, if the text ends before the other one, it will be considered smaller because the null terminator is smaller than any other byte

                        };
                        case (#Blob(b)) {

                            let bytes = Blob.toArray(b);
                            let size = Nat32.fromNat(b.size());

                            buffer.add(CandidTypeCode.Blob);

                            buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat(size & 0xff)));

                            var i = 0;
                            while (i < bytes.size()) {
                                buffer.add(bytes[i]);
                                i += 1;
                            };

                        };
                        case (#Float(f)) Debug.trap("Orchid does not support Float type");
                        case (#Int(int)) {
                            buffer.add(CandidTypeCode.Int);

                            let sign : Nat8 = if (int < 0) 0 else 1;

                            var num = Int.abs(int);
                            var size : Nat8 = 0;

                            while (num > 0) {
                                num /= 255;
                                size += 1;
                            };

                            buffer.add(sign);

                            // buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                            // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                            // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));

                            buffer.add(size & 0xff);

                            num := Int.abs(int);

                            let bytes = Array.tabulate(
                                Nat8.toNat(size),
                                func(i : Nat) : Nat8 {
                                    let tmp = num % 255;
                                    num /= 255;
                                    Nat8.fromNat(tmp);
                                },
                            );

                            for (i in Itertools.range(0, bytes.size())) {
                                buffer.add(bytes[bytes.size() - 1 - i]);
                            };

                        };
                        case (#Int64(i)) {
                            buffer.add(CandidTypeCode.Int64);

                            let n = Int64.toNat64(i);

                            let msbyte = Nat8.fromNat(Nat64.toNat(n >> 56));
                            let msbyte_with_flipped_msbit = msbyte ^ 0x80;

                            buffer.add(msbyte_with_flipped_msbit);
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat(n & 0xff)));

                        };
                        case (#Int32(i)) {
                            buffer.add(CandidTypeCode.Int32);

                            let n = Int32.toNat32(i);

                            let msbyte = Nat8.fromNat(Nat32.toNat(n >> 24));
                            let msbyte_with_flipped_msbit = msbyte ^ 0x80;

                            buffer.add(msbyte_with_flipped_msbit);
                            buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));

                        };
                        case (#Int16(i)) {
                            buffer.add(CandidTypeCode.Int16);

                            let n = Int16.toNat16(i);

                            let most_significant_byte = Nat8.fromNat(Nat16.toNat(n >> 8));
                            let msbyte_with_flipped_msbit = most_significant_byte ^ 0x80;

                            buffer.add(msbyte_with_flipped_msbit);
                            buffer.add(Nat8.fromNat(Nat16.toNat(n & 0xff)));
                        };
                        case (#Int8(i)) {
                            buffer.add(CandidTypeCode.Int8);

                            let byte = Int8.toNat8(i);
                            let int8_with_flipped_msb = byte ^ 0x80;

                            buffer.add(int8_with_flipped_msb);
                        };

                        case (#Nat64(n)) {
                            buffer.add(CandidTypeCode.Nat64);

                            buffer.add(Nat8.fromNat(Nat64.toNat(n >> 56)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 48) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 40) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 32) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 24) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat((n >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat64.toNat(n & 0xff)));

                        };
                        case (#Nat32(n)) {
                            buffer.add(CandidTypeCode.Nat32);

                            buffer.add(Nat8.fromNat(Nat32.toNat(n >> 24)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((n >> 16) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat((n >> 8) & 0xff)));
                            buffer.add(Nat8.fromNat(Nat32.toNat(n & 0xff)));
                        };
                        case (#Nat16(n)) {
                            buffer.add(CandidTypeCode.Nat16);

                            buffer.add(Nat8.fromNat(Nat16.toNat(n >> 8)));
                            buffer.add(Nat8.fromNat(Nat16.toNat(n & 0xff)));
                        };
                        case (#Nat8(n)) {
                            buffer.add(CandidTypeCode.Nat8);
                            buffer.add(n);
                        };
                        case (#Nat(n)) {
                            buffer.add(CandidTypeCode.Nat);
                            var num = n;
                            var size : Nat8 = 0;

                            while (num > 0) {
                                num /= 255;
                                size += 1;
                            };

                            // buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                            // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                            // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));
                            // nat is limited to (2 ^ (255 * 8)) - 1
                            buffer.add(size & 0xff);

                            num := n;

                            let bytes = Array.tabulate(
                                Nat8.toNat(size),
                                func(i : Nat) : Nat8 {
                                    let tmp = num % 255;
                                    num /= 255;
                                    Nat8.fromNat(tmp);
                                },
                            );

                            for (i in Itertools.range(0, bytes.size())) {
                                buffer.add(bytes[bytes.size() - 1 - i]);
                            };

                        };
                        case (#Bool(b)) {
                            buffer.add(CandidTypeCode.Bool);
                            buffer.add(if (b) 1 else 0);
                        };
                        case (#Empty) buffer.add(CandidTypeCode.Empty);
                        case (#Null) buffer.add(CandidTypeCode.Null);
                        case (#Maximum) buffer.add(255);
                    };
                };

                var i = 0;
                while (i < candid_values.size()) {
                    encode(buffer, candid_values[i]);
                    i += 1;
                };

                Blob.fromArray(
                    Buffer.toArray(buffer)
                );

            };
            from_blob = func(blob : Blob) : [Candid] {
                let bytes = Blob.toArray(blob);

                let size = bytes[0] |> Nat8.toNat(_);

                var i = 1;

                let buffer = Buffer.Buffer<Candid>(8);
                //                case (#Nat(n)) {
                //     buffer.add(CandidTypeCode.Nat);
                //     var num = n;
                //     var size : Nat8 = 0;

                //     while (num > 0) {
                //         num /= 255;
                //         size += 1;
                //     };

                //     // buffer.add(Nat8.fromNat(Nat32.toNat(size >> 24)));
                //     // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 16) & 0xff)));
                //     // buffer.add(Nat8.fromNat(Nat32.toNat((size >> 8) & 0xff)));
                //     // nat is limited to (2 ^ (255 * 8)) - 1
                //     buffer.add(size & 0xff);

                //     num := n;

                //     let bytes = Array.tabulate(
                //         Nat8.toNat(size),
                //         func(i : Nat) : Nat8 {
                //             let tmp = num % 255;
                //             num /= 255;
                //             Nat8.fromNat(tmp);
                //         },
                //     );

                //     for (i in Itertools.range(0, bytes.size())) {
                //         buffer.add(bytes[bytes.size() - 1 - i]);
                //     };

                // };

                func read() : Nat8 {
                    let byte = bytes[i];
                    i += 1;
                    byte;
                };

                label decoding while (i < bytes.size()) {

                    let type_code = read();

                    if (type_code == CandidTypeCode.Nat) {
                        let size = read() |> Nat8.toNat(_);

                        var num = 0;

                        for (i in Itertools.range(0, size)) {
                            let byte = read();
                            num *= 255;
                            num += Nat8.toNat(byte);
                        };

                        buffer.add(#Nat(num));

                    } else if (type_code == CandidTypeCode.Text) {
                        var text = "";

                        label extracting_text loop {

                            let byte = read();
                            if (byte == 0) break extracting_text;

                            let char = byte |> Nat8.toNat(_) |> Nat32.fromNat(_) |> Char.fromNat32(_);
                            text #= Char.toText(char);
                        };

                        buffer.add(#Text(text));

                    } else break decoding;

                };

                Buffer.toArray(buffer);
            };
        };
        cmp = TypeUtils.MemoryCmp.Default;

        hash = func(a : Candid, b : Candid) : Nat64 = Debug.trap("Orchid does not support hashing");
    };
};
