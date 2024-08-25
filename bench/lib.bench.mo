import Iter "mo:base/Iter";
import Debug "mo:base/Debug";
import Prelude "mo:base/Prelude";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Buffer "mo:base/Buffer";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";

import HydraDB "../src";

module {
    public func init() : Bench.Bench {
        let bench = Bench.Bench();

        bench.name("Benchmarking HydraDB");
        bench.description("Benchmarking the performance with 10k calls");

        bench.cols(["HydraDB"]);
        bench.rows([
            "put() no index",
            "updateById() 1",
            "create_index()",
            "clear collection data",
            "put() with 1 index",
            "updateById() 2",
            "create 2nd index",
            "clear collection data",
            "put() with 2 indexes",
            "updateById() 3",
            "create 3rd index",
            "clear collection data",
            "put() with 3 indexes",
            "updateById() 4",
            "get()",
            "scan(): all records",
            "find(): users named 'nam-do-dan'",
            "find(): users between the age of 20 and 35",
            "find(): users between the age of 20 and 35 and named 'nam-do-dan'",
            "find(): users between the age of 20 and 35 and named 'nam-do-dan' v2",
        ]);

        type Candid = Candid.Candid;

        let fuzz = Fuzz.Fuzz();
        let { QueryBuilder } = HydraDB;

        let hydra_db = HydraDB.new();

        let limit = 1_000;

        type CustomerReview = {
            username : Text;
            rating : Nat;
            comment : Text;
        };

        type AvailableSizes = { #xs; #s; #m; #l; #xl };

        type ColorOption = {
            name : Text;
            hex : Text;
        };

        type StoreItem = {
            name : Text;
            store : Text;
            months_in_stock : Nat;
            customer_reviews : [CustomerReview];
            available_sizes : AvailableSizes;
            color_options : [ColorOption];
            price : Float;
            in_stock : Bool;
            address : HydraDB.Quadruple<Text, Text, Text, Text>;
            contact : {
                email : Text;
                phone : ?Text;
            };
        };

        let candify_store_item = {
            from_blob = func(blob : Blob) : StoreItem {
                let ?c : ?StoreItem = from_candid (blob);
                c;
            };
            to_blob = func(c : StoreItem) : Blob { to_candid (c) };
        };

        let item_schema : HydraDB.Schema = #Record([
            ("name", #Text),
            ("store", #Text),
            ("months_in_stock", #Nat),
            ("customer_reviews", #Array(#Record([("username", #Text), ("rating", #Nat), ("comment", #Text)]))),
            ("available_sizes", #Variant([("xs", #Null), ("s", #Null), ("m", #Null), ("l", #Null), ("xl", #Null)])),
            ("color_options", #Array(#Record([("name", #Text), ("hex", #Text)]))),
            ("price", #Float),
            ("in_stock", #Bool),
            (
                "address",
                HydraDB.Schema.Quadruple(
                    #Text, // street
                    #Text, // city
                    #Text, // state
                    #Text, // zip
                ),
            ),
            ("contact", #Record([("email", #Text), ("phone", #Option(#Text))])),
        ]);

        let cities = ["Toronto", "Ottawa", "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"];
        let states = ["ON", "QC", "NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"];
        let streets = ["King St", "Queen St", "Yonge St", "Bay St", "Bloor St", "Dundas St", "College St", "Spadina Ave", "St Clair Ave", "Danforth Ave", "Eglinton Ave", "Lawrence Ave"];

        let stores = ["h&m", "zara", "gap", "old navy", "forever 21", "uniqlo", "urban outfitters", "american eagle", "aeropostale", "abercrombie & fitch", "hollister", "express"];
        let email_terminator = ["gmail.com", "yahoo.com", "outlook.com"];

        let cs_starter_kit = ["black hoodie", "M1 macbook", "white hoodie", "air forces", "Algorithms textbook", "c the hard way", "Udemy subscription", "Nvidea RTX"];

        let available_sizes = [#xs, #s, #m, #l, #xl];

        func new_item() : StoreItem {
            let store_name = fuzz.array.randomEntry(stores).1;
            let store_item = {
                name = fuzz.array.randomEntry(cs_starter_kit).1;
                store = store_name;
                months_in_stock = fuzz.nat.randomRange(1, 12);
                customer_reviews = [
                    {
                        username = "user1";
                        rating = fuzz.nat.randomRange(0, 5);
                        comment = "good";
                    },
                    {
                        username = "user2";
                        rating = fuzz.nat.randomRange(0, 5);
                        comment = "ok";
                    },
                ];
                available_sizes = fuzz.array.randomEntry(available_sizes).1;
                color_options = [
                    { name = "red"; hex = "#ff0000" },
                    { name = "blue"; hex = "#0000ff" },
                ];
                price = fuzz.float.randomRange(19.99, 399.99);
                in_stock = fuzz.bool.random();
                address = HydraDB.Quadruple(
                    fuzz.array.randomEntry(streets).1,
                    fuzz.array.randomEntry(cities).1,
                    fuzz.array.randomEntry(states).1,
                    fuzz.text.randomAlphanumeric(6),
                );
                contact = {
                    email = store_name # "@" # fuzz.array.randomEntry(email_terminator).1;
                    phone = if (fuzz.nat.randomRange(0, 100) % 3 == 0) { null } else {
                        ?Text.fromIter(
                            fuzz.array.randomArray<Char>(10, func() : Char { Char.fromNat32(fuzz.nat32.randomRange(0, 9) + Char.toNat32('0')) }).vals() : Iter.Iter<Char>
                        );
                    };
                };
            };
        };

        let buffer = Buffer.Buffer<StoreItem>(limit);
        let #ok(collection) = HydraDB.create_collection(hydra_db, "store_items", item_schema);

        Debug.print(debug_show collection.schema_keys);
        for (i in Itertools.range(0, limit)) {
            let item = new_item();
            buffer.add(item);
        };

        bench.runner(
            func(col, row) = switch (row, col) {

                case ("HydraDB", "put() no index") {
                    for (i in Itertools.range(0, limit)) {
                        let item = buffer.get(i);
                        let #ok(_) = HydraDB.put<StoreItem>(hydra_db, "store_items", candify_store_item, item);
                    };
                };
                case ("HydraDB", "create_index()") {
                    let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["store", "in_stock", "price"]);
                };
                case ("HydraDB", "clear collection data") {
                    HydraDB.clear_collection(hydra_db, "store_items");
                };
                case ("HydraDB", "put() with 1 index") {
                    for (i in Itertools.range(0, limit)) {
                        let item = buffer.get(i);
                        let #ok(_) = HydraDB.put<StoreItem>(hydra_db, "store_items", candify_store_item, item);
                    };
                };
                case ("HydraDB", "create 2nd index") {
                    let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["name", "price"]);
                };
                case ("HydraDB", "put() with 2 indexes") {
                    for (i in Itertools.range(0, limit)) {
                        let item = buffer.get(i);
                        let #ok(_) = HydraDB.put<StoreItem>(hydra_db, "store_items", candify_store_item, item);
                    };
                };
                case ("HydraDB", "create 3rd index") {
                    let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["name", "in_stock", "price"]);
                };
                case ("HydraDB", "put() with 3 indexes") {
                    for (i in Itertools.range(0, limit)) {
                        let item = buffer.get(i);
                        let #ok(_) = HydraDB.put<StoreItem>(hydra_db, "store_items", candify_store_item, item);
                    };
                };
                case ("HydraDB", "updateById() 1" or "updateById() 2" or "updateById() 3" or "updateById() 4") {
                    for (i in Itertools.range(0, limit)) {

                        let #ok(_) = HydraDB.updateById<StoreItem>(
                            hydra_db,
                            "store_items",
                            candify_store_item,
                            i,
                            func(prev : StoreItem) : StoreItem {
                                { prev with price = prev.price + 1 };
                            },
                        );
                    };
                };
                case ("HydraDB", "get()") {
                    for (i in Itertools.range(0, limit)) {
                        let #ok(item) = HydraDB.get<StoreItem>(hydra_db, "store_items", candify_store_item, i);
                    };
                };
                case ("HydraDB", "scan(): all records") {
                    let result = HydraDB.scan<StoreItem>(hydra_db, "store_items", candify_store_item, [], []);
                    Debug.print("results: " # debug_show (Iter.toArray(result)));
                };

                case ("HydraDB", "find(): users named 'nam-do-dan'") {
                    let _query = QueryBuilder()._where("name", #eq(#Text("nam-do-san")));
                    let result = HydraDB.find<StoreItem>(hydra_db, "store_items", candify_store_item, _query);
                    Debug.print("results: " # debug_show (Iter.toArray(result)));
                };
                case ("HydraDB", "find(): users between the age of 20 and 35") {
                    let _query = QueryBuilder()._where("age", #gte(#Nat(20)))._and("age", #lte(#Nat(35)));

                    let result = HydraDB.find<StoreItem>(hydra_db, "store_items", candify_store_item, _query);
                    Debug.print("results: " # debug_show (Iter.toArray(result)));
                };
                case ("HydraDB", "find(): users between the age of 20 and 35 and named 'nam-do-dan'") {
                    let _query = QueryBuilder()._where("name", #eq(#Text("nam-do-san")))._and("age", #gte(#Nat(20)))._and("age", #lte(#Nat(35)));

                    let result = HydraDB.find<StoreItem>(hydra_db, "store_items", candify_store_item, _query);
                    Debug.print("results: " # debug_show (Iter.toArray(result)));
                };

                case ("HydraDB", "find(): users between the age of 20 and 35 and named 'nam-do-dan' v2") {
                    let _query = QueryBuilder()._where("email", #eq(#Text("email")))._where("age", #gte(#Nat(20)))._and("age", #lte(#Nat(35)))._and("name", #eq(#Text("nam-do-san")));

                    let result = HydraDB.find<StoreItem>(hydra_db, "store_items", candify_store_item, _query);
                    Debug.print("results: " # debug_show (Iter.toArray(result)));
                };

                case (_) {
                    Debug.trap("Should be unreachable:\n row = \"" # debug_show row # "\" and col = \"" # debug_show col # "\"");
                };
            }
        );

        bench;
    };
};
