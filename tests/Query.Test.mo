// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Buffer "mo:base/Buffer";

import { test; suite } "mo:test";

import Bench "mo:bench";
import Fuzz "mo:fuzz";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import ZenDB "../src";
import TestUtils "TestUtils";

let fuzz = Fuzz.fromSeed(0x7eadbeef);
let { QueryBuilder } = ZenDB;

let sstore = ZenDB.newStableStore();
let hydra_db = ZenDB.launch(sstore);

let limit = 10_000;

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

type Address = {
    street : Text;
    city : Text;
    country : Text;
    street_no: Nat;
    zip_code : Nat;
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
    address : Address;
    // address : ZenDB.Quadruple<Text, Text, Text, Text>;
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

let item_schema : ZenDB.Schema = #Record([
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
        #Record([
            ("street_no", #Nat),
            ("street", #Text),
            ("city", #Text),
            ("country", #Text),
            ("zip_code", #Nat),
        ]),
    ),
    // (
    //     "address",
    //     ZenDB.Schema.Quadruple(
    //         #Text, // street
    //         #Text, // city
    //         #Text, // country
    //         #Text, // zip
    //     ),
    // ),
    ("contact", #Record([("email", #Text), ("phone", #Option(#Text))])),
]);


let addresses = [
    {
        country = "Canada";
        cities = [
            {
                name = "Toronto";
                streets = ["King St", "Queen St", "Yonge St", "Bay St", "Bloor St", "Dundas St", "College St", "Spadina Ave", "St Clair Ave", "Danforth Ave", "Eglinton Ave", "Lawrence Ave"];
            },
            {
                name = "Ottawa";
                streets = ["Rideau St", "Bank St", "Elgin St", "Wellington St", "Albert St", "Laurier Ave", "Somerset St", "Gladstone Ave", "Bronson Ave", "Carling Ave", "Baseline Rd", "Merivale Rd"];
            },
        ];
    },
    {
        country = "USA";
        cities = [
            {
                name = "New York";
                streets = ["Broadway", "Wall St", "5th Ave", "Madison Ave", "Park Ave", "Lexington Ave", "6th Ave", "7th Ave", "8th Ave", "9th Ave", "10th Ave", "11th Ave"];
            },
            {
                name = "Los Angeles";
                streets = ["Hollywood Blvd", "Sunset Blvd", "Santa Monica Blvd", "Wilshire Blvd", "Pico Blvd", "Olympic Blvd", "Venice Blvd", "La Cienega Blvd", "La Brea Ave", "Fairfax Ave", "Melrose Ave", "Beverly Blvd"];
            },
        ];
    },
    {
        country = "France";
        cities = [
            {
                name = "Paris";
                streets = ["Champs-Elysees", "Rue de Rivoli", "Boulevard Saint-Germain", "Avenue Montaigne", "Rue du Faubourg Saint-Honore", "Rue de la Paix", "Rue de la Huchette", "Rue de la Harpe", "Rue de la Montagne Sainte-Genevieve", "Rue de la Bucherie", "Rue de la Bûcherie", "Rue de la Bûcherie"];
            },
            {
                name = "Marseille";
                streets = ["Rue de la Republique", "Rue Paradis", "Rue de Rome", "Rue Saint-Ferreol", "Rue de la Canebiere"];
            }
        ];
    }
];

func get_address():Address {
    let country = fuzz.array.randomEntry(addresses).1;
    let city = fuzz.array.randomEntry(country.cities).1;
    let street = fuzz.array.randomEntry(city.streets).1;

    {
        country = country.country;
        city = city.name;
        street = street;
        street_no = (fuzz.nat.randomRange(1, 999));
        zip_code = (fuzz.nat.randomRange(1000, 9999));
    }
};

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
        address = get_address();
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

let #ok(store_items_collection) = hydra_db.create_collection("store_items", item_schema, candify_store_item);
let #ok(_) = store_items_collection.create_index(["price"]);
let #ok(_) = store_items_collection.create_index(["name"]);
// let #ok(_) = store_items_collection.create_index(["address.country", "address.city", "address.street"]);

for (i in Itertools.range(0, limit)) {
    let item = new_item();
    buffer.add(item);
    ignore store_items_collection.put(item);
};



suite(
    "testing with store data",
    func() {

           // test(
        //     "find(): records with available sizes #s or #m",
        //     func() {
        //         let db_query = QueryBuilder().Where("available_sizes", #eq(#Variant([("s", #Null)]))).Or("available_sizes", #eq(#Variant([("m", #Null)])));
        //     },
        // );

        test(
            "Negative Query Test",
            func() {
                let db_query = QueryBuilder().Where("name", #eq(#Text("item-not-in-store")));
                let #ok(records) = store_items_collection.find(db_query);

                assert records == [];
            },
        );

        test(
            "find(): Returns error if query fields are not in schema",
            func() {
                let db_query = QueryBuilder().Where("field-not-in-schema", #eq(#Empty));
                let result = store_items_collection.find(db_query);

                switch (result) {
                    case (#err(_)) {};
                    case (#ok(_)) assert false;
                };
            },
        );

        test(
            "find(): records with price between 100 and 200",
            func() {
                let db_query = QueryBuilder().Where(
                    "price",
                    #gt(#Float(100.0)),
                ).And(
                    "price",
                    #lt(#Float(200.0)),
                );

                let #ok(records) = store_items_collection.find(db_query);

                let test_records = get_test_records(
                    func(i : Nat, item : StoreItem) : Bool {
                        item.price > 100.0 and item.price < 200.0;
                    }
                );

                for ((id, record) in records.vals()) {
                    assert record.price > 100.0 and record.price < 200.0;
                };

                assert records.size() == test_records.size();
            },

        );

        test(
            "find(): records with months_in_stock between 7 and 12 (inclusive)",
            func() {
                let db_query = QueryBuilder().Where(
                    "months_in_stock",
                    #gte(#Nat(7)),
                ).And(
                    "months_in_stock",
                    #lte(#Nat(12)),
                );

                let #ok(records) = store_items_collection.find(db_query);

                let test_records = get_test_records(
                    func(i : Nat, item : StoreItem) : Bool {
                        item.months_in_stock >= 7 and item.months_in_stock <= 12;
                    }
                );

                for ((id, record) in records.vals()) {
                    assert record.months_in_stock >= 7 and record.months_in_stock <= 12;
                };

                assert records.size() == test_records.size();
            },

        );

        test(
            "find(): records match any item in subset",
            func() {
                let db_query = QueryBuilder().Where(
                    "name",
                    #In([
                        #Text("black hoodie"),
                        #Text("M1 macbook"),
                        #Text("white hoodie"),
                    ]),
                );

                let #ok(records) = store_items_collection.find(db_query);

                let test_records = get_test_records(
                    func(i : Nat, item : StoreItem) : Bool {
                        item.name == "black hoodie" or item.name == "M1 macbook" or item.name == "white hoodie";
                    }
                );

                for ((id, record) in records.vals()) {
                    assert record.name == "black hoodie" or record.name == "M1 macbook" or record.name == "white hoodie";
                };

                assert records.size() == test_records.size();
            },
        );

        test(
            "find(): records with address in 'Toronto, Canada'",
            func() {
                let db_query = QueryBuilder().Where(
                    "address.country",
                    #eq(#Text("Canada")),
                ).And(
                    "address.city",
                    #eq(#Text("Toronto")),
                );

                let #ok(records) = store_items_collection.find(db_query);

                let test_records = get_test_records(
                    func(i : Nat, item : StoreItem) : Bool {
                        item.address.city == "Toronto";
                    }
                );

                for ((id, record) in records.vals()) {
                    assert record.address.city == "Toronto";
                };

                assert records.size() == test_records.size();
            },
        );

        test (
            "find(): only records with a phone number (is not null)",
            func(){
                let db_query = QueryBuilder().Where(
                    "contact.phone",
                    #Not(#eq(#Null)),
                );

                let #ok(records) = store_items_collection.find(db_query);

                TestUtils.validate_records(
                    records, 
                    func(i : Nat, item : StoreItem) : Bool {
                        item.contact.phone != null;
                    },
                    func(item : StoreItem) : Text {
                        debug_show item;
                    }
                );

            }
        );

    },
);
