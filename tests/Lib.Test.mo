// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";

import { test; suite } "mo:test";
import Candid "mo:serde/Candid";
import MemoryBTree "../src/memory-buffer/src/MemoryBTree/Base";
import Fuzz "mo:fuzz";

import HydraDB "../src";

let fuzz = Fuzz.fromSeed(0x7eadbeef);

type User = {
    name : Text;
    age : Nat;
    email : Text;
};

type User2 = {
    name : Text;
    age : Nat;
    email : Text;
    phone: ?Text;
};

let candify_user = {
    from_blob = func(blob : Blob) : User { let ?c : ?User = from_candid (blob); c; };
    to_blob = func(c : User) : Blob { to_candid (c); };
};

let candify_user2 = {
    from_blob = func(blob : Blob) : User2 { let ?c : ?User2 = from_candid (blob); c; };
    to_blob = func(c : User2) : Blob { to_candid (c); };
};

type Candid = Candid.Candid;

let { QueryBuilder } = HydraDB;

suite(
    "HydraDB Test",
    func() {

        test("Single Layer Record", func(){
                
            let hydra_db = HydraDB.new();

            let users_schema = #Record([
                ("name", #Text),
                ("age", #Nat),
                ("email", #Text),
            ]);

            ignore HydraDB.create_collection(hydra_db, "users", users_schema);

            for (i in Iter.range(0, 10)) {
                let user = {
                    name = "nam-do-san";
                    age = i;
                    email = "email";
                };

                ignore HydraDB.put<User>(hydra_db, "users", candify_user, user);
            };

            for (i in Iter.range(0, 10)) {
                let user = {
                    name = "claude";
                    age = i;
                    email = "email";
                };

                ignore HydraDB.put<User>(hydra_db, "users", candify_user, user);
            };

            let #ok(_) = HydraDB.create_index(hydra_db, "users", [("age")]);
            let #ok(name_index) = HydraDB.create_index(hydra_db, "users", [("name")]);
            let #ok(_) = HydraDB.create_index(hydra_db, "users", [("email")]);

            // let index_data_utils = HydraDB.get_index_data_utils(name_index.key_details);
            // let entries = MemoryBTree.scan(name_index.data, index_data_utils, ?[("name", #Text("nam-do-san")), (":record_id", #Nat(0))], ?[("name", #Text("nam-do-san")), (":record_id", #Nat(2 ** 64))]);

            // Debug.print(debug_show Iter.toArray(entries));

            Debug.print("Retrieve every user with the name 'nam-do-san'");
            var _query = QueryBuilder()
                ._where("name", #eq(#Text("nam-do-san")));

            var result = HydraDB.find<User>(hydra_db, "users", candify_user, _query);
            Debug.print(debug_show Iter.toArray(result));

            Debug.print("Retrieve every user between the age of 3 and 7");
            _query := QueryBuilder()
                ._where("age", #gte( #Nat(3)))
                ._and("age", #lte( #Nat(7)));

            result := HydraDB.find<User>(hydra_db, "users", candify_user, _query);
            Debug.print(debug_show Iter.toArray(result));

            Debug.print("Retrieve every user with the name 'nam-do-san' and age between 3 and 7");
            _query := QueryBuilder()
                ._where("age", #gte(#Nat(3)))
                ._and("age", #lte( #Nat(7)))
                ._and("name", #eq(#Text("nam-do-san")));

            result := HydraDB.find<User>(hydra_db, "users", candify_user, _query);
            Debug.print(debug_show Iter.toArray(result));

            Debug.print("Retrieve users between the age 0 and 2 named 'nam-do-san' or between the age 8 and 10 named 'claude'");
            
            let q1 = QueryBuilder()
                ._where("age", #gte(#Nat(0)))
                ._and("age", #lte(#Nat(2)))
                ._and("name", #eq(#Text("nam-do-san")));   

            _query := QueryBuilder()
                ._where("age", #gte(#Nat(8)))
                ._and("age", #lte(#Nat(10)))
                ._and("name", #eq(#Text("claude")))
                ._or_query(q1);

            let res = HydraDB.find<User2>(hydra_db, "users", candify_user2, _query);
            let array = Iter.toArray(res);
            Debug.print(debug_show array);

            Debug.print("Retrieve every user with an age of 0 or 10");
            _query := QueryBuilder()
                ._where("age", #eq(#Nat(0)))
                ._or("age", #eq(#Nat(10)));

            result := HydraDB.find<User>(hydra_db, "users", candify_user, _query);
            Debug.print(debug_show Iter.toArray(result));

            Debug.print("Update age of users named 'nam-do-san' to 0");
            _query := QueryBuilder()
                ._where("name", #eq(#Text("nam-do-san")));

            let #ok = HydraDB.update<User>(hydra_db, "users", candify_user, _query, func(user: User) : User {
                { user with age = 0 }
            });

            Debug.print("Retrieve every user with an age of 0 or 10");
            _query := QueryBuilder()
                ._where("age", #eq(#Nat(0)))
                ._or("age", #eq(#Nat(10)));

            result := HydraDB.find<User>(hydra_db, "users", candify_user, _query);
            Debug.print(debug_show Iter.toArray(result));

            Debug.print("Delete every user with an age of 0");
            _query := QueryBuilder()
                ._where("age", #eq(#Nat(0)));

            let #ok = HydraDB.delete<User>(hydra_db, "users", candify_user, _query);

            Debug.print("Retrieve every user with an age of 0");
            _query := QueryBuilder()
                ._where("age", #eq(#Nat(0)));

            result := HydraDB.find<User>(hydra_db, "users", candify_user, _query);
            Debug.print(debug_show Iter.toArray(result));
        });

        // test("Nested Record", func(){
        //     let hydra_db = HydraDB.new();
            
        //     type CustomerReview = {
        //         username : Text;
        //         rating : Nat;
        //         comment : Text;
        //     };

        //     type AvailableSizes = { #xs; #s; #m; #l; #xl };

        //     type ColorOption = {
        //         name : Text;
        //         hex : Text;
        //     };

        //     type StoreItem = {
        //         name : Text;
        //         store : Text;
        //         customer_reviews : [CustomerReview];
        //         available_sizes : AvailableSizes;
        //         color_options : [ColorOption];
        //         price : Float;
        //         in_stock : Bool;
        //         address : HydraDB.Quadruple<Text, Text, Text, Text>;
        //         contact : {
        //             email : Text;
        //             phone : ?Text;
        //         };
        //     };

        //     let candify_store_item = {
        //         from_blob = func(blob : Blob) : StoreItem { let ?c : ?StoreItem = from_candid (blob); c; };
        //         to_blob = func(c : StoreItem) : Blob { to_candid (c); };
        //     };

        //     let schema : HydraDB.Schema = #Record([
        //         ("name", #Text),
        //         ("store", #Text),
        //         ("customer_reviews", #Array(#Record([
        //             ("username", #Text),
        //             ("rating", #Nat),
        //             ("comment", #Text),
        //         ]))),
        //         ("available_sizes", #Variant([
        //             ("xs", #Null),
        //             ("s", #Null),
        //             ("m", #Null),
        //             ("l", #Null),
        //             ("xl", #Null),
        //         ])),
        //         ("color_options", #Array(#Record([
        //             ("name", #Text),
        //             ("hex", #Text),
        //         ]))),
        //         ("price", #Float),
        //         ("in_stock", #Bool),
        //         ("address", #Quadruple(
        //             #Text, // street
        //             #Text, // city
        //             #Text, // state
        //             #Text, // zip
        //         )),
        //         ("contact", #Record([
        //             ("email", #Text),
        //             ("phone", #Option(#Text)),
        //         ])),
        //     ]);

        //     ignore HydraDB.create_collection(hydra_db, "store_items", schema);

        //     let cities = ["Toronto", "Ottawa", "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"];
        //     let states = ["ON", "QC", "NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"];
        //     let streets = ["King St", "Queen St", "Yonge St", "Bay St", "Bloor St", "Dundas St", "College St", "Spadina Ave", "St Clair Ave", "Danforth Ave", "Eglinton Ave", "Lawrence Ave"];
            
        //     let stores = ["h&m", "zara", "gap", "old navy", "forever 21", "uniqlo", "urban outfitters", "american eagle", "aeropostale", "abercrombie & fitch", "hollister", "express"];
        //     let email_terminator = ["gmail.com", "yahoo.com", "outlook.com"];

        //     let cs_starter_kid = ["black hoodie", "M1 macbook", "white hoodie", "air forces", "Algorithms textbook", "c the hard way", "Udemy subscription", "Nvidea RTX"];

        //     let available_sizes = [#xs, #s, #m, #l, #xl];

        //     for (i in Iter.range(0, 100)) {
        //         let store_name = fuzz.array.randomEntry(stores).1;
        //         let store_item = {
        //             name = fuzz.array.randomEntry(cs_starter_kid).1;
        //             store = store_name;
        //             customer_reviews = [
        //                 { username = "user1"; rating = fuzz.nat.randomRange(0, 5); comment = "good" },
        //                 { username = "user2"; rating = fuzz.nat.randomRange(0, 5); comment = "ok" },
        //             ];
        //             available_sizes = fuzz.array.randomEntry(available_sizes).1;
        //             color_options = [
        //                 { name = "red"; hex = "#ff0000" },
        //                 { name = "blue"; hex = "#0000ff" },
        //             ];
        //             price = fuzz.float.randomRange(19.99, 399.99);
        //             in_stock = fuzz.bool.random();
        //             address = HydraDB.Quadruple(
        //                 fuzz.array.randomEntry(streets).1,
        //                 fuzz.array.randomEntry(cities).1,
        //                 fuzz.array.randomEntry(states).1, 
        //                 fuzz.text.randomAlphanumeric(6)
        //             );
        //             contact = { 
        //                 email = store_name # "@" # fuzz.array.randomEntry(email_terminator).1; 
        //                 phone = if (i % 3 == 0) { null } else { ?Text.fromIter(
        //                     fuzz.array
        //                         .randomArray<Char>(10, func() : Char { Char.fromNat32(fuzz.nat32.randomRange(0, 9) + Char.toNat32('0'))})
        //                         .vals() : Iter.Iter<Char>
        //                 ) };
        //             };
        //         };

        //         ignore HydraDB.put<StoreItem>(hydra_db, "store_items", candify_store_item, store_item);
        //     };

        //     let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["store", "in_stock", "price"]);
        //     let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["name", "price"]);
        //     // let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["name", "in_stock", "price"]);
        //     // let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["name", "color_options.name"]);
        //     // let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["name", "customer_reviews.rating"]);
        //     // let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["name", "available_sizes"]);
        //     // let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["in_stock"]);
        //     // let #ok(_) = HydraDB.create_index(hydra_db, "store_items", ["address.2", "store"]);

        //     Debug.print("Retrieve every store item with the name 'black hoodie'");
        //     var _query = QueryBuilder()
        //         ._where("name", #eq(#Text("black hoodie")));

        //     var result = HydraDB.find<StoreItem>(hydra_db, "store_items", candify_store_item, _query);
        //     Debug.print(debug_show Iter.toArray(result));
        // });
       
    },
);