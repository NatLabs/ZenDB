import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";

import { test; suite } "mo:test";
import Candid "mo:serde/Candid";
import CandidMap "../src/CandidMap";
import ZenDB "../src";

let schema : ZenDB.Schema = #Record([
    ("name", #Text),
    ("age", #Nat),
    ("email", #Text),
    (
        "details",
        #Record([
            ("phone", #Text),
            ("address", #Text),
            (
                "settings",
                #Option(#Record([("theme", #Text), ("notifications", #Bool)])),
            ),
        ]),
    ),
    (
        "version",
        #Variant([
            ("v1", #Nat),
            ("v2", #Text),
            (
                "v3",
                #Record([("major", #Nat), ("minor", #Nat), ("patch", #Nat)]),
            ),
        ]),
    ),
    ("tuple", #Tuple([#Nat, #Text])),
    ("tags", #Array(#Text)),
    ("comments", #Array(#Record([("content", #Text), ("created_at", #Nat)]))),
]);

let candid : Candid.Candid = #Record([
    ("name", #Text("Alice")),
    ("age", #Nat(25)),
    ("email", #Text("user_email@gmail.com")),
    ("details", #Record([("phone", #Text("1234567890")), ("address", #Text("123, 4th Cross, 5th Main, Bangalore")), ("settings", #Option(#Record([("theme", #Text("dark")), ("notifications", #Bool(true))])))])),
    ("version", #Variant("v1", #Nat(1))),
    ("tuple", #Tuple([#Nat(1), #Text("text")])),
    ("tags", #Array([#Text("new"), #Text("popular")])),
    ("comments", #Array([#Record([("content", #Text("comment1")), ("created_at", #Nat(1234567890))])])),
]);

let candid_map = CandidMap.CandidMap(schema, candid);

suite(
    "CandidMap",
    func() {
        test(
            "get()",
            func() {

                assert candid_map.get("name") == ?#Text("Alice");
                assert candid_map.get("age") == ?#Nat(25);
                assert candid_map.get("email") == ?#Text("user_email@gmail.com");
                assert candid_map.get("details.phone") == ?#Text("1234567890");
                assert candid_map.get("details.address") == ?#Text("123, 4th Cross, 5th Main, Bangalore");
                assert candid_map.get("details.settings.theme") == ?#Option(#Text("dark"));
                assert candid_map.get("details.settings.notifications") == ?#Option(#Bool(true));

                // variant
                assert candid_map.get("version") == ?#Text("v1");
                assert candid_map.get("version.v1") == ?#Nat(1);

                assert candid_map.get("missing.key") == null;
                assert candid_map.get("details.missing.key") == null;

            },
        );

        test(
            "get() - tuple",
            func() {
                assert candid_map.get("tuple.0") == ?#Nat(1);
                assert candid_map.get("tuple.1") == ?#Text("text");
                assert candid_map.get("tuple.2") == null;
            },
        );

        test(
            "get() - array",
            func() {

                assert candid_map.get("tags.0") == ?#Text("new");
                assert candid_map.get("tags.1") == ?#Text("popular");

                assert candid_map.get("comments.0.content") == ?#Text("comment1");
                assert candid_map.get("comments.0.created_at") == ?#Nat(1234567890);
                assert candid_map.get("comments.1") == null;
            },
        );

        test(
            "set() - simple types",
            func() {
                let #ok(_) = candid_map.set("name", #Text("Bob"));
                assert candid_map.get("name") == ?#Text("Bob");

                let #ok(_) = candid_map.set("age", #Nat(30));
                assert candid_map.get("age") == ?#Nat(30);

                let #ok(_) = candid_map.set("email", #Text("another_users_email@gmail.com"));
                assert candid_map.get("email") == ?#Text("another_users_email@gmail.com");

                let #ok(_) = candid_map.set("details.phone", #Text("0987654321"));
                assert candid_map.get("details.phone") == ?#Text("0987654321");

                let #ok(_) = candid_map.set("details.address", #Text("456, 5th Cross, 6th Main, Bangalore"));
                assert candid_map.get("details.address") == ?#Text("456, 5th Cross, 6th Main, Bangalore");

                let #ok(_) = candid_map.set("details.settings.theme", #Text("light"));
                assert candid_map.get("details.settings.theme") == ?#Option(#Text("light"));

                let #ok(_) = candid_map.set("details.settings.notifications", #Bool(false));
                assert candid_map.get("details.settings.notifications") == ?#Option(#Bool(false));

                let #ok(_) = candid_map.set("version.v1", #Nat(2));
                assert candid_map.get("version.v1") == ?#Nat(2);

            },
        );

        test(
            "set() - compound types",
            func() {

                let #ok(_) = candid_map.set("details", #Record([("phone", #Text("2893749823")), ("address", #Text("789, 7th Cross, 8th Main, Bangalore")), ("settings", #Option(#Record([("theme", #Text("dark")), ("notifications", #Bool(true))])))]));

                assert candid_map.get("details.phone") == ?#Text("2893749823");
                assert candid_map.get("details.address") == ?#Text("789, 7th Cross, 8th Main, Bangalore");
                assert candid_map.get("details.settings.theme") == ?#Option(#Text("dark"));
                assert candid_map.get("details.settings.notifications") == ?#Option(#Bool(true));

                let #ok(_) = candid_map.set("version", #Variant("v2", #Text("1.0.2")));
                assert candid_map.get("version") == ?#Text("v2");
                assert candid_map.get("version.v2") == ?#Text("1.0.2");
                assert candid_map.get("version.v1") == ?#Null;

                let #ok(_) = candid_map.set("version", #Variant("v3", #Record([("major", #Nat(1)), ("minor", #Nat(0)), ("patch", #Nat(0))])));

                assert candid_map.get("version") == ?#Text("v3");
                assert candid_map.get("version.v3.major") == ?#Nat(1);
                assert candid_map.get("version.v3.minor") == ?#Nat(0);
                assert candid_map.get("version.v3.patch") == ?#Nat(0);
                assert candid_map.get("version.v2") == ?#Null;

            },
        );

        test(
            "set() - tuple",
            func() {
                let #ok(_) = candid_map.set("tuple", #Tuple([#Nat(2), #Text("txet")]));
                assert candid_map.get("tuple.0") == ?#Nat(2);
                assert candid_map.get("tuple.1") == ?#Text("txet");

                let #ok(_) = candid_map.set("tuple.0", #Nat(20));
                assert candid_map.get("tuple.0") == ?#Nat(20);

                let #ok(_) = candid_map.set("tuple.1", #Text("new_text"));
                assert candid_map.get("tuple.1") == ?#Text("new_text");

                let #err(_) = candid_map.set("tuple.2", #Text("new_text"));
                assert candid_map.get("tuple.2") == null;
            },
        );

        test(
            "set() - array",
            func() {

                let #ok(_) = candid_map.set("tags", #Array([#Text("archived"), #Text("trending"), #Text("new")]));

                assert candid_map.get("tags.0") == ?#Text("archived");
                assert candid_map.get("tags.1") == ?#Text("trending");
                assert candid_map.get("tags.2") == ?#Text("new");
                assert candid_map.get("tags.3") == null;

                let #ok(_) = candid_map.set("comments.0.content", #Text("comment2"));
                assert candid_map.get("comments.0.content") == ?#Text("comment2");

                let #ok(_) = candid_map.set("comments.0.created_at", #Nat(222_222_222));
                assert candid_map.get("comments.0.created_at") == ?#Nat(222_222_222);

                // cannot add nested field of a missing element
                let #err(_) = candid_map.set("comments.1.content", #Text("comment3"));
                assert candid_map.get("comments.1") == null;

                // instead, add the missing element first
                // does not support adding elements to then end of an array yet
                // let #err(_) = candid_map.set("comments.1", #Record([("content", #Text("comment3")), ("created_at", #Nat(333_333_333))]));

            },
        );

        test(
            "set() - option",
            func() {
                let #ok(_) = candid_map.set("details.settings", #Null);

                Debug.print("option - details.settings.theme: " # debug_show candid_map.get("details.settings.theme"));
                Debug.print("option - details.settings.notifications: " # debug_show candid_map.get("details.settings.notifications"));

                assert candid_map.get("details.settings.theme") == ?#Null;
                assert candid_map.get("details.settings.notifications") == ?#Null;
            },
        );

        test(
            "extract_candid()",
            func() {
                let extracted_candid = candid_map.extract_candid();

                assert extracted_candid == #Record([
                    ("name", #Text("Bob")),
                    ("age", #Nat(30)),
                    ("email", #Text("another_users_email@gmail.com")),
                    ("details", #Record([("phone", #Text("2893749823")), ("address", #Text("789, 7th Cross, 8th Main, Bangalore")), ("settings", #Null)])),
                    (
                        "version",
                        #Variant("v3", #Record([("major", #Nat(1)), ("minor", #Nat(0)), ("patch", #Nat(0))])),
                    ),
                    ("tuple", #Tuple([#Nat(20), #Text("new_text")])),
                    ("tags", #Array([#Text("archived"), #Text("trending"), #Text("new")])),
                    (
                        "comments",
                        #Array([
                            #Record([
                                ("content", #Text("comment2")),
                                ("created_at", #Nat(222_222_222)),
                            ]),
                        ]),
                    ),
                ]);
            },
        );
    },
);
