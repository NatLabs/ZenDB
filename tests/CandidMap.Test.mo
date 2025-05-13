import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";

import { test; suite } "mo:test";
import Candid "mo:serde/Candid";
import Map "mo:map/Map";

import SchemaMap "../src/Collection/SchemaMap";
import CandidMap "../src/CandidMap";
import ZenDB "../src";

let schema : ZenDB.Types.Schema = #Record([
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

let schema_map = SchemaMap.new(schema);

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

let candid_map = CandidMap.new(schema_map, candid);

suite(
    "CandidMap",
    func() {
        test(
            "get()",
            func() {
                Debug.print("candid_map: " # debug_show Map.toArray(candid_map.candid_map));
                Debug.print("name: " # debug_show CandidMap.get(candid_map, schema_map, "name"));
                Debug.print("candid_map: " # debug_show Map.toArray(candid_map.candid_map));

                assert CandidMap.get(candid_map, schema_map, "name") == ?#Text("Alice");
                assert CandidMap.get(candid_map, schema_map, "age") == ?#Nat(25);
                assert CandidMap.get(candid_map, schema_map, "email") == ?#Text("user_email@gmail.com");
                assert CandidMap.get(candid_map, schema_map, "details.phone") == ?#Text("1234567890");
                assert CandidMap.get(candid_map, schema_map, "details.address") == ?#Text("123, 4th Cross, 5th Main, Bangalore");
                assert CandidMap.get(candid_map, schema_map, "details.settings.theme") == ?#Text("dark");
                assert CandidMap.get(candid_map, schema_map, "details.settings.notifications") == ?#Bool(true);

                // variant
                assert CandidMap.get(candid_map, schema_map, "version") == ?#Text("v1");
                assert CandidMap.get(candid_map, schema_map, "version.v1") == ?#Nat(1);

                assert CandidMap.get(candid_map, schema_map, "missing.key") == null;
                assert CandidMap.get(candid_map, schema_map, "details.missing.key") == null;

            },
        );

        test(
            "get() - tuple",
            func() {
                assert CandidMap.get(candid_map, schema_map, "tuple.0") == ?#Nat(1);
                assert CandidMap.get(candid_map, schema_map, "tuple.1") == ?#Text("text");
                assert CandidMap.get(candid_map, schema_map, "tuple.2") == null;

            },
        );

        test(
            "get() - array",
            func() {

                Debug.print("tags.0: " # debug_show CandidMap.get(candid_map, schema_map, "tags.0"));

                assert CandidMap.get(candid_map, schema_map, "tags.0") == ?#Text("new");
                assert CandidMap.get(candid_map, schema_map, "tags.1") == ?#Text("popular");

                Debug.print("comments.0.content: " # debug_show CandidMap.get(candid_map, schema_map, "comments.0.content"));
                assert CandidMap.get(candid_map, schema_map, "comments.0.content") == ?#Text("comment1");
                assert CandidMap.get(candid_map, schema_map, "comments.0.created_at") == ?#Nat(1234567890);
                assert CandidMap.get(candid_map, schema_map, "comments.1") == null;
            },
        );

        test(
            "set() - simple types",
            func() {
                let #ok(_) = CandidMap.set(candid_map, schema_map, "name", #Text("Bob"));
                assert CandidMap.get(candid_map, schema_map, "name") == ?#Text("Bob");

                let #ok(_) = CandidMap.set(candid_map, schema_map, "age", #Nat(30));
                assert CandidMap.get(candid_map, schema_map, "age") == ?#Nat(30);

                let #ok(_) = CandidMap.set(candid_map, schema_map, "email", #Text("another_users_email@gmail.com"));
                assert CandidMap.get(candid_map, schema_map, "email") == ?#Text("another_users_email@gmail.com");

                let #ok(_) = CandidMap.set(candid_map, schema_map, "details.phone", #Text("0987654321"));
                assert CandidMap.get(candid_map, schema_map, "details.phone") == ?#Text("0987654321");

                let #ok(_) = CandidMap.set(candid_map, schema_map, "details.address", #Text("456, 5th Cross, 6th Main, Bangalore"));
                assert CandidMap.get(candid_map, schema_map, "details.address") == ?#Text("456, 5th Cross, 6th Main, Bangalore");

                let #ok(_) = CandidMap.set(candid_map, schema_map, "details.settings.theme", #Text("light"));
                assert CandidMap.get(candid_map, schema_map, "details.settings.theme") == ?#Text("light");

                let #ok(_) = CandidMap.set(candid_map, schema_map, "details.settings.notifications", #Bool(false));
                assert CandidMap.get(candid_map, schema_map, "details.settings.notifications") == ?#Bool(false);

                let #ok(_) = CandidMap.set(candid_map, schema_map, "version.v1", #Nat(2));
                assert CandidMap.get(candid_map, schema_map, "version.v1") == ?#Nat(2);

            },
        );

        test(
            "set() - compound types",
            func() {

                let #ok(_) = CandidMap.set(candid_map, schema_map, "details", #Record([("phone", #Text("2893749823")), ("address", #Text("789, 7th Cross, 8th Main, Bangalore")), ("settings", #Option(#Record([("theme", #Text("dark")), ("notifications", #Bool(true))])))]));

                assert CandidMap.get(candid_map, schema_map, "details.phone") == ?#Text("2893749823");
                assert CandidMap.get(candid_map, schema_map, "details.address") == ?#Text("789, 7th Cross, 8th Main, Bangalore");
                assert CandidMap.get(candid_map, schema_map, "details.settings.theme") == ?#Text("dark");
                assert CandidMap.get(candid_map, schema_map, "details.settings.notifications") == ?#Bool(true);

                let #ok(_) = CandidMap.set(candid_map, schema_map, "version", #Variant("v2", #Text("1.0.2")));
                assert CandidMap.get(candid_map, schema_map, "version") == ?#Text("v2");
                assert CandidMap.get(candid_map, schema_map, "version.v2") == ?#Text("1.0.2");
                assert CandidMap.get(candid_map, schema_map, "version.v1") == null;

                let #ok(_) = CandidMap.set(candid_map, schema_map, "version", #Variant("v3", #Record([("major", #Nat(1)), ("minor", #Nat(0)), ("patch", #Nat(0))])));

                assert CandidMap.get(candid_map, schema_map, "version") == ?#Text("v3");
                assert CandidMap.get(candid_map, schema_map, "version.v3.major") == ?#Nat(1);
                assert CandidMap.get(candid_map, schema_map, "version.v3.minor") == ?#Nat(0);
                assert CandidMap.get(candid_map, schema_map, "version.v3.patch") == ?#Nat(0);
                assert CandidMap.get(candid_map, schema_map, "version.v2") == null;

            },
        );

        test(
            "set() - tuple",
            func() {
                let #ok(_) = CandidMap.set(candid_map, schema_map, "tuple", #Tuple([#Nat(2), #Text("txet")]));
                assert CandidMap.get(candid_map, schema_map, "tuple.0") == ?#Nat(2);
                assert CandidMap.get(candid_map, schema_map, "tuple.1") == ?#Text("txet");

                let #ok(_) = CandidMap.set(candid_map, schema_map, "tuple.0", #Nat(20));
                assert CandidMap.get(candid_map, schema_map, "tuple.0") == ?#Nat(20);

                let #ok(_) = CandidMap.set(candid_map, schema_map, "tuple.1", #Text("new_text"));
                assert CandidMap.get(candid_map, schema_map, "tuple.1") == ?#Text("new_text");

                let #err(_) = CandidMap.set(candid_map, schema_map, "tuple.2", #Text("new_text"));
                assert CandidMap.get(candid_map, schema_map, "tuple.2") == null;
            },
        );

        test(
            "set() - array",
            func() {

                let #ok(_) = CandidMap.set(candid_map, schema_map, "tags", #Array([#Text("archived"), #Text("trending"), #Text("new")]));

                assert CandidMap.get(candid_map, schema_map, "tags.0") == ?#Text("archived");
                assert CandidMap.get(candid_map, schema_map, "tags.1") == ?#Text("trending");
                assert CandidMap.get(candid_map, schema_map, "tags.2") == ?#Text("new");
                assert CandidMap.get(candid_map, schema_map, "tags.3") == null;

                let #ok(_) = CandidMap.set(candid_map, schema_map, "comments.0.content", #Text("comment2"));
                assert CandidMap.get(candid_map, schema_map, "comments.0.content") == ?#Text("comment2");

                let #ok(_) = CandidMap.set(candid_map, schema_map, "comments.0.created_at", #Nat(222_222_222));
                assert CandidMap.get(candid_map, schema_map, "comments.0.created_at") == ?#Nat(222_222_222);

                // cannot update nested field of a missing element ...
                let #err(_) = CandidMap.set(candid_map, schema_map, "comments.1.content", #Text("comment3"));
                assert CandidMap.get(candid_map, schema_map, "comments.1") == null;

                // ... instead, add the missing array element first
                // does not support adding elements to the end of an array yet
                // let #err(_) = CandidMap.set(candid_map, schema_map,"comments.1", #Record([("content", #Text("comment3")), ("created_at", #Nat(333_333_333))]));

            },
        );

        test(
            "set() - option",
            func() {
                let #ok(_) = CandidMap.set(candid_map, schema_map, "details.settings.theme", #Option(#Text("light")));
                assert CandidMap.get(candid_map, schema_map, "details.settings.theme") == ?#Option(#Text("light"));

                let #ok(_) = CandidMap.set(candid_map, schema_map, "details.settings.notifications", #Option(#Bool(true)));
                assert CandidMap.get(candid_map, schema_map, "details.settings.notifications") == ?#Option(#Bool(true));

                let #ok(_) = CandidMap.set(candid_map, schema_map, "details.settings", #Option(#Record([("theme", #Text("dark")), ("notifications", #Bool(false))])));
                assert CandidMap.get(candid_map, schema_map, "details.settings.theme") == ?#Text("dark");
                assert CandidMap.get(candid_map, schema_map, "details.settings.notifications") == ?#Bool(false);

                // todo: does not support retrieving compound types, not sure if there would be any real use case for this
                // assert CandidMap.get(candid_map, schema_map, "details.settings") == ?#Option(#Record([("theme", #Text("dark")), ("notifications", #Bool(false))]));

                // todo: the following code successfully updates the value to #Null, even though the type is not an #Option
                // todo: the confusion is that the parant type is an #Option, but the child type is not
                // ignore CandidMap.set(candid_map, schema_map,"details.settings.theme", #Null);
                // CandidMap.get(candid_map, schema_map, "details.settings.theme") |> Debug.print("option - details.settings.theme: " # debug_show _);
                // CandidMap.get(candid_map, schema_map, "details.settings.notifications") |> Debug.print("option - details.settings.notifications: " # debug_show _);
                // let #err(_) = CandidMap.set(candid_map, schema_map,"details.settings.notifications", #Null);

                let #ok(_) = CandidMap.set(candid_map, schema_map, "details.settings", #Null);

                Debug.print("option - details.settings.theme: " # debug_show CandidMap.get(candid_map, schema_map, "details.settings.theme"));
                Debug.print("option - details.settings.notifications: " # debug_show CandidMap.get(candid_map, schema_map, "details.settings.notifications"));

                assert CandidMap.get(candid_map, schema_map, "details.settings.theme") == null;
                assert CandidMap.get(candid_map, schema_map, "details.settings.notifications") == null;
            },
        );

        test(
            "extract_candid()",
            func() {
                let extracted_candid = CandidMap.extract_candid(candid_map);

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

        test(
            "CandidMap: store primitive types",
            func() {
                let schema_map = SchemaMap.new(#Nat);
                let candid_map = CandidMap.new(schema_map, #Nat(42));
                assert CandidMap.get(candid_map, schema_map, "") == ?#Nat(42);
                assert CandidMap.get(candid_map, schema_map, "0") == null;

            },
        );
    },
);
