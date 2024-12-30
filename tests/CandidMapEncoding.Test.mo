import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Char "mo:base/Char";

import { test; suite } "mo:test";
import Candid "mo:serde/Candid";
import CandidMap "../src/CandidMapEncoding";

suite(
    "CandidMap",
    func() {
        test(
            "fromCandid",
            func() {

                let candid : Candid.Candid = #Record([
                    ("name", #Text("Alice")),
                    ("age", #Nat(25)),
                    ("email", #Text("user_email@gmail.com")),
                    ("details", #Record([("phone", #Text("1234567890")), ("address", #Text("123, 4th Cross, 5th Main, Bangalore")), ("settings", #Record([("theme", #Text("dark")), ("notifications", #Bool(true))]))])),
                ]);

                let candid_map = CandidMap.fromCandid(candid);

                func retrieve_fields(candid_map : CandidMap.CandidMap) {
                    assert candid_map.get("name") == ?#Text("Alice");
                    assert candid_map.get("age") == ?#Nat(25);
                    assert candid_map.get("email") == ?#Text("user_email@gmail.com");
                    assert candid_map.get("details.phone") == ?#Text("1234567890");
                    assert candid_map.get("details.address") == ?#Text("123, 4th Cross, 5th Main, Bangalore");
                    assert candid_map.get("details.settings.theme") == ?#Text("dark");
                    assert candid_map.get("details.settings.notifications") == ?#Bool(true);

                    assert candid_map.get("missing.key") == null;
                    assert candid_map.get("details.missing.key") == null;
                };

                retrieve_fields(candid_map);

                let encoded_map : Blob = candid_map.encode();
                let candid_map2 = CandidMap.fromBlob(encoded_map);

                retrieve_fields(candid_map2);

            },
        );
    },
);
