import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Result "mo:base/Result";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Hash "mo:base/Hash";
import Float "mo:base/Float";
import Int "mo:base/Int";

import Map "mo:map/Map";
import Set "mo:map/Set";
import Serde "mo:serde";
import Decoder "mo:serde/Candid/Blob/Decoder";
import Candid "mo:serde/Candid";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";

import MemoryBTree "mo:memory-collection/MemoryBTree/Stable";
import TypeUtils "mo:memory-collection/TypeUtils";
import Int8Cmp "mo:memory-collection/TypeUtils/Int8Cmp";

import Collection "../Collection";
import Utils "../Utils";
import T "../Types";
import Schema "../Collection/Schema";

import StableDatabase "StableDatabase";

module {

    public type Candify<T> = T.Candify<T>;
    public type Map<K, V> = Map.Map<K, V>;
    public type Set<K> = Set.Set<K>;
    let { thash; bhash } = Map;

    public type Result<A, B> = Result.Result<A, B>;
    public type Buffer<A> = Buffer.Buffer<A>;
    public type Iter<A> = Iter.Iter<A>;
    public type RevIter<A> = RevIter.RevIter<A>;

    public type StableCollection = T.StableCollection;
    public type Collection<Record> = Collection.Collection<Record>;

    public class Database(zendb : T.ZenDB) = self {

        func convert_to_internal_candify<A>(collection_name : Text, external_candify : T.Candify<A>) : T.InternalCandify<A> {
            {
                from_blob = func(blob : Blob) : A {
                    switch (external_candify.from_blob(blob)) {
                        case (?record) record;
                        case (null) Debug.trap("
                        Could not convert candid blob (" # debug_show blob # ") to motoko using the '" # collection_name # "' collection's schema.
                        If the schema and the candify encoding function are correct, then the blob might be corrupted or not a valid candid blob.
                        Please report this issue to the developers by creating a new issue on the GitHub repository.  ");
                    };
                };
                to_blob = external_candify.to_blob;
            };
        };

        func validate_candify<A>(schema : T.Schema, external_candify : T.Candify<A>) : Result<T.InternalCandify<A>, Text> {

            let default_candid_value : T.Candid = switch (Schema.generate_default_value(schema)) {
                case (#ok(default_candid)) default_candid;
                case (#err(msg)) return #err("Failed to generate default value for schema for candify validation: " # msg);
            };

            let default_candid_blob = switch (Serde.Candid.encodeOne(default_candid_value, null)) {
                case (#ok(blob)) blob;
                case (#err(msg)) return #err("Failed to encode generated schema record to Candid blob using serde: " # msg);
            };

            let opt_deserialized_candid_value = external_candify.from_blob(default_candid_blob);

            switch (opt_deserialized_candid_value) {
                case (?deserialized_candid_value) {}; // successful round trip encoding
                case (null) {
                    return #err("Failed to deserialize the default candid value. There are a few reasons why this might happen:
                        - The motoko type does not match the given schema: " # debug_show schema # "
                        - The candify function uses a different encoding format other than candid");
                };
            };

            let internal_candify = convert_to_internal_candify(external_candify);

            #ok(internal_candify);

        };

        public func create_collection<Record>(name : Text, schema : T.Schema, external_candify : T.Candify<Record>) : Result<Collection<Record>, Text> {

            let internal_candify : T.InternalCandify<Record> = switch (validate_candify(schema, external_candify)) {
                case (#ok(internal_candify)) internal_candify;
                case (#err(msg)) return #err(msg);
            };

            switch (StableDatabase.create_collection(zendb, name, schema)) {
                case (#ok(stable_collection)) {
                    #ok(
                        Collection.Collection<Record>(
                            name,
                            stable_collection,
                            internal_candify,
                        )
                    );
                };
                case (#err(msg)) #err(msg);
            };
        };

        public func get_collection<Record>(
            name : Text,
            external_candify : T.Candify<Record>,
        ) : Result<Collection<Record>, Text> {

            switch (StableDatabase.get_collection(zendb, name)) {
                case (#ok(stable_collection)) {
                    #ok(
                        Collection.Collection<Record>(
                            name,
                            stable_collection,
                            convert_to_internal_candify(external_candify),
                        )
                    );
                };
                case (#err(msg)) #err(msg);
            };
        };

        public func get_or_create_collection<Record>(
            name : Text,
            schema : T.Schema,
            blobify : T.Candify<Record>,
        ) : Result<Collection<Record>, Text> {

            switch (create_collection(name, schema, blobify)) {
                case (#ok(collection)) #ok(collection);
                case (#err(msg)) {
                    switch (get_collection(name, blobify)) {
                        case (#ok(collection)) #ok(collection);
                        case (#err(_)) #err(msg);
                    };
                };
            };

        };

    };

};
