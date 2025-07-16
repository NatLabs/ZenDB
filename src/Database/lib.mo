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

import Collection "../Collection";
import Utils "../Utils";
import T "../Types";
import Schema "../Collection/Schema";

import StableDatabase "StableDatabase";
import StableCollection "../Collection/StableCollection";
import CollectionUtils "../Collection/Utils";

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

    /// The database class provides an interface for creating and managing collections.
    public class Database(stable_db : T.StableDatabase) = self {

        func convert_to_internal_candify<A>(collection_name : Text, external_candify : T.Candify<A>) : T.InternalCandify<A> {
            {
                from_blob = func(blob : Blob) : A {
                    switch (external_candify.from_blob(blob)) {
                        case (?document) document;
                        case (null) Debug.trap("
                        Could not convert candid blob (" # debug_show blob # ") to motoko using the '" # collection_name # "' collection's schema.
                        If the schema and the candify encoding function are correct, then the blob might be corrupted or not a valid candid blob.
                        Please report this issue to the developers by creating a new issue on the GitHub repository.  ");
                    };
                };
                to_blob = external_candify.to_blob;
            };
        };

        func validate_candify<A>(collection_name : Text, schema : T.Schema, external_candify : T.Candify<A>) : Result<T.InternalCandify<A>, Text> {

            let default_candid_value : T.Candid = switch (Schema.generateDefaultValue(schema)) {
                case (#ok(default_candid)) default_candid;
                case (#err(msg)) return #err("Failed to generate default value for schema for candify validation: " # msg);
            };

            let default_candid_blob = switch (Serde.Candid.encodeOne(default_candid_value, null)) {
                case (#ok(blob)) blob;
                case (#err(msg)) return #err("Failed to encode generated schema document to Candid blob using serde: " # msg);
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

            let internal_candify : T.InternalCandify<A> = convert_to_internal_candify<A>(collection_name, external_candify);

            #ok(internal_candify);

        };

        /// Retrieves the total number of collections in the database.
        public func size() : Nat {
            StableDatabase.size(stable_db);
        };

        /// Creates a new collection in the database with the given name and schema.
        /// The name must be unique and not already exist in the database, unless the function will return an #err.
        /// The schema must be the exact representation of the motoko type you want to store in the collection.
        ///
        /// Example:
        /// ```motoko
        /// type User = {
        ///   id: Nat;
        ///   name: Text;
        ///   email: Text;
        ///   profile: {
        ///     age: ?Nat;
        ///     location: Text;
        ///     interests: [Text];
        ///   };
        ///   created_at: Int;
        /// };
        ///
        /// let UsersSchema : ZenDB.Types.Schema = #Record([
        ///   ("id", #Nat),
        ///   ("name", #Text),
        ///   ("email", #Text),
        ///   ("profile", #Record([
        ///     ("age", #Option(#Nat)),
        ///     ("location", #Text),
        ///     ("interests", #Array(#Text)),
        ///   ])),
        ///   ("created_at", #Int),
        /// ]);
        ///
        /// let candify_users : ZenDB.Types.Candify<User> = {
        ///   to_blob = func(user: User) : Blob { to_candid(user) };
        ///   from_blob = func(blob: Blob) : ?User { from_candid(blob) };
        /// };
        ///
        /// let #ok(users_collection) = db.createCollection<User>("users", UsersSchema, candify_users, []);
        /// ```

        public func createCollection<Record>(name : Text, schema : T.Schema, external_candify : T.Candify<Record>, options : ?T.CreateCollectionOptions) : Result<Collection<Record>, Text> {
            let validate_candify_result = Utils.handleResult(
                stable_db.logger,
                validate_candify(name, schema, external_candify),
                "Failed to validate candify function: ",
            );

            let internal_candify : T.InternalCandify<Record> = switch (validate_candify_result) {
                case (#ok(internal_candify)) internal_candify;
                case (#err(msg)) return Utils.logErrorMsg(stable_db.logger, msg);
            };

            switch (StableDatabase.createCollection(stable_db, name, schema, options)) {
                case (#ok(stable_collection)) {
                    #ok(
                        Collection.Collection<Record>(
                            name,
                            stable_collection,
                            internal_candify,
                        )
                    );
                };
                case (#err(msg)) Utils.logErrorMsg(stable_db.logger, msg);
            };
        };

        /// Retrieves an existing collection by its name.
        /// It is required to pass the correct `Candify` function that matches the schema of the collection.
        public func getCollection<Record>(
            name : Text,
            external_candify : T.Candify<Record>,
        ) : Result<Collection<Record>, Text> {

            switch (StableDatabase.getCollection(stable_db, name)) {
                case (#ok(stable_collection)) {
                    #ok(
                        Collection.Collection<Record>(
                            name,
                            stable_collection,
                            convert_to_internal_candify(name, external_candify),
                        )
                    );
                };
                case (#err(msg)) #err(msg);
            };
        };

        public func _create_index_on_collection(
            collection_name : Text,
            index_name : Text,
            index_fields : [(Text, T.SortDirection)],
            is_unique : Bool,
        ) : T.Result<(), Text> {
            let stable_collection = switch (StableDatabase.getCollection(stable_db, collection_name)) {
                case (#ok(stable_collection)) stable_collection;
                case (#err(msg)) return #err(msg);
            };

            switch (
                StableCollection.createIndex(
                    stable_collection,
                    CollectionUtils.getMainBtreeUtils(stable_collection),
                    index_name,
                    index_fields,
                    is_unique,
                )
            ) {
                case (#ok(_)) #ok();
                case (#err(msg)) return #err(msg);
            };

        };

    };

};
