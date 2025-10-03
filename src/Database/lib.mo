import Principal "mo:base@0.16.0/Principal";
import Array "mo:base@0.16.0/Array";
import Debug "mo:base@0.16.0/Debug";
import Text "mo:base@0.16.0/Text";
import Char "mo:base@0.16.0/Char";
import Nat32 "mo:base@0.16.0/Nat32";
import Result "mo:base@0.16.0/Result";
import Order "mo:base@0.16.0/Order";
import Iter "mo:base@0.16.0/Iter";
import Buffer "mo:base@0.16.0/Buffer";
import Nat "mo:base@0.16.0/Nat";
import Option "mo:base@0.16.0/Option";
import Hash "mo:base@0.16.0/Hash";
import Float "mo:base@0.16.0/Float";
import Int "mo:base@0.16.0/Int";

import Map "mo:map@9.0.1/Map";
import Set "mo:map@9.0.1/Set";
import Serde "mo:serde@3.3.2";
import Decoder "mo:serde@3.3.2/Candid/Blob/Decoder";
import Candid "mo:serde@3.3.2/Candid";
import Itertools "mo:itertools@0.2.2/Iter";
import RevIter "mo:itertools@0.2.2/RevIter";

import Collection "../Collection";
import Utils "../Utils";
import T "../Types";
import Schema "../Collection/Schema";

import StableDatabase "StableDatabase";
import StableCollection "../Collection/StableCollection";
import CollectionUtils "../Collection/CollectionUtils";

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

        func validate_candify<A>(collection_name : Text, schema : T.Schema, external_candify : T.Candify<A>) : T.Result<T.InternalCandify<A>, Text> {

            let default_candid_value : T.Candid = switch (Schema.generate_default_value(schema)) {
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

        public func createCollection<Record>(name : Text, schema : T.Schema, external_candify : T.Candify<Record>, options : ?T.CreateCollectionOptions) : T.Result<Collection<Record>, Text> {
            let validate_candify_result = Utils.handle_result(
                stable_db.logger,
                validate_candify(name, schema, external_candify),
                "Failed to validate candify function: ",
            );

            let internal_candify : T.InternalCandify<Record> = switch (validate_candify_result) {
                case (#ok(internal_candify)) internal_candify;
                case (#err(msg)) return Utils.log_error_msg(stable_db.logger, msg);
            };

            switch (StableDatabase.create_collection(stable_db, name, schema, options)) {
                case (#ok(stable_collection)) {
                    #ok(
                        Collection.Collection<Record>(
                            name,
                            stable_collection,
                            internal_candify,
                        )
                    );
                };
                case (#err(msg)) Utils.log_error_msg(stable_db.logger, msg);
            };
        };

        /// Retrieves an existing collection by its name.
        /// It is required to pass the correct `Candify` function that matches the schema of the collection.
        public func getCollection<Record>(
            name : Text,
            external_candify : T.Candify<Record>,
        ) : T.Result<Collection<Record>, Text> {

            switch (StableDatabase.get_collection(stable_db, name)) {
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

        public func getStableState() : T.StableDatabase {
            stable_db;
        };

        public func _create_index_on_collection(
            collection_name : Text,
            index_name : Text,
            index_fields : [(Text, T.SortDirection)],
            is_unique : Bool,
        ) : T.Result<(), Text> {
            let stable_collection = switch (StableDatabase.get_collection(stable_db, collection_name)) {
                case (#ok(stable_collection)) stable_collection;
                case (#err(msg)) return #err(msg);
            };

            switch (
                StableCollection.create_composite_index(
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
