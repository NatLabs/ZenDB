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

import Map "mo:map/Map";
import Serde "mo:serde";
import Record "mo:serde/Candid/Text/Parser/Record";
import Itertools "mo:itertools/Iter";
import RevIter "mo:itertools/RevIter";

import MemoryIdBTree "memory-buffer/src/MemoryIdBTree/Base";
import MemoryBTree "memory-buffer/src/MemoryBTree/Base";
import BTreeUtils "memory-buffer/src/MemoryBTree/BTreeUtils";
import Int8Cmp "memory-buffer/src/Int8Cmp";

import HydraDB "";

module {

    public type HydraDB = HydraDB.HydraDB;

    public type Candify<T> = HydraDB.Candify<T>;
    public type Result<T, E> = Result.Result<T, E>;

    public func newStableStore() : HydraDB {
        return HydraDB.new();
    };

    public class HydraDBClass(stableStore : HydraDB.HydraDB) = self {

        public func getCollection<Record>(name: Text, blobify: Candify<Record>) : Collection<Record> {
            return Collection<Record>(hydra_db, name, candify);
        };

    };

    /// A collection is a set of records of the same type.
    ///
    /// ```motoko
    /// type User = { name: Text, age: Nat };
    /// let hydra_db = HydraDB();
    /// let db = hydra_db.getDB("my_db");
    ///
    /// let candify_users = { 
    ///     to_blob = func(user: User) : Blob { to_candid(user) };
    ///     from_blob = func(blob: Blob) : User { let ?user : ?User = from_candid(blob); user; };
    /// };
    ///
    /// let users = db.getCollection<User>("users", candify_users);
    ///
    /// let alice = { name = "Alice", age = 30 };
    /// let bob = { name = "Bob", age = 25 };
    ///
    /// let alice_id = users.put(alice);
    /// let bob_id = users.put(bob);
    ///
    /// ```
    class Collection<Record>(db: HydraDBClass, collection_name : Text, blobify: Candify<Record>) = self {

        public func getName() : Text = collection_name;
        public func getBlobifyFn() : Candify<Record> = blobify;

        public func create_index(index_key_details : [(Text)]) : Result<(), Text> {
            let result = HydraDB.create_index(db, collection_name, index_key_details);

            switch(result) {
                case (#ok(())) return #ok(());
                case (#err(errMsg)) return #err(errMsg);
            };
        };

        public func get(id: Nat) : Document {
            return Document(self, id);
        };

        public func put(record: Record) : Result<Nat, Text> {
            let result = HydraDB.put<Record>(hydra_db, collection_name, blobify, record);

            switch(result) {
                case (#ok(id)) return #ok(id);
                case (#err(errMsg)) return #err(errMsg);
            };
        };

        // public func find()
    };

    class Document<Record>(collection: Collection<Record>, id : Nat) {
        public func getId() : Nat = id;

        public func toRecord() : Result<Record, Text> {
            let result = HydraDB.lookup_record<Record>(collection, collection.getBlobifyFn(), id: Nat);

            switch(result) {
                case (null) return #err("Record not found");
                case (?record) return #ok(record);
            };
        };
    };
}