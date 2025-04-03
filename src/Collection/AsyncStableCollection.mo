module {

    // public func async_populate_indexes(
    //     collection : StableCollection,
    //     _main_btree_utils : BTreeUtils<Nat, Blob>,
    //     indexes_key_details : [[Text]],
    //     opt_batch_size : ?Nat,
    // ) : async* Result<(), Text> {

    //     let recommended_batch_size = recommended_entries_to_populate_based_on_benchmarks(indexes_key_details.size());

    //     let BATCH_SIZE = Option.get(opt_batch_size, recommended_batch_size);

    //     let indexes = Buffer.Buffer<Index>(indexes_key_details.size());

    //     for (index_key_details in indexes_key_details.vals()) {
    //         let index_name = Text.join(
    //             "_",
    //             Iter.map<Text, Text>(
    //                 index_key_details.vals(),
    //                 func(key : Text) : Text {
    //                     key;
    //                 },
    //             ),
    //         );

    //         let ?index = Map.get(collection.indexes, thash, index_name) else return #err("Index with key_details '" # debug_show index_key_details # "' does not exist");

    //         indexes.add(index);
    //     };

    //     var size = 0;

    //     while (size < MemoryBTree.size(collection.main)) {

    //         let start = size;
    //         let end = Nat.min(size + BATCH_SIZE, MemoryBTree.size(collection.main));

    //         let res = await internal_populate_indexes(
    //             collection,
    //             indexes,
    //             MemoryBTree.range(collection.main, _main_btree_utils, start, end),
    //         );

    //         switch (res) {
    //             case (#err(err)) return #err(err);
    //             case (#ok(_)) {};
    //         };

    //         size += BATCH_SIZE;

    //     };

    //     #ok()

    // };
};
