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

    // func async_skip_helper(iter : Iter<Nat>, skip : Nat) : async (Nat) {
    //     let performance_start = InternetComputer.performanceCounter(0);

    //     func instructions() : Nat64 {
    //         InternetComputer.performanceCounter(0) - performance_start;
    //     };

    //     var i = 0;
    //     while ((instructions() + 10_000_000 < MAX_UPDATE_INSTRUCTIONS) and i < skip) {
    //         ignore iter.next();
    //         i += 1;
    //     };

    //     i;
    // };

    // func async_skip(iter : Iter<Nat>, skip : Nat, performance_start : Nat64) : async* () {

    //     var skipped = 0;
    //     // Debug.print("starting async_skip: " # debug_show skip);
    //     while (skipped < skip) {
    //         skipped += await async_skip_helper(iter, skip - skipped);
    //         // Debug.print("skipped: " # debug_show skipped);
    //     };

    // };

    // public func async_find(query_builder : QueryBuilder, buffer : Buffer<T.WrapId<Record>>) : async* Result<(), Text> {
    //     switch (search_iter(query_builder)) {
    //         case (#err(err)) #err(err);
    //         case (#ok(records)) {
    //             for (record in records) {
    //                 buffer.add(record);
    //             };
    //             #ok(());
    //         };
    //     };
    // };

};
