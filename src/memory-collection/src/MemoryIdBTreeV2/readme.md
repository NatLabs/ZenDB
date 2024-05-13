## Memory B+Tree
The Memory B+Tree is a persistent B+Tree that stores all its data, including nodes and key-value pairs in stable memory. The B+Tree is a balanced tree data structure that allows for efficient search, insertion, and deletion operations. The Memory B+Tree is designed to store large amounts of data that need to persist across canister upgrades. 

### Design
The Memory B+Tree is implemented using three [MemoryRegion](https://github.com/NatLabs/memory-region). A key-value blob region, a memory block region and a metadata region.

- **Blob Region**: 
  - Stores all key-value pairs as blobs in stable memory, re-allocating memory as needed. The key and value are concatenated and stored as a single blob.
- **Block Region**:
  - Tracks the location of the key-value pairs in the Blob Region. The location of each key-value pair is stored as a combination the address and size of the key and value. The address takes 8 bytes, the key size takes 2 bytes and the value size takes 4 bytes for a total of 14 bytes per key-value pair. Because the block region is also a memory region, each piece of data stored has an address, which we can use to access the key-value blocks. We use these addresses as unique identifiers to access the key-value pairs using indirection.
- **Metadata Region**:
  - Stores the metadata of the B+Tree, including the root node, the order of the tree, the number of entries, and nodes information. The metadata region is used to keep track of the structure of the B+Tree and to perform operations such as search, insertion, and deletion. The addresses (or ids) where the key-value blocks are stored in the Blob Region are stored in the nodes instead of the actual key-value pairs. This allows internal operations like splitting and merging of nodes to be done without having to interact with the key-value pairs which might often be larger than the unique identifier of the key-value pair.

Because of this design each key-value pair gains an extra memory overhead of 22 bytes. This overhead is due to the 14 bytes of the block region and 8 bytes for the unique identifier (address pointing to the block region) of the key-value pair.

### Blobify and Comparison Functions
In addition to the serialization functions required by data-structurs in the memory-collection, the Memory B+Tree requires a comparison function to compare keys. This is used to locate the correct node in the B+Tree for search, insertion, and deletion operations. 
The comparison function is used in high demand by the B+Tree, so it is important that it is efficient and fast as possible. The Memory B+Tree takes a variant, an option between two comparison functions: `#BlobCmp` and `#GenCmp`. The `#GenCmp` comparison function converts the keys to the original generic type defined by the user and then compares them. While the `#BlobCmp` comparison function compares the keys directly as blobs. The `#BlobCmp` avoids the overhead of deserializing the keys but requires that the keys be comparable in their serialized format. This is not the case for every type, so the `#GenCmp` comparison function is provided as an alternative. The [first benchmark](#serialization-and-comparison-functions) compares the performance between the two types of comparison functions. For example, `Text` and `Principal` are comparable as blobs, but `Int` and some `Nat` serializations can not be compared as blobs. It's good to note what i mean when i say comparable as blobs. I mean that no additional operation needs to be done to compare the two blobs. Using motoko's comparison operators should just work. Any operation, such as looping through and converting the blobs to a byte array creates overhead that builds up and slows down the comparison function. 
`Int` cannot be compared without first converting with two's complement. `Nat` in little endian cannot be compared without first converting to a byte array and looping through the bytes in reverse order. 
`Nat` in big endian however can be compared directly.
This is to say that the performance of the B+Tree is highly dependant on which Blobify (serialization) and comparison function you choose to use. We aim to guide you in making the best choice for your use case.

### Usage Examples
```motoko
import MemoryBTree "mo:memory-collection/MemoryBTree";
import BTreeUtils "mo:memory-collection/BTreeUtils";

let sstore = MemoryBTree.newStableStore(?256);
let mbtree = MemoryBTree.new(sstore, BTreeUtils.createUtils(BTreeUtils.BigEndian.Nat, BTreeUtils.BigEndian.Nat));

mbtree.insert(1, 10);
mbtree.insert(2, 20);
mbtree.insert(3, 30);

assert mbtree.get(2) == ?20;
assert mbtree.getMin() == ?(1, 10);
assert mbtree.getMax() == ?(3, 30);

assert mbtree.remove(2) == ?20;
assert mbtree.get(2) == null;

assert mbtree.toArray() == [(1, 10), (3, 30)];
```

### Benchmarks

Benchmarking the performance with 10k entries

#### Serialization and Comparison Functions
-  Comparing B+Tree and Memory B+Tree with different serialization formats and comparison functions

**Instructions**

|                                  |    insert() |       get() |   replace() |  entries() |    remove() |
| :------------------------------- | ----------: | ----------: | ----------: | ---------: | ----------: |
| B+Tree                           | 231_642_288 | 133_638_737 | 140_358_299 |  4_731_130 | 248_729_817 |
| MotokoStableBTree                | 807_443_649 |   3_564_967 | 807_444_761 |     11_805 |   2_817_569 |
| Memory B+Tree (#BlobCmp)         | 487_915_458 | 395_727_585 | 462_884_499 | 44_253_968 | 555_325_070 |
| Memory B+Tree (#GenCmp)          | 589_312_436 | 496_647_362 | 563_804_276 | 44_254_716 | 645_381_183 |


**Heap**

|                                  |   insert() |     get() |   replace() | entries() |    remove() |
| :------------------------------- | ---------: | --------: | ----------: | --------: | ----------: |
| B+Tree                           |    686_732 |   208_960 |     608_964 |     9_084 |     208_964 |
| MotokoStableBTree                | 15_902_952 |     8_960 | -15_477_004 |     9_424 |       8_964 |
| Memory B+Tree (#BlobCmp)         |  8_354_268 | 4_359_672 |   4_759_676 |   889_328 | -21_555_192 |
| Memory B+Tree (#GenCmp)          |  8_354_268 | 4_359_672 |   4_759_676 |   889_328 | -19_379_852 |


#### BTree order / fanout
*Benchmarking the performance with 10k entries*

**Instructions**

|                            |    insert() |       get() |     replace() |  entries() |      remove() |
| :------------------------- | ----------: | ----------: | ------------: | ---------: | ------------: |
| B+Tree                     | 175_321_548 | 144_101_415 |   154_390_977 |  4_851_558 |   184_602_098 |
| MotokoStableBTree          | 807_443_679 |   3_564_997 |   807_444_791 |     11_835 |     2_817_599 |
| Memory B+Tree (order 4)    | 922_483_169 | 694_818_910 | 1_015_931_824 | 48_674_343 | 1_123_705_405 |
| Memory B+Tree (order 32)   | 554_132_203 | 451_027_102 |   554_455_016 | 44_683_690 |   644_690_502 |
| Memory B+Tree (order 64)   | 529_100_546 | 441_496_659 |   544_916_573 | 44_399_399 |   610_079_963 |
| Memory B+Tree (order 128)  | 487_917_817 | 395_729_727 |   462_886_641 | 44_256_110 |   555_326_995 |
| Memory B+Tree (order 256)  | 483_818_328 | 393_881_668 |   461_038_582 | 44_183_035 |   545_110_924 |
| Memory B+Tree (order 512)  | 490_366_041 | 392_434_089 |   459_591_003 | 44_144_864 |   547_117_261 |
| Memory B+Tree (order 1024) | 511_739_965 | 390_561_172 |   457_718_086 | 44_129_801 |   561_298_355 |
| Memory B+Tree (order 2048) | 553_458_184 | 387_792_544 |   454_949_675 | 44_129_046 |   595_467_035 |
| Memory B+Tree (order 4096) | 630_554_887 | 382_160_975 |   449_317_889 | 44_144_389 |   663_961_787 |


**Heap**

|                            |    insert() |     get() |   replace() | entries() |    remove() |
| :------------------------- | ----------: | --------: | ----------: | --------: | ----------: |
| B+Tree                     |     730_052 |   208_960 |     608_964 |     9_084 |     208_964 |
| MotokoStableBTree          |  15_914_780 |     8_960 | -15_464_916 |     9_424 |       8_964 |
| Memory B+Tree (order 4)    |   7_297_212 | 7_244_864 |   7_644_868 |   889_552 | -21_431_736 |
| Memory B+Tree (order 32)   |   6_120_864 | 4_911_008 |   5_311_012 |   889_360 | -21_221_864 |
| Memory B+Tree (order 64)   |   6_861_048 | 4_806_632 |   5_206_636 |   889_360 |   9_570_264 |
| Memory B+Tree (order 128)  | -23_979_732 | 4_359_672 |   4_759_676 |   889_328 |  10_790_788 |
| Memory B+Tree (order 256)  | -18_315_128 | 4_341_120 |   4_741_124 |   889_328 |  13_621_712 |
| Memory B+Tree (order 512)  | -13_413_108 | 4_327_608 |   4_727_612 |   889_328 | -10_334_048 |
| Memory B+Tree (order 1024) |     475_600 | 4_309_008 |   4_709_012 |   889_328 |   1_398_564 |
| Memory B+Tree (order 2048) |  -5_471_140 | 4_280_016 | -27_653_832 |   889_328 |  24_466_008 |
| Memory B+Tree (order 4096) | -26_688_348 | 4_218_480 |   4_618_484 |   889_328 |     757_288 |


#### Search Operations

**Instructions**

|                |      B+Tree |   MemoryBTree |
| :------------- | ----------: | ------------: |
| getFromIndex() |  68_084_526 |   982_867_372 |
| getIndex()     | 167_272_704 | 2_056_032_208 |
| getFloor()     |  79_745_706 |   944_184_979 |
| getCeiling()   |  79_746_359 |   944_185_764 |
| removeMin()    | 151_750_473 | 1_518_343_522 |
| removeMax()    | 115_673_205 | 1_459_495_725 |


**Heap**

|                |  B+Tree | MemoryBTree |
| :------------- | ------: | ----------: |
| getFromIndex() | 328_960 | -18_588_204 |
| getIndex()     | 586_764 |    -464_796 |
| getFloor()     | 213_804 |  -3_850_692 |
| getCeiling()   | 213_804 |  -1_678_896 |
| removeMin()    | 213_864 |  18_259_436 |
| removeMax()    | 209_944 |   5_447_972 |