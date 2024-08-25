## Benchmarks

Benchmarking the performance with 1k calls

#### Initial Benchmark

Instructions

|                       |       HydraDB |
| :-------------------- | ------------: |
| put() no index        | 1_899_187_839 |
| updateById() 1        | 3_647_231_474 |
| create_index()        | 2_063_325_128 |
| clear collection data |       119_818 |
| put() with 1 index    | 2_270_448_590 |
| updateById() 2        | 4_424_335_620 |
| create 2nd index      | 2_020_902_161 |
| clear collection data |       119_818 |
| put() with 2 indexes  | 2_586_551_855 |
| updateById() 3        | 5_105_632_746 |
| create 3rd index      | 2_065_202_391 |
| clear collection data |       119_818 |
| put() with 3 indexes  | 2_947_911_052 |
| updateById() 4        | 5_887_579_294 |
| get()                 |    38_278_232 |

Heap

|                       |     HydraDB |
| :-------------------- | ----------: |
| put() no index        |  23_291_128 |
| updateById() 1        | -25_904_696 |
| create_index()        |  28_717_080 |
| clear collection data |      18_316 |
| put() with 1 index    |     748_656 |
| updateById() 2        |  -5_707_884 |
| create 2nd index      |  -5_703_536 |
| clear collection data |      18_316 |
| put() with 2 indexes  |   9_185_956 |
| updateById() 3        | -21_075_380 |
| create 3rd index      |  -4_714_152 |
| clear collection data |      18_316 |
| put() with 3 indexes  |  18_620_052 |
| updateById() 4        |    -883_708 |
| get()                 |   1_005_276 |

#### Candid one_shot decoding improvement

Instructions

|                       |       HydraDB |
| :-------------------- | ------------: |
| put() no index        |   551_368_371 |
| updateById() 1        |   950_699_496 |
| create_index()        |   724_322_792 |
| clear collection data |       118_392 |
| put() with 1 index    |   932_321_785 |
| updateById() 2        | 1_747_222_449 |
| create 2nd index      |   680_660_016 |
| clear collection data |       118_392 |
| put() with 2 indexes  | 1_255_275_259 |
| updateById() 3        | 2_462_128_519 |
| create 3rd index      |   725_148_937 |
| clear collection data |       118_392 |
| put() with 3 indexes  | 1_623_156_396 |
| updateById() 4        | 3_265_037_610 |
| get()                 |    38_221_613 |

Heap

|                       |     HydraDB |
| :-------------------- | ----------: |
| put() no index        |  24_272_784 |
| updateById() 1        | -18_899_252 |
| create_index()        |  -3_415_400 |
| clear collection data |      18_256 |
| put() with 1 index    |   4_347_784 |
| updateById() 2        |   1_222_128 |
| create 2nd index      |  -4_335_912 |
| clear collection data |      18_256 |
| put() with 2 indexes  |  12_792_152 |
| updateById() 3        | -13_932_120 |
| create 3rd index      |  -3_327_016 |
| clear collection data |      18_256 |
| put() with 3 indexes  |  22_270_948 |
| updateById() 4        |   6_708_396 |
| get()                 |   1_006_120 |
