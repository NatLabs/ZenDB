#### Initial Tests

Instructions

|                                                                        |          ZenDB |
| :--------------------------------------------------------------------- | -------------: |
| put() no index                                                         |  4_346_787_546 |
| createIndex()                                                         |  5_455_578_597 |
| clear collection data                                                  |        103_376 |
| put() with 1 index                                                     |  7_687_600_646 |
| create 2nd index                                                       |  5_842_120_856 |
| clear collection data                                                  |        103_376 |
| put() with 2 indexes                                                   | 11_440_193_814 |
| create 3rd index                                                       |  5_738_070_889 |
| clear collection data                                                  |        103_376 |
| put() with 3 indexes                                                   | 15_308_851_855 |
| search(): users named 'nam-do-dan'                                     |    155_213_385 |
| search(): users between the age of 20 and 35                           |    104_219_040 |
| search(): users between the age of 20 and 35 and named 'nam-do-dan'    |    481_958_812 |
| search(): users between the age of 20 and 35 and named 'nam-do-dan' v2 |    481_959_328 |

Heap

|                                                                        |       ZenDB |
| :--------------------------------------------------------------------- | ----------: |
| put() no index                                                         |  22_022_328 |
| createIndex()                                                         |  -5_565_608 |
| clear collection data                                                  |      16_084 |
| put() with 1 index                                                     |  12_096_844 |
| create 2nd index                                                       | -12_444_772 |
| clear collection data                                                  |      16_084 |
| put() with 2 indexes                                                   |   4_199_464 |
| create 3rd index                                                       | -18_898_324 |
| clear collection data                                                  |      16_084 |
| put() with 3 indexes                                                   |     189_056 |
| search(): users named 'nam-do-dan'                                     |   5_668_152 |
| search(): users between the age of 20 and 35                           |   3_881_108 |
| search(): users between the age of 20 and 35 and named 'nam-do-dan'    |   3_957_368 |
| search(): users between the age of 20 and 35 and named 'nam-do-dan' v2 |   6_137_252 |
