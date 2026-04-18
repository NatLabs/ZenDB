# Benchmark Results


No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/Orchid.bench.json"

<details>

<summary>bench/modules/Orchid.bench.mo $({\color{gray}0\%})$</summary>

### Benchmarking Orchid Encoder/Decoder

_Benchmarking the performance with 1k random values per type_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|             |   encode() |   decode() |
| :---------- | ---------: | ---------: |
| Null        |  3_656_361 |  2_666_819 |
| Empty       |  3_646_671 |  2_662_111 |
| Bool        |  3_972_099 |  2_887_352 |
| Nat8        |  3_939_477 |  2_814_607 |
| Nat16       |  4_365_597 |  3_010_037 |
| Nat32       |  5_084_494 |  3_536_620 |
| Nat64       |  6_525_656 |  4_437_891 |
| Nat         |  6_592_859 |  4_375_130 |
| Int8        |  3_914_333 |  2_842_750 |
| Int16       |  4_308_904 |  3_038_262 |
| Int32       |  5_047_662 |  3_576_282 |
| Int64       |  6_508_553 |  4_474_157 |
| Int         |  6_549_920 |  4_421_396 |
| Float       | 37_518_617 | 30_234_043 |
| Principal   | 16_655_179 | 26_494_532 |
| Text        | 35_886_138 | 50_413_755 |
| Blob        | 28_581_515 | 50_612_213 |
| Option(Nat) |  6_301_672 |  4_298_203 |


**Heap**

|             | encode() | decode() |
| :---------- | -------: | -------: |
| Null        |    272 B |    272 B |
| Empty       |    272 B |    272 B |
| Bool        |    272 B |    272 B |
| Nat8        |    272 B |    272 B |
| Nat16       |    272 B |    272 B |
| Nat32       |    272 B |    272 B |
| Nat64       |    272 B |    272 B |
| Nat         |    272 B |    272 B |
| Int8        |    272 B |    272 B |
| Int16       |    272 B |    272 B |
| Int32       |    272 B |    272 B |
| Int64       |    272 B |    272 B |
| Int         |    272 B |    272 B |
| Float       |    272 B |    272 B |
| Principal   |    272 B |    272 B |
| Text        |    272 B |    272 B |
| Blob        |    272 B |    272 B |
| Option(Nat) |    272 B |    272 B |


**Garbage Collection**

|             |    encode() |   decode() |
| :---------- | ----------: | ---------: |
| Null        |  953.85 KiB | 739.01 KiB |
| Empty       |  953.85 KiB | 739.01 KiB |
| Bool        |  957.76 KiB | 750.73 KiB |
| Nat8        |  957.76 KiB | 750.73 KiB |
| Nat16       |  992.91 KiB | 750.73 KiB |
| Nat32       | 1020.26 KiB | 758.37 KiB |
| Nat64       |    1.05 MiB | 774.16 KiB |
| Nat         |    1.05 MiB | 750.73 KiB |
| Int8        |  969.48 KiB | 750.73 KiB |
| Int16       |  992.91 KiB | 750.73 KiB |
| Int32       | 1020.26 KiB | 758.62 KiB |
| Int64       |    1.05 MiB | 774.16 KiB |
| Int         |    1.05 MiB | 750.73 KiB |
| Float       |    2.08 MiB |   1.55 MiB |
| Principal   |    1.18 MiB |   1.73 MiB |
| Text        |    1.35 MiB |   2.26 MiB |
| Blob        |    1.31 MiB |   2.21 MiB |
| Option(Nat) |    1.03 MiB | 769.95 KiB |


</details>
Saving results to .bench/Orchid.bench.json
No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/Serde.bench.json"

<details>

<summary>bench/modules/Serde.bench.mo $({\color{gray}0\%})$</summary>

### Benchmarking Candid Encoding/Decoding

_Measuring Candid blob encode/decode performance during insert operations_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|                            |    encode() |    decode() |
| :------------------------- | ----------: | ----------: |
| Simple Record (5 fields)   |  67_225_987 |  38_241_681 |
| Medium Record (15 fields)  | 215_836_075 |  97_548_366 |
| Complex Record (30 fields) | 281_136_433 | 205_975_131 |
| Nested Record (3 levels)   |  94_061_447 |  45_755_469 |
| Array Fields (10 items)    | 273_378_212 | 275_217_949 |
| Large Record (50 fields)   | 469_188_535 | 352_830_035 |


**Heap**

|                            | encode() | decode() |
| :------------------------- | -------: | -------: |
| Simple Record (5 fields)   |    272 B |    272 B |
| Medium Record (15 fields)  |    272 B |    272 B |
| Complex Record (30 fields) |    272 B |    272 B |
| Nested Record (3 levels)   |    272 B |    272 B |
| Array Fields (10 items)    |    272 B |    272 B |
| Large Record (50 fields)   |    272 B |    272 B |


**Garbage Collection**

|                            |  encode() |  decode() |
| :------------------------- | --------: | --------: |
| Simple Record (5 fields)   |   3.9 MiB |   1.7 MiB |
| Medium Record (15 fields)  |  6.59 MiB |  4.16 MiB |
| Complex Record (30 fields) | 10.25 MiB |  8.63 MiB |
| Nested Record (3 levels)   |  4.95 MiB |  2.45 MiB |
| Array Fields (10 items)    | 10.31 MiB |  9.07 MiB |
| Large Record (50 fields)   |  16.1 MiB | 14.42 MiB |


</details>
Saving results to .bench/Serde.bench.json
No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/stable-memory.txs.bench.json"

<details>

<summary>bench/stable-memory.txs.bench.mo $({\color{gray}0\%})$</summary>

### Benchmarking zenDB with icrc3 txs

_Benchmarking the performance with 10k txs_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |            233_852_865 |                          233_955_731 |                           233_837_454 |
| create and populate indexes                                                                     |                  2_258 |                          902_672_652 |                         1_136_640_926 |
| clear collection entries and indexes                                                            |                130_951 |                            1_034_019 |                             1_044_574 |
| insert with indexes                                                                             |            245_534_162 |                        1_176_249_665 |                         1_428_088_317 |
| query(): no filter (all txs)                                                                    |             96_469_310 |                           96_466_384 |                            96_336_040 |
| query(): single field (btype = '1mint')                                                         |            176_922_894 |                           19_607_601 |                            19_722_483 |
| query(): number range (250 < tx.amt <= 400)                                                     |            187_495_947 |                           14_599_870 |                            14_599_799 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |            171_185_233 |                           35_576_207 |                             5_817_771 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |            189_349_994 |                           77_015_710 |                            76_804_309 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |            269_791_919 |                           58_379_207 |                            59_195_798 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |            259_866_293 |                           64_741_889 |                            64_995_637 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |            289_959_883 |                           89_432_882 |                            89_482_878 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |            388_411_148 |                           77_834_740 |                            77_676_776 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |            301_363_100 |                           67_296_594 |                            67_976_329 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |            195_905_828 |                            1_417_817 |                             1_433_868 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |            728_132_508 |                           17_852_912 |                            18_098_554 |
| query() -> all txs involving principals[0]                                                      |            354_831_711 |                            5_488_351 |                             5_578_109 |
| query() -> all txs involving principals[0..10]                                                  |          1_905_720_311 |                           54_351_107 |                            55_906_756 |
| update(): single operation -> #add amt += 100                                                   |            553_790_772 |                          842_791_540 |                         1_179_974_839 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |            895_970_182 |                        1_191_889_406 |                         1_537_352_733 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |            614_754_530 |                          902_647_219 |                         1_245_123_566 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |            855_620_956 |                        1_713_054_559 |                         3_121_383_795 |
| replace() -> replace half the tx with new tx                                                    |            406_900_899 |                        2_322_334_939 |                         2_699_313_978 |
| delete()                                                                                        |            248_193_105 |                        1_102_223_566 |                         1_354_210_185 |


**Heap**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |              39.42 KiB |                            39.42 KiB |                             39.42 KiB |
| create and populate indexes                                                                     |                  272 B |                            30.88 KiB |                             37.91 KiB |
| clear collection entries and indexes                                                            |                  384 B |                             1.13 KiB |                              1.14 KiB |
| insert with indexes                                                                             |              43.78 KiB |                            57.28 KiB |                             60.57 KiB |
| query(): no filter (all txs)                                                                    |                  272 B |                                272 B |                                 272 B |
| query(): single field (btype = '1mint')                                                         |                  272 B |                                272 B |                                 272 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                  272 B |                                272 B |                                 272 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                  272 B |                                272 B |                                 272 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                  272 B |                                272 B |                                 272 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                  272 B |                                272 B |                                 272 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                  272 B |                                272 B |                                 272 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                  272 B |                                272 B |                                 272 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                  272 B |                                272 B |                                 272 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                  272 B |                                272 B |                                 272 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                  272 B |                                272 B |                                 272 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                  272 B |                                272 B |                                 272 B |
| query() -> all txs involving principals[0]                                                      |                  272 B |                                272 B |                                 272 B |
| query() -> all txs involving principals[0..10]                                                  |                  308 B |                                344 B |                                 344 B |
| update(): single operation -> #add amt += 100                                                   |               2.57 KiB |                             7.61 KiB |                             11.53 KiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                  952 B |                             7.58 KiB |                              12.2 KiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                  344 B |                             5.73 KiB |                             10.13 KiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |               6.87 KiB |                            19.95 KiB |                             27.89 KiB |
| replace() -> replace half the tx with new tx                                                    |               6.55 KiB |                            25.66 KiB |                             36.22 KiB |
| delete()                                                                                        |               1.22 KiB |                             11.5 KiB |                             17.44 KiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |              17.08 MiB |                            17.09 MiB |                             17.09 MiB |
| create and populate indexes                                                                     |              16.36 KiB |                            78.66 MiB |                             99.12 MiB |
| clear collection entries and indexes                                                            |              16.93 KiB |                            18.46 KiB |                             18.46 KiB |
| insert with indexes                                                                             |              17.21 MiB |                            94.98 MiB |                            115.71 MiB |
| query(): no filter (all txs)                                                                    |               5.12 MiB |                             5.12 MiB |                              5.11 MiB |
| query(): single field (btype = '1mint')                                                         |              13.59 MiB |                          1021.59 KiB |                              1.01 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |              14.07 MiB |                           804.21 KiB |                            803.43 KiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |              13.64 MiB |                              2.6 MiB |                            334.57 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |              14.42 MiB |                             5.43 MiB |                              5.41 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |              21.33 MiB |                              3.1 MiB |                              3.15 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |              19.47 MiB |                             3.47 MiB |                              3.48 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |              21.55 MiB |                             4.75 MiB |                              4.75 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |               28.1 MiB |                             4.14 MiB |                              4.13 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |              23.92 MiB |                              3.6 MiB |                              3.64 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |              15.63 MiB |                           108.91 KiB |                            110.28 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |              69.57 MiB |                             1.12 MiB |                              1.14 MiB |
| query() -> all txs involving principals[0]                                                      |              31.71 MiB |                           372.91 KiB |                            379.01 KiB |
| query() -> all txs involving principals[0..10]                                                  |             189.78 MiB |                             3.33 MiB |                              3.39 MiB |
| update(): single operation -> #add amt += 100                                                   |              40.91 MiB |                               62 MiB |                             89.78 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |              73.71 MiB |                            94.93 MiB |                            122.92 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |              46.84 MiB |                            67.82 MiB |                             95.58 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |              68.21 MiB |                           125.57 MiB |                            253.67 MiB |
| replace() -> replace half the tx with new tx                                                    |              28.34 MiB |                           177.45 MiB |                            214.65 MiB |
| delete()                                                                                        |               13.3 MiB |                            79.89 MiB |                             98.18 MiB |


**Stable Memory**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |                    0 B |                                  0 B |                                   0 B |
| create and populate indexes                                                                     |                    0 B |                              224 MiB |                               224 MiB |
| clear collection entries and indexes                                                            |                    0 B |                                  0 B |                                   0 B |
| insert with indexes                                                                             |                    0 B |                                  0 B |                                   0 B |
| query(): no filter (all txs)                                                                    |                    0 B |                                  0 B |                                   0 B |
| query(): single field (btype = '1mint')                                                         |                    0 B |                                  0 B |                                   0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                    0 B |                                  0 B |                                   0 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                    0 B |                                  0 B |                                   0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                    0 B |                                  0 B |                                   0 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                    0 B |                                  0 B |                                   0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                    0 B |                                  0 B |                                   0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                    0 B |                                  0 B |                                   0 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                    0 B |                                  0 B |                                   0 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                    0 B |                                  0 B |                                   0 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                    0 B |                                  0 B |                                   0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                    0 B |                                  0 B |                                   0 B |
| query() -> all txs involving principals[0]                                                      |                    0 B |                                  0 B |                                   0 B |
| query() -> all txs involving principals[0..10]                                                  |                    0 B |                                  0 B |                                   0 B |
| update(): single operation -> #add amt += 100                                                   |                    0 B |                                  0 B |                                   0 B |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                    0 B |                                  0 B |                                   0 B |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                    0 B |                                  0 B |                                   0 B |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                    0 B |                                  0 B |                                   0 B |
| replace() -> replace half the tx with new tx                                                    |                    0 B |                                  0 B |                                   0 B |
| delete()                                                                                        |                    0 B |                                  0 B |                                   0 B |

</details>
Saving results to .bench/stable-memory.txs.bench.json
No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/stable-memory.txs.sorted.bench.json"

<details>

<summary>bench/stable-memory.txs.sorted.bench.mo $({\color{gray}0\%})$</summary>

### Benchmarking zenDB with icrc3 txs

_Benchmarking the performance of sorted queries with 1k txs_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by ts) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | --------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                           233_804_124 |                                         233_883_312 |                                          233_841_609 |
| create and populate indexes                                                                     |                                 5_282 |                                         902_676_295 |                                        1_136_644_945 |
| clear collection entries and indexes                                                            |                               133_975 |                                           1_037_540 |                                            1_048_598 |
| insert with indexes                                                                             |                           245_527_888 |                                       1_176_236_080 |                                        1_428_079_441 |
| query(): no filter (all txs)                                                                    |                         2_443_529_185 |                                          94_820_370 |                                           94_737_689 |
| query(): single field (btype = '1mint')                                                         |                           535_393_015 |                                         198_176_831 |                                           19_848_687 |
| query(): number range (250 < tx.amt <= 400)                                                     |                           431_162_508 |                                         209_897_121 |                                          209_815_250 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                           238_741_538 |                                         103_283_841 |                                           41_144_229 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                           533_786_348 |                                         119_896_552 |                                          119_796_112 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                         1_732_296_041 |                                         598_482_354 |                                           81_219_135 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                         1_712_862_492 |                                         435_351_839 |                                          262_543_986 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                         2_332_627_596 |                                         471_169_974 |                                          298_470_138 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                         2_765_040_224 |                                         280_842_701 |                                          280_688_066 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                         1_782_204_805 |                                         814_661_966 |                                          468_870_974 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                           202_572_927 |                                           8_810_406 |                                            1_559_902 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                           968_350_238 |                                         370_860_261 |                                           27_979_488 |
| query() -> all txs involving principals[0]                                                      |                           396_009_863 |                                          50_288_933 |                                            7_159_165 |
| query() -> all txs involving principals[0..10]                                                  |                         2_644_443_913 |                                       1_325_573_308 |                                           79_015_306 |
| update(): single operation -> #add amt += 100                                                   |                           553_793_793 |                                         842_795_609 |                                        1_179_978_913 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                           895_972_939 |                                       1_191_971_478 |                                        1_537_422_300 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                           614_756_485 |                                         902_681_936 |                                        1_245_073_384 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                           855_648_401 |                                       1_713_298_335 |                                        3_121_127_066 |
| replace() -> replace half the tx with new tx                                                    |                           406_902_379 |                                       2_322_344_766 |                                        2_699_324_719 |
| delete()                                                                                        |                           248_177_904 |                                       1_102_145_221 |                                        1_353_502_475 |


**Heap**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by ts) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | --------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                             39.42 KiB |                                           39.42 KiB |                                            39.42 KiB |
| create and populate indexes                                                                     |                                 272 B |                                           30.88 KiB |                                            37.91 KiB |
| clear collection entries and indexes                                                            |                                 384 B |                                            1.13 KiB |                                             1.14 KiB |
| insert with indexes                                                                             |                             43.78 KiB |                                           57.28 KiB |                                            60.57 KiB |
| query(): no filter (all txs)                                                                    |                                 308 B |                                               272 B |                                                272 B |
| query(): single field (btype = '1mint')                                                         |                                 272 B |                                               272 B |                                                272 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                                 272 B |                                               272 B |                                                272 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                                 272 B |                                               272 B |                                                272 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                                 272 B |                                               272 B |                                                272 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                                 308 B |                                               272 B |                                                344 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                                 380 B |                                               344 B |                                                344 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                                 380 B |                                               344 B |                                                344 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                                 380 B |                                               344 B |                                                344 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                                 380 B |                                               344 B |                                                344 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                                 344 B |                                               344 B |                                                344 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                                 344 B |                                               344 B |                                                344 B |
| query() -> all txs involving principals[0]                                                      |                                 344 B |                                               344 B |                                                344 B |
| query() -> all txs involving principals[0..10]                                                  |                                 380 B |                                               380 B |                                                344 B |
| update(): single operation -> #add amt += 100                                                   |                              2.57 KiB |                                            7.61 KiB |                                            11.53 KiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                                 952 B |                                            7.58 KiB |                                             12.2 KiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                                 344 B |                                            5.73 KiB |                                            10.13 KiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                              6.87 KiB |                                           19.95 KiB |                                            27.89 KiB |
| replace() -> replace half the tx with new tx                                                    |                              6.55 KiB |                                           25.66 KiB |                                            36.22 KiB |
| delete()                                                                                        |                              1.22 KiB |                                            11.5 KiB |                                            17.44 KiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by ts) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | --------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                             17.08 MiB |                                           17.09 MiB |                                            17.09 MiB |
| create and populate indexes                                                                     |                             16.36 KiB |                                           78.66 MiB |                                            99.12 MiB |
| clear collection entries and indexes                                                            |                             16.93 KiB |                                           18.43 KiB |                                            18.48 KiB |
| insert with indexes                                                                             |                             17.21 MiB |                                           94.98 MiB |                                           115.71 MiB |
| query(): no filter (all txs)                                                                    |                            184.26 MiB |                                            4.93 MiB |                                             4.92 MiB |
| query(): single field (btype = '1mint')                                                         |                             41.14 MiB |                                           15.37 MiB |                                             1.01 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                             32.66 MiB |                                            15.9 MiB |                                            15.89 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                             18.83 MiB |                                             7.8 MiB |                                              3.1 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                             40.73 MiB |                                            8.83 MiB |                                             8.82 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                            133.06 MiB |                                           46.44 MiB |                                             5.01 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                            130.32 MiB |                                            32.7 MiB |                                            18.86 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                            177.42 MiB |                                           35.03 MiB |                                            21.19 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                            209.44 MiB |                                           19.98 MiB |                                            19.98 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                            137.06 MiB |                                           62.51 MiB |                                            34.79 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                             16.14 MiB |                                          676.39 KiB |                                           118.46 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                             87.95 MiB |                                           27.99 MiB |                                             1.74 MiB |
| query() -> all txs involving principals[0]                                                      |                             34.85 MiB |                                            3.76 MiB |                                           488.33 KiB |
| query() -> all txs involving principals[0..10]                                                  |                            246.06 MiB |                                           99.89 MiB |                                             4.92 MiB |
| update(): single operation -> #add amt += 100                                                   |                             40.91 MiB |                                              62 MiB |                                            89.78 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                             73.71 MiB |                                           94.93 MiB |                                           122.92 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                             46.84 MiB |                                           67.82 MiB |                                            95.58 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                             68.21 MiB |                                          125.57 MiB |                                           253.67 MiB |
| replace() -> replace half the tx with new tx                                                    |                             28.34 MiB |                                          177.45 MiB |                                           214.65 MiB |
| delete()                                                                                        |                              13.3 MiB |                                           79.89 MiB |                                            98.18 MiB |


**Stable Memory**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by ts) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | --------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                                   0 B |                                                 0 B |                                                  0 B |
| create and populate indexes                                                                     |                                   0 B |                                             224 MiB |                                              224 MiB |
| clear collection entries and indexes                                                            |                                   0 B |                                                 0 B |                                                  0 B |
| insert with indexes                                                                             |                                   0 B |                                                 0 B |                                                  0 B |
| query(): no filter (all txs)                                                                    |                                   0 B |                                                 0 B |                                                  0 B |
| query(): single field (btype = '1mint')                                                         |                                   0 B |                                                 0 B |                                                  0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                                   0 B |                                                 0 B |                                                  0 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                                   0 B |                                                 0 B |                                                  0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                                   0 B |                                                 0 B |                                                  0 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                                   0 B |                                                 0 B |                                                  0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                                   0 B |                                                 0 B |                                                  0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                                   0 B |                                                 0 B |                                                  0 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                                   0 B |                                                 0 B |                                                  0 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                                   0 B |                                                 0 B |                                                  0 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                                   0 B |                                                 0 B |                                                  0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                                   0 B |                                                 0 B |                                                  0 B |
| query() -> all txs involving principals[0]                                                      |                                   0 B |                                                 0 B |                                                  0 B |
| query() -> all txs involving principals[0..10]                                                  |                                   0 B |                                                 0 B |                                                  0 B |
| update(): single operation -> #add amt += 100                                                   |                                   0 B |                                                 0 B |                                                  0 B |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                                   0 B |                                                 0 B |                                                  0 B |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                                   0 B |                                                 0 B |                                                  0 B |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                                   0 B |                                                 0 B |                                                  0 B |
| replace() -> replace half the tx with new tx                                                    |                                   0 B |                                                 0 B |                                                  0 B |
| delete()                                                                                        |                                   0 B |                                                 0 B |                                                  0 B |

</details>
Saving results to .bench/stable-memory.txs.sorted.bench.json
No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/text-index.bench.json"

<details>

<summary>bench/text-index.bench.mo $({\color{gray}0\%})$</summary>

### Text Index Operations

_Benchmarking createTextIndex, insert, and search operators with 1 000 articles (body: 25–100 tokens, vocab: 80 words)_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|                                                      | #stableMemory  with text index |
| :--------------------------------------------------- | -----------------------------: |
| insert 1k articles (no text index yet)               |                    105_438_074 |
| createTextIndex() on populated collection (backfill) |                  6_716_855_478 |
| search(): #word — rare word  (~10 docs)              |                      1_093_274 |
| search(): #word — common word (~200 docs)            |                      8_532_994 |
| search(): #startsWith — partial prefix               |                      8_900_557 |
| search(): #phrase — 2-word sequence                  |                      4_394_069 |
| search(): #anyOf — 3 common words (union)            |                     16_198_242 |
| search(): #allOf — 2 common words (intersect)        |                      8_066_038 |
| search(): #not_(#word) — bracket complement scan     |                    877_030_037 |
| search(): #not_(#phrase) — De Morgan complement scan |                  1_721_760_274 |
| search(): #word + .And() category filter             |                     86_962_967 |


**Heap**

|                                                      | #stableMemory  with text index |
| :--------------------------------------------------- | -----------------------------: |
| insert 1k articles (no text index yet)               |                          284 B |
| createTextIndex() on populated collection (backfill) |                     118.24 KiB |
| search(): #word — rare word  (~10 docs)              |                          272 B |
| search(): #word — common word (~200 docs)            |                          272 B |
| search(): #startsWith — partial prefix               |                          272 B |
| search(): #phrase — 2-word sequence                  |                          272 B |
| search(): #anyOf — 3 common words (union)            |                          272 B |
| search(): #allOf — 2 common words (intersect)        |                          272 B |
| search(): #not_(#word) — bracket complement scan     |                          272 B |
| search(): #not_(#phrase) — De Morgan complement scan |                          308 B |
| search(): #word + .And() category filter             |                          272 B |


**Garbage Collection**

|                                                      | #stableMemory  with text index |
| :--------------------------------------------------- | -----------------------------: |
| insert 1k articles (no text index yet)               |                       7.17 MiB |
| createTextIndex() on populated collection (backfill) |                     367.31 MiB |
| search(): #word — rare word  (~10 docs)              |                      87.58 KiB |
| search(): #word — common word (~200 docs)            |                     659.55 KiB |
| search(): #startsWith — partial prefix               |                     679.31 KiB |
| search(): #phrase — 2-word sequence                  |                     214.36 KiB |
| search(): #anyOf — 3 common words (union)            |                       1.15 MiB |
| search(): #allOf — 2 common words (intersect)        |                     584.15 KiB |
| search(): #not_(#word) — bracket complement scan     |                      71.93 MiB |
| search(): #not_(#phrase) — De Morgan complement scan |                     141.38 MiB |
| search(): #word + .And() category filter             |                        6.1 MiB |


**Stable Memory**

|                                                      | #stableMemory  with text index |
| :--------------------------------------------------- | -----------------------------: |
| insert 1k articles (no text index yet)               |                            0 B |
| createTextIndex() on populated collection (backfill) |                         32 MiB |
| search(): #word — rare word  (~10 docs)              |                            0 B |
| search(): #word — common word (~200 docs)            |                            0 B |
| search(): #startsWith — partial prefix               |                            0 B |
| search(): #phrase — 2-word sequence                  |                            0 B |
| search(): #anyOf — 3 common words (union)            |                            0 B |
| search(): #allOf — 2 common words (intersect)        |                            0 B |
| search(): #not_(#word) — bracket complement scan     |                            0 B |
| search(): #not_(#phrase) — De Morgan complement scan |                            0 B |
| search(): #word + .And() category filter             |                            0 B |

</details>
Saving results to .bench/text-index.bench.json
