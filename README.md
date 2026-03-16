# Benchmark Results


No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/heap.txs.bench.json"

<details>

<summary>bench/heap.txs.bench.mo $({\color{gray}0\%})$</summary>

### Benchmarking zenDB with icrc3 txs

_Benchmarking the performance with 10k txs_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|                                                                                                 | #heap no index | #heap 7 single field indexes | #heap 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | -------------: | ---------------------------: | ----------------------------: |
| insert with no index                                                                            |    150_293_631 |                  150_281_009 |                   150_283_765 |
| create and populate indexes                                                                     |          1_936 |                1_167_692_910 |                 1_383_866_677 |
| clear collection entries and indexes                                                            |          7_236 |                       49_254 |                        50_442 |
| insert with indexes                                                                             |    157_829_495 |                1_298_386_213 |                 1_515_165_321 |
| query(): no filter (all txs)                                                                    |     24_733_442 |                   24_726_308 |                    24_726_077 |
| query(): single field (btype = '1mint')                                                         |    124_280_162 |                    5_420_420 |                     5_703_738 |
| query(): number range (250 < tx.amt <= 400)                                                     |    138_458_209 |                    4_044_446 |                     4_045_144 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |    128_269_561 |                   25_118_098 |                     2_412_103 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |    138_072_724 |                   50_518_905 |                    50_471_342 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |    188_674_298 |                   16_955_913 |                    18_493_287 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |    179_967_539 |                   23_274_113 |                    23_778_940 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |    195_626_622 |                   32_981_304 |                    33_550_371 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |    264_664_850 |                   27_941_899 |                    27_687_252 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |    219_662_010 |                   25_188_554 |                    26_640_334 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |    154_908_357 |                    1_180_570 |                     1_232_361 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |    659_943_577 |                   21_571_872 |                    19_125_141 |
| query() -> all txs involving principals[0]                                                      |    305_828_827 |                    5_993_311 |                     6_098_652 |
| query() -> all txs involving principals[0..10]                                                  |  1_775_992_316 |                   61_505_542 |                    60_551_676 |
| update(): single operation -> #add amt += 100                                                   |    507_961_074 |                  784_070_145 |                 1_188_801_488 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |    845_501_291 |                1_122_322_018 |                 1_526_675_473 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |    571_069_582 |                  849_530_683 |                 1_253_023_934 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |    790_278_384 |                1_619_476_956 |                 3_567_983_648 |
| replace() -> replace half the tx with new tx                                                    |    411_697_699 |                2_828_271_748 |                 3_187_886_724 |
| delete()                                                                                        |    155_478_739 |                1_189_587_923 |                 1_385_209_865 |


**Heap**

|                                                                                                 | #heap no index | #heap 7 single field indexes | #heap 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | -------------: | ---------------------------: | ----------------------------: |
| insert with no index                                                                            |     265.11 KiB |                   265.11 KiB |                    265.11 KiB |
| create and populate indexes                                                                     |          272 B |                   375.54 KiB |                    490.05 KiB |
| clear collection entries and indexes                                                            |          284 B |                        368 B |                         368 B |
| insert with indexes                                                                             |     304.18 KiB |                   676.88 KiB |                    791.38 KiB |
| query(): no filter (all txs)                                                                    |          272 B |                        272 B |                         272 B |
| query(): single field (btype = '1mint')                                                         |          272 B |                        272 B |                         272 B |
| query(): number range (250 < tx.amt <= 400)                                                     |          272 B |                        272 B |                         272 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |          272 B |                        272 B |                         272 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |          272 B |                        272 B |                         272 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |          272 B |                        272 B |                         272 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |          272 B |                        272 B |                         272 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |          272 B |                        272 B |                         272 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |          272 B |                        272 B |                         272 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |          272 B |                        272 B |                         272 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |          272 B |                        272 B |                         272 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |          272 B |                        272 B |                         272 B |
| query() -> all txs involving principals[0]                                                      |          272 B |                        272 B |                         272 B |
| query() -> all txs involving principals[0..10]                                                  |          308 B |                        272 B |                         272 B |
| update(): single operation -> #add amt += 100                                                   |     251.55 KiB |                   304.27 KiB |                    364.04 KiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |     251.63 KiB |                   303.74 KiB |                    362.55 KiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |     251.63 KiB |                   303.39 KiB |                    362.23 KiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |     252.58 KiB |                   421.91 KiB |                    677.08 KiB |
| replace() -> replace half the tx with new tx                                                    |      252.2 KiB |                   572.96 KiB |                    680.29 KiB |
| delete()                                                                                        |          344 B |                        380 B |                         380 B |


**Garbage Collection**

|                                                                                                 | #heap no index | #heap 7 single field indexes | #heap 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | -------------: | ---------------------------: | ----------------------------: |
| insert with no index                                                                            |      10.92 MiB |                    10.92 MiB |                     10.92 MiB |
| create and populate indexes                                                                     |      16.36 KiB |                    92.95 MiB |                    115.17 MiB |
| clear collection entries and indexes                                                            |      16.71 KiB |                    16.71 KiB |                     16.71 KiB |
| insert with indexes                                                                             |      11.48 MiB |                   105.31 MiB |                    127.53 MiB |
| query(): no filter (all txs)                                                                    |     381.02 KiB |                   380.95 KiB |                    380.95 KiB |
| query(): single field (btype = '1mint')                                                         |      10.04 MiB |                   129.99 KiB |                    151.95 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |       10.7 MiB |                   116.56 KiB |                    116.56 KiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |       10.8 MiB |                     1.96 MiB |                    132.39 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |      11.15 MiB |                     3.68 MiB |                      3.67 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |      16.81 MiB |                   434.34 KiB |                    552.86 KiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |      14.64 MiB |                   788.54 KiB |                    825.93 KiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |      15.99 MiB |                     1.12 MiB |                      1.16 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |      19.68 MiB |                   929.61 KiB |                    912.15 KiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |      19.69 MiB |                   937.17 KiB |                      1.02 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |      12.87 MiB |                    73.66 KiB |                     75.66 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |      71.03 MiB |                   839.72 KiB |                    809.64 KiB |
| query() -> all txs involving principals[0]                                                      |      30.07 MiB |                   286.21 KiB |                    291.22 KiB |
| query() -> all txs involving principals[0..10]                                                  |     199.27 MiB |                     2.52 MiB |                      2.53 MiB |
| update(): single operation -> #add amt += 100                                                   |      36.22 MiB |                    59.64 MiB |                     94.99 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |      72.27 MiB |                    95.71 MiB |                    131.07 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |      42.16 MiB |                    65.71 MiB |                    101.03 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |      66.69 MiB |                   134.75 MiB |                    291.82 MiB |
| replace() -> replace half the tx with new tx                                                    |      25.33 MiB |                   211.99 MiB |                    251.16 MiB |
| delete()                                                                                        |       9.29 MiB |                    91.27 MiB |                    110.81 MiB |


</details>
Saving results to .bench/heap.txs.bench.json
No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/heap.txs.sorted.bench.json"

<details>

<summary>bench/heap.txs.sorted.bench.mo $({\color{gray}0\%})$</summary>

### Benchmarking zenDB with icrc3 txs

_Benchmarking the performance of sorted queries with 1k txs_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|                                                                                                 | #heap no index (sorted by ts) | #heap 7 single field indexes (sorted by tx.amt) | #heap 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ----------------------------: | ----------------------------------------------: | -------------------------------------------: |
| insert with no index                                                                            |                   150_295_370 |                                     150_284_603 |                                  150_285_557 |
| create and populate indexes                                                                     |                         3_675 |                                   1_167_696_448 |                                1_383_868_413 |
| clear collection entries and indexes                                                            |                         8_975 |                                          52_848 |                                       52_234 |
| insert with indexes                                                                             |                   157_832_348 |                                   1_298_394_683 |                                1_515_173_808 |
| query(): no filter (all txs)                                                                    |                 1_794_046_688 |                                      24_730_549 |                                   24_729_530 |
| query(): single field (btype = '1mint')                                                         |                   374_382_530 |                                     136_836_113 |                                    5_843_260 |
| query(): number range (250 < tx.amt <= 400)                                                     |                   310_325_154 |                                       4_060_732 |                                  151_908_353 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                   171_314_011 |                                      34_542_932 |                                   28_481_687 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                   406_875_263 |                                      54_457_724 |                                   82_330_710 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                 1_286_963_910 |                                     414_494_108 |                                   30_680_883 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                 1_290_623_903 |                                     155_865_288 |                                  169_944_138 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                 1_737_079_591 |                                     283_385_665 |                                  183_745_989 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                 2_066_946_290 |                                     145_498_410 |                                  175_541_208 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                 1_329_433_136 |                                     288_782_909 |                                  324_609_005 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                   158_369_701 |                                       5_844_288 |                                    1_371_672 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                   829_015_722 |                                     279_445_395 |                                   25_516_141 |
| query() -> all txs involving principals[0]                                                      |                   331_011_333 |                                      40_136_027 |                                    6_976_027 |
| query() -> all txs involving principals[0..10]                                                  |                 2_309_180_365 |                                   1_016_972_289 |                                   73_708_369 |
| update(): single operation -> #add amt += 100                                                   |                   507_963_023 |                                     784_074_588 |                                1_188_802_159 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                   845_501_764 |                                   1_122_317_407 |                                1_526_687_544 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                   571_071_531 |                                     849_545_234 |                                1_253_036_411 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                   790_279_677 |                                   1_619_469_141 |                                3_567_738_017 |
| replace() -> replace half the tx with new tx                                                    |                   411_699_361 |                                   2_828_276_707 |                                3_187_911_472 |
| delete()                                                                                        |                   155_479_412 |                                   1_189_592_624 |                                1_385_208_869 |


**Heap**

|                                                                                                 | #heap no index (sorted by ts) | #heap 7 single field indexes (sorted by tx.amt) | #heap 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ----------------------------: | ----------------------------------------------: | -------------------------------------------: |
| insert with no index                                                                            |                    265.11 KiB |                                      265.11 KiB |                                   265.11 KiB |
| create and populate indexes                                                                     |                         272 B |                                      375.54 KiB |                                   490.05 KiB |
| clear collection entries and indexes                                                            |                         284 B |                                           368 B |                                        368 B |
| insert with indexes                                                                             |                    304.18 KiB |                                      676.88 KiB |                                   791.38 KiB |
| query(): no filter (all txs)                                                                    |                         308 B |                                           272 B |                                        272 B |
| query(): single field (btype = '1mint')                                                         |                         272 B |                                           272 B |                                        272 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                         272 B |                                           272 B |                                        272 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                         272 B |                                           272 B |                                        272 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                         272 B |                                           272 B |                                        272 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                         308 B |                                           272 B |                                        272 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                         308 B |                                           272 B |                                        344 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                         380 B |                                           344 B |                                        344 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                         380 B |                                           344 B |                                        344 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                         380 B |                                           344 B |                                        344 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                         344 B |                                           344 B |                                        344 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                         344 B |                                           344 B |                                        344 B |
| query() -> all txs involving principals[0]                                                      |                         344 B |                                           344 B |                                        344 B |
| query() -> all txs involving principals[0..10]                                                  |                         380 B |                                           344 B |                                        344 B |
| update(): single operation -> #add amt += 100                                                   |                    251.63 KiB |                                      304.34 KiB |                                   364.04 KiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                    251.63 KiB |                                      303.74 KiB |                                   362.55 KiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                    251.63 KiB |                                      303.39 KiB |                                   362.23 KiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                    252.58 KiB |                                      421.91 KiB |                                   677.08 KiB |
| replace() -> replace half the tx with new tx                                                    |                     252.2 KiB |                                      572.96 KiB |                                   680.29 KiB |
| delete()                                                                                        |                         344 B |                                           380 B |                                        380 B |


**Garbage Collection**

|                                                                                                 | #heap no index (sorted by ts) | #heap 7 single field indexes (sorted by tx.amt) | #heap 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ----------------------------: | ----------------------------------------------: | -------------------------------------------: |
| insert with no index                                                                            |                     10.92 MiB |                                       10.92 MiB |                                    10.92 MiB |
| create and populate indexes                                                                     |                     16.36 KiB |                                       92.95 MiB |                                   115.17 MiB |
| clear collection entries and indexes                                                            |                     16.71 KiB |                                       16.71 KiB |                                    16.71 KiB |
| insert with indexes                                                                             |                     11.48 MiB |                                      105.31 MiB |                                   127.53 MiB |
| query(): no filter (all txs)                                                                    |                       140 MiB |                                      405.56 KiB |                                   405.77 KiB |
| query(): single field (btype = '1mint')                                                         |                     30.46 MiB |                                       11.14 MiB |                                    167.8 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                     24.27 MiB |                                      116.97 KiB |                                    11.83 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                     14.32 MiB |                                        2.83 MiB |                                      2.2 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                     32.36 MiB |                                        4.03 MiB |                                     6.22 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                    103.59 MiB |                                       33.74 MiB |                                     1.51 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                    102.19 MiB |                                        12.1 MiB |                                    12.57 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                    137.69 MiB |                                       21.96 MiB |                                    13.44 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                    161.98 MiB |                                       10.54 MiB |                                     12.9 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                    107.69 MiB |                                       23.22 MiB |                                    24.83 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                     13.14 MiB |                                      447.48 KiB |                                    91.47 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                     84.41 MiB |                                       21.11 MiB |                                     1.33 MiB |
| query() -> all txs involving principals[0]                                                      |                     32.04 MiB |                                        2.98 MiB |                                   401.28 KiB |
| query() -> all txs involving principals[0..10]                                                  |                    241.24 MiB |                                       77.52 MiB |                                     3.95 MiB |
| update(): single operation -> #add amt += 100                                                   |                     36.22 MiB |                                       59.64 MiB |                                    94.99 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                     72.27 MiB |                                       95.71 MiB |                                   131.07 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                     42.16 MiB |                                       65.72 MiB |                                   101.03 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                     66.69 MiB |                                      134.74 MiB |                                   291.82 MiB |
| replace() -> replace half the tx with new tx                                                    |                     25.33 MiB |                                      211.99 MiB |                                   251.16 MiB |
| delete()                                                                                        |                      9.29 MiB |                                       91.27 MiB |                                   110.81 MiB |


</details>
Saving results to .bench/heap.txs.sorted.bench.json
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
| insert with no index                                                                            |            210_469_682 |                          210_433_442 |                           210_434_377 |
| create and populate indexes                                                                     |                  2_266 |                        1_249_780_121 |                         1_497_651_056 |
| clear collection entries and indexes                                                            |                 42_826 |                              315_799 |                               325_083 |
| insert with indexes                                                                             |            219_388_322 |                        1_409_764_483 |                         1_660_022_885 |
| query(): no filter (all txs)                                                                    |             73_843_910 |                           73_842_864 |                            73_845_206 |
| query(): single field (btype = '1mint')                                                         |            183_787_363 |                           14_879_767 |                            15_105_457 |
| query(): number range (250 < tx.amt <= 400)                                                     |            195_612_204 |                           11_094_923 |                            11_095_877 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |            180_836_131 |                           35_511_345 |                             4_552_942 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |            198_284_844 |                           80_669_116 |                            80_622_138 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |            266_934_968 |                           46_589_528 |                            47_976_004 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |            258_737_323 |                           53_683_898 |                            54_097_217 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |            283_785_884 |                           73_435_004 |                            73_912_914 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |            398_856_977 |                           63_740_878 |                            63_487_439 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |            298_221_770 |                           55_057_551 |                            56_191_978 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |            205_578_571 |                            1_390_430 |                             1_429_568 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |            715_092_689 |                           21_137_451 |                            21_639_271 |
| query() -> all txs involving principals[0]                                                      |            356_316_032 |                            6_246_682 |                             6_419_975 |
| query() -> all txs involving principals[0..10]                                                  |          1_840_716_066 |                           64_950_224 |                            67_159_995 |
| update(): single operation -> #add amt += 100                                                   |            653_361_982 |                          965_385_562 |                         1_394_985_511 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |            990_383_011 |                        1_302_589_953 |                         1_737_712_233 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |            715_350_567 |                        1_027_299_109 |                         1_461_818_377 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |            948_736_143 |                        1_866_189_602 |                         3_915_874_189 |
| replace() -> replace half the tx with new tx                                                    |            585_043_340 |                        3_154_541_805 |                         3_566_939_729 |
| delete()                                                                                        |            224_032_438 |                        1_324_513_925 |                         1_534_833_814 |


**Heap**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |                  284 B |                                284 B |                                 284 B |
| create and populate indexes                                                                     |                  272 B |                            12.63 KiB |                             12.65 KiB |
| clear collection entries and indexes                                                            |                  320 B |                                624 B |                                 656 B |
| insert with indexes                                                                             |              39.34 KiB |                            39.38 KiB |                             39.38 KiB |
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
| update(): single operation -> #add amt += 100                                                   |               2.63 KiB |                             2.63 KiB |                              2.67 KiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |               1.11 KiB |                             1.14 KiB |                              1.14 KiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                  344 B |                                344 B |                                 380 B |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |               9.95 KiB |                               10 KiB |                              9.98 KiB |
| replace() -> replace half the tx with new tx                                                    |              13.13 KiB |                            13.73 KiB |                             12.59 KiB |
| delete()                                                                                        |                  856 B |                             1.02 KiB |                              1.11 KiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |              15.39 MiB |                            15.39 MiB |                             15.39 MiB |
| create and populate indexes                                                                     |              16.36 KiB |                           102.94 MiB |                            129.49 MiB |
| clear collection entries and indexes                                                            |              21.96 KiB |                            58.31 KiB |                              58.7 KiB |
| insert with indexes                                                                             |              15.99 MiB |                           115.73 MiB |                            142.35 MiB |
| query(): no filter (all txs)                                                                    |               4.86 MiB |                             4.86 MiB |                              4.86 MiB |
| query(): single field (btype = '1mint')                                                         |              15.58 MiB |                           984.88 KiB |                           1002.34 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |              16.04 MiB |                           770.55 KiB |                            770.55 KiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |              15.71 MiB |                             2.88 MiB |                            327.77 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |              16.78 MiB |                             6.43 MiB |                              6.43 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |              23.99 MiB |                             3.11 MiB |                              3.21 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |              21.86 MiB |                             3.51 MiB |                              3.54 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |              24.08 MiB |                             4.74 MiB |                              4.77 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |              32.08 MiB |                             4.13 MiB |                              4.12 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |              26.89 MiB |                              3.6 MiB |                              3.68 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |               17.6 MiB |                           107.87 KiB |                            110.21 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |              76.07 MiB |                             1.26 MiB |                               1.3 MiB |
| query() -> all txs involving principals[0]                                                      |              34.68 MiB |                           393.13 KiB |                            403.03 KiB |
| query() -> all txs involving principals[0..10]                                                  |              205.2 MiB |                             3.76 MiB |                              3.86 MiB |
| update(): single operation -> #add amt += 100                                                   |              49.43 MiB |                            76.23 MiB |                            115.55 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |              85.47 MiB |                           112.23 MiB |                            150.87 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |              55.36 MiB |                            82.15 MiB |                            120.63 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |              80.06 MiB |                           157.95 MiB |                            332.93 MiB |
| replace() -> replace half the tx with new tx                                                    |              38.95 MiB |                           241.63 MiB |                            288.21 MiB |
| delete()                                                                                        |              16.28 MiB |                            105.8 MiB |                            127.09 MiB |


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
| insert with no index                                                                            |                           210_472_706 |                                         210_437_085 |                                          210_438_340 |
| create and populate indexes                                                                     |                                 5_290 |                                       1_249_783_708 |                                        1_497_655_019 |
| clear collection entries and indexes                                                            |                                45_850 |                                             319_442 |                                              329_046 |
| insert with indexes                                                                             |                           219_392_114 |                                       1_409_766_831 |                                        1_660_029_395 |
| query(): no filter (all txs)                                                                    |                         2_685_779_941 |                                          74_303_216 |                                           74_304_159 |
| query(): single field (btype = '1mint')                                                         |                           552_154_151 |                                         196_097_017 |                                           15_247_512 |
| query(): number range (250 < tx.amt <= 400)                                                     |                           449_408_642 |                                         209_019_958 |                                          209_021_488 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                           244_425_729 |                                          99_255_385 |                                           38_901_545 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                           595_163_292 |                                         118_210_541 |                                          118_212_192 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                         1_889_028_389 |                                         593_169_407 |                                           60_565_696 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                         1_901_552_778 |                                         427_669_115 |                                          250_031_718 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                         2_562_234_790 |                                         451_796_006 |                                          274_216_145 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                         3_058_967_297 |                                         261_445_058 |                                          261_447_585 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                         1_937_300_668 |                                         809_895_408 |                                          454_421_366 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                           210_773_735 |                                           7_053_092 |                                            1_571_371 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                           964_652_334 |                                         304_170_355 |                                           28_119_252 |
| query() -> all txs involving principals[0]                                                      |                           393_877_260 |                                          46_528_363 |                                            7_319_181 |
| query() -> all txs involving principals[0..10]                                                  |                         2_628_649_983 |                                       1_147_509_427 |                                           80_561_757 |
| update(): single operation -> #add amt += 100                                                   |                           653_365_416 |                                         965_390_189 |                                        1_394_988_777 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                           990_384_828 |                                       1_302_593_637 |                                        1_737_717_057 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                           715_353_386 |                                       1_027_302_875 |                                        1_461_822_709 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                           948_738_115 |                                       1_866_193_940 |                                        3_915_497_507 |
| replace() -> replace half the tx with new tx                                                    |                           585_042_091 |                                       3_154_545_006 |                                        3_566_943_114 |
| delete()                                                                                        |                           224_037_088 |                                       1_324_517_480 |                                        1_534_809_583 |


**Heap**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by ts) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | --------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                                 284 B |                                               284 B |                                                284 B |
| create and populate indexes                                                                     |                                 272 B |                                           12.63 KiB |                                            12.65 KiB |
| clear collection entries and indexes                                                            |                                 320 B |                                               624 B |                                                656 B |
| insert with indexes                                                                             |                             39.34 KiB |                                           39.38 KiB |                                            39.38 KiB |
| query(): no filter (all txs)                                                                    |                                 308 B |                                               272 B |                                                272 B |
| query(): single field (btype = '1mint')                                                         |                                 272 B |                                               272 B |                                                272 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                                 272 B |                                               272 B |                                                272 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                                 272 B |                                               272 B |                                                272 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                                 272 B |                                               272 B |                                                272 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                                 380 B |                                               344 B |                                                344 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                                 380 B |                                               344 B |                                                344 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                                 380 B |                                               344 B |                                                344 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                                 380 B |                                               344 B |                                                344 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                                 380 B |                                               344 B |                                                344 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                                 344 B |                                               344 B |                                                344 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                                 344 B |                                               344 B |                                                344 B |
| query() -> all txs involving principals[0]                                                      |                                 344 B |                                               344 B |                                                344 B |
| query() -> all txs involving principals[0..10]                                                  |                                 380 B |                                               380 B |                                                344 B |
| update(): single operation -> #add amt += 100                                                   |                              2.63 KiB |                                            2.63 KiB |                                             2.67 KiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                              1.11 KiB |                                            1.14 KiB |                                             1.14 KiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                                 344 B |                                               344 B |                                                380 B |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                              9.95 KiB |                                              10 KiB |                                             9.98 KiB |
| replace() -> replace half the tx with new tx                                                    |                             13.13 KiB |                                           13.73 KiB |                                            12.59 KiB |
| delete()                                                                                        |                                 856 B |                                            1.02 KiB |                                             1.11 KiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by ts) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | --------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                             15.39 MiB |                                           15.39 MiB |                                            15.39 MiB |
| create and populate indexes                                                                     |                             16.36 KiB |                                          102.94 MiB |                                           129.49 MiB |
| clear collection entries and indexes                                                            |                             21.96 KiB |                                           58.31 KiB |                                             58.7 KiB |
| insert with indexes                                                                             |                             15.99 MiB |                                          115.73 MiB |                                           142.35 MiB |
| query(): no filter (all txs)                                                                    |                            222.25 MiB |                                            4.88 MiB |                                             4.88 MiB |
| query(): single field (btype = '1mint')                                                         |                             46.67 MiB |                                           16.49 MiB |                                          1015.28 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                             37.18 MiB |                                           16.98 MiB |                                            16.98 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                             21.07 MiB |                                            8.26 MiB |                                             3.12 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                             49.85 MiB |                                            9.47 MiB |                                             9.47 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                            159.17 MiB |                                           49.95 MiB |                                             4.19 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                            158.49 MiB |                                           35.07 MiB |                                            19.81 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                            213.75 MiB |                                           36.83 MiB |                                            21.57 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                            253.72 MiB |                                           20.65 MiB |                                            20.65 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                            163.66 MiB |                                           67.06 MiB |                                            36.52 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                             18.04 MiB |                                          583.95 KiB |                                           123.11 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                             96.88 MiB |                                           24.77 MiB |                                             1.79 MiB |
| query() -> all txs involving principals[0]                                                      |                             37.78 MiB |                                            3.73 MiB |                                           497.27 KiB |
| query() -> all txs involving principals[0..10]                                                  |                            270.71 MiB |                                           93.71 MiB |                                             5.12 MiB |
| update(): single operation -> #add amt += 100                                                   |                             49.43 MiB |                                           76.23 MiB |                                           115.55 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                             85.47 MiB |                                          112.23 MiB |                                           150.87 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                             55.36 MiB |                                           82.15 MiB |                                           120.63 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                             80.06 MiB |                                          157.95 MiB |                                           332.93 MiB |
| replace() -> replace half the tx with new tx                                                    |                             38.95 MiB |                                          241.63 MiB |                                           288.21 MiB |
| delete()                                                                                        |                             16.28 MiB |                                           105.8 MiB |                                           127.09 MiB |


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
