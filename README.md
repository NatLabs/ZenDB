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
| insert with no index                                                                            |    150_159_736 |                  150_147_893 |                   150_149_993 |
| create and populate indexes                                                                     |          1_872 |                1_166_869_126 |                 1_382_958_048 |
| clear collection entries and indexes                                                            |          7_172 |                       49_190 |                        50_378 |
| insert with indexes                                                                             |    157_911_305 |                1_297_200_869 |                 1_513_894_476 |
| query(): no filter (all txs)                                                                    |     24_764_672 |                   24_759_502 |                    24_760_941 |
| query(): single field (btype = '1mint')                                                         |    124_334_658 |                    5_425_806 |                     5_708_939 |
| query(): number range (250 < tx.amt <= 400)                                                     |    138_516_658 |                    4_047_436 |                     4_048_134 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |    128_298_817 |                   25_125_616 |                     2_411_520 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |    138_150_055 |                   50_569_023 |                    50_516_730 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |    188_780_074 |                   16_971_368 |                    18_509_298 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |    180_110_896 |                   23_411_803 |                    23_906_571 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |    195_831_350 |                   33_165_684 |                    33_729_903 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |    264_927_138 |                   28_103_572 |                    27_820_843 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |    219_789_576 |                   25_283_462 |                    26_746_739 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |    154_940_563 |                    1_179_321 |                     1_231_086 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |    659_807_469 |                   21_563_408 |                    19_116_317 |
| query() -> all txs involving principals[0]                                                      |    305_750_611 |                    5_990_711 |                     6_095_118 |
| query() -> all txs involving principals[0..10]                                                  |  1_775_073_235 |                   61_514_319 |                    60_556_070 |
| update(): single operation -> #add amt += 100                                                   |    507_526_104 |                  783_398_000 |                 1_187_836_418 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |    844_276_501 |                1_120_862_570 |                 1_524_920_456 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |    570_129_587 |                  848_355_588 |                 1_251_557_246 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |    789_089_143 |                1_617_603_741 |                 3_564_881_508 |
| replace() -> replace half the tx with new tx                                                    |    413_041_521 |                2_827_786_915 |                 3_187_301_322 |
| delete()                                                                                        |    155_564_900 |                1_189_124_553 |                 1_384_734_641 |


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
| query(): no filter (all txs)                                                                    |     380.95 KiB |                   380.95 KiB |                    381.02 KiB |
| query(): single field (btype = '1mint')                                                         |      10.04 MiB |                   129.99 KiB |                    151.95 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |       10.7 MiB |                   116.56 KiB |                    116.56 KiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |       10.8 MiB |                     1.96 MiB |                    132.39 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |      11.15 MiB |                     3.68 MiB |                      3.67 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |      16.81 MiB |                   434.34 KiB |                    552.93 KiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |      14.64 MiB |                   788.54 KiB |                       826 KiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |      15.99 MiB |                     1.12 MiB |                      1.16 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |      19.68 MiB |                   929.61 KiB |                    912.22 KiB |
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
| insert with no index                                                                            |                   150_161_477 |                                     150_151_489 |                                  150_151_787 |
| create and populate indexes                                                                     |                         3_613 |                                   1_166_872_778 |                                1_382_959_842 |
| clear collection entries and indexes                                                            |                         8_913 |                                          52_786 |                                       52_172 |
| insert with indexes                                                                             |                   157_913_840 |                                   1_297_209_259 |                                1_513_901_891 |
| query(): no filter (all txs)                                                                    |                 1_795_093_173 |                                      24_763_115 |                                   24_763_848 |
| query(): single field (btype = '1mint')                                                         |                   374_566_659 |                                     136_870_225 |                                    5_848_044 |
| query(): number range (250 < tx.amt <= 400)                                                     |                   310_473_446 |                                       4_063_627 |                                  151_955_372 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                   171_366_916 |                                      34_548_811 |                                   28_486_803 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                   407_086_655 |                                      54_485_104 |                                   82_359_169 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                 1_287_607_743 |                                     414_592_547 |                                   30_692_135 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                 1_291_303_684 |                                     155_910_006 |                                  170_004_450 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                 1_738_016_706 |                                     283_492_590 |                                  183_811_767 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                 2_068_103_298 |                                     145_575_916 |                                  175_606_247 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                 1_330_088_985 |                                     288_852_668 |                                  324_705_688 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                   158_403_399 |                                       5_843_648 |                                    1_370_103 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                   828_971_286 |                                     279_513_668 |                                   25_502_093 |
| query() -> all txs involving principals[0]                                                      |                   330_946_167 |                                      40_140_133 |                                    6_970_725 |
| query() -> all txs involving principals[0..10]                                                  |                 2_308_533_134 |                                   1_017_238_047 |                                   73_665_490 |
| update(): single operation -> #add amt += 100                                                   |                   507_528_096 |                                     783_403_470 |                                1_187_838_403 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                   844_277_304 |                                   1_120_857_469 |                                1_524_933_365 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                   570_131_825 |                                     848_368_870 |                                1_251_570_020 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                   789_091_012 |                                   1_617_595_641 |                                3_564_635_003 |
| replace() -> replace half the tx with new tx                                                    |                   413_044_579 |                                   2_827_790_400 |                                3_187_322_128 |
| delete()                                                                                        |                   155_566_723 |                                   1_189_127_728 |                                1_384_736_271 |


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
| query(): no filter (all txs)                                                                    |                       140 MiB |                                      405.56 KiB |                                   405.84 KiB |
| query(): single field (btype = '1mint')                                                         |                     30.46 MiB |                                       11.14 MiB |                                    167.8 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                     24.27 MiB |                                      116.97 KiB |                                    11.83 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                     14.32 MiB |                                        2.83 MiB |                                      2.2 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                     32.36 MiB |                                        4.03 MiB |                                     6.22 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                    103.59 MiB |                                       33.74 MiB |                                     1.51 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                    102.19 MiB |                                        12.1 MiB |                                    12.57 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                    137.69 MiB |                                       21.96 MiB |                                    13.44 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                    161.97 MiB |                                       10.54 MiB |                                     12.9 MiB |
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
| insert with no index                                                                            |            210_335_611 |                          210_300_027 |                           210_300_388 |
| create and populate indexes                                                                     |                  2_202 |                        1_248_962_201 |                         1_496_742_180 |
| clear collection entries and indexes                                                            |                 42_762 |                              315_735 |                               325_019 |
| insert with indexes                                                                             |            219_470_251 |                        1_408_583_145 |                         1_658_753_496 |
| query(): no filter (all txs)                                                                    |             73_877_063 |                           73_877_769 |                            73_877_091 |
| query(): single field (btype = '1mint')                                                         |            183_841_818 |                           14_885_276 |                            15_110_576 |
| query(): number range (250 < tx.amt <= 400)                                                     |            195_670_284 |                           11_097_831 |                            11_098_908 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |            180_864_526 |                           35_518_945 |                             4_552_359 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |            198_362_417 |                           80_718_742 |                            80_665_938 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |            267_041_601 |                           46_606_408 |                            47_988_422 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |            258_880_452 |                           53_821_793 |                            54_222_768 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |            283_991_510 |                           73_620_481 |                            74_090_899 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |            399_123_074 |                           63_902_797 |                            63_619_524 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |            298_349_653 |                           55_153_720 |                            56_298_506 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |            205_610_039 |                            1_389_181 |                             1_428_375 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |            714_956_864 |                           21_129_110 |                            21_630_816 |
| query() -> all txs involving principals[0]                                                      |            356_237_734 |                            6_244_123 |                             6_416_359 |
| query() -> all txs involving principals[0..10]                                                  |          1_839_799_728 |                           64_959_288 |                            67_164_512 |
| update(): single operation -> #add amt += 100                                                   |            652_925_577 |                          964_713_868 |                         1_394_023_758 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |            989_157_852 |                        1_301_128_902 |                         1_735_960_935 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |            714_412_048 |                        1_026_123_866 |                         1_460_354_141 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |            947_547_435 |                        1_864_316_264 |                         3_912_773_886 |
| replace() -> replace half the tx with new tx                                                    |            586_388_019 |                        3_154_055_595 |                         3_566_355_943 |
| delete()                                                                                        |            224_117_783 |                        1_324_048_217 |                         1_534_358_769 |


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
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |              21.85 MiB |                             3.51 MiB |                              3.54 MiB |
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
| insert with no index                                                                            |                           210_338_637 |                                         210_303_672 |                                          210_304_353 |
| create and populate indexes                                                                     |                                 5_228 |                                       1_248_965_846 |                                        1_496_746_145 |
| clear collection entries and indexes                                                            |                                45_788 |                                             319_380 |                                              328_984 |
| insert with indexes                                                                             |                           219_473_299 |                                       1_408_588_521 |                                        1_658_756_728 |
| query(): no filter (all txs)                                                                    |                         2_686_826_012 |                                          74_335_495 |                                           74_336_971 |
| query(): single field (btype = '1mint')                                                         |                           552_338_280 |                                         196_130_227 |                                           15_252_132 |
| query(): number range (250 < tx.amt <= 400)                                                     |                           449_555_622 |                                         209_066_895 |                                          209_067_933 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                           244_478_921 |                                          99_286_809 |                                           38_906_169 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                           595_376_894 |                                         118_236_879 |                                          118_238_653 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                         1_889_670_783 |                                         593_267_108 |                                           60_575_319 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                         1_902_237_229 |                                         427_759_146 |                                          250_089_991 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                         2_563_173_776 |                                         451_889_464 |                                          274_279_351 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                         3_060_128_524 |                                         261_508_345 |                                          261_511_897 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                         1_937_959_797 |                                         810_051_103 |                                          454_519_332 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                           210_807_802 |                                           7_053_049 |                                            1_569_884 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                           964_611_133 |                                         304_260_269 |                                           28_105_245 |
| query() -> all txs involving principals[0]                                                      |                           393_811_848 |                                          46_536_573 |                                            7_313_961 |
| query() -> all txs involving principals[0..10]                                                  |                         2_628_005_208 |                                       1_147_867_914 |                                           80_519_975 |
| update(): single operation -> #add amt += 100                                                   |                           652_928_603 |                                         964_718_005 |                                        1_394_026_657 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                           989_160_040 |                                       1_301_130_784 |                                        1_735_964_572 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                           714_413_434 |                                       1_026_126_978 |                                        1_460_356_917 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                           947_549_081 |                                       1_864_321_301 |                                        3_912_394_008 |
| replace() -> replace half the tx with new tx                                                    |                           586_384_845 |                                       3_154_058_429 |                                        3_566_355_189 |
| delete()                                                                                        |                           224_123_538 |                                       1_324_051_077 |                                        1_534_334_209 |


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
