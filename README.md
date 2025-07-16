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
| insert with no index                                                                            |    372_367_898 |                  372_368_838 |                   372_369_312 |
| create and populate indexes                                                                     |          3_446 |                3_652_804_864 |                 4_197_628_607 |
| clear collection entries and indexes                                                            |          8_593 |                       40_454 |                        41_618 |
| insert with indexes                                                                             |    372_563_204 |                2_153_050_863 |                 2_682_944_714 |
| query(): no filter (all txs)                                                                    |     28_499_773 |                   28_498_536 |                    28_500_578 |
| query(): single field (btype = '1mint')                                                         |    368_287_490 |                    6_448_476 |                     6_960_363 |
| query(): number range (250 < tx.amt <= 400)                                                     |    388_310_894 |                    4_893_088 |                     4_893_612 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |    381_343_377 |                   69_905_419 |                     3_794_039 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |    391_114_437 |                  142_734_521 |                   142_734_882 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |    533_326_746 |                   18_933_039 |                    20_271_577 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |    484_666_070 |                   20_060_103 |                    20_506_239 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |    521_875_432 |                   26_868_808 |                    27_316_556 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |    740_934_385 |                   23_754_445 |                    23_755_015 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |    614_049_479 |                   22_671_135 |                    23_690_891 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |    427_529_103 |                    2_601_207 |                     2_737_741 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |  1_892_478_371 |                   28_450_128 |                    25_227_664 |
| query() -> all txs involving principals[0]                                                      |    831_723_976 |                    7_417_879 |                     7_540_609 |
| query() -> all txs involving principals[0..10]                                                  |  4_943_073_616 |                   74_596_338 |                    72_704_016 |
| update(): single operation -> #add amt += 100                                                   |  1_463_541_256 |                1_897_781_852 |                 2_594_451_084 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |  2_066_041_541 |                2_500_405_335 |                 3_196_477_923 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |  1_534_953_740 |                1_972_423_791 |                 2_667_478_479 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |  1_956_705_378 |                3_136_689_147 |                 6_580_372_535 |
| replace() -> replace half the tx with new tx                                                    |    834_391_530 |                4_466_100_212 |                 5_464_140_822 |
| delete()                                                                                        |    329_906_759 |                1_893_723_549 |                 2_358_599_919 |


**Heap**

|                                                                                                 | #heap no index | #heap 7 single field indexes | #heap 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | -------------: | ---------------------------: | ----------------------------: |
| insert with no index                                                                            |      18.76 MiB |                   -10.54 MiB |                    -12.18 MiB |
| create and populate indexes                                                                     |       9.89 KiB |                    11.54 MiB |                     12.02 MiB |
| clear collection entries and indexes                                                            |       10.2 KiB |                    10.31 KiB |                     10.31 KiB |
| insert with indexes                                                                             |     -12.88 MiB |                    14.88 MiB |                    -16.89 MiB |
| query(): no filter (all txs)                                                                    |     584.57 KiB |                   584.49 KiB |                    584.57 KiB |
| query(): single field (btype = '1mint')                                                         |      -7.98 MiB |                   175.05 KiB |                    201.25 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |      -5.06 MiB |                   145.57 KiB |                    145.57 KiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |      23.06 MiB |                      4.1 MiB |                    168.59 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |      -6.64 MiB |                   -19.58 MiB |                      8.32 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |       2.78 MiB |                   490.34 KiB |                    558.91 KiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |       1.03 MiB |                   513.82 KiB |                    536.73 KiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |       1.22 MiB |                   678.67 KiB |                    701.66 KiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |     -16.68 MiB |                   595.79 KiB |                    595.79 KiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |        7.6 MiB |                   653.87 KiB |                    706.52 KiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |      -2.27 MiB |                     96.4 KiB |                     99.92 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |      -4.02 MiB |                    821.9 KiB |                    781.09 KiB |
| query() -> all txs involving principals[0]                                                      |         -8 MiB |                   251.29 KiB |                    254.99 KiB |
| query() -> all txs involving principals[0..10]                                                  |       2.07 MiB |                     2.24 MiB |                      2.24 MiB |
| update(): single operation -> #add amt += 100                                                   |       6.85 MiB |                    -2.36 MiB |                      3.65 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |      11.78 MiB |                   -27.87 MiB |                       8.6 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |       9.02 MiB |                   -67.47 KiB |                       5.9 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |        4.8 MiB |                      1.2 MiB |                    -12.51 MiB |
| replace() -> replace half the tx with new tx                                                    |     -16.17 MiB |                     3.55 MiB |                   -682.39 KiB |
| delete()                                                                                        |       17.5 MiB |                      6.4 MiB |                      1.59 MiB |


**Garbage Collection**

|                                                                                                 | #heap no index | #heap 7 single field indexes | #heap 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | -------------: | ---------------------------: | ----------------------------: |
| insert with no index                                                                            |            0 B |                     29.3 MiB |                     30.94 MiB |
| create and populate indexes                                                                     |            0 B |                   188.44 MiB |                    219.96 MiB |
| clear collection entries and indexes                                                            |            0 B |                          0 B |                           0 B |
| insert with indexes                                                                             |      31.64 MiB |                    92.85 MiB |                    155.97 MiB |
| query(): no filter (all txs)                                                                    |            0 B |                          0 B |                           0 B |
| query(): single field (btype = '1mint')                                                         |         30 MiB |                          0 B |                           0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |      27.91 MiB |                          0 B |                           0 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |            0 B |                          0 B |                           0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |         30 MiB |                    27.91 MiB |                           0 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |         30 MiB |                          0 B |                           0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |      27.91 MiB |                          0 B |                           0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |         30 MiB |                          0 B |                           0 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |      59.91 MiB |                          0 B |                           0 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |         30 MiB |                          0 B |                           0 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |      27.91 MiB |                          0 B |                           0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |     123.91 MiB |                          0 B |                           0 B |
| query() -> all txs involving principals[0]                                                      |      59.91 MiB |                          0 B |                           0 B |
| query() -> all txs involving principals[0..10]                                                  |     315.91 MiB |                          0 B |                           0 B |
| update(): single operation -> #add amt += 100                                                   |      59.96 MiB |                    91.95 MiB |                    123.95 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |      91.97 MiB |                   154.39 MiB |                    155.92 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |      59.97 MiB |                    91.96 MiB |                    123.97 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |      91.98 MiB |                   155.98 MiB |                    347.93 MiB |
| replace() -> replace half the tx with new tx                                                    |       59.9 MiB |                   219.91 MiB |                     283.9 MiB |
| delete()                                                                                        |            0 B |                    92.82 MiB |                    125.59 MiB |


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
| insert with no index                                                                            |                   372_369_789 |                                     372_370_972 |                                  372_371_206 |
| create and populate indexes                                                                     |                         5_337 |                                   3_652_806_997 |                                4_197_630_501 |
| clear collection entries and indexes                                                            |                        10_484 |                                          42_588 |                                       43_512 |
| insert with indexes                                                                             |                   372_565_095 |                                   2_153_052_997 |                                2_682_946_610 |
| query(): no filter (all txs)                                                                    |                 6_028_370_401 |                                      26_611_289 |                                   26_618_439 |
| query(): single field (btype = '1mint')                                                         |                 1_239_278_778 |                                     366_580_336 |                                    7_216_991 |
| query(): number range (250 < tx.amt <= 400)                                                     |                   989_261_721 |                                       4_907_253 |                                  386_582_847 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                   532_519_044 |                                      92_243_636 |                                   70_728_405 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                 1_268_174_910 |                                     142_748_542 |                                  204_391_719 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                 3_863_375_921 |                                   1_811_928_151 |                                  680_855_066 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                 3_819_288_523 |                                     911_542_515 |                                  865_739_734 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                 5_075_294_659 |                                   2_012_884_796 |                                1_645_999_455 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                 6_264_116_976 |                                     889_760_242 |                                1_012_584_736 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                 3_981_568_169 |                                   1_602_151_790 |                                1_915_708_610 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                   438_786_640 |                                      15_089_553 |                                    2_993_490 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                 2_364_279_991 |                                     656_807_110 |                                  336_348_600 |
| query() -> all txs involving principals[0]                                                      |                   895_947_063 |                                      79_450_884 |                                   35_254_838 |
| query() -> all txs involving principals[0..10]                                                  |                 6_489_109_350 |                                   2_529_919_191 |                                1_566_301_352 |
| update(): single operation -> #add amt += 100                                                   |                 1_463_543_146 |                                   1_897_783_984 |                                2_594_452_980 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                 2_066_043_432 |                                   2_500_407_471 |                                3_196_480_043 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                 1_534_955_631 |                                   1_972_425_924 |                                2_667_480_374 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                 1_956_707_268 |                                   3_136_691_282 |                                6_580_374_428 |
| replace() -> replace half the tx with new tx                                                    |                   834_393_259 |                                   4_466_102_350 |                                5_464_142_451 |
| delete()                                                                                        |                   329_908_650 |                                   1_893_724_531 |                                2_358_603_375 |


**Heap**

|                                                                                                 | #heap no index (sorted by ts) | #heap 7 single field indexes (sorted by tx.amt) | #heap 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ----------------------------: | ----------------------------------------------: | -------------------------------------------: |
| insert with no index                                                                            |                     18.76 MiB |                                      -10.54 MiB |                                   -12.18 MiB |
| create and populate indexes                                                                     |                      9.89 KiB |                                       11.54 MiB |                                    12.02 MiB |
| clear collection entries and indexes                                                            |                      10.2 KiB |                                       10.31 KiB |                                    10.31 KiB |
| insert with indexes                                                                             |                    -12.88 MiB |                                       14.88 MiB |                                   -16.89 MiB |
| query(): no filter (all txs)                                                                    |                     10.12 MiB |                                      548.71 KiB |                                   549.27 KiB |
| query(): single field (btype = '1mint')                                                         |                    -17.89 MiB |                                          22 MiB |                                   219.27 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                     -1.22 MiB |                                      145.97 KiB |                                    -7.18 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                      4.18 MiB |                                        5.53 MiB |                                   -25.85 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                     15.79 MiB |                                        8.32 MiB |                                   -18.08 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                     11.67 MiB |                                      -15.72 MiB |                                     9.99 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                      8.01 MiB |                                       -6.05 MiB |                                    -8.98 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                     19.01 MiB |                                       -4.78 MiB |                                     5.38 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                      -7.1 MiB |                                       -7.77 MiB |                                  -361.48 KiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                    -13.05 MiB |                                        3.38 MiB |                                    21.59 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                     -3.69 MiB |                                      857.91 KiB |                                   117.81 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                     -7.88 MiB |                                        8.04 MiB |                                    -8.55 MiB |
| query() -> all txs involving principals[0]                                                      |                     -4.17 MiB |                                        4.53 MiB |                                     1.92 MiB |
| query() -> all txs involving principals[0..10]                                                  |                     -1.71 MiB |                                       -8.09 MiB |                                  -424.63 KiB |
| update(): single operation -> #add amt += 100                                                   |                      6.86 MiB |                                       -2.36 MiB |                                     3.66 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                     11.79 MiB |                                        2.55 MiB |                                   -23.39 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                      9.03 MiB |                                      -56.77 KiB |                                      5.9 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                      4.81 MiB |                                        1.21 MiB |                                    -12.5 MiB |
| replace() -> replace half the tx with new tx                                                    |                     13.58 MiB |                                      -27.39 MiB |                                   -10.66 KiB |
| delete()                                                                                        |                      17.5 MiB |                                         6.4 MiB |                                     1.59 MiB |


**Garbage Collection**

|                                                                                                 | #heap no index (sorted by ts) | #heap 7 single field indexes (sorted by tx.amt) | #heap 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ----------------------------: | ----------------------------------------------: | -------------------------------------------: |
| insert with no index                                                                            |                           0 B |                                        29.3 MiB |                                    30.94 MiB |
| create and populate indexes                                                                     |                           0 B |                                      188.44 MiB |                                   219.96 MiB |
| clear collection entries and indexes                                                            |                           0 B |                                             0 B |                                          0 B |
| insert with indexes                                                                             |                     31.64 MiB |                                       92.85 MiB |                                   155.97 MiB |
| query(): no filter (all txs)                                                                    |                    347.91 MiB |                                             0 B |                                          0 B |
| query(): single field (btype = '1mint')                                                         |                     91.91 MiB |                                             0 B |                                          0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                     59.91 MiB |                                             0 B |                                       30 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                     27.91 MiB |                                             0 B |                                       30 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                     59.91 MiB |                                             0 B |                                       30 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                    219.91 MiB |                                      123.91 MiB |                                       30 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                    219.91 MiB |                                       59.91 MiB |                                    59.91 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                    283.91 MiB |                                      123.91 MiB |                                    91.91 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                    379.91 MiB |                                       59.91 MiB |                                    59.91 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                    251.68 MiB |                                       91.91 MiB |                                    91.91 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                        30 MiB |                                             0 B |                                          0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                    155.91 MiB |                                          30 MiB |                                    27.91 MiB |
| query() -> all txs involving principals[0]                                                      |                     59.91 MiB |                                             0 B |                                          0 B |
| query() -> all txs involving principals[0..10]                                                  |                    411.91 MiB |                                      155.91 MiB |                                    91.91 MiB |
| update(): single operation -> #add amt += 100                                                   |                     59.96 MiB |                                       91.94 MiB |                                   123.94 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                     91.97 MiB |                                      123.97 MiB |                                   187.91 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                     59.95 MiB |                                       91.95 MiB |                                   123.96 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                     91.97 MiB |                                      155.97 MiB |                                   347.92 MiB |
| replace() -> replace half the tx with new tx                                                    |                     30.15 MiB |                                      250.85 MiB |                                   283.25 MiB |
| delete()                                                                                        |                           0 B |                                       92.82 MiB |                                   125.59 MiB |


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
| insert with no index                                                                            |            464_962_981 |                          464_963_780 |                           464_964_603 |
| create and populate indexes                                                                     |                  3_801 |                        4_183_697_021 |                         4_750_340_644 |
| clear collection entries and indexes                                                            |                 83_277 |                              626_000 |                               629_875 |
| insert with indexes                                                                             |            465_157_373 |                        2_289_858_113 |                         2_841_771_825 |
| query(): no filter (all txs)                                                                    |            110_269_657 |                          110_270_619 |                           110_271_282 |
| query(): single field (btype = '1mint')                                                         |            466_789_567 |                           22_369_939 |                            22_729_756 |
| query(): number range (250 < tx.amt <= 400)                                                     |            483_022_799 |                           16_874_948 |                            16_875_657 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |            468_327_387 |                           87_097_571 |                             7_051_391 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |            490_971_055 |                          193_748_581 |                           193_749_290 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |            665_300_427 |                           69_340_920 |                            70_508_818 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |            617_437_109 |                           71_944_999 |                            72_321_781 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |            670_361_706 |                           96_348_786 |                            96_728_041 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |            965_558_180 |                           85_108_835 |                            85_109_590 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |            746_464_409 |                           73_574_825 |                            74_330_712 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |            511_377_562 |                            2_718_363 |                             2_826_106 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |          1_985_747_129 |                           29_767_643 |                            30_588_197 |
| query() -> all txs involving principals[0]                                                      |            917_183_798 |                            8_156_138 |                             8_386_937 |
| query() -> all txs involving principals[0..10]                                                  |          5_052_390_042 |                           84_268_139 |                            86_802_886 |
| update(): single operation -> #add amt += 100                                                   |          1_705_656_981 |                        2_181_078_527 |                         2_899_754_042 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |          2_307_657_570 |                        2_783_018_806 |                         3_511_907_882 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |          1_776_121_300 |                        2_251_021_070 |                         2_979_375_907 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |          2_207_043_276 |                        3_505_349_889 |                         7_022_510_452 |
| replace() -> replace half the tx with new tx                                                    |          1_087_079_535 |                        4_929_335_509 |                         6_009_551_669 |
| delete()                                                                                        |            424_964_255 |                        2_062_305_105 |                         2_552_185_860 |


**Heap**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |              -5.86 MiB |                            23.74 MiB |                             -7.95 MiB |
| create and populate indexes                                                                     |               9.89 KiB |                           -11.86 MiB |                             23.79 MiB |
| clear collection entries and indexes                                                            |              17.11 KiB |                            65.55 KiB |                             65.56 KiB |
| insert with indexes                                                                             |              -7.92 MiB |                            -6.58 MiB |                             -3.59 MiB |
| query(): no filter (all txs)                                                                    |               5.85 MiB |                             5.85 MiB |                              5.85 MiB |
| query(): single field (btype = '1mint')                                                         |              -3.16 MiB |                             1.16 MiB |                            -28.39 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |               29.1 MiB |                           923.86 KiB |                            923.86 KiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |              -2.86 MiB |                           -24.38 MiB |                            384.35 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |              -1.71 MiB |                            11.59 MiB |                             11.59 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |             -20.12 MiB |                             3.68 MiB |                              3.74 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                  6 MiB |                             3.78 MiB |                               3.8 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |             -20.62 MiB |                             5.01 MiB |                              5.03 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |              -3.56 MiB |                             4.45 MiB |                              4.45 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |             -15.27 MiB |                             3.85 MiB |                              3.89 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |            -495.79 KiB |                           131.35 KiB |                            135.62 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |             478.46 KiB |                             1.32 MiB |                              1.35 MiB |
| query() -> all txs involving principals[0]                                                      |              -4.02 MiB |                           375.98 KiB |                             385.4 KiB |
| query() -> all txs involving principals[0..10]                                                  |               7.61 MiB |                             3.69 MiB |                               3.8 MiB |
| update(): single operation -> #add amt += 100                                                   |             -11.42 MiB |                           -17.15 MiB |                             24.84 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |              -6.47 MiB |                           -12.27 MiB |                             -2.88 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |              22.76 MiB |                           -15.01 MiB |                             -5.77 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |              18.69 MiB |                            -6.21 MiB |                             -2.55 MiB |
| replace() -> replace half the tx with new tx                                                    |              -2.28 MiB |                             3.63 MiB |                               5.9 MiB |
| delete()                                                                                        |              -6.32 MiB |                           -10.38 MiB |                             18.94 MiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |               29.6 MiB |                                  0 B |                             31.69 MiB |
| create and populate indexes                                                                     |                    0 B |                           253.59 MiB |                            253.57 MiB |
| clear collection entries and indexes                                                            |                    0 B |                                  0 B |                                   0 B |
| insert with indexes                                                                             |              31.66 MiB |                           125.57 MiB |                            157.57 MiB |
| query(): no filter (all txs)                                                                    |                    0 B |                                  0 B |                                   0 B |
| query(): single field (btype = '1mint')                                                         |              31.66 MiB |                                  0 B |                             29.57 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                    0 B |                                  0 B |                                   0 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |              31.66 MiB |                            29.57 MiB |                                   0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |              31.66 MiB |                                  0 B |                                   0 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |              61.57 MiB |                                  0 B |                                   0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |              31.66 MiB |                                  0 B |                                   0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |              61.57 MiB |                                  0 B |                                   0 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |              61.57 MiB |                                  0 B |                                   0 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |              61.57 MiB |                                  0 B |                                   0 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |              31.66 MiB |                                  0 B |                                   0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |             125.57 MiB |                                  0 B |                                   0 B |
| query() -> all txs involving principals[0]                                                      |              61.57 MiB |                                  0 B |                                   0 B |
| query() -> all txs involving principals[0..10]                                                  |             317.57 MiB |                                  0 B |                                   0 B |
| update(): single operation -> #add amt += 100                                                   |              93.57 MiB |                           125.57 MiB |                            125.56 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |             125.56 MiB |                           157.56 MiB |                            189.56 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |              61.56 MiB |                           125.56 MiB |                            157.56 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |              93.55 MiB |                           189.53 MiB |                            381.52 MiB |
| replace() -> replace half the tx with new tx                                                    |              61.52 MiB |                           253.49 MiB |                            317.44 MiB |
| delete()                                                                                        |              31.55 MiB |                           125.51 MiB |                            125.57 MiB |


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

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by tx.amt) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | ------------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                           464_965_769 |                                             464_966_810 |                                          464_967_392 |
| create and populate indexes                                                                     |                                 6_589 |                                           4_183_700_051 |                                        4_750_343_433 |
| clear collection entries and indexes                                                            |                                86_065 |                                                 629_030 |                                              632_664 |
| insert with indexes                                                                             |                           465_160_161 |                                           2_289_861_143 |                                        2_841_774_614 |
| query(): no filter (all txs)                                                                    |                         7_605_113_280 |                                             111_048_622 |                                          111_035_755 |
| query(): single field (btype = '1mint')                                                         |                         1_538_519_278 |                                             466_282_240 |                                           22_955_517 |
| query(): number range (250 < tx.amt <= 400)                                                     |                         1_232_045_457 |                                              16_890_009 |                                          482_511_567 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                           654_572_919 |                                             116_626_483 |                                           87_722_416 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                         1_584_999_936 |                                             193_763_661 |                                          264_717_530 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                         4_819_629_804 |                                           2_267_297_870 |                                          893_925_636 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                         4_780_834_470 |                                           1_164_411_718 |                                        1_116_850_848 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                         6_350_032_215 |                                           2_530_441_721 |                                        2_106_761_834 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                         7_853_987_262 |                                           1_147_583_821 |                                        1_307_466_350 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                         4_940_964_262 |                                           2_004_262_522 |                                        2_410_820_692 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                           525_515_211 |                                              17_915_200 |                                            3_051_315 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                         2_572_988_144 |                                             791_587_936 |                                          417_514_144 |
| query() -> all txs involving principals[0]                                                      |                           997_807_828 |                                              96_036_058 |                                           42_921_787 |
| query() -> all txs involving principals[0..10]                                                  |                         6_982_023_431 |                                           3_071_296_916 |                                        1_950_961_542 |
| update(): single operation -> #add amt += 100                                                   |                         1_705_659_588 |                                           2_181_081_574 |                                        2_899_757_038 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                         2_307_660_157 |                                           2_783_021_857 |                                        3_511_910_671 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                         1_776_124_270 |                                           2_251_023_882 |                                        2_979_378_711 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                         2_207_046_248 |                                           3_505_352_692 |                                        7_022_435_592 |
| replace() -> replace half the tx with new tx                                                    |                         1_087_082_324 |                                           4_929_338_536 |                                        6_009_554_726 |
| delete()                                                                                        |                           424_965_319 |                                           2_062_308_092 |                                        2_552_188_870 |


**Heap**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by tx.amt) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | ------------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                             -5.86 MiB |                                               23.74 MiB |                                            -7.95 MiB |
| create and populate indexes                                                                     |                              9.89 KiB |                                              -11.86 MiB |                                            23.79 MiB |
| clear collection entries and indexes                                                            |                             17.11 KiB |                                               65.55 KiB |                                            65.56 KiB |
| insert with indexes                                                                             |                             -7.92 MiB |                                               -6.58 MiB |                                            -3.59 MiB |
| query(): no filter (all txs)                                                                    |                             14.86 MiB |                                              -25.78 MiB |                                             5.88 MiB |
| query(): single field (btype = '1mint')                                                         |                           -223.06 KiB |                                               -3.34 MiB |                                              1.2 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                                13 MiB |                                              924.27 KiB |                                            -2.75 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                              10.5 MiB |                                              -24.58 MiB |                                             5.22 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                              2.82 MiB |                                               11.59 MiB |                                           -15.89 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                              8.25 MiB |                                               11.79 MiB |                                            -7.81 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                              4.85 MiB |                                              -23.46 MiB |                                             5.53 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                              4.19 MiB |                                               -5.13 MiB |                                             1.48 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                             -1.23 MiB |                                                7.14 MiB |                                            16.97 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                             -16.6 MiB |                                               -4.54 MiB |                                            19.73 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                            382.76 KiB |                                                1.04 MiB |                                           -29.42 MiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                               4.1 MiB |                                               15.55 MiB |                                            -4.73 MiB |
| query() -> all txs involving principals[0]                                                      |                            898.63 KiB |                                                5.67 MiB |                                             2.48 MiB |
| query() -> all txs involving principals[0..10]                                                  |                             -3.27 MiB |                                               -5.81 MiB |                                            -8.52 MiB |
| update(): single operation -> #add amt += 100                                                   |                             20.58 MiB |                                              -17.15 MiB |                                            -7.16 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                             25.53 MiB |                                              -12.27 MiB |                                            -2.88 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                             -9.24 MiB |                                                  17 MiB |                                            -5.77 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                            -13.31 MiB |                                               25.79 MiB |                                            -2.55 MiB |
| replace() -> replace half the tx with new tx                                                    |                             -2.28 MiB |                                                3.63 MiB |                                           -26.09 MiB |
| delete()                                                                                        |                             25.23 MiB |                                              -10.38 MiB |                                           -13.06 MiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by tx.amt) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | ------------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                              29.6 MiB |                                                     0 B |                                            31.69 MiB |
| create and populate indexes                                                                     |                                   0 B |                                              253.59 MiB |                                           253.57 MiB |
| clear collection entries and indexes                                                            |                                   0 B |                                                     0 B |                                                  0 B |
| insert with indexes                                                                             |                             31.66 MiB |                                              125.57 MiB |                                           157.57 MiB |
| query(): no filter (all txs)                                                                    |                            445.57 MiB |                                               31.66 MiB |                                                  0 B |
| query(): single field (btype = '1mint')                                                         |                             93.57 MiB |                                               31.66 MiB |                                                  0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                             61.57 MiB |                                                     0 B |                                            31.66 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                             29.57 MiB |                                               31.66 MiB |                                                  0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                             93.57 MiB |                                                     0 B |                                            31.66 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                            285.57 MiB |                                              125.57 MiB |                                            61.57 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                            285.57 MiB |                                               93.57 MiB |                                            61.57 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                            381.57 MiB |                                              157.57 MiB |                                           125.57 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                            477.57 MiB |                                               61.57 MiB |                                            61.57 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                            317.57 MiB |                                              125.57 MiB |                                           125.57 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                             31.66 MiB |                                                     0 B |                                            29.57 MiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                            157.57 MiB |                                               31.66 MiB |                                            29.57 MiB |
| query() -> all txs involving principals[0]                                                      |                             61.57 MiB |                                                     0 B |                                                  0 B |
| query() -> all txs involving principals[0..10]                                                  |                            445.57 MiB |                                              189.57 MiB |                                           125.57 MiB |
| update(): single operation -> #add amt += 100                                                   |                             61.57 MiB |                                              125.57 MiB |                                           157.56 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                             93.56 MiB |                                              157.56 MiB |                                           189.56 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                             93.56 MiB |                                               93.56 MiB |                                           157.56 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                            125.55 MiB |                                              157.53 MiB |                                           381.52 MiB |
| replace() -> replace half the tx with new tx                                                    |                             61.52 MiB |                                              253.49 MiB |                                           349.44 MiB |
| delete()                                                                                        |                                   0 B |                                              125.51 MiB |                                           157.57 MiB |


**Stable Memory**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by tx.amt) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | ------------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                                   0 B |                                                     0 B |                                                  0 B |
| create and populate indexes                                                                     |                                   0 B |                                                 224 MiB |                                              224 MiB |
| clear collection entries and indexes                                                            |                                   0 B |                                                     0 B |                                                  0 B |
| insert with indexes                                                                             |                                   0 B |                                                     0 B |                                                  0 B |
| query(): no filter (all txs)                                                                    |                                   0 B |                                                     0 B |                                                  0 B |
| query(): single field (btype = '1mint')                                                         |                                   0 B |                                                     0 B |                                                  0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                                   0 B |                                                     0 B |                                                  0 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                                   0 B |                                                     0 B |                                                  0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                                   0 B |                                                     0 B |                                                  0 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                                   0 B |                                                     0 B |                                                  0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                                   0 B |                                                     0 B |                                                  0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                                   0 B |                                                     0 B |                                                  0 B |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                                   0 B |                                                     0 B |                                                  0 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                                   0 B |                                                     0 B |                                                  0 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                                   0 B |                                                     0 B |                                                  0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                                   0 B |                                                     0 B |                                                  0 B |
| query() -> all txs involving principals[0]                                                      |                                   0 B |                                                     0 B |                                                  0 B |
| query() -> all txs involving principals[0..10]                                                  |                                   0 B |                                                     0 B |                                                  0 B |
| update(): single operation -> #add amt += 100                                                   |                                   0 B |                                                     0 B |                                                  0 B |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                                   0 B |                                                     0 B |                                                  0 B |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                                   0 B |                                                     0 B |                                                  0 B |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                                   0 B |                                                     0 B |                                                  0 B |
| replace() -> replace half the tx with new tx                                                    |                                   0 B |                                                     0 B |                                                  0 B |
| delete()                                                                                        |                                   0 B |                                                     0 B |                                                  0 B |

</details>
Saving results to .bench/stable-memory.txs.sorted.bench.json
