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
| insert with no index                                                                            |    357_097_258 |                  357_098_218 |                   357_098_692 |
| create and populate indexes                                                                     |          3_266 |                3_474_511_827 |                 3_988_432_287 |
| clear collection entries and indexes                                                            |          8_253 |                       39_974 |                        41_138 |
| insert with indexes                                                                             |    357_292_583 |                2_048_696_880 |                 2_548_168_969 |
| query(): no filter (all txs)                                                                    |     28_214_241 |                   28_213_034 |                    28_215_046 |
| query(): single field (btype = '1mint')                                                         |    349_489_794 |                    6_345_598 |                     6_832_479 |
| query(): number range (250 < tx.amt <= 400)                                                     |    368_928_868 |                    4_806_043 |                     4_806_567 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |    361_537_010 |                   66_407_647 |                     3_656_030 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |    371_218_577 |                  135_791_978 |                   135_792_319 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |    504_443_381 |                   18_645_767 |                    19_919_598 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |    459_603_870 |                   19_785_209 |                    20_209_490 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |    494_767_070 |                   26_504_453 |                    26_930_316 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |    704_583_052 |                   23_442_860 |                    23_443_430 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |    580_826_267 |                   22_266_180 |                    23_235_113 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |    405_411_316 |                    2_514_104 |                     2_647_082 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |  1_782_194_150 |                   27_611_843 |                    24_462_193 |
| query() -> all txs involving principals[0]                                                      |    784_686_126 |                    7_177_352 |                     7_298_402 |
| query() -> all txs involving principals[0..10]                                                  |  4_644_955_482 |                   72_314_952 |                    70_469_842 |
| update(): single operation -> #add amt += 100                                                   |  1_412_119_891 |                1_824_607_585 |                 2_483_343_492 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |  1_977_651_195 |                2_390_262_520 |                 3_048_415_329 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |  1_480_532_375 |                1_896_100_883 |                 2_553_274_450 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |  1_875_080_105 |                2_996_635_605 |                 6_261_342_341 |
| replace() -> replace half the tx with new tx                                                    |    799_764_051 |                4_250_752_708 |                 5_191_354_290 |
| delete()                                                                                        |    315_379_260 |                1_799_066_154 |                 2_237_247_916 |


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
| insert with no index                                                                            |                   357_099_149 |                                     357_100_352 |                                  357_100_586 |
| create and populate indexes                                                                     |                         5_157 |                                   3_474_513_961 |                                3_988_434_181 |
| clear collection entries and indexes                                                            |                        10_144 |                                          42_108 |                                       43_032 |
| insert with indexes                                                                             |                   357_294_474 |                                   2_048_699_014 |                                2_548_170_863 |
| query(): no filter (all txs)                                                                    |                 5_729_502_194 |                                      26_377_235 |                                   26_383_869 |
| query(): single field (btype = '1mint')                                                         |                 1_176_764_768 |                                     347_821_076 |                                    7_073_384 |
| query(): number range (250 < tx.amt <= 400)                                                     |                   939_921_269 |                                       4_819_808 |                                  367_244_080 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                   505_120_703 |                                      87_527_001 |                                   67_188_950 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                 1_204_520_397 |                                     135_805_579 |                                  194_315_746 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                 3_668_141_879 |                                   1_719_910_990 |                                  647_499_251 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                 3_627_957_215 |                                     865_915_405 |                                  823_009_783 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                 4_821_041_111 |                                   1_912_221_185 |                                1_564_556_232 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                 5_952_134_683 |                                     845_915_602 |                                  962_617_864 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                 3_780_049_252 |                                   1_521_146_409 |                                1_820_260_100 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                   416_105_539 |                                      14_372_041 |                                    2_887_168 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                 2_230_475_225 |                                     624_445_295 |                                  320_026_157 |
| query() -> all txs involving principals[0]                                                      |                   845_707_771 |                                      75_588_546 |                                   33_619_688 |
| query() -> all txs involving principals[0..10]                                                  |                 6_113_998_324 |                                   2_404_511_605 |                                1_489_556_594 |
| update(): single operation -> #add amt += 100                                                   |                 1_412_121_782 |                                   1_824_609_719 |                                2_483_345_386 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                 1_977_653_086 |                                   2_390_264_654 |                                3_048_417_469 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                 1_480_534_266 |                                   1_896_103_017 |                                2_553_276_344 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                 1_875_081_996 |                                   2_996_637_739 |                                6_261_344_235 |
| replace() -> replace half the tx with new tx                                                    |                   799_765_759 |                                   4_250_754_846 |                                5_191_355_900 |
| delete()                                                                                        |                   315_381_151 |                                   1_799_067_165 |                                2_237_251_344 |


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
| insert with no index                                                                            |            445_816_796 |                          445_817_575 |                           445_818_417 |
| create and populate indexes                                                                     |                  3_621 |                        3_979_591_844 |                         4_515_808_444 |
| clear collection entries and indexes                                                            |                 78_933 |                              585_300 |                               597_363 |
| insert with indexes                                                                             |            447_566_575 |                        2_194_501_249 |                         2_718_620_787 |
| query(): no filter (all txs)                                                                    |            106_039_995 |                          106_040_957 |                           106_041_620 |
| query(): single field (btype = '1mint')                                                         |            443_304_852 |                           21_529_383 |                            21_873_318 |
| query(): number range (250 < tx.amt <= 400)                                                     |            459_122_220 |                           16_218_253 |                            16_219_141 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |            444_384_952 |                           82_802_418 |                             6_770_747 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |            466_302_785 |                          184_347_906 |                           184_348_615 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |            630_076_499 |                           66_656_401 |                            67_772_094 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |            585_999_142 |                           69_205_915 |                            69_568_333 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |            636_110_313 |                           92_714_210 |                            93_076_676 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |            918_452_275 |                           81_889_842 |                            81_890_597 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |            706_884_768 |                           70_774_221 |                            71_494_546 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |            485_272_917 |                            2_615_618 |                             2_719_809 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |          1_871_016_374 |                           28_678_582 |                            29_469_320 |
| query() -> all txs involving principals[0]                                                      |            866_080_713 |                            7_851_951 |                             8_076_518 |
| query() -> all txs involving principals[0..10]                                                  |          4_749_041_512 |                           81_180_643 |                            83_642_956 |
| update(): single operation -> #add amt += 100                                                   |          1_642_539_555 |                        2_097_017_446 |                         2_779_504_288 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |          2_207_506_816 |                        2_661_927_795 |                         3_354_132_862 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |          1_709_747_856 |                        2_163_729_263 |                         2_855_395_908 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |          2_120_226_235 |                        3_350_880_226 |                         6_707_674_131 |
| replace() -> replace half the tx with new tx                                                    |          1_061_467_957 |                        4_696_221_832 |                         5_679_958_448 |
| delete()                                                                                        |            414_366_849 |                        1_982_721_332 |                         2_432_090_786 |


**Heap**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |              -5.86 MiB |                            23.74 MiB |                             -7.95 MiB |
| create and populate indexes                                                                     |               9.89 KiB |                           -11.86 MiB |                             23.79 MiB |
| clear collection entries and indexes                                                            |              16.61 KiB |                            61.07 KiB |                             61.56 KiB |
| insert with indexes                                                                             |              -7.87 MiB |                            -6.23 MiB |                             -3.16 MiB |
| query(): no filter (all txs)                                                                    |               5.85 MiB |                             5.85 MiB |                              5.85 MiB |
| query(): single field (btype = '1mint')                                                         |              -3.16 MiB |                             1.16 MiB |                            -28.39 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |               29.1 MiB |                           923.86 KiB |                            -30.76 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |               28.8 MiB |                           -24.38 MiB |                            384.35 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |              -1.71 MiB |                            11.59 MiB |                             11.59 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |             -20.12 MiB |                             3.68 MiB |                              3.74 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                  6 MiB |                             3.78 MiB |                               3.8 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |             -20.62 MiB |                             5.01 MiB |                              5.03 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |              -3.56 MiB |                             4.45 MiB |                              4.45 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |             -15.27 MiB |                             3.85 MiB |                              3.89 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |             -495.8 KiB |                           131.35 KiB |                            135.62 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |             478.47 KiB |                             1.32 MiB |                              1.35 MiB |
| query() -> all txs involving principals[0]                                                      |              -4.02 MiB |                           375.98 KiB |                             385.4 KiB |
| query() -> all txs involving principals[0..10]                                                  |               7.61 MiB |                             3.69 MiB |                               3.8 MiB |
| update(): single operation -> #add amt += 100                                                   |             -11.41 MiB |                           -17.11 MiB |                             24.92 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |              -6.47 MiB |                           -12.23 MiB |                              -2.8 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |              22.76 MiB |                           -14.97 MiB |                             -5.69 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |              18.77 MiB |                            -6.14 MiB |                             -2.21 MiB |
| replace() -> replace half the tx with new tx                                                    |              -1.96 MiB |                             3.58 MiB |                              5.91 MiB |
| delete()                                                                                        |              -6.32 MiB |                           -10.02 MiB |                              19.4 MiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |               29.6 MiB |                                  0 B |                             31.69 MiB |
| create and populate indexes                                                                     |                    0 B |                           253.59 MiB |                            253.57 MiB |
| clear collection entries and indexes                                                            |                    0 B |                                  0 B |                                   0 B |
| insert with indexes                                                                             |              31.66 MiB |                           125.57 MiB |                            157.57 MiB |
| query(): no filter (all txs)                                                                    |                    0 B |                                  0 B |                                   0 B |
| query(): single field (btype = '1mint')                                                         |              31.66 MiB |                                  0 B |                             29.57 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                    0 B |                                  0 B |                             31.66 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                    0 B |                            29.57 MiB |                                   0 B |
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
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |              93.55 MiB |                           189.54 MiB |                            381.53 MiB |
| replace() -> replace half the tx with new tx                                                    |              61.53 MiB |                           253.52 MiB |                            317.51 MiB |
| delete()                                                                                        |              31.62 MiB |                           125.55 MiB |                            125.57 MiB |


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
| insert with no index                                                                            |                           445_819_584 |                                             445_820_605 |                                          445_821_206 |
| create and populate indexes                                                                     |                                 6_409 |                                           3_979_594_874 |                                        4_515_811_233 |
| clear collection entries and indexes                                                            |                                81_721 |                                                 588_330 |                                              600_152 |
| insert with indexes                                                                             |                           447_569_363 |                                           2_194_504_279 |                                        2_718_623_576 |
| query(): no filter (all txs)                                                                    |                         7_229_400_587 |                                             106_790_932 |                                          106_780_075 |
| query(): single field (btype = '1mint')                                                         |                         1_461_717_194 |                                             442_772_116 |                                           22_087_674 |
| query(): number range (250 < tx.amt <= 400)                                                     |                         1_170_939_667 |                                              16_232_914 |                                          458_588_697 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                           621_366_358 |                                             110_755_745 |                                           83_399_199 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                         1_505_945_542 |                                             184_362_586 |                                          251_743_749 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                         4_577_720_938 |                                           2_153_335_693 |                                          850_227_184 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                         4_542_668_429 |                                           1_106_540_650 |                                        1_061_947_258 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                         6_033_635_400 |                                           2_404_688_179 |                                        2_002_903_643 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                         7_464_444_353 |                                           1_091_267_832 |                                        1_243_192_062 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                         4_692_736_902 |                                           1_903_914_270 |                                        2_291_479_073 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                           498_708_259 |                                              17_053_952 |                                            2_933_529 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                         2_429_101_719 |                                             752_478_420 |                                          397_179_008 |
| query() -> all txs involving principals[0]                                                      |                           942_706_456 |                                              91_349_863 |                                           40_895_247 |
| query() -> all txs involving principals[0..10]                                                  |                         6_582_937_586 |                                           2_919_163_235 |                                        1_855_274_457 |
| update(): single operation -> #add amt += 100                                                   |                         1_642_542_143 |                                           2_097_020_493 |                                        2_779_507_302 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                         2_207_509_383 |                                           2_661_930_846 |                                        3_354_135_651 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                         1_709_750_844 |                                           2_163_732_055 |                                        2_855_398_714 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                         2_120_229_227 |                                           3_350_883_010 |                                        6_707_590_977 |
| replace() -> replace half the tx with new tx                                                    |                         1_061_470_745 |                                           4_696_224_862 |                                        5_679_961_525 |
| delete()                                                                                        |                           414_369_458 |                                           1_982_724_320 |                                        2_432_093_817 |


**Heap**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by tx.amt) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | ------------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                             -5.86 MiB |                                               23.74 MiB |                                            -7.95 MiB |
| create and populate indexes                                                                     |                              9.89 KiB |                                              -11.86 MiB |                                            23.79 MiB |
| clear collection entries and indexes                                                            |                             16.61 KiB |                                               61.07 KiB |                                            61.56 KiB |
| insert with indexes                                                                             |                             -7.87 MiB |                                               -6.23 MiB |                                            -3.16 MiB |
| query(): no filter (all txs)                                                                    |                             14.86 MiB |                                              -25.78 MiB |                                             5.88 MiB |
| query(): single field (btype = '1mint')                                                         |                           -223.19 KiB |                                               -3.34 MiB |                                              1.2 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                                13 MiB |                                              924.27 KiB |                                            -2.75 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                              10.5 MiB |                                              -24.58 MiB |                                             5.22 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                              2.82 MiB |                                               11.59 MiB |                                           -15.89 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                              8.25 MiB |                                               11.79 MiB |                                            -7.81 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                              4.85 MiB |                                              -23.46 MiB |                                             5.53 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                              4.19 MiB |                                               -5.13 MiB |                                             1.48 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                             -1.23 MiB |                                                7.14 MiB |                                            16.97 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                             -16.6 MiB |                                               -4.54 MiB |                                            19.73 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                            382.77 KiB |                                              -28.53 MiB |                                           149.61 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                               4.1 MiB |                                               15.55 MiB |                                            -4.73 MiB |
| query() -> all txs involving principals[0]                                                      |                            898.63 KiB |                                                5.67 MiB |                                             2.48 MiB |
| query() -> all txs involving principals[0..10]                                                  |                             -3.27 MiB |                                               -5.81 MiB |                                            -8.52 MiB |
| update(): single operation -> #add amt += 100                                                   |                             20.59 MiB |                                              -17.11 MiB |                                            -7.08 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                             25.53 MiB |                                              -12.23 MiB |                                             -2.8 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                             -9.24 MiB |                                               17.03 MiB |                                            -5.69 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                            -13.23 MiB |                                               25.86 MiB |                                            -2.21 MiB |
| replace() -> replace half the tx with new tx                                                    |                             -1.96 MiB |                                                3.58 MiB |                                           -26.09 MiB |
| delete()                                                                                        |                              25.3 MiB |                                              -10.02 MiB |                                            -12.6 MiB |


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
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                             31.66 MiB |                                               29.57 MiB |                                                  0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                            157.57 MiB |                                               31.66 MiB |                                            29.57 MiB |
| query() -> all txs involving principals[0]                                                      |                             61.57 MiB |                                                     0 B |                                                  0 B |
| query() -> all txs involving principals[0..10]                                                  |                            445.57 MiB |                                              189.57 MiB |                                           125.57 MiB |
| update(): single operation -> #add amt += 100                                                   |                             61.57 MiB |                                              125.57 MiB |                                           157.56 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                             93.56 MiB |                                              157.56 MiB |                                           189.56 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                             93.56 MiB |                                               93.56 MiB |                                           157.56 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                            125.55 MiB |                                              157.54 MiB |                                           381.53 MiB |
| replace() -> replace half the tx with new tx                                                    |                             61.53 MiB |                                              253.52 MiB |                                           349.51 MiB |
| delete()                                                                                        |                                   0 B |                                              125.55 MiB |                                           157.57 MiB |


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
