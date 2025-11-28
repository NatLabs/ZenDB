# Benchmark Results


No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/Orchid.bench.json"

<details>

<summary>bench/Orchid.bench.mo $({\color{gray}0\%})$</summary>

### Benchmarking Orchid Encoder/Decoder

_Benchmarking the performance with 1k random values per type_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|             |   encode() |   decode() |
| :---------- | ---------: | ---------: |
| Null        |  6_973_329 |  7_384_895 |
| Empty       |  6_962_650 |  7_380_060 |
| Bool        |  7_307_925 |  7_782_312 |
| Nat8        |  7_283_273 |  7_705_660 |
| Nat16       |  7_970_732 |  7_964_142 |
| Nat32       |  8_733_170 |  8_710_610 |
| Nat64       | 10_258_608 |  9_956_018 |
| Nat         | 10_328_986 |  9_669_350 |
| Int8        |  7_370_717 |  7_734_104 |
| Int16       |  7_908_176 |  7_985_586 |
| Int32       |  8_692_614 |  8_753_374 |
| Int64       | 10_240_052 |  9_992_462 |
| Int         | 10_281_430 |  9_705_794 |
| Float       | 42_337_055 | 41_132_370 |
| Principal   | 22_057_644 | 18_489_146 |
| Text        | 43_786_585 | 27_891_387 |
| Blob        | 36_007_768 | 27_490_700 |
| Option(Nat) |  9_972_346 |  9_700_398 |


**Heap**

|             |   encode() |   decode() |
| :---------- | ---------: | ---------: |
| Null        |   1.03 MiB | -28.48 MiB |
| Empty       |   1.03 MiB | 884.78 KiB |
| Bool        |   1.04 MiB |  900.4 KiB |
| Nat8        |   1.04 MiB |  900.4 KiB |
| Nat16       |   1.08 MiB |  900.4 KiB |
| Nat32       |    1.1 MiB | 911.86 KiB |
| Nat64       |   1.15 MiB | 931.65 KiB |
| Nat         |   1.15 MiB |  900.4 KiB |
| Int8        |   1.05 MiB |  900.4 KiB |
| Int16       |   1.08 MiB |  900.4 KiB |
| Int32       |    1.1 MiB | 912.24 KiB |
| Int64       |   1.15 MiB | 931.65 KiB |
| Int         |   1.15 MiB |  900.4 KiB |
| Float       | -24.64 MiB |   1.83 MiB |
| Principal   |   1.29 MiB |    1.1 MiB |
| Text        |   1.49 MiB |   1.25 MiB |
| Blob        |   1.44 MiB |   1.19 MiB |
| Option(Nat) |   1.13 MiB |    925 KiB |


**Garbage Collection**

|             | encode() |  decode() |
| :---------- | -------: | --------: |
| Null        |      0 B | 29.34 MiB |
| Empty       |      0 B |       0 B |
| Bool        |      0 B |       0 B |
| Nat8        |      0 B |       0 B |
| Nat16       |      0 B |       0 B |
| Nat32       |      0 B |       0 B |
| Nat64       |      0 B |       0 B |
| Nat         |      0 B |       0 B |
| Int8        |      0 B |       0 B |
| Int16       |      0 B |       0 B |
| Int32       |      0 B |       0 B |
| Int64       |      0 B |       0 B |
| Int         |      0 B |       0 B |
| Float       | 27.3 MiB |       0 B |
| Principal   |      0 B |       0 B |
| Text        |      0 B |       0 B |
| Blob        |      0 B |       0 B |
| Option(Nat) |      0 B |       0 B |


</details>
Saving results to .bench/Orchid.bench.json
No previous results found "/home/runner/work/ZenDB/ZenDB/.bench/Tokenizers.bench.json"

<details>

<summary>bench/Tokenizers.bench.mo $({\color{gray}0\%})$</summary>

### Comparing Text Tokenizers

_Benchmarking the performance with 10k entries_


Instructions: ${\color{gray}0\\%}$
Heap: ${\color{gray}0\\%}$
Stable Memory: ${\color{gray}0\\%}$
Garbage Collection: ${\color{gray}0\\%}$


**Instructions**

|                | tokenize() lorem ipsum | tokenize() random text |
| :------------- | ---------------------: | ---------------------: |
| BasicTokenizer |          1_377_341_405 |          2_427_578_013 |


**Heap**

|                | tokenize() lorem ipsum | tokenize() random text |
| :------------- | ---------------------: | ---------------------: |
| BasicTokenizer |               1.45 MiB |               3.56 MiB |


**Garbage Collection**

|                | tokenize() lorem ipsum | tokenize() random text |
| :------------- | ---------------------: | ---------------------: |
| BasicTokenizer |              29.27 MiB |              31.31 MiB |


</details>
Saving results to .bench/Tokenizers.bench.json
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
| insert with no index                                                                            |    229_305_203 |                  229_306_613 |                   229_308_348 |
| create and populate indexes                                                                     |          2_438 |                1_952_239_735 |                 2_347_981_349 |
| clear collection entries and indexes                                                            |          8_960 |                       47_352 |                        48_516 |
| insert with indexes                                                                             |    240_214_420 |                2_163_063_854 |                 2_559_018_155 |
| query(): no filter (all txs)                                                                    |     26_843_132 |                   26_838_582 |                    26_838_310 |
| query(): single field (btype = '1mint')                                                         |    203_237_605 |                    6_163_760 |                     6_632_229 |
| query(): number range (250 < tx.amt <= 400)                                                     |    221_950_982 |                    4_693_064 |                     4_693_828 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |    213_472_970 |                   40_367_729 |                     3_302_872 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |    225_044_119 |                   78_833_279 |                    78_767_565 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |    320_890_969 |                   19_720_650 |                    22_245_915 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |    294_081_568 |                   27_867_153 |                    28_705_930 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |    319_978_932 |                   39_741_986 |                    40_656_108 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |    417_256_182 |                   33_343_729 |                    33_003_916 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |    374_526_537 |                   30_979_888 |                    33_287_620 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |    256_972_968 |                    1_676_436 |                     1_750_820 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |  1_237_623_091 |                   29_817_009 |                    26_652_149 |
| query() -> all txs involving principals[0]                                                      |    549_214_573 |                    8_403_463 |                     8_550_316 |
| query() -> all txs involving principals[0..10]                                                  |  3_406_482_118 |                   84_987_993 |                    83_736_664 |
| update(): single operation -> #add amt += 100                                                   |    755_416_046 |                1_220_930_209 |                 1_923_225_329 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |  1_371_861_295 |                1_838_355_175 |                 2_540_112_353 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |    858_067_373 |                1_327_517_462 |                 2_028_175_055 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |  1_269_293_760 |                2_664_668_660 |                 5_958_071_919 |
| replace() -> replace half the tx with new tx                                                    |    572_721_264 |                4_599_003_985 |                 5_264_302_848 |
| delete()                                                                                        |    220_312_998 |                1_952_575_836 |                 2_304_944_539 |


**Heap**

|                                                                                                 | #heap no index | #heap 7 single field indexes | #heap 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | -------------: | ---------------------------: | ----------------------------: |
| insert with no index                                                                            |      13.56 MiB |                    -15.6 MiB |                     13.56 MiB |
| create and populate indexes                                                                     |       9.79 KiB |                   -10.33 MiB |                     17.47 MiB |
| clear collection entries and indexes                                                            |       10.2 KiB |                    10.31 KiB |                     10.31 KiB |
| insert with indexes                                                                             |     -17.34 MiB |                     4.49 MiB |                     685.2 KiB |
| query(): no filter (all txs)                                                                    |     427.91 KiB |                   427.98 KiB |                    427.98 KiB |
| query(): single field (btype = '1mint')                                                         |      12.42 MiB |                   143.06 KiB |                    169.89 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |     -16.53 MiB |                   127.66 KiB |                    127.66 KiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |      13.38 MiB |                   -25.23 MiB |                    149.74 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |      13.76 MiB |                     4.52 MiB |                      4.51 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |      -8.94 MiB |                   501.65 KiB |                    646.17 KiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |      -9.57 MiB |                    910.1 KiB |                    956.38 KiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |      19.73 MiB |                      1.3 MiB |                     -28.4 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |      24.26 MiB |                     1.05 MiB |                      1.03 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |      -3.28 MiB |                     1.07 MiB |                      1.19 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |     -13.81 MiB |                       79 KiB |                     81.48 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |      -3.28 MiB |                   1006.5 KiB |                    969.13 KiB |
| query() -> all txs involving principals[0]                                                      |       7.64 MiB |                      335 KiB |                    341.15 KiB |
| query() -> all txs involving principals[0..10]                                                  |       -3.3 MiB |                     3.03 MiB |                      3.04 MiB |
| update(): single operation -> #add amt += 100                                                   |     -15.86 MiB |                    12.56 MiB |                     -8.08 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |      -3.36 MiB |                    -6.87 MiB |                       4.4 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |      21.13 MiB |                   -12.07 MiB |                   -840.81 KiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |     -10.33 MiB |                     8.32 MiB |                      9.03 MiB |
| replace() -> replace half the tx with new tx                                                    |     949.55 KiB |                     7.41 MiB |                      -8.6 MiB |
| delete()                                                                                        |     -18.71 MiB |                       19 MiB |                    -21.93 MiB |


**Garbage Collection**

|                                                                                                 | #heap no index | #heap 7 single field indexes | #heap 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | -------------: | ---------------------------: | ----------------------------: |
| insert with no index                                                                            |            0 B |                    29.15 MiB |                           0 B |
| create and populate indexes                                                                     |            0 B |                   124.34 MiB |                    123.86 MiB |
| clear collection entries and indexes                                                            |            0 B |                          0 B |                           0 B |
| insert with indexes                                                                             |      31.59 MiB |                   124.58 MiB |                    155.72 MiB |
| query(): no filter (all txs)                                                                    |            0 B |                          0 B |                           0 B |
| query(): single field (btype = '1mint')                                                         |            0 B |                          0 B |                           0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |      29.74 MiB |                          0 B |                           0 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |            0 B |                    27.64 MiB |                           0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |            0 B |                          0 B |                           0 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |      29.74 MiB |                          0 B |                           0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |      27.64 MiB |                          0 B |                           0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |            0 B |                          0 B |                     29.74 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |            0 B |                          0 B |                           0 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |      27.64 MiB |                          0 B |                           0 B |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |      29.74 MiB |                          0 B |                           0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |      91.64 MiB |                          0 B |                           0 B |
| query() -> all txs involving principals[0]                                                      |      29.74 MiB |                          0 B |                           0 B |
| query() -> all txs involving principals[0..10]                                                  |     251.64 MiB |                          0 B |                           0 B |
| update(): single operation -> #add amt += 100                                                   |      59.68 MiB |                     59.7 MiB |                    123.63 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |      91.61 MiB |                   123.59 MiB |                     155.6 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |      29.83 MiB |                    91.65 MiB |                    123.63 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |       91.6 MiB |                   155.62 MiB |                    347.65 MiB |
| replace() -> replace half the tx with new tx                                                    |       29.9 MiB |                    251.7 MiB |                    315.68 MiB |
| delete()                                                                                        |      30.01 MiB |                     92.6 MiB |                    157.42 MiB |


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
| insert with no index                                                                            |                   229_307_094 |                                     229_310_409 |                                  229_310_242 |
| create and populate indexes                                                                     |                         4_329 |                                   1_952_243_587 |                                2_347_983_187 |
| clear collection entries and indexes                                                            |                        10_851 |                                          51_148 |                                       50_410 |
| insert with indexes                                                                             |                   240_216_311 |                                   2_163_067_650 |                                2_559_020_049 |
| query(): no filter (all txs)                                                                    |                 3_015_552_896 |                                      26_986_730 |                                   26_989_551 |
| query(): single field (btype = '1mint')                                                         |                   614_732_756 |                                     224_450_173 |                                    6_884_130 |
| query(): number range (250 < tx.amt <= 400)                                                     |                   500_300_618 |                                       4_712_588 |                                  244_293_609 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                   284_332_993 |                                      56_682_410 |                                   45_747_681 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                   660_380_043 |                                      85_758_642 |                                  130_712_751 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                 2_100_512_525 |                                     680_085_601 |                                   42_444_118 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                 2_091_044_300 |                                     250_782_977 |                                  267_938_306 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                 2_815_994_552 |                                     455_533_451 |                                  288_573_786 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                 3_336_100_183 |                                     227_595_401 |                                  276_068_021 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                 2_175_478_351 |                                     471_505_296 |                                  518_858_410 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                   262_599_119 |                                       9_190_773 |                                    2_002_202 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                 1_511_683_462 |                                     446_447_254 |                                   37_284_883 |
| query() -> all txs involving principals[0]                                                      |                   589_882_833 |                                      63_646_899 |                                   10_298_028 |
| query() -> all txs involving principals[0..10]                                                  |                 4_268_360_263 |                                   1_627_374_968 |                                  108_491_862 |
| update(): single operation -> #add amt += 100                                                   |                   755_417_758 |                                   1_220_934_005 |                                1_923_226_998 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                 1_371_863_190 |                                   1_838_358_967 |                                2_540_114_247 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                   858_069_447 |                                   1_327_521_050 |                                2_028_176_953 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                 1_269_295_651 |                                   2_664_672_456 |                                5_957_987_851 |
| replace() -> replace half the tx with new tx                                                    |                   572_723_155 |                                   4_599_007_781 |                                5_264_304_742 |
| delete()                                                                                        |                   220_314_710 |                                   1_952_577_827 |                                2_304_944_224 |


**Heap**

|                                                                                                 | #heap no index (sorted by ts) | #heap 7 single field indexes (sorted by tx.amt) | #heap 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ----------------------------: | ----------------------------------------------: | -------------------------------------------: |
| insert with no index                                                                            |                     13.56 MiB |                                       -15.6 MiB |                                    13.56 MiB |
| create and populate indexes                                                                     |                      9.79 KiB |                                      -10.33 MiB |                                    17.47 MiB |
| clear collection entries and indexes                                                            |                      10.2 KiB |                                       10.31 KiB |                                    10.31 KiB |
| insert with indexes                                                                             |                    -17.34 MiB |                                        4.49 MiB |                                    685.1 KiB |
| query(): no filter (all txs)                                                                    |                     -7.77 MiB |                                      457.57 KiB |                                   457.91 KiB |
| query(): single field (btype = '1mint')                                                         |                      7.96 MiB |                                      -13.89 MiB |                                   189.11 KiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                    247.71 KiB |                                      128.15 KiB |                                    14.58 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                     -9.91 MiB |                                        3.48 MiB |                                     2.71 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                     10.26 MiB |                                      -22.69 MiB |                                     7.65 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                      4.51 MiB |                                       11.93 MiB |                                     1.81 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                    -27.25 MiB |                                        14.9 MiB |                                   -14.28 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                      14.6 MiB |                                       -2.68 MiB |                                   -11.13 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                     12.62 MiB |                                       12.96 MiB |                                   -13.88 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                      9.58 MiB |                                       -1.08 MiB |                                     2.94 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                    -13.47 MiB |                                       540.2 KiB |                                   100.72 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                     13.28 MiB |                                        -3.7 MiB |                                      1.6 MiB |
| query() -> all txs involving principals[0]                                                      |                    -19.81 MiB |                                        3.66 MiB |                                   475.06 KiB |
| query() -> all txs involving principals[0..10]                                                  |                     16.58 MiB |                                        4.04 MiB |                                   -24.98 MiB |
| update(): single operation -> #add amt += 100                                                   |                     16.06 MiB |                                      -19.23 MiB |                                    23.88 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                      -3.4 MiB |                                        -6.9 MiB |                                     4.37 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                     -8.68 MiB |                                       19.87 MiB |                                  -887.29 KiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                    -10.38 MiB |                                        8.27 MiB |                                   -22.94 MiB |
| replace() -> replace half the tx with new tx                                                    |                      1.08 MiB |                                        7.46 MiB |                                    -8.56 MiB |
| delete()                                                                                        |                      11.3 MiB |                                         -13 MiB |                                    10.07 MiB |


**Garbage Collection**

|                                                                                                 | #heap no index (sorted by ts) | #heap 7 single field indexes (sorted by tx.amt) | #heap 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ----------------------------: | ----------------------------------------------: | -------------------------------------------: |
| insert with no index                                                                            |                           0 B |                                       29.15 MiB |                                          0 B |
| create and populate indexes                                                                     |                           0 B |                                      124.34 MiB |                                   123.86 MiB |
| clear collection entries and indexes                                                            |                           0 B |                                             0 B |                                          0 B |
| insert with indexes                                                                             |                     31.59 MiB |                                      124.58 MiB |                                   155.72 MiB |
| query(): no filter (all txs)                                                                    |                    187.64 MiB |                                             0 B |                                          0 B |
| query(): single field (btype = '1mint')                                                         |                     29.74 MiB |                                       27.64 MiB |                                          0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                     29.74 MiB |                                             0 B |                                          0 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                     27.64 MiB |                                             0 B |                                          0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                     29.74 MiB |                                       27.64 MiB |                                          0 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                    123.64 MiB |                                       29.74 MiB |                                          0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                    153.58 MiB |                                             0 B |                                    29.74 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                    155.64 MiB |                                       29.74 MiB |                                    27.64 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                    187.64 MiB |                                             0 B |                                    29.74 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                    123.64 MiB |                                       29.74 MiB |                                    27.64 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                     29.74 MiB |                                             0 B |                                          0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                     91.64 MiB |                                       29.74 MiB |                                          0 B |
| query() -> all txs involving principals[0]                                                      |                     59.64 MiB |                                             0 B |                                          0 B |
| query() -> all txs involving principals[0..10]                                                  |                    283.64 MiB |                                       91.64 MiB |                                    29.74 MiB |
| update(): single operation -> #add amt += 100                                                   |                     27.75 MiB |                                       91.49 MiB |                                    91.67 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                     91.65 MiB |                                      123.63 MiB |                                   155.63 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                     59.64 MiB |                                       59.71 MiB |                                   123.68 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                     91.65 MiB |                                      155.66 MiB |                                   379.61 MiB |
| replace() -> replace half the tx with new tx                                                    |                     29.75 MiB |                                      251.66 MiB |                                   315.64 MiB |
| delete()                                                                                        |                           0 B |                                       124.6 MiB |                                   125.42 MiB |


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
| insert with no index                                                                            |            317_973_230 |                          317_974_075 |                           317_974_464 |
| create and populate indexes                                                                     |                  2_793 |                        2_044_043_403 |                         2_477_367_631 |
| clear collection entries and indexes                                                            |                 79_647 |                              592_734 |                               604_797 |
| insert with indexes                                                                             |            330_442_005 |                        2_286_332_989 |                         2_722_010_142 |
| query(): no filter (all txs)                                                                    |            104_572_349 |                          104_572_424 |                           104_574_577 |
| query(): single field (btype = '1mint')                                                         |            296_932_140 |                           20_946_034 |                            21_303_728 |
| query(): number range (250 < tx.amt <= 400)                                                     |            312_027_691 |                           15_811_084 |                            15_812_153 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |            296_218_161 |                           56_603_518 |                             6_578_934 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |            320_003_893 |                          126_564_504 |                           126_499_572 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |            444_970_059 |                           66_395_360 |                            68_583_784 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |            418_933_800 |                           75_739_901 |                            76_407_929 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |            459_755_059 |                          103_278_816 |                           104_021_079 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |            629_460_359 |                           89_702_354 |                            89_367_834 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |            499_033_694 |                           77_821_559 |                            79_547_529 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |            336_735_779 |                            2_045_873 |                             2_105_848 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |          1_324_943_017 |                           30_608_798 |                            31_385_812 |
| query() -> all txs involving principals[0]                                                      |            629_121_704 |                            9_022_304 |                             9_272_704 |
| query() -> all txs involving principals[0..10]                                                  |          3_509_088_945 |                           93_104_148 |                            96_161_262 |
| update(): single operation -> #add amt += 100                                                   |            985_618_454 |                        1_489_684_522 |                         2_212_021_372 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |          1_601_560_524 |                        2_105_740_733 |                         2_837_764_195 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |          1_087_071_425 |                        1_590_976_375 |                         2_322_378_308 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |          1_514_296_403 |                        2_992_967_949 |                         6_377_991_920 |
| replace() -> replace half the tx with new tx                                                    |            834_571_569 |                        4_993_002_793 |                         5_727_130_665 |
| delete()                                                                                        |            319_632_417 |                        2_106_097_408 |                         2_477_903_421 |


**Heap**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |              18.57 MiB |                           -10.98 MiB |                            -13.08 MiB |
| create and populate indexes                                                                     |               9.79 KiB |                            -1.39 MiB |                             -1.68 MiB |
| clear collection entries and indexes                                                            |              16.61 KiB |                            61.07 KiB |                             61.56 KiB |
| insert with indexes                                                                             |              19.31 MiB |                           -18.01 MiB |                              13.8 MiB |
| query(): no filter (all txs)                                                                    |               5.73 MiB |                             5.73 MiB |                            -25.76 MiB |
| query(): single field (btype = '1mint')                                                         |              18.95 MiB |                             1.13 MiB |                              1.15 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |              -9.89 MiB |                            902.7 KiB |                             902.7 KiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |             -12.34 MiB |                              3.5 MiB |                            380.04 KiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |               20.4 MiB |                           -21.61 MiB |                              7.78 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |              -2.19 MiB |                             3.67 MiB |                              3.79 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |              -2.77 MiB |                             4.14 MiB |                              4.17 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |              -2.18 MiB |                             5.59 MiB |                            -23.76 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |               7.43 MiB |                             4.87 MiB |                              4.85 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                3.5 MiB |                             4.25 MiB |                            -27.16 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |              21.51 MiB |                           119.12 KiB |                            121.98 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |             967.01 KiB |                              1.5 MiB |                              1.54 MiB |
| query() -> all txs involving principals[0]                                                      |             -18.54 MiB |                            459.4 KiB |                            471.34 KiB |
| query() -> all txs involving principals[0..10]                                                  |               1.97 MiB |                             4.47 MiB |                              4.59 MiB |
| update(): single operation -> #add amt += 100                                                   |              -2.16 MiB |                            -2.25 MiB |                             12.94 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |             -21.72 MiB |                            10.13 MiB |                             -7.28 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |               4.98 MiB |                             4.87 MiB |                            -12.69 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |               3.52 MiB |                           -31.27 KiB |                              18.5 MiB |
| replace() -> replace half the tx with new tx                                                    |              -14.6 MiB |                             5.49 MiB |                             -2.83 MiB |
| delete()                                                                                        |             -12.29 MiB |                             1.52 MiB |                             -4.69 MiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index | #stableMemory 7 single field indexes | #stableMemory 6 fully covered indexes |
| :---------------------------------------------------------------------------------------------- | ---------------------: | -----------------------------------: | ------------------------------------: |
| insert with no index                                                                            |                    0 B |                            29.55 MiB |                             31.65 MiB |
| create and populate indexes                                                                     |                    0 B |                           125.53 MiB |                            157.52 MiB |
| clear collection entries and indexes                                                            |                    0 B |                                  0 B |                                   0 B |
| insert with indexes                                                                             |                    0 B |                           157.44 MiB |                             157.4 MiB |
| query(): no filter (all txs)                                                                    |                    0 B |                                  0 B |                              31.5 MiB |
| query(): single field (btype = '1mint')                                                         |                    0 B |                                  0 B |                                   0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |              29.39 MiB |                                  0 B |                                   0 B |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |               31.5 MiB |                                  0 B |                                   0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                    0 B |                            29.39 MiB |                                   0 B |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |               31.5 MiB |                                  0 B |                                   0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |              29.39 MiB |                                  0 B |                                   0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |               31.5 MiB |                                  0 B |                             29.39 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |               31.5 MiB |                                  0 B |                                   0 B |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |              29.39 MiB |                                  0 B |                              31.5 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                    0 B |                                  0 B |                                   0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |              93.39 MiB |                                  0 B |                                   0 B |
| query() -> all txs involving principals[0]                                                      |              61.39 MiB |                                  0 B |                                   0 B |
| query() -> all txs involving principals[0..10]                                                  |             253.39 MiB |                                  0 B |                                   0 B |
| update(): single operation -> #add amt += 100                                                   |              61.39 MiB |                            93.39 MiB |                            125.39 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |             125.38 MiB |                           125.38 MiB |                            189.38 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |              61.38 MiB |                            93.38 MiB |                            157.38 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |              93.37 MiB |                           189.36 MiB |                            381.35 MiB |
| replace() -> replace half the tx with new tx                                                    |              61.35 MiB |                           285.34 MiB |                            349.33 MiB |
| delete()                                                                                        |              31.46 MiB |                           125.37 MiB |                            157.39 MiB |


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
| insert with no index                                                                            |                           317_976_531 |                                             317_979_362 |                                          317_978_704 |
| create and populate indexes                                                                     |                                 6_094 |                                           2_044_048_746 |                                        2_477_371_815 |
| clear collection entries and indexes                                                            |                                82_948 |                                                 598_021 |                                              609_037 |
| insert with indexes                                                                             |                           330_445_306 |                                           2_286_338_276 |                                        2_722_014_382 |
| query(): no filter (all txs)                                                                    |                         4_508_461_423 |                                             105_196_808 |                                          105_191_034 |
| query(): single field (btype = '1mint')                                                         |                           894_920_403 |                                             317_933_020 |                                           21_533_128 |
| query(): number range (250 < tx.amt <= 400)                                                     |                           720_693_630 |                                              15_832_099 |                                          334_186_521 |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                           399_429_931 |                                              79_552_023 |                                           61_924_019 |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                           959_135_633 |                                             133_797_368 |                                          187_311_740 |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                         3_057_552_327 |                                             962_848_974 |                                           89_087_696 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                         3_061_563_127 |                                             377_745_618 |                                          394_010_175 |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                         4_126_921_432 |                                             676_443_024 |                                          430_702_651 |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                         4_912_741_988 |                                             362_567_243 |                                          411_240_828 |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                         3_140_843_845 |                                             676_066_538 |                                          722_665_797 |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                           345_103_150 |                                              12_146_949 |                                            2_332_745 |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                         1_726_902_613 |                                             594_241_858 |                                           41_581_080 |
| query() -> all txs involving principals[0]                                                      |                           689_386_660 |                                              84_189_814 |                                           10_882_641 |
| query() -> all txs involving principals[0..10]                                                  |                         4_776_148_184 |                                           2_191_765_472 |                                          119_573_824 |
| update(): single operation -> #add amt += 100                                                   |                           985_621_755 |                                           1_489_689_809 |                                        2_212_025_837 |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                         1_601_563_621 |                                           2_105_746_024 |                                        2_837_768_435 |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                         1_087_074_726 |                                           1_590_981_662 |                                        2_322_382_548 |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                         1_514_299_704 |                                           2_992_973_236 |                                        6_377_534_741 |
| replace() -> replace half the tx with new tx                                                    |                           834_574_687 |                                           4_993_008_084 |                                        5_727_134_905 |
| delete()                                                                                        |                           319_635_718 |                                           2_106_102_695 |                                        2_477_908_784 |


**Heap**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by tx.amt) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | ------------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                             18.57 MiB |                                              -10.98 MiB |                                           -13.08 MiB |
| create and populate indexes                                                                     |                              9.79 KiB |                                               -1.39 MiB |                                            -1.68 MiB |
| clear collection entries and indexes                                                            |                             16.61 KiB |                                               61.07 KiB |                                            61.56 KiB |
| insert with indexes                                                                             |                             19.31 MiB |                                              -18.01 MiB |                                             13.8 MiB |
| query(): no filter (all txs)                                                                    |                             -2.77 MiB |                                                5.76 MiB |                                             5.76 MiB |
| query(): single field (btype = '1mint')                                                         |                              -4.5 MiB |                                              -11.41 MiB |                                             1.17 MiB |
| query(): number range (250 < tx.amt <= 400)                                                     |                             15.87 MiB |                                              903.27 KiB |                                           -10.81 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                             -3.69 MiB |                                                5.03 MiB |                                              3.8 MiB |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                           -719.15 KiB |                                                8.24 MiB |                                           -19.99 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                              4.64 MiB |                                             -522.06 KiB |                                             4.98 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                              3.67 MiB |                                               -7.98 MiB |                                            -5.35 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                                 7 MiB |                                               10.56 MiB |                                            -3.24 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                             -8.43 MiB |                                               -9.36 MiB |                                            -4.35 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                             10.17 MiB |                                               11.04 MiB |                                           -16.97 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                             22.04 MiB |                                              759.37 KiB |                                           137.52 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                              -5.7 MiB |                                                5.15 MiB |                                           -27.26 MiB |
| query() -> all txs involving principals[0]                                                      |                             15.13 MiB |                                                5.13 MiB |                                           584.74 KiB |
| query() -> all txs involving principals[0..10]                                                  |                            -14.23 MiB |                                                9.95 MiB |                                             6.12 MiB |
| update(): single operation -> #add amt += 100                                                   |                             -2.16 MiB |                                               -2.25 MiB |                                           -19.06 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                             10.28 MiB |                                               10.13 MiB |                                            -7.28 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                              4.98 MiB |                                                4.87 MiB |                                           -12.69 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                              3.52 MiB |                                              -31.27 KiB |                                            -13.5 MiB |
| replace() -> replace half the tx with new tx                                                    |                              15.3 MiB |                                                5.49 MiB |                                            -2.83 MiB |
| delete()                                                                                        |                            -12.29 MiB |                                                1.52 MiB |                                            -4.69 MiB |


**Garbage Collection**

|                                                                                                 | #stableMemory no index (sorted by ts) | #stableMemory 7 single field indexes (sorted by tx.amt) | #stableMemory 6 fully covered indexes (sorted by ts) |
| :---------------------------------------------------------------------------------------------- | ------------------------------------: | ------------------------------------------------------: | ---------------------------------------------------: |
| insert with no index                                                                            |                                   0 B |                                               29.55 MiB |                                            31.65 MiB |
| create and populate indexes                                                                     |                                   0 B |                                              125.53 MiB |                                           157.52 MiB |
| clear collection entries and indexes                                                            |                                   0 B |                                                     0 B |                                                  0 B |
| insert with indexes                                                                             |                                   0 B |                                              157.44 MiB |                                            157.4 MiB |
| query(): no filter (all txs)                                                                    |                            285.39 MiB |                                                     0 B |                                                  0 B |
| query(): single field (btype = '1mint')                                                         |                             61.39 MiB |                                                31.5 MiB |                                                  0 B |
| query(): number range (250 < tx.amt <= 400)                                                     |                             29.39 MiB |                                                     0 B |                                             31.5 MiB |
| query(): #And (btype='1burn' AND tx.amt>=750)                                                   |                             29.39 MiB |                                                     0 B |                                                  0 B |
| query(): #And (500_000<ts<=1_000_000 AND 200<amt<=600)                                          |                             61.39 MiB |                                                     0 B |                                             31.5 MiB |
| query(): #Or (btype == '1xfer' OR '2xfer' OR '1mint')                                           |                            189.39 MiB |                                               61.39 MiB |                                                  0 B |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500)                                                |                            189.39 MiB |                                                31.5 MiB |                                            29.39 MiB |
| query(): #Or (btype == '1xfer' OR tx.amt >= 500 OR ts > 500_000)                                |                            253.39 MiB |                                                31.5 MiB |                                            29.39 MiB |
| query(): #Or (500_000<ts<=1_000_000 OR 200<amt<=600)                                            |                            317.39 MiB |                                                31.5 MiB |                                            29.39 MiB |
| query(): #Or (btype in ['1xfer', '1burn'] OR (tx.amt < 200 OR tx.amt >= 800))                   |                            189.39 MiB |                                                31.5 MiB |                                            61.39 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)                                          |                                   0 B |                                                     0 B |                                                  0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient)                                      |                            125.39 MiB |                                                31.5 MiB |                                            29.39 MiB |
| query() -> all txs involving principals[0]                                                      |                              31.5 MiB |                                                     0 B |                                                  0 B |
| query() -> all txs involving principals[0..10]                                                  |                            349.39 MiB |                                              125.39 MiB |                                                  0 B |
| update(): single operation -> #add amt += 100                                                   |                             61.39 MiB |                                               93.39 MiB |                                           157.39 MiB |
| update(): multiple independent operations -> #add, #sub, #mul, #div on tx.amt                   |                             93.38 MiB |                                              125.38 MiB |                                           189.38 MiB |
| update(): multiple nested operations -> #add, #sub, #mul, #div on tx.amt                        |                             61.38 MiB |                                               93.38 MiB |                                           157.38 MiB |
| update(): multiple operations on multiple fields -> #add, #sub, #mul, #div on (tx.amt, ts, fee) |                             93.38 MiB |                                              189.36 MiB |                                           413.35 MiB |
| replace() -> replace half the tx with new tx                                                    |                             31.46 MiB |                                              285.34 MiB |                                           349.33 MiB |
| delete()                                                                                        |                             31.46 MiB |                                              125.37 MiB |                                           157.39 MiB |


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
