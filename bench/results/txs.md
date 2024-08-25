## Benchmarks

Benchmarking zenDB with icrc3 txs

Benchmarking the performance with 10k txs

#### Initial Benchmark

Instructions

|                                                 | zenDB (using index intersection) | zenDB (using full scan) |
| :---------------------------------------------- | -------------------------------: | ----------------------: |
| insert                                          |                    4_304_940_564 |                   2_595 |
| clear                                           |                           70_365 |                   2_781 |
| insert with 5 indexes pt.1                      |                   17_931_066_845 |                   3_617 |
| insert with 5 indexes pt.2                      |                   21_438_856_823 |                   4_408 |
| insert with 5 indexes pt.3                      |                   22_854_307_210 |                   5_199 |
| insert with 5 indexes pt.4                      |                   24_092_527_208 |                   5_990 |
| btype == '1mint'                                |                      162_561_871 |             228_832_574 |
| btype == '1xfer' or '2xfer'                     |                      342_575_817 |             233_090_367 |
| principals[0] == tx.to.owner (is recipient)     |                       51_393_981 |             226_830_139 |
| principals[0..10] == tx.to.owner (is recipient) |                      618_623_289 |             235_830_447 |
| all txs involving principals[0]                 |                      214_103_139 |             228_319_250 |
| all txs involving principals[0..10]             |                    1_849_189_177 |             242_973_890 |
| 250 < tx.amt <= 400                             |                      125_344_996 |             226_874_716 |
| btype == 1burn and tx.amt >= 750                |                       71_001_693 |             227_518_098 |

Heap

|                                                 | zenDB (using index intersection) | zenDB (using full scan) |
| :---------------------------------------------- | -------------------------------: | ----------------------: |
| insert                                          |                        5_617_228 |                   8_884 |
| clear                                           |                           16_412 |                   8_884 |
| insert with 5 indexes pt.1                      |                        7_399_460 |                   8_964 |
| insert with 5 indexes pt.2                      |                        7_932_508 |                   8_964 |
| insert with 5 indexes pt.3                      |                      -26_982_312 |                   8_964 |
| insert with 5 indexes pt.4                      |                       29_364_544 |                   8_964 |
| btype == '1mint'                                |                      -27_978_244 |               6_179_472 |
| btype == '1xfer' or '2xfer'                     |                        7_336_780 |               6_223_512 |
| principals[0] == tx.to.owner (is recipient)     |                          907_620 |             -22_964_672 |
| principals[0..10] == tx.to.owner (is recipient) |                        9_135_324 |               6_288_780 |
| all txs involving principals[0]                 |                        3_272_924 |               6_345_772 |
| all txs involving principals[0..10]             |                       -3_661_132 |               6_379_340 |
| 250 < tx.amt <= 400                             |                      -26_513_832 |               6_167_760 |
| btype == 1burn and tx.amt >= 750                |                        1_317_780 |               6_152_704 |

#### Pointer Mapping to contiguous range

Instructions

|                                                 | zenDB (using index intersection) | zenDB (using full scan) |
| :---------------------------------------------- | -------------------------------: | ----------------------: |
| insert                                          |                      422_536_900 |                   2_607 |
| clear                                           |                           70_377 |                   2_793 |
| insert with 5 indexes pt.1                      |                    6_170_699_688 |                   3_629 |
| btype == '1mint'                                |                       13_285_458 |              22_882_509 |
| btype == '1xfer' or '2xfer'                     |                       29_252_049 |              23_309_323 |
| principals[0] == tx.to.owner (is recipient)     |                        2_831_166 |              22_688_047 |
| principals[0..10] == tx.to.owner (is recipient) |                       32_781_326 |              23_574_600 |
| all txs involving principals[0]                 |                       10_655_333 |              22_843_123 |
| all txs involving principals[0..10]             |                      102_847_644 |              24_292_787 |
| 250 < tx.amt <= 400                             |                        9_790_640 |              22_719_930 |
| btype == 1burn and tx.amt >= 750                |                        4_880_917 |              22_771_904 |

Heap

|                                                 | zenDB (using index intersection) | zenDB (using full scan) |
| :---------------------------------------------- | -------------------------------: | ----------------------: |
| insert                                          |                       20_176_904 |                   8_884 |
| clear                                           |                           16_412 |                   8_884 |
| insert with 5 indexes pt.1                      |                      -16_143_328 |                   8_884 |
| btype == '1mint'                                |                          386_032 |                 629_988 |
| btype == '1xfer' or '2xfer'                     |                          872_392 |                 634_108 |
| principals[0] == tx.to.owner (is recipient)     |                          166_584 |                 638_832 |
| principals[0..10] == tx.to.owner (is recipient) |                        1_778_560 |                 641_148 |
| all txs involving principals[0]                 |                          606_420 |                 646_932 |
| all txs involving principals[0..10]             |                        5_843_020 |                 650_288 |
| 250 < tx.amt <= 400                             |                          302_144 |                 629_800 |
| btype == 1burn and tx.amt >= 750                |                          195_952 |                 627_680 |
