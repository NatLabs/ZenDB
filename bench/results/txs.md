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

#### Introduced `Orchid` to order candid without deserializing in each index

Instructions

|                                                 | zenDB (using index intersection) | zenDB (using full scan) |
| :---------------------------------------------- | -------------------------------: | ----------------------: |
| insert                                          |                    4_270_673_592 |                   2_607 |
| clear                                           |                           70_353 |                   2_793 |
| insert with 5 indexes pt.1                      |                   11_903_341_164 |                   3_629 |
| btype == '1mint'                                |                       53_425_921 |             228_879_850 |
| btype == '1xfer' or '2xfer'                     |                      114_472_569 |             233_134_950 |
| principals[0] == tx.to.owner (is recipient)     |                        3_655_573 |             226_885_225 |
| principals[0..10] == tx.to.owner (is recipient) |                       36_994_729 |             235_882_893 |
| all txs involving principals[0]                 |                       11_091_473 |             228_368_504 |
| all txs involving principals[0..10]             |                      107_673_361 |             243_027_680 |
| 250 < tx.amt <= 400                             |                      121_804_255 |             226_921_974 |
| btype == 1burn and tx.amt >= 750                |                        2_214_673 |             227_566_420 |

Heap

|                                                 | zenDB (using index intersection) | zenDB (using full scan) |
| :---------------------------------------------- | -------------------------------: | ----------------------: |
| insert                                          |                        1_186_560 |                   8_928 |
| clear                                           |                           16_456 |                   8_928 |
| insert with 5 indexes pt.1                      |                       10_892_644 |                   8_968 |
| btype == '1mint'                                |                        1_532_452 |               6_219_316 |
| btype == '1xfer' or '2xfer'                     |                        3_497_900 |             -25_156_332 |
| principals[0] == tx.to.owner (is recipient)     |                          129_724 |               6_306_712 |
| principals[0..10] == tx.to.owner (is recipient) |                        1_194_256 |               6_328_944 |
| all txs involving principals[0]                 |                          363_268 |               6_385_696 |
| all txs involving principals[0..10]             |                      -25_730_180 |               6_419_504 |
| 250 < tx.amt <= 400                             |                        3_595_032 |               6_207_604 |
| btype == 1burn and tx.amt >= 750                |                           73_052 |               6_192_628 |
