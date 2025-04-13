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

#### Last Commit - 9fd3315421d72d981ce80761813f7f11325ea85a

Instructions

|                                                            | (full scan, -> array) | (index intersection, -> array) |
| :--------------------------------------------------------- | --------------------: | -----------------------------: |
| insert                                                     |         8_555_173_872 |                          1_915 |
| clear                                                      |                94_750 |                          2_051 |
| insert with 5 indexes                                      |        38_992_079_117 |                          2_783 |
| query() -> btype == '1mint'                                |           226_089_897 |                    108_027_498 |
| query() -> btype == '1xfer' or '2xfer'                     |           229_989_430 |                    221_614_606 |
| query() -> principals[0] == tx.to.owner (is recipient)     |           225_029_696 |                      8_296_470 |
| query() -> principals[0..10] == tx.to.owner (is recipient) |           234_028_794 |                     81_920_335 |
| query() -> all txs involving principals[0]                 |           226_829_606 |                     24_698_866 |
| query() -> all txs involving principals[0..10]             |           241_490_452 |                    231_781_309 |
| query() -> 250 < tx.amt <= 400                             |           224_587_575 |                     81_757_035 |
| query() -> btype == 1burn and tx.amt >= 750                |           224_778_451 |                     27_668_956 |
| update() -> #add amt += 100                                |                 4_251 |                 31_510_535_687 |
| update() -> #sub amt -= 100                                |                 4_756 |                 31_460_604_207 |
| update() -> #div amt /= 2                                  |                 5_204 |                 31_481_887_293 |
| update() -> #mul amt *= 2                                  |                 5_709 |                 31_484_706_184 |
| update() -> #set amt = 100                                 |                 6_266 |                 31_619_299_489 |
| replaceRecord() -> replace half the tx with new tx         |                 5_659 |                 40_459_568_397 |


Heap

|                                                            | (full scan, -> array) | (index intersection, -> array) |
| :--------------------------------------------------------- | --------------------: | -----------------------------: |
| insert                                                     |              7.23 MiB |                       9.76 KiB |
| clear                                                      |             17.11 KiB |                       9.76 KiB |
| insert with 5 indexes                                      |             -2.49 MiB |                       9.88 KiB |
| query() -> btype == '1mint'                                |              5.93 MiB |                       2.06 MiB |
| query() -> btype == '1xfer' or '2xfer'                     |              5.97 MiB |                     -25.42 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)     |              6.02 MiB |                      216.8 KiB |
| query() -> principals[0..10] == tx.to.owner (is recipient) |              6.04 MiB |                       1.95 MiB |
| query() -> all txs involving principals[0]                 |              6.09 MiB |                     616.21 KiB |
| query() -> all txs involving principals[0..10]             |            -21.71 MiB |                       5.72 MiB |
| query() -> 250 < tx.amt <= 400                             |              5.92 MiB |                       1.63 MiB |
| query() -> btype == 1burn and tx.amt >= 750                |              5.91 MiB |                     588.03 KiB |
| update() -> #add amt += 100                                |              9.88 KiB |                      -2.54 MiB |
| update() -> #sub amt -= 100                                |              9.88 KiB |                      -2.91 MiB |
| update() -> #div amt /= 2                                  |              9.88 KiB |                    -244.25 KiB |
| update() -> #mul amt *= 2                                  |              9.88 KiB |                      -2.26 MiB |
| update() -> #set amt = 100                                 |              9.88 KiB |                      -6.08 MiB |
| replaceRecord() -> replace half the tx with new tx         |             17.89 KiB |                       4.02 MiB |


Garbage Collection

|                                                            | (full scan, -> array) | (index intersection, -> array) |
| :--------------------------------------------------------- | --------------------: | -----------------------------: |
| insert                                                     |            315.85 MiB |                            0 B |
| clear                                                      |                   0 B |                            0 B |
| insert with 5 indexes                                      |              1.84 GiB |                            0 B |
| query() -> btype == '1mint'                                |                   0 B |                            0 B |
| query() -> btype == '1xfer' or '2xfer'                     |                   0 B |                      29.92 MiB |
| query() -> principals[0] == tx.to.owner (is recipient)     |                   0 B |                            0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient) |                   0 B |                            0 B |
| query() -> all txs involving principals[0]                 |                   0 B |                            0 B |
| query() -> all txs involving principals[0..10]             |             27.83 MiB |                            0 B |
| query() -> 250 < tx.amt <= 400                             |                   0 B |                            0 B |
| query() -> btype == 1burn and tx.amt >= 750                |                   0 B |                            0 B |
| update() -> #add amt += 100                                |                   0 B |                       1.34 GiB |
| update() -> #sub amt -= 100                                |                   0 B |                       1.34 GiB |
| update() -> #div amt /= 2                                  |                   0 B |                       1.34 GiB |
| update() -> #mul amt *= 2                                  |                   0 B |                       1.34 GiB |
| update() -> #set amt = 100                                 |                   0 B |                       1.34 GiB |
| replaceRecord() -> replace half the tx with new tx         |                   0 B |                       1.87 GiB |


Stable Memory

|                                                            | (full scan, -> array) | (index intersection, -> array) |
| :--------------------------------------------------------- | --------------------: | -----------------------------: |
| insert                                                     |                   0 B |                            0 B |
| clear                                                      |                   0 B |                            0 B |
| insert with 5 indexes                                      |               224 MiB |                            0 B |
| query() -> btype == '1mint'                                |                   0 B |                            0 B |
| query() -> btype == '1xfer' or '2xfer'                     |                   0 B |                            0 B |
| query() -> principals[0] == tx.to.owner (is recipient)     |                   0 B |                            0 B |
| query() -> principals[0..10] == tx.to.owner (is recipient) |                   0 B |                            0 B |
| query() -> all txs involving principals[0]                 |                   0 B |                            0 B |
| query() -> all txs involving principals[0..10]             |                   0 B |                            0 B |
| query() -> 250 < tx.amt <= 400                             |                   0 B |                            0 B |
| query() -> btype == 1burn and tx.amt >= 750                |                   0 B |                            0 B |
| update() -> #add amt += 100                                |                   0 B |                            0 B |
| update() -> #sub amt -= 100                                |                   0 B |                            0 B |
| update() -> #div amt /= 2                                  |                   0 B |                            0 B |
| update() -> #mul amt *= 2                                  |                   0 B |                            0 B |
| update() -> #set amt = 100                                 |                   0 B |                            0 B |
| replaceRecord() -> replace half the tx with new tx         |                   0 B |                            0 B |
