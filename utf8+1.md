## UTF-8 + 1
We use this format when serializing index keys for the Principal and Blob type because they can store arbitrary bytes. The +1 is added to push the bytes up by one, ensuring that the byte 0x00 (which is used as a terminator in our systems) does not conflict with Blob or Principal data.
We don't need to do this for our number types because they are all bounded types and the index only compares the same data type against itself.

### Why UTF-8 Actually Works
UTF-8 was specifically designed so that:

Single-byte characters (0-127) encode as 0x00-0x7F
Multi-byte characters (128+) start with bytes 0xC2 and higher
This ensures 0x7F < 0xC2, preserving order

> This is needed to maintain the performance of our indexes, as we found that accessing bytes within the index keys and comparing slices of them is suboptimal compared to comparing the whole key using the default Blob comparator.