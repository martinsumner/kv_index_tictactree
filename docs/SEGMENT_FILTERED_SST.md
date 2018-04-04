# Segment filtering in LSM trees

In data stores designed around Log Structured Merge Trees, data it stored in a tree of sorted files (SST files).  To find a key in the tree, the SST file whose key range
covers the key is checked starting at the top level, and working down until the first instance of the key is found.

It is important to read performance that SST files that don't contain the key can provide a negative response in an efficient manner, so that the level can be skipped through without delay.  In order to achieve this, some form of [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) is generally used.

Within the [leveled LSM tree](https://github.com/martinsumner/leveled/tree/master/src), there has been attempt to align these filters with the hashing to a position in a (Tictac) Merkle tree - to allow for the same index to be used to both accelerate fetch misses, but also to skip blocks within an SST when scanning an LSM tree in key order to find a subset of keys associated with particular leaves within a Merkle Tree that represents the data in the store.

There are two methods which have been investigated for implement this capability:

- A simple slot-based segment-index (the actual method currently implemented in Leveled);
- A potentially more efficient rice-encoded filter.

The idea for both is that a single hash function is used (rather than multiple hash functions as in a bloom filter), and that hash function produces the position in the Merkle tree.  The same filter can then be used to check for presence, of an individual keys, or for multiple keys located in a subset of segment IDs.  

## Slot-based Segment Index

Within Leveled, two bloom filters are used.  Each file has its own multi-hash bloom filter, which is designed to be small with a relatively high false positive rate.  This is used as an initial filter to prevent lookups to a file.

Once this has been passed, the SST file is divided into compressed blocks of (24 to 28 keys and values), each set of five blocks is held within a slot, and the SST file maintains a mapping of key ranges to slots.  A slot can contain up to 128 Keys/Values, some of which may be non-lookup keys (e.g. index entries) which don't require creation of a bloom entry as they will never be directly access outside of folds.

The second level of bloom filter, the simple segment index, is kept in-memory for each slot.  The segment index is built from either 2-byte positions, or 1-byte gaps.  

2-byte positions are of the form `<<1:1/integer, SegID:15/integer>>`, where SegID is 15 bits of the Merkle tree segment ID for the key.

1-byte gaps are of the form `<<0:1/integer, Gap:7/integer>>`, where Gap is the count of entries in the slot between the last indexed entry and this which have no index entry.

This is less efficient than a bloom filter, as for the size the false positive rate is 1:256 as opposed to 1:2180 for an optimised bloom of equivalent size.  However, it yields additional information, notably the block or blocks which contain the Key, and the position of the Key within the block.

## Rice-encoded Segment Filter

A Rice-encoded filter is a way of providing improved memory efficiency compared to the Slot-based Segment Index.  Rice-encoding filter is a bloom filter based on a single hash function (as above), but now the filter is packed using [rice encoding](https://en.wikipedia.org/wiki/Golomb_coding), which encodes the bloom an array of deltas based on the assumption of roughly equal spacing between deltas - which should be an expected outcome of using a 'good' hash function.

So if there are 15-bits to the hash in the bloom, and 128 keys in the bloom, then it can be assumed that the deltas are around 256 numbers apart, and so an 8-bit remainder is used, and:

- A delta of 255 would be represented as 0 1111 1111;
- A delta of 257 would be represented as 10 0000 0001;
- A delta of 1000 would be represented as 1110 1101 1000.

So the approximate overall size of the filter will be around 10 bits per key.  This would no longer yield the block and position, so to extend this, the full position can be added by bit-shifting the SegmentID and adding the position to each entry before rice encoded.  This would provide equivalent size and false positive rate to the Slot-based Segment Index, but with the added advantage that the filter would only need to be processed until the accumulated count exceeds the bit-shifted SegmentID that is being searched.

Most of the efficiency is gained from identifying the block, and not the full position - so a compromise might be just to add a 3-bit block ID rather than a 7-bit position, and this would allow for either a 4-bit reduction in size, or a 4x improvement in false positive rate.


## Efficiency of Segment-filtering

In the slot-based segment index there is a false-positive rate per slot of 1:256.  With the same size Rice-encoded filter this can be improved to 1:4096 if the exact position is dropped from the requirement, and the same size filter is used.

If there is a LSM tree containing 10M keys, than across the tree there will be approximately 90K slots, and 450K blocks.

If we want to scan for 32 different segment IDs  - there will be around 600 blocks which need to be opened to find all the keys within those segments.  Using a slot-based segment index around 90% of the blocks will be skipped, but 100 times more blocks will be opened than is optimal.  Using the 4-bits of improvement in the rice encoded example will lead to an improvement greater than one order of magnitude - with > 99% skipped, and only around 3K blocks unnecessarily being opened.

However, this assumes that the Segment IDs we're looking for are evenly distributed.  In the current implementation when we look for a subset of SegmentIDs, it will look for the subset SegmentIDs that are numerically closest.

In these filters we use only 15 or 19-bits of the SegmentID.  If we chose those bits by performing a bsr operation on the 20-bit SegmentID, we can gain further efficiency by finding sets of SegmentIDs that overlap to the common SegmentIDs within filters.
