# Merkle Trees and Tictac Trees

A [Merkle Tree](https://en.wikipedia.org/wiki/Merkle_tree) is a tree of hashes arranges so that the value of the hash of any branch, is the hash of the accumulation of hashes below it.  This allows for trees which represent the same data, to confirm this synchronisation by transferring only the root of the tree.  Also, where there are small deltas between the trees, for the tree to be traversed to quickly identify which tree-positions those deltas are in.


## Standard Merkle Tree (Riak)

In Riak KV 2.2.5, the hashtree has two levels, each 1024 hashes wide - meaning o(1m) overall segments within each tree.  The position of a key in the tree is determined by taking the `erlang:phash2/1` hash of the Bucket and Key.  The hash of an individual element to be added to the tree is found by taking a `erlang:phash2/1` [hash of the sorted version vector](https://github.com/basho/riak_kv/blob/2.1.7/src/riak_object.erl#L667-L670).  To calculate the hash of a segment, the hashtree process takes all of the Keys and Hashes in the segment and performs:

`hash([{K1, H1}, {K2, H2} .... {Kn, Hn}])`

This time using a sha hash from the erlang crypto library.  This hashing of the list of all sub-elements is then used up to the root of the tree to calculate the parent hashes.

## Alternative Merkle Tree (Tictac)

The Tictac trees still use as the value to hash the sorted list of the version vector, but now the hash for the segment ID is built up as follows:

`hash({K1, VV1}) xor hash({K2, VV2}) xor .... hash({Kn, VVn})`

This change weakens the cryptographic security of the Merkle tree, in that it directly exposes deltas i.e. the addition of the same Key and Version Vector will always result in the same hash delta in the tree, regardless of the starting point of the tree.  Whereas, if the same change has been made to two different trees with strong Merkle trees, the delta in the tree would not be predictable.

In this context, it is determined that the cryptographic strength isn't important.  All actors already have access to data, have a secure communication path, and the purpose of the tree is simply to identify deltas and not to determine the integrity of a change.

### Supporting PUT

The result of this change is that if we know for K1, the old version vector (VV1a) and a new version vector (VV1b), we can determine the hash change to be applied to the tree with just this knowledge:

`Delta = hash({K1, VV1a}) xor hash({K1, VV1b})`

This Delta can then be applied to reach level of the tree up to the root, and the tree reflects the change.  Whereas, with the traditional Merkle tree it is first necessary to find **all** the Keys and Hashes within the changed segment, so that a *new* hash for that segment can be calculated (rather than a *delta* applied).

As well as changing the process of combining hashes, the hash algorithm is changed to (4-bytes of) md5 for both keys and version vectors (relaxing the unnecessary cryptographic strength, and making it easier to produce the Trees outside of Riak, by not depending on the erlang hash function).

In the current Merkle tree implementation, every change to a tree requires a scan of a key store, but with a TicTac tree, prior knowledge of the old version vector and the current version vector is all that is required to produce the delta.  

Within Riak, for most PUTs the `riak_kv_vnode` will read before write, and so the old version vector is already known - so no extra read cost is required to update the tree.  If the update is following the write once path, by definition (assuming developer competence) the previous version vector can be assumed to be empty.

The exceptional scenario is for updates using Last Write Wins, with a backend not supporting secondary indexes (currently only Bitcask), where the old version vector will not be known by the `riak_kv_vnode` and cannot be assumed to be empty.  There are four options for handling this scenario:

- Do not support AAE for such buckets;
- Force the riak_kv_vnode to end the read-less write optimisation if Tictac AAE is enabled;
- Pass the old version vector through as undefined, and require the Tictac AAE process to use its own Keystore to discover the old version vector before updating;
- Pass the old version vector through as undefined, and require the Tictac AAE process to seek all Keys and Hashes in the segment to recalculate the hash in this case.

The best approach is to be determined, but it is assumed it would be better to be one that places responsibility for change on the AAE process, not the existing vnode code.

### Coverage Implications

In most cases, the cost of altering the tree on PUT is reduced dramatically by switching to Tictac trees.  However, the biggest benefits is with regards to merging trees.

Currently, when trying to perform a full-sync operation between two Riak clusters, this can be done either through key-listing or AAE exchange.  The key-listing approach compares the two clusters one vnode at a time (over a covering set of vnodes), and in this case this is an implementation choice to throttle the process.  

The AAE exchange approach also runs the comparison one vnode at a time, but there is no choice in this regards - as the AAE trees are separated out on a per-vnode basis, and it is impossible to merge two Merkle trees without access to all the underlying keys and hashes within both Merkle Trees.

However, to merge two Tictac trees that cover non-overlapping sets of data, for each segment the result is simply:

`hash(SegA) xor hash(SegB)`

So it would be possible to take just the trees from a covering set of vnodes, and without any knowledge of the underlying Keys and Hashes merge those trees (or indeed just the roots of those trees).  This means that a covering set of vnodes can efficiently combined all their Tictac trees to produce a single Tictac tree to represent the whole cluster.

Crucially, this allows for synchronisation between database clusters with different patterns of data partitioning.  This would mean that an AAE full-sync process could be run:

- To aid in migration between clusters of different ring size, working around the issues of ring re-sizing being deprecated in Riak.
- As part of a backup approach, as it will be possible to AAE full-sync replica to a cluster that not just has a different node counts(e.g. a node count 1), backends (e.g. to one that is rsync friendly), ring-size (one that is optimal for a smaller cluster size) but also different n-vals (e.g. n-val of 1).
- To make synchronisation between Riak and an alternate database management system easier, assuming that alternative database can also maintain a database-wide Tictac tree.


## Naming

The name Tictac is taken from the [Tic-Tac language used by on-course bookmakers](https://en.wikipedia.org/wiki/Tic-tac), which was a non-secure but efficient way of communicating deltas in a wide market to a participant in the market.

This variation in Merkle trees is not novel, in that the use of the less secure XOR operation is known to be used within the [Cassandra database](http://distributeddatastore.blogspot.co.uk/2013/07/cassandra-using-merkle-trees-to-detect.html).  However, the overall pattern of anti-entropy in Cassandra is different, with [trees being built and destroyed on demand](https://wiki.apache.org/cassandra/AntiEntropy) rather than being cached and merged.
