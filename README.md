# KV Tictac Tree

An Active Anti-Entropy library for Key-Value stores in Erlang.

## Overview

Library to provide an Active-Anti-Entropy (AAE) capability in a KV store.  The AAE functionality is based on that normally provided through [Merkle Trees](https://github.com/basho/riak_core/blob/2.1.9/src/hashtree.erl), but with two changes from standard practice:

- The Merkle trees are not cryptographically secure (as it is assumed that the system will use them only for comparison between trusted actors over secure channels).  This relaxation of security reduces significantly the cost of maintenance, without reducing their effectiveness for comparison over private channels.  To differentiate from secure Merkle trees the name TicTac Merkle trees is used.  [Further details on Tictac trees can be found here](docs/TICTAC.md).

- Indexing of key stores within the AAE system can be 2-dimensional, where the store supports scanning by segment within the store as well as the natural order for the store (e.g. key order).  The key store used is a Log-Structured Merge tree but the bloom-style indexes that are used within the store to accelerate normal access have been dual-purposed to align with the hashes used to map to a key into the Merkle tree, and therefore to accelerate access per-segment without requiring ordering by segment.  [Further details on making bloom-based indexes in LSM trees dual prupose can be found here](docs/SEGMENT_FILTERED_SST.md)

The purpose of these changes, and other small improvements to standard Merkle tree anti-entropy, are to allow for:

- Supporting Active Anti-Entropy without the need to maintain and synchronise additional `parallel` key stores to provide a tree-ordered view of the store.  this depends on the primary store having `native` AAE support, and a `parallel` store may still be used where this support is not available.

- Cached views of TicTac Merkle trees to be maintained in memory by applying deltas to the trees, so as to avoid the scanning of dirty segments at the point of exchange and allow for immediate exchanges.  Also the cache will be maintained and kept up-to-date during rebuild activity, to prevent loss of anti-entropy validation during any background rebuild processes.

- False positive avoidance by double-checking each stage of the exchange (separated by a pause), utilising the low cost of querying the tree, and avoiding the false-negative exchanges associated with timing differences between changes reaching different vnodes.

- The rapid merging of TicTac Merkle trees across data partitions - so a tree for the whole store can be quickly built from cached views of partitions within the store, and be compared with a matching store that may be partitioned using a different layout.

- A consistent set of features to be made available between AAE in both `parallel` and `native` key store mode - including the ability to query the AAE store to discover information which otherwise would require expensive object folds.

- Fully asynchronous API to the AAE controller so that the actual partition (vnode) management process can run an AAE controller without being blocked by AAE activity.

- Allow for AAE exchanges to compare Keys and Clocks for mismatched segments, not just Keys and Hashes, so repair functions can be targeted at the side of the exchange which is behind - avoiding needlessly duplicated 2-way repairs.


## Primary Actors

The primary actor in the library is the controller (`aae_controller`) - which provides the API to startup and shutdown a server for which will manage TicTac tree caches (`aae_treecache`) and a parallel Key Store (`aae_keystore` - which may be empty when run in `native` mode).  The `aae_controller` can be updated by the actual vnode (partition) manager, and accessed by AAE Exchanges (either directly or also via the vnode manager).

The AAE exchanges (`aae_exchange`) are finite-state machines which are initialised with a Blue List and a Pink List to compare.  In the simplest form the two lists can be a single vnode and partition identifier each - or they could be different coverage plans consisting of multiple vnodes and multiple partition identifiers by vnode.  The exchanges pass through two root comparison stages (to compare the root of the trees, taking the intersection of branch mismatches from both comparisons), two branch comparison stages, and then a Key and logical identifier exchange based on the leaf segment ID differences found, and finally a repair.

The AAE exchange should work the same way if two partitions are bing compared, or two coverage queries across multiple partitions are being compared.

[More detail on the design can be found here](docs/DESIGN.md).

[Some further background information can be found here](https://github.com/martinsumner/leveled/blob/master/docs/ANTI_ENTROPY.md).

## Using the Library

The library is currently tested for use as a proof of concept, running OTP versions 16 to 19.  If further testing is successful it may go on to be maintained as part of the Riak KV store (targeting release 3.0).

Following the [current tests](https://github.com/martinsumner/kv_index_tictactree/blob/master/test/end_to_end/basic_SUITE.erl) presently provides the simplest guide to using the library.  There is also a [`mock_kv_vnode`](https://github.com/martinsumner/kv_index_tictactree/blob/master/test/end_to_end/mock_kv_vnode.erl) process used in these tests, and provides a sample view of how an `aae_controller` could be integrated.
