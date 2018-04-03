# KV Tictac Tree

An Active Anti-Entropy library for Key-Value stores.

## Overview

Library to provide an Active-Anti-Entropy (AAE) capability in a KV store.  The AAE functionality is based on that normally provided through [Merkle Trees](https://github.com/basho/riak_core/blob/2.1.9/src/hashtree.erl), but with two changes from standard practice:

- The Merkle trees are not cryptographically secure (as it is assumed that the system will use them only for comparison between trusted actors over secure channels).  This relaxation of security reduces significantly the cost of maintenance, without reducing their effectiveness for comparison over private channels.  To differentiate from secure Merkle trees the name TicTac Merkle trees is used.

- Indexing of key stores within the AAE system can be 2-dimensional, where the store supports scanning by segment within the store as well as the natural order for the store (e.g. key order).  The key store used is a Log-Structured Merge tree but the bloom-style indexes that are used within the store to accelerate normal access have been dual-purposed to align with the hashes used to map to a key into the Merkle tree, and therefore to accelerate access per-segment without requiring ordering by segment.

The purpose of these changes, and other small improvements, to standard Merkle tree anti-entropy are to allow for:

- Cached views of TicTac Merkle trees to be maintained in memory by applying deltas to the store, so as to avoid the scanning of dirty segments at the point of exchange and allow for immediate exchanges.

- Repeated comparisons during exchanges and the repair only of tree segments which are mismatched in each exchange - exploiting the relative low cost of exchange because of the online cached nature of the trees.

- The rapid merging of TicTac Merkle trees across data partitions - so a tree for the whole store can be quickly built from cached views of partitions within the store, and be compared with a matching store that may be partitioned using a different layout.

- Parallel stores to be maintained during rebuild so that AAE is always on, and entropy managers don't have to consider long periods of downtime for individual partitions during rebuild.

- A partition controller to support a `parallel` key store for finding keys and logical value identifiers (e.g. version vectors or object hashes) from tree segments, where the partition's actual Key/Value store does not support accelerated lookup by segment.  This store can either use segment-ordering, or key-ordering (with segment acceleration) so that it is also usable for other purposes (e.g. building AAE trees by bucket).

- A partition controller to as an alternative to a `parallel` store to be run in `native` mode should the actual partition store support the AAE API with appropriate acceleration.  

- A consistent set of features to be made available between AAE in both `parallel` and `native` key store mode.

- Full async API to the AAE partition controller so that the actual partition (vnode) management process can run an AAE controller without being blocked by AAE activity.

## Actors

The primary actor is the controller - which provides the API to startup and shutdown an AAE controller for partition which will manage TicTac tree caches (`aae_treecache`) and a parallel Key Store (`aae_keystore`), which can be updated by the actual vnode (partition) manager, and accessed by AAE Exchanges.

The AAE exchanges (`aae_exchange`) are finite-state machines which are initialised with a Blue List and a Pink List to compare.  In the simplest form the two lists can be a single vnode and partition identifier each - or they could be different coverage plans consisting of multiple vnodes and multiple partition identifiers by vnode.  The exchanges pass through two root comparison stages (to compare the root of the trees, taking the intersection of branch mismatches from both comparisons), two branch comparison stages, and then a Key and logical identifier exchange based on the leaf segment ID differences found, and finally a repair.

## Using the Library

The library is currently tested for use as a proof of concept, running OTP versions 16 to 19.  If further testing is successful it may go on to be maintained as part of the Riak KV store (targetting release 3.0).

Following the [current tests](https://github.com/martinsumner/kv_index_tictactree/blob/master/test/end_to_end/basic_SUITE.erl) presently provides the simplest guide to using the library.
