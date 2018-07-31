# TicTac Tree - design

## Objective

The purpose of the KV TicTac Tree is to be able to make comparisons of groups of data partitions within and between database clusters, and prompt repair should differences be found.

### Sample Scenario

Consider two different data stores.  

One store (Store A) stores data split across two virtual nodes (A1 and A2), and each node has the data split into three different partitions (A1.1, A1.2, A1.3, and A2.1, A2.2, A2.3).

- Store A
    - Vnode A1
        - Partition A1.1
        - Partition A1.2
        - Partition A1.3
    - Vnode A2
        - Partition A2.1
        - Partition A2.2
        - Partition A3.3

A second store (Store B) stores data in one virtual node (B1), but within that node data is split evenly across 4 partitions (B1.1, B1.2, B1.3 and B1.4).

- Store B
    - Vnode B1
        - Partition B1.1
        - Partition B1.2
        - Partition B1.3
        - Partition B1.4

There are a number of different relationships with regards to the data ownership that we expect to be true within this system, for example:

- union(A1.1, A1.2, A1.3) == union(A2.1, A2.2, A2.3)
- union(A1.1, A1.2, A1.3) == union(B1.1, B1.2, B1.3, B1.4)
- union(A2.1, A2.2, A2.3) == union(B1.1, B1.2, B1.3, B1.4)
- A1.1 == A2.1
- A1.2 == A2.2
- A1.3 == A2.3

### Constraints

The objective is to have a simple and efficient way of validating all these relationships subject to the following constraints and conditions:

- The AAE system should not place a dependancy on how the vnodes store their data in the partitions.

- The AAE system should confirm that not only has all data reach each location but also that all data remains in that location, in particular entropy of persisted data must also be considered.

- The AAE system should be always on and available to support as many comparisons as possible, management of anti-entropy should not require to schedule around downtime of AAE.

- The AAE system should allow for throttling of exchanges and repairs so as not to overwhelm the system, especially when an alternative process may currently be managing an efficient repair (e.g. hinted handoff).

- Any rebuild process (where state is refreshed to reflect current on-disk status in the main store), must be safe to throttle without impacting exchange availability.

- It can be generally assumed that the vnode is aware of both the before and after state of objects subject to change (to inform the AAE process), but there may be exceptional circumstances (e.g. in Riak with LWW=true on a non-2i backend), where the AAE process itself may need to determine the before state.  It may be that this process is less efficient as it is generally assumed that a system that cares enough about data loss to run anti-entropy, will also care enough to read before a write.

The issue of how to handle timestamped objects with automatic background expiry is important, but is not currently thought through.

## Actors

It is assumed that there are actor currently managing vnodes within the stores, and mechanisms for communicating within and between the vnodes in the stores, and determining membership relationships between partitions and vnodes.

For each vnode an `aae_controller` will be started, with the controller requested to handle data for each partition supported by the vnode.  The `aae_controller` will start an `aae_treecache` for each of the partitions, and a single `aae_keystore` for the vnode.

### Controller

The `aae_controller` is responsible for marshalling all requests from the vnode, and for checking that the keystore, treecache and vnode partition stores remain locally synchronised.  It primarily receives the follow requests:

- put
    - Make a change to a TreeCache and update the KeyStore to represent a vnode change.  The put request should inform the controller of the current clock, the previous clock, and the partition reference.
- merge_root/merge_branches
    - Return the merged root of the tree roots for a list of partitions, or the merged branches of a list of Branch IDs.
- fetch_clocks
    - for a given set of leaf identifiers and partitions return all the keys and version clocks for the objects in the system (from the key store).
- rebuild
    - prompt the store to rebuild from the vnode store all state.
- open/close
    - Open and close, using a shutdown GUID reference on close (then open) to confirm if open and close events are known to have returned the data and the AAE system to a consistent state (the same shutdown GUID should be persisted in the vnode data store at shutdown).
- fold_keystore
    - Allow a general fold over the keystore covering all of the store, or a list of buckets, with a function to apply to each key/metadata pair in the store.


### TreeCache

The `aae_treecache` is responsible for an individual partition reference.  The partition reference is expected to be a combination of {n_val, partition_id} - so in a cluster any given vnode will have as many partition references as the sum of the n_vals supported by the cluster.

The tree cache is an in-memory tictac tree using the `leveled_tictac` module.  Changes are made as they are received (via async message).

The `aae_treecache` process can also be placed in a load mode.  When in load mode, deltas are queued as well as being applied to the current cache.  When ready, `complete_load` can be called with a TicTac Tree formed from a snapshot taken as part of the same unit of work when the load was initialised.  At this stage, the original tree can be destroyed, and the queue of changes can be applied to the new tree.  This process can be used by the `aae_controller` to refresh the tree from Key Store, without ever having the tree cache go inactive.


### KeyStore

The `aae_keystore` is a FSM that can be in three states:

- `loading`
    - In the `loading` state store updates are PUT into the store, but queued for a second (replacement) store.  The keystore can also receive load requests, which are only added into the replacement store.  When the load is complete, the queued requests are put into the replacement store and the original store may be discarded.  This allows the keystore to be rebuilt.
- `parallel`
    - In the `parallel` state, a keystore is kept in parallel to the vnode store, to resolve any fold requests passed in.  A `parallel` store may transition in and out of the `loading` state (back into the `parallel` state).
- `native`
    - In the `native` state, not parallel store is kept, but a reference is kept by the `aae_keystore` process to the vnode backend, and queries are resolved by calling the actual vnode backend.  This requires the vnode backend to support the same API is the parallel `aae_keystore` (and so would currently in riak need to be the leveled backend).  There is no transition in and out of `loading` from the `native` state.

There are two types of parallel stores currently supported (but adding other stores should be simple):

- `leveled_so` (leveled backend but with a key that is ordered first by segment ID)
- `leveled_ko` (leveled backend but ordered by the actual object {Bucket, Key} pair, but with accelerated scanning over a sublist of segment IDs).


### Exchange

The `aae_exchange` is a FSM used for managing a single anti-entropy exchange to find keys to repair based on comparison between two lists - the `blue` and `pink` lists.  The lists for the comparison are a list of `[{SendFun, PartitionRefList}]` tuples, where the SendFun encapsulates a mechanism for reaching a given `aae_controller`, and the PartitionRefList is a list of Partition References which are required from that controller.  

The lists can have multiple items (e.g. require contact with multiple controllers), and request multiple partition references from each controller - which would be normal for comparing coverage plans.  The lists do not need to be of equivalent dimensions between `blue` and `pink`.

The FSM process will alternate between multiple 'checking' states and the special state `waiting_all_results`, until a 'checking' state reveals a full match.  The 'checking' state are:

- `root_compare` - fetch the tree roots and compare.
- `root_confirm` - fetch the tree roots and compare, select the intersection of branch IDs from this first pass and the last pass to use at the next stage.
- `branch_compare` - fetch the tree branches which differ in the root and compare.
- `branch_confirm` - fetch the tree branches which differ in the root and compare, select the intersection of segment leaf IDs from the first pass and last pass to use at the next stages.
- `clock_compare` - fetch the keys and clocks associated with the segment leaf IDs and compare - passing any deltas to a RepairFun provided by the calling process to repair.

The exchange is throttled in two ways.  Firstly, there is a jittered pause between each state transition.  Secondly, the number of IDs (branch or segment leaf IDs) that can be passed from a confirm state is limited.  This will increase the number of iterations required to fill-in an entirely diverse vnode.  The RepairFun that makes the repair is passed-in, and may apply its own throttle, but the `aae_exchange` will not explicitly throttle the repairs.


## Notes on Riak Implementation

Although the AAE library is intended to be generic, it is primarily focused on being a new AAE mechanism for Riak.  Some notes on how this should be implemented within Riak, and functionality that can be expected.

### Transition

Transition between AAE releases is hard (as demonstrated by the difficulties of the hash algorithm change from legacy to version 0 in the existing riak_kv_index_hashtree implementation).  The intention is to allow this AAE to be a plugin over and above the existing AAE implementation, making transition an administrative task: the tictac tree AAE can be run in Riak oblivious to whether existing AAE versions are running.

### Startup, Shutdown and Synchronisation

The `riak_kv_vnode` is expected to be responsible for stopping and starting the `aae_controller` should this version of AAE be implemented.  The `aae_controller` should only be started after the vnode backend has been started, but before the vnode is marked as ready.  The trees, parallel keystore (in parallel mode) and vnode store may at this stage be out of sync, if the vnode had not previously shut down cleanly.  Whilst stores are out of sync, they will still operate but return false negative results: however, false negative results will prompt incremental repair of the synchronisation issue.  Incremental repair of a parallel keystore is done using the per-vnode rehash.  Incremental repair of the trees is done through a rehashing of the segments undertaken as part of the `aae_controller:aae_fetchclocks/5`.

If the `aae_treecache` was not shutdown correctly, then the whole cache may be incorrect (e.g. empty).  This would take a long time to incrementally repair, and so this scenario is detected and flagged at startup.  It is therefore recommended at vnode startup, that the `aae_controller:aae_rebuildtrees/5` be called with the `OnlyIfBroken = true`.  This will return `skipped` if the treecache appeared to have been recovered successfully and not rebuild, but will rebuild if a potential issue with any of the tree_caches had been flagged at startup.

Whilst the stores are potentially out of sync, then the controller should operate as normal - this will potentially lead to false repairs until the rebuild is complete.  If to an administrator, the possibility of non-synchronisation is a known possibility, such as when a node is restarting following a hard crash - then the [participate in coverage](https://github.com/basho/riak_core/pull/917) feature can be used to remove the node's vnodes from any coverage plan based AAE exchanges.

There exists the potential for further improvements of vnode store to aae coordination, should the aae store be used for additional functional reasons in the future.

### Intra-Cluster AAE

The `aae_exchange` is flexible so that intra-cluster AAE can be done pairwise or between coverage offsets.  If we have a ring size of 128, and a single n-val of 3, there are 384 pairwise exchanges.  So an entropy_manager could be elected in the cluster which rotates around those pairwise exchanges.

It would be quicker to just perform the 3 comparisons necessary to rotate around the 3 coverage plans (with the 3 different offsets), and compare those coverage plans.  However, in the scenario where a single

### AAE Cluster Full-Sync

....

### MapFold Changes - Backend Independent

Previously there had been some work down to add [MapFold](https://github.com/martinsumner/riak_kv/blob/mas-2.1.7-foldobjects/docs/MAPFOLD.md) as a feature to Riak.  This is in someways an alternative to the work done by Basho on riak_kv_sweeper - there is a generic need to have functions that fold over objects, that produce outputs that aren't required immediately.  This is especially true for operational reasons e.g.:

- find all sibling'd objects;
- count then number of objects in a bucket;
- what is the average object size in the database;
- provide a histogram of last modified dates on objects in a bucket).

There may also be functional reasons whereby we might want to have non-disruptive folds with bespoke functions and accumulators - especially for reporting (e.g. count all the people by age-range and gender), that currently require a secondary index and for all 2i terms to be fed back to the application for processing, with the application needing to control throttling of the query.

Riak previously had Map/Reduce which could answer these problems, but Map/Reduce was designed to be fast.  It was controlled in the sense it has back pressure to prevent the reading of data from overwhelming the processing of that data - but it was not controlled to prevent a Map/Reduce workload from overloading standard K/V GET/PUT activity.  Also Map/Reduce required the reading of the whole object, so didn't offer any optimisation if the interesting information was on a 2i term or in object metadata.

The Mapfold work provided to a solution to this, but to be efficient it depended on
the backend supporting secondary indexes and/or fold_heads.  The Mapfold work was optimised for the leveled backend, but left other backends behind.

One side effect of kv_index_tictactree is that provides a parallel store (when leveled is not used), that can still be key-ordered.  The metadata that gets put into that parallel store could be extended to include the full object head.  So the same queries that work with a native leveled backend, will work with a parallel AAE leveled key-ordered backend.  Potentially this would mean that MapFold could be supported efficiently with any backend where AAE has been enabled.

### Per-Bucket MDC Replication

...

### Bitcask and HEAD requests

....

### Bitcask and 2i Support

....

### Improved Vnode Synchronisation on Abrupt Shutdown

....

### Backup Use-case

Backups in Riak are hard.  Hard for good reasons:

- the difficulty of co-ordinating a snapshot in a point in time across many processes on many machines;
- the volume of data traditionally used by people who need a distributed database;
- the inherent duplication of data in Riak;
- the write amplification in some Riak backends (leveldb) increasing the cost of any rsync based mechanism for backup.

Historically different approaches have been tried, and ultimately most Riak systems either end up running without historic backups (just MDC replication), or with a bespoke backup approach integrated into either the database and/or the application.

One possibility is to be able to run a very small cluster with dense storage machines, in a backup configuration:  e.g. node count of 1, ring size of 8, n/r/w-val of 1, vnode backend rsync friendly (leveled/bitcask) with 2i disabled.  If we can now replicate from a production scale cluster to this (using rabl for real time-replication so that peak load is queued), then stopping this single node cluster and running rsync periodically could produce a more traditional backup approach without impeding on decision making wrt production database setup (e.g. ring size, n-val and write-amplification and query support in the backend).

The combination of repl replication, and AAE full-sync independent of ring-size and n-val might make such a solution possible without bespoke application effort.

### AAE for 2i Terms

....

### 2i Repair

....

### Rehash Support - Consideration for W1 Misuse

....

### Support for LWW on Bitcask

....
