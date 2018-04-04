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

The objective is to have a simple and efficient way of validating all these relationships subject to the following constraints:

- The AAE system should not place a dependancy on how the vnodes store their data in the partitions.

- The AAE system should confirm that not only has all data reach each location but also that all data remains in that location, in particular entropy of persisted data must also be considered.

- The AAE system should be always on and available to support as many comparisons as possible, management of anti-entropy should not require to schedule around downtime of AAE.

- The AAE system should allow for throttling of exchanges and repairs so as not to overwhelm the system, especially when an alternative process may currently be managing an efficient repair (e.g. hinted handoff).

- Any rebuild process (where state is refreshed to reflect current on-disk status in the main store), must be safe to throttle without impacting exchange availability.

- It can be generally assumed that the vnode is aware of both the before and after state of objects subject to change (to inform the AAE process), but there may be exceptional circumstances (e.g. in Riak with LWW=true on a non-2i backend), where the AAe process itself may need to determine the before state.  It may be that this process is less efficient as it is generally assumed that a system that cares enough about data loss to run anti-entropy, will also care enough to read before a write.


## Actors

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

The `riak_kv_vnode` is expected to be responsible for stopping and starting the `aae_controller` should this version of AAE be implemented.  The `aae_controller` should only be started after the vnode backend has been started, and the Shutdown GUID read - but before the vnode is marked as ready.  If there is wither a match on the Shutdown GUIDs in the controller with the vnode backend, then the two can be considered in sync.  If the two are not in sync, then the rebuild time should be set to the current time, so that a rebuild can bring the stores back into sync.  

Whilst the stores are potentially out of sync, then the controller should operate as normal - this will potentially lead to false repairs until the rebuild is complete.  If to an administrator, the possibility of non-synchronisation is a known possibility, such as when a node is restarting following a hard crash - then the [participate in coverage](https://github.com/basho/riak_core/pull/917) feature can be used to remove the node's vnodes from any coverage plan based AAE exchanges.

There may be options to better automate this by communication of the synchronisation status to the riak_core to be gossiped through the ring.  It is assumed that deferring the vnode being active until the rebuild is complete is not acceptable, as the time required to complete the rebuild is unknown, and may be many minutes.

On startup any previous Shutdown GUID should be removed from both backend vnode and `aae_keystore`.  On shutdown, the backend vnode should have  anew Shutdown GUID inserted before close, and the shutdown of the `aae_controller` should be deferred until the close of the vnode backend is complete.

The `aae_treecache` is not persisted other than at shutdown.  This is as the cost of rebuilding a tree cache is relatively once a parallel key_store is up to date (or using a native key store) is low.

There exists the potential for further improvements of vnode store to aae coordination, should the aae store be used for additional functional reasons in the future.

### Intra-Cluster AAE

The `aae_exchange` is flexible so that intra-cluster AAE can be done pairwise or between coverage offsets.  If we have a ring size of 128, and a single n-val of 3, there are 384 pairwise exchanges.  So an entropy_manager could be elected in the cluster which rotates around those pairwise exchanges.

It would be quicker to just perform the 3 comparisons necessary to rotate around the 3 coverage plans (with the 3 different offsets), and compare those coverage plans.  However, in the scenario where a single

### AAE cluster Full-Sync
