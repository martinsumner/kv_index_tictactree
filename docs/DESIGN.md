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


### TreeCache


### KeyStore


### Exchange
