# Proposed AAE Implementation in Riak 3.0

- Each riak vnode has a kv_index_tictactree `aae_controller` process if Tictac AAE is enabled.  It may also run [Riak 2 AAE](RIAK_2_AAE.md) in parallel - the two AAE implementations should not care about the existence of the other.

- That `aae_controller` keeps a single `aae_keystore` process which is responsible for keeping all the keys and AAE metadata for the store.  The `aae_keystore` process can be any backend that supports query by Merkle tree segment that duplicates the storage of keys; this is `parallel` mode.  The `aae_keystore` could also be a reference to the vnode backend PID, if the vnode backend can itself support query by AAE segment; this is `native` mode and doesn't require the duplictaion of key/metadata storage.

- As well as the key store, the `aae_controller` process keeps a `aae_treecache` process for each combination of preflist and n_val the vnode supports (or IndexN).  This process is an in-memory cache of the current state of the Tictac Merkle tree for that IndexN.

- IndexN is a reference to a combination of a n-val and a preflist supported by the vnode.  If all buckets in the store are n-val 3, then there will be 3 IndexNs per vnode (and hence 3 merkle trees).  If there are some buckets with an n-val of 3 and some with an n-val of 4 - there will be 7 merkle trees.

- There is no longer a concept of dirty segments, each tree cache represents the current live state of the vnode.

## On PUT

- When a vnode receives a PUT request, it passes a change note to the `aae_controller`.  The change consists of the {Bucket, Key}, the IndexN, the CurrentClock and the PreviousClock for the PUT - as well as some useful metadata about the value (e.g. sibling count, index_hash, perhaps the whole object head).  

    - If the change is a delete the CurrentClock should be none.  

    - If the change is a fresh put, the PreviousClock should be none.

    - If the change is a LWW PUT in an non-index backend, the PreviousClock should be undefined.

- The `aae_controller` receives this update as a cast.  It has two tasks now:

    - If the `aae_keystore` is running in `parallel` mode, cast the change on to the keystore.  The `aae_keystore` should then queue up the new version for the next batch of keys/metadata to be stored. In `native` mode, no keystore change is required.

    - Based on the IndexN, the `aae_controller` should cast the delta to the appropriate `aae_treecache`.  This should update the Merkle tree by removing the old version from the tree (by XORing the hash of the {Key, PreviousClock} again), and adding the new version (by XORing the segment by the hash of the {Key, CurrentClock}).

## On Exchange

- Riak can then be prompted to do exchanges (maybe via an entropy manager, maybe through scheduled external requests).  An exchange could be:

    - An exchange for a given n_val between two coverage plans (with different offsets);

    - An exchange between each locally stored Preflist, and another remote Preflist for that n_val.

    - An exchange between a randomly chosen set of pairs of common IndexNs between vnodes.

- An exchange is done by starting an `aae_exchange` process.  The `aae_exchange` is a FSM and should be initiated by the calling service via sidejob.  The `aae_exchange` process takes as input:

    - A BlueList and a PinkList - lists of {SendFun, [IndexN]} tuples, where the SendFun isa function that can send a message to a given controller, and the list of IndexNs are the preflist/n_val pairs releative to this exchange at that destination.  The SendFun thin this case should use the riak_core message passing to reach the riak_kv_vnode - and the riak_kv_vnode will be extended to detect AAE commands and forward them to the `aae_controller`.

    -  A RepairFun - that will be passed any deltas, and in the case of intra-cluster anti-entropy the RepairFun should just send a throttled stream of GET requests to invoke read_repair

    - A ReplyFun - to send a response back tot he client (giving the state at which the FSM exited, and the number of KeyDeltas discovered.

- The exchange will request all the tree roots to be fetched from the Blue List and the Pink List - merging to give a Blue Root and a Pink root, and comparing those roots.  This will provide a list of branch IDs that may potentially have deltas.  If the list is empty, the process will reply and exit.

- The exchange will then pause, and then re-request all the tree roots.  This will produce a new list of BranchID deltas from the comparison of the merged roots, and this will be intersected with the first list.  If the list is empty, the process will reply and exit.

- The exchange will then pause, and then request all the branches from the Blue and Pink lists.  This will again be a repeated request, with the intersection of the SegmentIDs that differ being taken forward to the next stage, and the process will reply and exit if the list is empty.

- The number of SegmentIDs that are taken forward for the clock comparison is bounded, and the code will attempt to chose the set of SegmentIDs that are closest together as the subset to be used.  Those SegmentIDs will then be forwarded in a request to `fetch_clocks`.  These requests will be passed by the `aae_controller` to the `aae_keystore`, and this will fold over the store (and this will be the vnode store if the `aae_keystore` is running in native mode), looking for all Keys and Version Vectors within those segments.  If the keystore is segment-ordered, this will be a series of range folds on the snapshot.  If the keystore is key-ordered, then there will be a single fold across the whole store, but before a slot of keys is passed into the fold process it will be checked to see if it contains any key in the segment - and skipped if not.

- When the Keys and Clocks are returned they are compared, and then deltas are passed to the RepairFun for read repair.


## On startup and shutdown

Before a vnode backend is shutdown, a special object should be stored where the value is a Shutdown GUID.  When a vnode backend, the object should be read, and if present shoudl then be deleted.  

When the `aae_controller` is started it is passed the IsEmpty status of the vnode backend as well as the shutdown GUID.  The `aae_keystore` should likewise have the Shutdown GUID on shutdown (and erased it on startup), and on startup an confirm that the IsEmpty status and Shutdown GUIDs match between the vnode and the `parallel` keystore.

If there is no match on startup, then it cannot be consumed that the two stores are consistent, and the next rebuild time should be set into the past.  this should then prompt a rebuild.  Until the rebuild is complete, the `aae_controller` should continue on a best endeavours basis, assuming that the data in the `aae_treecache` and `aae_keystore` is good enough until the rebuild completes.


## On rebuild
