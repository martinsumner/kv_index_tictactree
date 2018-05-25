# Proposed AAE Implementation in Riak 3.0

- Each riak vnode has a kv_index_tictactree `aae_controller` process if Tictac AAE is enabled.  It may also run [Riak 2 AAE](RIAK_2_AAE.md) in parallel - the two AAE implementations should not care about the existence of the other.

- That `aae_controller` keeps a single `aae_keystore` process which is responsible for keeping all the keys and AAE metadata for the store.  The `aae_keystore` process can be any backend that supports query by Merkle tree segment that duplicates the storage of keys; this is `parallel` mode.  The `aae_keystore` could also be a reference to the vnode backend PID, if the vnode backend can itself support query by AAE segment; this is `native` mode and doesn't require the duplictaion of key/metadata storage.

- As well as the key store, the `aae_controller` process keeps a `aae_treecache` process for each combination of preflist and n_val the vnode supports (or IndexN).  This process is an in-memory cache of the current state of the Tictac Merkle tree for that IndexN.

- IndexN is a reference to a combination of a n-val and a preflist supported by the vnode.  If all buckets in the store are n-val 3, then there will be 3 IndexNs per vnode (and hence 3 merkle trees).  If there are some buckets with an n-val of 3 and some with an n-val of 4 - there will be 7 merkle trees.

- There is no longer a concept of dirty segments, each tree cache represents the current live state of the vnode.

## On PUT

- When a vnode receives a PUT request, it passes a change note to the `aae_controller`.  The change consists of the {Bucket, Key}, the IndexN, the CurrentClock and the PreviousClock for the PUT - as well as some useful metadata about the value (e.g. sibling count, index_hash, perhaps the whole object head).  

    - If the change is a delete the CurrentClock should be none.  

    - If the change is a fresh put, the PreviousClock should be none (this includes any PUt on the write once path).

    - If the change is a LWW PUT in an non-index backend, the PreviousClock should be undefined.

    - If the change is a rehash, then the PreviousClock should be undefined.

- The `aae_controller` receives this update as a cast.  It has two tasks now:

    - If the `aae_keystore` is running in `parallel` mode, cast the change on to the keystore.  The `aae_keystore` should then queue up the new version for the next batch of keys/metadata to be stored. In `native` mode, no keystore change is required.

    - Based on the IndexN, the `aae_controller` should cast the delta to the appropriate `aae_treecache`.  This should update the Merkle tree by removing the old version from the tree (by XORing the hash of the {Key, PreviousClock} again), and adding the new version (by XORing the segment by the hash of the {Key, CurrentClock}).

    - The exception to this is when the PreviousClock is undefined - meaning there was no read before write.  In this case, the PreviousClock needs to be filled in with a read against the `aae_keystore` before processing.  

        - One scenario where the previous clock is `undefined` is when Last Write Wins is used with a non-indexing backend.  This removes some of the efficiency advantages of Last Write Wins writes, though doesn't eliminate the latency improvement (as the AAE read does not block the update from proceeding).

        - The other scenario is on a `rehash` request (when a read_repair GET request has not discovered an expected anomaly between the vnode values).

        - The `aae_keystore` may fill-in this information two ways.  It could simple read the previous object in the keystore, or it could fold over all objects in that segment and IndexN to calculate an entirely new hash value for that segment.  Perhaps the latter should be a fallback for `rehash` requests (i.e. on a dice roll on a rehash request, so rehash eventually causes this)

- Before any fold operation on the `aae_keystore` the batch of changes waiting to be written are flushed.

## On Exchange

- Riak can then be prompted to do exchanges (maybe via an entropy manager, maybe through scheduled external requests).  An exchange could be:

    - An exchange for a given n_val between two coverage plans (with different offsets);

    - An exchange between each locally stored Preflist, and another remote Preflist for that n_val.

    - An exchange between a randomly chosen set of pairs of common IndexNs between vnodes.

- An exchange is done by starting an `aae_exchange` process.  The `aae_exchange` is a FSM and should be initiated by the calling service via sidejob.  The `aae_exchange` process takes as input:

    - A BlueList and a PinkList - lists of {SendFun, [IndexN]} tuples, where the SendFun is a function that can send a message to a given controller, and the list of IndexNs are the preflist/n_val pairs relative to this exchange at that destination.  The SendFun in this case should use the riak_core message passing to reach the riak_kv_vnode - and the riak_kv_vnode will be extended to detect AAE commands and forward them to the `aae_controller`.

    -  A RepairFun - that will be passed any deltas, and in the case of intra-cluster anti-entropy the RepairFun should just send a throttled stream of GET requests to invoke read_repair

    - A ReplyFun - to send a response back to the client (giving the state at which the FSM exited, and the number of KeyDeltas discovered.

- The exchange will request all the tree roots to be fetched from the Blue List and the Pink List - merging to give a Blue root and a Pink root, and comparing those roots.  This will provide a list of branch IDs that may potentially have deltas.  If the list is empty, the process will reply and exit.

- The exchange will then pause, and then re-request all the tree roots.  This will produce a new list of BranchID deltas from the comparison of the merged roots, and this will be intersected with the first list.  If the list is empty, the process will reply and exit.

- The exchange will then pause, and then request all the branches that were in the intersected list of BranchIDs from the Blue and Pink lists.  This will again be a repeated request, with the intersection of the SegmentIDs that differ being taken forward to the next stage, and the process will reply and exit if the list is empty.

- The number of SegmentIDs that are taken forward for the clock comparison is bounded, and the code will attempt to chose the set of SegmentIDs that are closest together as the subset to be used.  Those SegmentIDs will then be forwarded in a request to `fetch_clocks`.  These requests will be passed by the `aae_controller` to the `aae_keystore`, and this will fold over the store (and this will be the vnode store if the `aae_keystore` is running in native mode), looking for all Keys and Version Vectors within those segments and IndexNs.  If the keystore is segment-ordered, this will be a series of range folds on the snapshot.  If the keystore is key-ordered, then there will be a single fold across the whole store, but before a slot of keys is passed into the fold process it will be checked to see if it contains any key in the segment - and skipped if not.

- When the Keys and Clocks are returned they are compared, and then deltas are passed to the RepairFun for read repair.


## On startup and shutdown

- Before a vnode backend is shutdown, a special object should be stored where the value is a Shutdown GUID.  When a vnode backend, the object should be read, and if present should then be deleted.  

- When the `aae_controller` is started it is passed the IsEmpty status of the vnode backend as well as the shutdown GUID.  The `aae_keystore` should likewise have the Shutdown GUID on shutdown (and erased it on startup), and on startup an confirm that the IsEmpty status and Shutdown GUIDs match between the vnode and the `parallel` keystore.

- If there is no match on startup, then it cannot be assumed that the two stores are consistent, and the next rebuild time should be set into the past.  this should then prompt a rebuild.  Until the rebuild is complete, the `aae_controller` should continue on a best endeavours basis, assuming that the data in the `aae_treecache` and `aae_keystore` is good enough until the rebuild completes.


## On rebuild

- Rebuilds are not prompted by the `aae_controller`, they require an external actor to prompt them.

- The `aae_controller` trackes the next rebuild time, the time when it should be next rebuild.  This is based on adding a rebuild schedule (a fixed time between rebuilds and a random variable time to reduce the chances of rebuild synchronisation across vnodes) to the last rebuild time.  The last rebuild time is persisted to be preserved across restarts.

- The next rebuild time is reset to the past on startup, if a failure to shutdown cleanly and consistently either the vnode or the aae service is detected through a mismatch on the Shutdown GUID.

- The vnode should check the next rebuild time after startup of the `aae_controller`, and schedule a callback to prompt the rebuild at that time.

- When the rebuild time is reached, the vnode should prompt the rebuild of the store via the `aae_controller`

    - the prompt should pass in a SplitObjFun which can extract/calculate the IndexN and version vector for a Riak object in the store (this is required only in `parallel` mode).

    - the prompt should first flush all batched updates to the `aae_keystore`, and trigger the `aae_keystore` to enter the `loading` state.

    - in the `loading` state a separate backend for the keystore is started.  All updates received from that point are added in batches to the main keystore as normal, but also queued up for the new keystore.

    - a fold objects function and a finish function is returned in response to the prompt request.

        - the fold fun will load all passed objects in batches as object_specs (by extracting out the vector clock etc) directly into the new load store of the `aae_keystore`.

        - the finish fun will prompt the worker running the fold to prompt the keystore to complete the load.  This will involve, loading all the queued object specs (of updates received since the fold was started) into the store, deleting the old key store, and making the newly rebuilt key store the master.

    - the vnode process takes these fold functions, and starts an object fold using the functions (if snapshot fold is supported in the backend, then via a riak_core_node_worker so as not to avoid multiple parallel folds).

    - the vnode should also request a callback from the worker when the work is completed, to prompt it to prompt the `aae_controller` to now rebuild the tree caches.

- If the `aae_keystore` is in `native` mode, none of the above happens, as the store is the store, and so there is no need for a rebuild.

- the `aae_controller` should be prompted to rebuild_trees, and for this IndexNs (the list of IndexNs the vnode currently manages), PreflistFun (a fun to calculate the IndexN from a {B, K} pair - required only in `native` mode), and a WorkerFun (a node/vnode worker to tun the next fold) is passed.

    - the `aae_controller` should inform the `aae_treecaches` to start loading, this requires them to queue new updates as well as making the changes in the cache.

    - A fold is then run over the key store (or the vnode store in the case of `native` backends), using the WorkerFun.

    - the fold should incrementally build a separate TicTac tree for each IndexN.

    - when the fold is complete, the trees are sent to the `aae_treecache` processes for loading.  Loading discards the previous tree, takes the new tree, and loads all the changes which have been queued since the point the snapshot for the fold to build the trees was taken.

- This completes the rebuild process.  It is important that the folds in the rebuild process use snapshots which are co-ordinated with the formation of the load queues - so that the deltas being applied to the load queues takes the system to a consistent point.

- If there is a shutdown during a rebuild process, all the partially built elements are discarded, and the rebuild will be due again at startup.

- Scheduling of rebuilds is not ventrally managed (so the locks required to reduce concurrency in the existing AAE process are discarded).  There is instead a combination of some random factor added to the schedule time, plus the use of the core_node_worker_pool - which prevents more than one M folds being on a node concurrently (where M is the size of the pool, normally 1).

## Secondary Uses

- A proof of concept on coverage folds showed that there were some interesting operations that could be managed more safely and efficiently than Map Reduce, using folds over the heads of objects, using a `core_node_worker_pool` and a backend which stores heads separate to objects.  In introducing an AEE store where the `aae_keystore` can store additional metadata, perhaps including the whole object head - there exists the potential to bring these features to all backends.

- Another possibility is the efficient handling of `HEAD` not `GET` requests (for example where only version vector is required).  This is only supported in the leveled backend at present, in other backends it can be supported by still reading the object, and just stripping to the head to avoid the network overhead.  It may be possible for a `riak_kv_vnode` with a bitcask backend to handle `HEAD` requests in this way, unless co-ordination between backend and AAE store is confirmed (because of matching Shutdown GUIDs at startup or a rebuild since startup).  In this case the `HEAD` request could instead be handled by the AAE store, avoiding the actula object read.
