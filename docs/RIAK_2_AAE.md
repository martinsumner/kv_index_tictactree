# AAE Implementation in Riak 2.2.5

- Each riak vnode has a kv_index_hashtree process if AAE is enabled.

- That kv_index_hashtree process keeps a single key store (that contains all the keys and hashes of all the keys and values within that vnode store) - so it is a parallel key store duplicating the data (but with just hashes and not the whole object).

- The AAE key store is ordered by Segment ID - with Segment ID being the hash of the Key representing the Key's location in the Merkle Tree.

    - So if the object key is {Bucket1, Key1}, the Key in the AAE store is something like (hash{Bucket1, Key1}, Bucket1, Key1) - and the value is the hash for that object.

    - "the hash for that object" used to mean the hash of the whole object, but now it is just the hash of the version vector.

- As well as the parallel key store, the kv_index_hashtree process keeps a merkle tree for each IndexN that the vnode supports.

- IndexN is a reference to a combination of a n-val and a preflist supported by the vnode.  If all buckets in the store are n-val 3, then there will be 3 IndexNs per vnode (and hence 3 merkle trees).  If there are some buckets with an n-val of 3 and some with an n-val of 4 - there will be 7 merkle trees.

- Each merkle tree also has an associated list of "Dirty Segments" - with a dirty segment meaning a SegmentId within the tree which has had a change since it was last calculated, and so shouldn't be trusted any more.

- When a vnode receives a PUT request, it passes the new key, the new value, and the IndexN to the kv_index_hashtree process after the vnode has been updated.
the kv_index_hashtree process hashes the value (actually the version vector), and hashes the Key to get the Segment ID; and queues up an insert into the Key store that represents this new Key and Hash.

- The hashtree process then marks the SegmentID as being dirty for the Merkle tree whose IndexN matches the update.

- The Riak cluster has a kv_entropy_manager process, and this will determine what vnodes have common IndexNs with what other vnodes - and it will schedule exchanges to take place between these vnodes to compare the merkle trees for their common IndexNs.

- When an exchange request is received by the kv_index_hashtree process, it first must update the Merkle tree for that IndexN.  

    - To do that it looks at the list of dirty segments, and for each dirty segment it fold over all keys in that segment in its local AAE keystore to get a list of [{K, H}] for that SegmentID and IndexN.

    - It then does a sha hash on that list - and that represents the new hash value for that segment in the tree.  Once all leaves have been updated, it works up the tree recalculating branch and root hashes as necessary.

- the AAE processes then exchange and compare the trees - to find a list of segment IDs that differ between the vnodes for that IndexN.  Hopefully the list is small.

- Now for each segmentID in this list of mismatched segments it has to fold over the key store again to find all the [{K, H}] for those SegmentIds and the relevant IndexN - and this is then compared with exchange vnodes.

- If any keys are found to have different hashes, then read repair is invoked for those keys.  This essential is managed just by doing a normal GET request, with a subtle difference.

- If the difference in hashes is because of a real difference between the values in the vnodes, then read_repair should fix it ... however if no difference is found, it prompts a rehash at each riak_kv_vnode for that Key.

    - The rehash just takes the current value for the key out of the vnode backend and passes it as if it were a new PUT to the kv_index_hashtree process.

    - This means that if there are discrepancies between the kv_index_hashtree key store and the vnode store (normally due to uncontrolled shutdown), they don't keep prompting read repairs over and over again - the rehash should fix the kv_index_hashtree store.

- The anti-entropy process described so far fixes differences, when an update is never received by the vnode, but doesn't handle the situation where an update is received, but subsequently lost by a vnode (e.g. disk corruption).

- To protect against disk-based loss, the AAE keystore is periodically (normally once per month) rebuilt from the actual vnode store.  This is a carefully scheduled and throttled process, as it requires a full vnode scan to complete, with page cache side effects etc.

- For the period of the rebuild, which can be many hours, AAE exchanges stop for that vnode (but continue for other vnodes).

This covers off most of what happens for intra-cluster AAE. The same mechanism can also be used for inter-cluster AAE - but the two clusters have to be partitioned identically.

- This is used for some Riak <-> Riak DC synchronisation (the alternative is a key-listing comparison), and also I think for the riak <-> solr synchronisation
the current Riak <-> Riak one is mangled though, and has all kinds of issues (although not ones that are too hard to resolve or workaround)

- There are some issues with this setup:

    - there is an overhead of running a parallel keystore (which is necessary due to the need to frequently find things by segment)

    - every distinct key updated ultimately leeds to a range query in the keystore (because of dirty segments) - this has an overhead

    - it is possible for the rebuild to cause a "one slow node" scenario

    - although it is much, much more efficient than key-listing for full-sync multi-DC replication - it requires the n-vals and ring-sizes to be consistent (which is not helpful when trying to find a way to safely change your ring size, or trying to find an efficient backup process)

Those are the issues scheduled to be addressed in Riak 3.0
