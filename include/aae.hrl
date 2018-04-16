-define(TREE_SIZE, large).
-define(MAGIC, 53).
-define(HEAD_TAG, h). 
    % Used in leveled as a Tag for head-only objectsm, used in parallel store
-define(RIAK_TAG, o_rkv).
    % Tag to be used for finding Riakobjects in native store