%% -------- Overview ---------
%%
%% An AAE controller, that should receive updates from a KV store along with
%% requests for:
%% - Tictac trees, and parts thereof
%% - Keys and clocks by portion of the tree
%% - Snapshots of the KeyStore to support async object folds
%% - Periodic requests to rebuild
%% - Requests to startup and shutdown
%%
%% 


-module(aae_controller).

-behaviour(gen_server).
-include("include/aae.hrl").

-export([init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3]).

-export([aae_start/6,
            aae_start/7,
            aae_nextrebuild/1,
            aae_put/7,
            aae_close/1,
            aae_destroy/1,
            aae_fetchroot/3,
            aae_mergeroot/3,
            aae_fetchbranches/4,
            aae_mergebranches/4,
            aae_fetchclocks/5,
            aae_fetchclocks/7,
            aae_rebuildtrees/5,
            aae_rebuildtrees/4,
            aae_rebuildstore/2,
            aae_fold/6,
            aae_fold/8,
            aae_bucketlist/1,
            aae_loglevel/2,
            aae_ping/3,
            aae_runnerprompt/1]).

-export([foldobjects_buildtrees/2,
            hash_clocks/2,
            wrapped_splitobjfun/1]).

-export([rebuild_worker/1, wait_on_sync/5]).

-export([generate_returnfun/2]).

-include_lib("eunit/include/eunit.hrl").

-define(STORE_PATH, "keystore/").
-define(TREE_PATH, "aaetree/").
-define(MEGA, 1000000).
-define(BATCH_LENGTH, 128).
-define(DEFAULT_REBUILD_SCHEDULE, {1, 300}).
-define(EMPTY, <<>>).
-define(EMPTY_MD, term_to_binary([])).
-define(SYNC_TIMEOUT, 60000).
    % May depend on x2 underlying 30s timeout
-define(MAX_RUNNER_QUEUEDEPTH, 4).


-record(state, {key_store :: pid()|undefined,
                tree_caches = [] :: tree_caches(),
                index_ns = [] :: list(responsible_preflist()),
                initiate_node_worker_fun,
                object_splitfun,
                reliable = false :: boolean(),
                next_rebuild = os:timestamp() :: erlang:timestamp(),
                rebuild_schedule = ?DEFAULT_REBUILD_SCHEDULE 
                    :: rebuild_schedule(),
                broken_trees = false :: boolean(),
                parallel_keystore = true :: boolean(),
                objectspecs_queue = [] :: list(),
                root_path :: list()|undefined,
                runner :: pid()|undefined,
                log_levels :: aae_util:log_levels()|undefined,
                runner_queue = [] :: list(runner_work()),
                queue_backlog = false :: boolean(),
                block_next_put = false :: boolean()}).

-record(options, {keystore_type :: keystore_type(),
                    store_isempty :: boolean(),
                    rebuild_schedule :: rebuild_schedule(),
                    index_ns :: list(responsible_preflist()),
                    object_splitfun,
                    root_path :: list(),
                    log_levels :: aae_util:log_levels()|undefined}).

-type controller_state() :: #state{}.

-type responsible_preflist() :: {integer(), integer()}. 
        % The responsible preflist is a reference to the partition associated 
        % with an AAE requirement.  The preflist is a reference to the id of 
        % the head partition in preflist, and and n-val.
-type tree_caches() 
        :: list({responsible_preflist(), pid()}).
        % A map between the responsible_preflist reference and the tree_cache 
        % for that preflist
-type keystore_type() 
        :: {parallel, aae_keystore:parallel_stores()}|
            {native, aae_keystore:native_stores(), pid()}.
        % Key Store can be native (no separate AAE store required) or
        % parallel when a seperate Key Store is needed for AAE.  The Type
        % for parallel stores must be a supported KV store by the aae_keystore
        % module 
-type rebuild_schedule() 
        :: {non_neg_integer(), pos_integer()}.
        % A rebuild schedule, the first integer being the minimum number of 
        % hours to wait between rebuilds.  The second integer is a number of 
        % seconds by which to jitter the rebuild.  The actual rebuild will be 
        % scheduled by adding a random integer number of seconds (between 0 
        % and the jitter value) to the minimum time
-type version_vector()
        :: list(tuple())|none|undefined.
        % The version vector is normally a list of tuples.  The vector could 
        % be none if this is a put of a new item (when the previous vector 
        % would be none), or a deletion of an existing item (when the current
        % vector would be none).
        %
        % `undefined` is speficially resrved for the case that an object may 
        % be being replaced but the vnode does not know if it is being 
        % replaced.  In this case, it is the responsiblity of the controller 
        % to best determine what the previous version was.
-type returner() :: fun((term()) -> ok).
-type runner_work()
        :: {work,
            fun(() -> term()),
            returner(),
            fun((term()) -> non_neg_integer())}.
-type object_splitter()
        :: fun((binary()) ->
                {pos_integer(), pos_integer(), non_neg_integer(),
                    list(erlang:timestamp()), binary()}).


-export_type([responsible_preflist/0,
                keystore_type/0,
                rebuild_schedule/0,
                version_vector/0,
                runner_work/0]).



%%%============================================================================
%%% API
%%%============================================================================

-spec aae_start(keystore_type(), 
                boolean(), 
                rebuild_schedule(),
                list(responsible_preflist()), 
                list(),
                object_splitter()) -> {ok, pid()}.
%% @doc
%% Start an AAE controller 
%% The ObjectsplitFun must take a vnode object in a binary form and output 
%% {Size, SibCount, IndexHash, LMD, MD}.  If the SplitFun previously outputted
%% {Size, SibCount, IndexHash, null} that output will be converted
aae_start(KeyStoreT, IsEmpty, RebuildSch, Preflists, RootPath, ObjSplitFun) ->
    aae_start(KeyStoreT, IsEmpty, RebuildSch,
                Preflists, RootPath, ObjSplitFun, undefined).

-spec aae_start(keystore_type(), 
                boolean(), 
                rebuild_schedule(),
                list(responsible_preflist()), 
                list(),
                object_splitter(),
                aae_util:log_levels()|undefined) -> {ok, pid()}.

aae_start(KeyStoreT, IsEmpty, RebuildSch,
                Preflists, RootPath, ObjSplitFun, LogLevels) ->
    WrapObjSplitFun = wrapped_splitobjfun(ObjSplitFun),
    AAEopts =
        #options{keystore_type = KeyStoreT,
                    store_isempty = IsEmpty,
                    rebuild_schedule = RebuildSch,
                    index_ns = Preflists,
                    root_path = RootPath,
                    object_splitfun = WrapObjSplitFun,
                    log_levels = LogLevels},
    gen_server:start(?MODULE, [AAEopts], []).


-spec aae_nextrebuild(pid()) -> erlang:timestamp().
%% @doc
%% When is the next keystore rebuild process scheduled for
aae_nextrebuild(Pid) ->
    gen_server:call(Pid, rebuild_time, ?SYNC_TIMEOUT).

-spec aae_put(pid(), responsible_preflist(), 
                            aae_keystore:bucket(), aae_keystore:key(),
                            version_vector(), version_vector(),
                            binary()) -> ok.
%% @doc
%% Put a change into the AAE system - updating the TicTac tree, and the 
%% KeyStore where a parallel Keystore is used.
aae_put(Pid, IndexN, Bucket, Key, CurrentVV, PrevVV, BinaryObj) ->
    gen_server:cast(Pid, 
                    {put, IndexN, Bucket, Key, CurrentVV, PrevVV, BinaryObj}).


-spec aae_fetchroot(pid(), list(responsible_preflist()), returner()) -> ok.
%% @doc
%% Fetch the roots of AAE tree caches for a list of IndexNs returning an
%% indexed list of results using ReturnFun - with a result of `false` in the 
%% special case where no TreeCache exists for that preflist
%%
%% The tree cache could be doing a rebuild, and so may not respond immediately
%% and hence the use of cast/ReturnFun.  Assume that ReturnFun would be a wrap
%% around reply/2 in riak_kv_vnode.
aae_fetchroot(Pid, IndexNs, ReturnFun) ->
    gen_server:cast(Pid, {fetch_root, IndexNs, ReturnFun}).

-spec aae_mergeroot(pid(), list(responsible_preflist()), returner()) -> ok.
%% @doc
%% As with aae_fetch root, but now the reply will be just a single root rather
%% than an indexed list.  The response will now always be a binary - an empty 
%% one where the response is false.
aae_mergeroot(Pid, IndexNs, ReturnFun) ->
    MergeFoldFun =
        fun({_IndexN, Root}, RootAcc) ->
            aae_exchange:merge_root(Root, RootAcc)
        end,
    WrappedReturnFun = 
        fun(Result) ->
            MergedResult = lists:foldl(MergeFoldFun, <<>>, Result),
            ReturnFun(MergedResult)
        end,
    gen_server:cast(Pid, {fetch_root, IndexNs, WrappedReturnFun}).

-spec aae_fetchbranches(pid(), 
                        list(responsible_preflist()), list(integer()), 
                        returner()) -> ok.
%% @doc
%% Fetch the branches of AAE tree caches for a list of IndexNs returning an
%% indexed list of results using ReturnFun - with a result of `false` in the 
%% special case where no TreeCache exists for that preflist
aae_fetchbranches(Pid, IndexNs, BranchIDs, ReturnFun) ->
    gen_server:cast(Pid, {fetch_branches, IndexNs, BranchIDs, ReturnFun}).
    


-spec aae_mergebranches(pid(), 
                        list(responsible_preflist()), list(integer()), 
                        returner()) -> ok.
%% @doc
%% As with fetch branches but the results are merged before sending
aae_mergebranches(Pid, IndexNs, BranchIDs, ReturnFun) ->
    MergeFoldFun =
        fun({_IndexN, Branches}, BranchesAcc) ->
            aae_exchange:merge_branches(Branches, BranchesAcc)
        end,
    WrappedReturnFun = 
        fun(Result) ->
            MergedResult = lists:foldl(MergeFoldFun, [], Result),
            ReturnFun(MergedResult)
        end,
    gen_server:cast(Pid, 
                    {fetch_branches, IndexNs, BranchIDs, WrappedReturnFun}).
    

-spec aae_fetchclocks(pid(), 
                        list(responsible_preflist()),
                        list(non_neg_integer()), 
                            % fetch_clocks assumes "large" tree size
                        returner(),
                        null|fun((term(), term()) -> responsible_preflist()))
                            -> ok.
%% @doc
%% Fetch all the keys and clocks but use the passed in 2-arity function to 
%% determine the IndexN of the object, by applying the function to the bucket
%% and key
%%
%% This is a call, to allow for rehashing of any trees as part of the fetch
%% operation.  If no rebuild is running, then as well as fetching the clocks
%% new segment values can be calculated, replacing the old segment values.
%% By making this a call, the snapshot for the fold is made before any new 
%% PUTs are received by the vnode - so we know any subseqent changes are not
%% included in the fold result. 
aae_fetchclocks(Pid, IndexNs, SegmentIDs, ReturnFun, PrefLFun) ->
    aae_fetchclocks(Pid, IndexNs, all, SegmentIDs, all, ReturnFun, PrefLFun).

-spec aae_fetchclocks(pid(),
                        list(responsible_preflist()),
                        aae_keystore:range_limiter(),
                        list(non_neg_integer()),
                        aae_keystore:modified_limiter(),
                        returner(),
                        null|fun((term(), term()) -> responsible_preflist()))
                            -> ok.
aae_fetchclocks(Pid, IndexNs,
                RLimiter, SLimiter, LMDLimiter,
                ReturnFun, PrefLFun) ->
    gen_server:call(Pid, 
                    {fetch_clocks, IndexNs, 
                        RLimiter, SLimiter, LMDLimiter,
                        ReturnFun, PrefLFun},
                    ?SYNC_TIMEOUT).

-spec aae_fold(pid(), 
                aae_keystore:range_limiter(),
                aae_keystore:segment_limiter(),
                fun((term(), term(), term(), term()) -> term()), 
                any(), 
                list(aae_keystore:value_element())) ->
                    {async, fun(() -> term())}.
%% @doc
%% Return a folder to fold over the keys in the aae_keystore (or native 
%% keystore if in native mode)
aae_fold(Pid, RLimiter, SLimiter, FoldObjectsFun, InitAcc, Elements) ->
    aae_fold(Pid, RLimiter, SLimiter, all, false, 
                FoldObjectsFun, InitAcc, Elements).

-spec aae_fold(pid(), 
                aae_keystore:range_limiter(),
                aae_keystore:segment_limiter(),
                aae_keystore:modified_limiter(),
                aae_keystore:count_limiter(),
                fun((term(), term(), term(), term()) -> term()),
                any(), 
                list(aae_keystore:value_element())) ->
                    {async, fun(() -> term())}.
%% @doc
%% Return a folder to fold over the keys in the aae_keystore (or native 
%% keystore if in native mode)
%%
%% The fold function will need to be of form fun(B, K, EL, Acc), where EL is
%% a list of elements in the object.  Extra elements may be present other than
%% the request elements, so fold functions should use lists:keyfind/3 to fetch
%% specifically requested elements rather than assuming the structure of the
%% ElementList
aae_fold(Pid, RLimiter, SLimiter, LMDLimiter, MaxObjectCount,
            FoldObjectsFun, InitAcc, Elements) ->
    gen_server:call(Pid, 
                    {fold, 
                        RLimiter, SLimiter, LMDLimiter, MaxObjectCount,
                        FoldObjectsFun, InitAcc, 
                        Elements},
                    ?SYNC_TIMEOUT).

-spec aae_close(pid()) -> ok.
%% @doc
%% Closedown the AAE controller.
aae_close(Pid) ->
    gen_server:call(Pid, close, ?SYNC_TIMEOUT).

-spec aae_destroy(pid()) -> ok.
%% @doc
%% Closedown the AAE controller.
aae_destroy(Pid) ->
    gen_server:call(Pid, destroy, ?SYNC_TIMEOUT).

-spec aae_rebuildtrees(pid(), 
                        list(responsible_preflist()), fun()|null, fun(),
                        boolean()) -> ok|skipped|loading. 
%% @doc
%% Rebuild the tree caches for a store.  Note that this rebuilds the caches
%% but not the actual key_store itself (required in the case of parallel 
%% stores).  For parallel store, first call aae_rebuildstore before rebuilding
%% the treecaches.
%%
%% This rebuild requires as inputs:
%% - the Preflists to be rebuilt (we do not assume that preflists stay 
%% constant within a controller)
%% - a Preflist Fun for native stores (to calculate the IndexN as the preflist
%% is not stored).  PreflistFun should be a 2-arity function on the Bucket and
%% Key and return the IndexN
%% - a 2-arity WorkerFun which can be passed a Fold and a FinishFun e.g. 
%% WorkerFun(Folder, FinishFun), with the FinishFun to be called once the 
%% Fold is complete (being passed the result of the Folder()).
aae_rebuildtrees(Pid, IndexNs, PreflistFun, WorkerFun, OnlyIfBroken) ->
    case aae_rebuildtrees(Pid, IndexNs, PreflistFun, OnlyIfBroken) of
        {ok, FoldFun, FinishFun} ->
            WorkerFun(FoldFun, FinishFun),
            ok;
        R ->
            R
    end.

-spec aae_rebuildtrees(pid(), 
                        list(responsible_preflist()), fun()|null,
                        boolean()) -> 
                            {ok, fun(), fun()}|skipped|loading.
%% @doc
%% Call aae_rebuildtrees/4 to avoid use of a passed in WorkerFun
aae_rebuildtrees(Pid, IndexNs, PreflistFun, OnlyIfBroken) ->
    gen_server:call(Pid,
                    {rebuild_trees, 
                        IndexNs, PreflistFun, 
                        OnlyIfBroken},
                    infinity).

-spec aae_rebuildstore(pid(), fun()) -> {ok, fun()|skip, fun()}|ok.
%% @doc
%% Prompt the rebuild of the actual AAE key store.  This should return an
%% object fold fun, and a finish fun.  The object fold fun may be skip if it 
%% is a native store and so no fold is required.  The finish fun should be 
%% called once the fold is completed (or immediately if the fold fun is skip).
%%
%% The SplitValueFun must be able to take the {B, K, V} to be used in the 
%% object fold and convert it into {B, K, {IndexN, CurrentClock}} 
aae_rebuildstore(Pid, SplitObjectFun) ->
    gen_server:call(Pid,
                    {rebuild_store, SplitObjectFun},
                    infinity).

-spec aae_loglevel(pid(), aae_util:log_levels()) -> ok.
%% @doc
%% Set the level for logging in the aae processes this controller manages
aae_loglevel(Pid, LogLevels) ->
    gen_server:cast(Pid, {log_levels, LogLevels}).

-spec aae_bucketlist(pid()) -> {async, fun(() -> list(aae_keystore:bucket()))}.
%% @doc
%% List buckets within the aae_keystore.  This will be efficient for native or
%% key-prdered parallel stores - but not for segment-ordered stores.
aae_bucketlist(Pid) ->
    gen_server:call(Pid, bucket_list, ?SYNC_TIMEOUT).

-spec aae_ping(pid(), erlang:timestamp(), pid()|{sync, pos_integer()})
                                                                -> ok|timeout.
%% @doc
%% Ping the AAE process and it will return (async) the timer difference between
%% now and the passed in timestamp.  The calling process may set a threshold, 
%% and if the timing is over the threshold it may assume the mailbox of the
%% controller is too large, and instead next send a sync ping so that the
%% calling process is blocked until the controller can catch up.  It is
%% expected that this may be used by a vnode that "owns" the controller to
%% resolve the case whereby a vnode may be able to handle PUT load faster than
%% the controller.
%% The sync ping will return 'ok'.
aae_ping(Pid, RequestTime, {sync, Timeout}) ->
    wait_on_sync(gen_server, call, Pid, {ping, RequestTime}, Timeout);
aae_ping(Pid, RequestTime, From) ->
    gen_server:cast(Pid, {ping, RequestTime, From}).

-spec aae_runnerprompt(pid()) -> ok.
%% @doc
%% Let runner prompt to see if there is work for it on the queue
aae_runnerprompt(Pid) ->
    gen_server:cast(Pid, runner_prompt).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    RootPath = Opts#options.root_path,
    RebuildSchedule = Opts#options.rebuild_schedule,
    LogLevels = Opts#options.log_levels,
    % Start the KeyStore
    % Need to update the state to reflect the potential need to rebuild the 
    % key store if the shutdown was not clean as expected
    {ok, State0} = 
        case Opts#options.keystore_type of 
            {parallel, StoreType} ->
                StoreRP = filename:join([RootPath, StoreType, ?STORE_PATH]),
                {ok, {LastRebuild, IsEmpty}, Pid} =
                    aae_keystore:store_parallelstart(StoreRP,
                                                        StoreType,
                                                        LogLevels),
                case Opts#options.store_isempty of 
                    IsEmpty ->
                        RebuildTS = 
                            schedule_rebuild(LastRebuild, RebuildSchedule),
                        {ok, #state{key_store = Pid, 
                                        next_rebuild = RebuildTS, 
                                        reliable = true,
                                        parallel_keystore = true}};
                    StoreState ->
                        aae_util:log("AAE01", 
                                        [StoreState, IsEmpty],
                                        logs(),
                                        LogLevels),
                        {ok, #state{key_store = Pid, 
                                        next_rebuild = os:timestamp(), 
                                        rebuild_schedule = RebuildSchedule,
                                        reliable = false,
                                        parallel_keystore = true}}
                end;
            {native, StoreType, BackendPid} ->
                aae_util:log("AAE02", [StoreType], logs(), LogLevels),
                StoreRP = filename:join([RootPath, StoreType, ?STORE_PATH]),
                {ok, {LastRebuild, _IsE}, KeyStorePid} =
                    aae_keystore:store_nativestart(StoreRP, 
                                                    StoreType, 
                                                    BackendPid,
                                                    LogLevels),
                RebuildTS = 
                    schedule_rebuild(LastRebuild, RebuildSchedule),
                {ok, #state{key_store = KeyStorePid, 
                                next_rebuild = RebuildTS, 
                                rebuild_schedule = RebuildSchedule,
                                reliable = true,
                                parallel_keystore = false}}
        end,

    % Start the TreeCaches
    % Trust any cache that is neatly shutdown, and ignore any cache if the 
    % vnode store is empty.  If caches are not started cleanly as expected 
    % then the prompt_cacherebuild should trigger for them to be rebuilt from
    % the AAE KeyStore (if that itself is not pending a rebuild)  
    StartCacheFun = 
        fun(IndexN, {AllRestored, Caches}) ->
            {IsRestored, Cache} = 
                case Opts#options.store_isempty of 
                    true ->
                        cache(new, IndexN, RootPath, LogLevels);
                    false ->
                        cache(open, IndexN, RootPath, LogLevels)
                end,
            {IsRestored and AllRestored, [{IndexN, Cache}|Caches]}
        end,
    
    {AllTreesOK, TreeCaches} = 
        lists:foldl(StartCacheFun, {true, []}, Opts#options.index_ns),
    
    % Start fetch_clocks runner
    {ok, Runner} = aae_runner:runner_start(LogLevels),

    aae_util:log("AAE10", 
                    [Opts#options.index_ns, Opts#options.keystore_type], 
                    logs(),
                    LogLevels),
    {ok, State0#state{object_splitfun = Opts#options.object_splitfun,
                        index_ns = Opts#options.index_ns,
                        tree_caches = TreeCaches,
                        broken_trees = not AllTreesOK,
                        root_path = RootPath,
                        runner = Runner,
                        log_levels = LogLevels}}.


handle_call(rebuild_time, _From, State) ->  
    {reply, State#state.next_rebuild, State};
handle_call(close, _From, State) ->
    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore,
                            true),
    ok = aae_keystore:store_close(State#state.key_store),
    CloseTCFun = 
        fun({_IndexN, TreeCache}) ->
            ok = aae_treecache:cache_close(TreeCache)
        end,
    lists:foreach(CloseTCFun, State#state.tree_caches),
    ok = aae_runner:runner_stop(State#state.runner),
    {stop, normal, ok, State};
handle_call(destroy, _From, State) ->
    DestroyTCFun = 
        fun({_IndexN, TreeCache}) ->
            ok = aae_treecache:cache_destroy(TreeCache)
        end,
    lists:foreach(DestroyTCFun, State#state.tree_caches),
    ok = aae_keystore:store_destroy(State#state.key_store),
    ok = aae_runner:runner_stop(State#state.runner),
    {stop, normal, ok, State};
handle_call({rebuild_trees, IndexNs, PreflistFun, OnlyIfBroken},
                _From, State) ->
    KeyStore = State#state.key_store,
    LogLevels = State#state.log_levels,
    DontRebuild = (OnlyIfBroken and not State#state.broken_trees),
    case DontRebuild of
        true ->
            {reply, skipped, State};
        false ->
            case aae_keystore:store_currentstatus(KeyStore) of
                {StateName, _GUID}
                        when StateName == native; StateName == parallel ->
                    aae_util:log("AAE06", [IndexNs], logs(), LogLevels),
                    SW = os:timestamp(),
                    % Before the fold flush all the PUTs (if a parallel store)
                    ok = maybe_flush_puts(KeyStore, 
                                            State#state.objectspecs_queue,
                                            State#state.parallel_keystore,
                                            true),
                    
                    % Setup a fold over the store
                    {FoldFun, InitAcc} =
                        foldobjects_buildtrees(IndexNs, LogLevels),
                    CheckPresence = (StateName == native) and not OnlyIfBroken,
                    % If performing a scheduled rebuild on a native store
                    % then the fold needs to check for the presence of the key
                    % in the journal, not just the ledger.  Special range value
                    % is used to trigger CheckPresence 
                    Range =
                        case CheckPresence of
                            true ->
                                all_check;
                            false ->
                                all
                        end,
                    {async, Folder} = 
                        aae_keystore:store_fold(KeyStore, 
                                                Range, all,
                                                all, false,
                                                FoldFun, InitAcc, 
                                                [{preflist, PreflistFun}, 
                                                    {hash, null}]),
                    
                    % Handle the current list of responsible preflists for this
                    % vnode having changed since the last call to start or
                    % rebuild the cached trees
                    SetupCacheFun = 
                        fun(IndexN, TreeCachesAcc) ->
                            TreeCache1 = get_treecache(IndexN, State),
                            ok = aae_treecache:cache_startload(TreeCache1),
                            [{IndexN, TreeCache1}|TreeCachesAcc]
                        end,
                    TreeCaches = lists:foldl(SetupCacheFun, [], IndexNs),

                    % Produce a Finishfun to be called at the end of the Folder
                    % with the input as the results.  This should call 
                    % rebuild_complete on each Tree cache in turn
                    FinishTreeFun =
                        fun({FoldIndexN, FoldTree}) ->
                            {FoldIndexN, TreeCache} = 
                                lists:keyfind(FoldIndexN, 1, TreeCaches),
                            aae_treecache:cache_completeload(TreeCache,
                                                                FoldTree)
                        end,
                    FinishFun = 
                        fun(FoldTreeCaches) ->
                            lists:foreach(FinishTreeFun, FoldTreeCaches),
                            aae_util:log_timer("AAE13",
                                                [], SW, logs(), LogLevels)
                        end,

                    % The IndexNs and TreeCaches supported by the controller
                    % must now be updated to match the lasted provided list of
                    % responsible preflists
                    %
                    % Also should schedule the next rebuild time to the future
                    % based on now as the last rebuild time (we assume the
                    % rebuild of the trees will be successful, and a rebuild of
                    % the store has just been completed)
                    %
                    % Reschedule will not be required if this was an
                    % OnlyIfBroken rebuild (which would normally follow restart
                    % not a store rebuild) and the store is parallel.  This
                    % might otherwise reschedule an outstanding requirement to
                    % rebuild the store.
                    RescheduleRequired = 
                        not (OnlyIfBroken and State#state.parallel_keystore),
                    RebuildTS =
                        case RescheduleRequired of
                            true ->
                                TS =
                                    schedule_rebuild(os:timestamp(), 
                                                State#state.rebuild_schedule),
                                aae_util:log("AAE11", [TS], logs(), LogLevels),
                                TS;
                            false ->
                                State#state.next_rebuild
                        end,
                    {reply, 
                        {ok, Folder, FinishFun}, 
                        State#state{tree_caches = TreeCaches, 
                                        index_ns = IndexNs, 
                                        next_rebuild = RebuildTS,
                                        broken_trees = false}};
                NotReady ->
                    % Normally loading - but could be timeout, because of
                    % loading
                    aae_util:log("AAE16", [NotReady], logs(), LogLevels),
                    {reply, loading, State}
            end
    end;
handle_call({rebuild_store, SplitObjFun}, _From, State)->
    aae_util:log("AAE12", [State#state.parallel_keystore],
                    logs(), State#state.log_levels),
    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore,
                            true),
    ok = aae_keystore:store_prompt(State#state.key_store, rebuild_start),
    case State#state.parallel_keystore of 
        true ->
            FoldObjectsFun = 
                fun(B, K, V, Acc) ->
                    {IdxN, VC} = SplitObjFun(B, K, V),
                    BinaryKey = aae_util:make_binarykey(B, K),
                    SegmentID = 
                        leveled_tictac:keyto_segment48(BinaryKey),
                    {CH, _OH} = hash_clocks(VC, none),
                    ObjSpec = 
                        generate_objectspec(B, K, SegmentID, IdxN,
                                            V, VC, CH, 
                                            State#state.object_splitfun),
                    UpdSpecL = [ObjSpec|Acc],
                    case length(Acc) >= ?BATCH_LENGTH of
                        true ->
                            flush_load(State#state.key_store, UpdSpecL),
                            [];
                        false ->
                            [ObjSpec|UpdSpecL]
                    end
                end,
            FinishFun =
                fun(Acc) ->
                    flush_load(State#state.key_store, Acc),
                    ok = aae_keystore:store_prompt(State#state.key_store,
                                                    rebuild_complete)
                end,
            {reply, {ok, FoldObjectsFun, FinishFun}, State};
        false ->
            ok = aae_keystore:store_prompt(State#state.key_store,
                                            rebuild_complete),
            {reply, ok, State}
    end;
handle_call({fold, RLimiter, SLimiter, LMDLimiter, MaxObjectCount,
                    FoldObjectsFun, InitAcc, Elements},  _From, State) ->
    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore,
                            true),
    R = aae_keystore:store_fold(State#state.key_store, 
                                RLimiter,
                                SLimiter,
                                LMDLimiter,
                                MaxObjectCount,
                                FoldObjectsFun, 
                                InitAcc,
                                Elements),
    {reply, R, State};
handle_call({fetch_clocks,
                _IndexNs,
                _RLimiter, _SegmentIDs, _LMDLimiter,
                ReturnFun, _PreflFun},
                    _From, State=#state{queue_backlog=QB}) when QB ->
    %% There is a backlog of queries for the runner.  Add some dummy work to
    %% the queue.  The dummy work will prevent a snapshot being taken which may
    %% otherwise have to live the length of the queue.  It also makes sure the
    %% exchange response is delayed by the length of the queue - so that the
    %% exchange will still slow down.
    Folder = fun() -> query_backlog end,
    SizeFun = fun() -> 0 end,
    Queue = State#state.runner_queue ++ [{work, Folder, ReturnFun, SizeFun}],
    {reply, ok, State#state{runner_queue = Queue}};
handle_call({fetch_clocks,
                IndexNs, 
                all, SegmentIDs, all, 
                ReturnFun, PreflFun},
                    From, State) ->
    ImmediateReply = State#state.parallel_keystore,
    case ImmediateReply of
        true ->
            %% When this is a parallel store, there is no need benefit on
            %% waiting before replying - as no race conditions will be
            %% avoided.
            gen_server:reply(From, ok);
        _ ->
            %% In native mode, don't want a new PUT to be received before the
            %% snapshot has been taken, so block until the snapshot has been
            %% taken
            ok
    end,
    SegmentMap = lists:map(fun(S) -> {S, 0} end, SegmentIDs),
    InitMap = 
        lists:map(fun(IdxN) -> 
                        {IdxN, get_treecache(IdxN, State), SegmentMap} 
                    end, 
                    IndexNs),

    GUID = leveled_util:generate_uuid(),
    lists:foreach(fun({_IndexN, Tree, _SegMap}) -> 
                        ok = aae_treecache:cache_markdirtysegments(Tree, 
                                                                    SegmentIDs,
                                                                    GUID)
                    end,
                    InitMap),
    
    FoldObjFun = 
        fun(B, K, V, {Acc, SubTreeAcc}) ->
            {clock, VC} = lists:keyfind(clock, 1, V),
            {aae_segment, SegID} = lists:keyfind(aae_segment, 1, V),
            {hash, H} = lists:keyfind(hash, 1, V),
            {preflist, PL} = lists:keyfind(preflist, 1, V),
            {PL, T, SegMap} = lists:keyfind(PL, 1, SubTreeAcc),
            {SegID, HashAcc} = lists:keyfind(SegID, 1, SegMap),
            BinK = aae_util:make_binarykey(B, K),
            {_, HashToAdd} =  leveled_tictac:tictac_hash(BinK, {is_hash, H}),
            UpdHash = HashAcc bxor HashToAdd,
            SegMap0 = lists:keyreplace(SegID, 1, SegMap, {SegID, UpdHash}),
            SubTreeAcc0 =
                lists:keyreplace(PL, 1, SubTreeAcc,  {PL, T, SegMap0}),
            {[{B, K, VC}|Acc], SubTreeAcc0}
        end,
    WrappedFoldObjFun = preflist_wrapper_fun(FoldObjFun, IndexNs),
    
    ReturnFun0 = generate_returnfun(GUID, ReturnFun),
    SizeFun =
        fun({KeyClockList, _SubTree}) ->
            length(KeyClockList)
        end,
    
    Range =
        case (not State#state.parallel_keystore) of
            true ->
                % If we discover a broken journal file via a rebuild, don't
                % want to falsely repair it through the fetch_clocks process.
                all_check;
            _ ->
                all
        end,

    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore,
                            true),
    {async, Folder} = 
        aae_keystore:store_fold(State#state.key_store, 
                                Range,
                                {segments, SegmentIDs, ?TREE_SIZE},
                                all,
                                false, 
                                WrappedFoldObjFun, 
                                {[], InitMap},
                                [{preflist, PreflFun}, 
                                    {clock, null},
                                    {aae_segment, null},
                                    {hash, null}]),
    Queue = State#state.runner_queue ++ [{work, Folder, ReturnFun0, SizeFun}],
    Backlog = length(Queue) >= ?MAX_RUNNER_QUEUEDEPTH,
    %% If there is a backlog, the queue will remain in backlog state until it
    %% is empty.  Whilst in backlog state, query requests will result in dummy
    %% work being added to the queue not real work (which may cause the queue
    %% to continue to grow)
    S0 = State#state{queue_backlog = Backlog, runner_queue = Queue},
    case ImmediateReply of
        true ->
            {noreply, S0};
        _ ->
            {reply, ok, S0}
    end;
handle_call({fetch_clocks,
                IndexNs, 
                RLimiter, SegmentIDs, LMDLimiter, 
                ReturnFun, PreflFun},
                    From, State) ->
    ImmediateReply = State#state.parallel_keystore,
    case ImmediateReply of
        true ->
            %% When this is a parallel store, there is no need benefit on
            %% waiting before replying - as no race conditions will be
            %% avoided.  Also, there may be a build up of PUTs, and a risk of
            %% timeout when waiting for maybe_flush_puts/4.  There are
            %% better mechanisms available to slow the calling process in the
            %% case of backlog, avoiding overload - see aae_ping/3.  So, reply
            %% immediately if in parallel mode 
            gen_server:reply(From, ok);
        _ ->
            %% In native mode, don't want a new PUT to be received before the
            %% snapshot has been taken, so block until the snapshot has been
            %% taken
            ok
    end,
    
    FoldObjFun = 
        fun(B, K, V, Acc) ->
            {clock, VC} = lists:keyfind(clock, 1, V),
            [{B, K, VC}|Acc]
        end,
    WrappedFoldObjFun = preflist_wrapper_fun(FoldObjFun, IndexNs),
    SizeFun =
        fun(KeyClockList) ->
            length(KeyClockList)
        end,

    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore,
                            true),
    {async, Folder} = 
        aae_keystore:store_fold(State#state.key_store, 
                                RLimiter,
                                {segments, SegmentIDs, ?TREE_SIZE},
                                LMDLimiter,
                                false, 
                                WrappedFoldObjFun, 
                                [],
                                [{preflist, PreflFun}, 
                                    {clock, null},
                                    {aae_segment, null}]),
    Queue = State#state.runner_queue ++ [{work, Folder, ReturnFun, SizeFun}],
    Backlog = length(Queue) >= ?MAX_RUNNER_QUEUEDEPTH,
    %% If there is a backlog, the queue will remain in backlog state until it
    %% is empty.  Whilst in backlog state, query requests will result in dummy
    %% work being added to the queue not real work (which may cause the queue
    %% to continue to grow)
    S0 = State#state{queue_backlog = Backlog, runner_queue = Queue},
    case ImmediateReply of
        true ->
            %% In parallel mode a reply has already been sent
            {noreply, S0};
        _ ->
            {reply, ok, S0}
    end;
handle_call(bucket_list,  _From, State) ->
    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore,
                            true),
    R = aae_keystore:store_bucketlist(State#state.key_store),
    {reply, R, State};
handle_call({ping, RequestTime}, _From, State) ->
    T = max(0, timer:now_diff(os:timestamp(), RequestTime)),
    aae_util:log("AAE15", [T div 1000], logs(), State#state.log_levels),
    {reply, ok, State#state{block_next_put = true}}.


handle_cast({put, IndexN, Bucket, Key, Clock, PrevClock, BinaryObj}, State) ->
    % Setup
    TreeCaches = State#state.tree_caches,
    BinaryKey = aae_util:make_binarykey(Bucket, Key),
    PrevClock0 = 
        case PrevClock of 
            undefined ->
                case State#state.parallel_keystore of 
                    true ->
                        resolve_clock(Bucket, Key, 
                                        State#state.key_store, 
                                        State#state.objectspecs_queue);
                    false ->
                        % An inert change will be generated
                        Clock
                end;
            _ ->
                PrevClock
        end,
                
    {CH, OH} = hash_clocks(Clock, PrevClock0),
    
    % Update the TreeCache associated with the Key (should a cache exist for
    % that store)
    State0 = 
        case lists:keyfind(IndexN, 1, TreeCaches) of 
            false ->
                % Note that this will eventually end up in the Tree Cache if in
                % the future the IndexN combination is added to the list of
                % responsible preflists
                handle_unexpected_key(Bucket, Key, IndexN, TreeCaches,
                                        State#state.log_levels),
                State#state{next_rebuild = os:timestamp()};
            {IndexN, TreeCache} ->
                ok = aae_treecache:cache_alter(TreeCache, BinaryKey, CH, OH),
                State
        end,

    % Batch up an update to the Key Store
    %
    % If we receive an unexpected key - still include it in the Key Store,
    % perhaps a new bucket has been configured withe a new IndexN.  When 
    % the next cache rebuild happens, the latest IndexNs will be passed in and
    % then the unexpected key will be included in the cache
    case State0#state.parallel_keystore of 
        true ->
            SegmentID = leveled_tictac:keyto_segment48(BinaryKey),
            ObjSpec =  generate_objectspec(Bucket, Key, SegmentID, IndexN,
                                            BinaryObj, 
                                            Clock, CH, 
                                            State#state.object_splitfun),
            UpdSpecL = [ObjSpec|State0#state.objectspecs_queue],
            case length(UpdSpecL) >= ?BATCH_LENGTH of 
                true ->
                    % Push to the KeyStore as batch is now at least full
                    maybe_flush_puts(State0#state.key_store,
                                        UpdSpecL,
                                        true,
                                        State#state.block_next_put),
                    {noreply, State0#state{objectspecs_queue = [],
                                            block_next_put = false}};
                false ->
                    {noreply, State0#state{objectspecs_queue = UpdSpecL}}
            end;
        false ->
            {noreply, State0}
    end;
handle_cast({fetch_root, IndexNs, ReturnFun}, State) ->
    FetchRootFun = 
        fun(IndexN) ->
            Root = 
                case lists:keyfind(IndexN, 1, State#state.tree_caches) of 
                    {IndexN, TreeCache} ->
                        aae_treecache:cache_root(TreeCache);
                    false ->
                        aae_util:log("AAE04", [IndexN],
                                        logs(), State#state.log_levels),
                        ?EMPTY
                end,
            {IndexN, Root}
        end,
    Result = lists:map(FetchRootFun, IndexNs),
    ReturnFun(Result),
    {noreply, State};
handle_cast({fetch_branches, IndexNs, BranchIDs, ReturnFun}, State) ->
    FetchBranchFun = 
        fun(IndexN) ->
            Leaves = 
                case lists:keyfind(IndexN, 1, State#state.tree_caches) of 
                    {IndexN, TreeCache} ->
                        aae_treecache:cache_leaves(TreeCache, BranchIDs);
                    false ->
                        aae_util:log("AAE04", [IndexN],
                                        logs(), State#state.log_levels),
                        lists:map(fun(X) -> {X, ?EMPTY} end, BranchIDs)
                end,
            {IndexN, Leaves}
        end,
    Result = lists:map(FetchBranchFun, IndexNs),
    ReturnFun(Result),
    {noreply, State};
handle_cast({log_levels, LogLevels}, State) ->
    UpdateCache =
        fun({_I, TC}) -> aae_treecache:cache_loglevel(TC, LogLevels) end,
    lists:foreach(UpdateCache, State#state.tree_caches),
    ok = aae_keystore:store_loglevel(State#state.key_store, LogLevels),
    {noreply, State#state{log_levels = LogLevels}};
handle_cast({ping, RequestTime, From}, State) ->
    From ! {aae_pong, max(0, timer:now_diff(os:timestamp(), RequestTime))},
    {noreply, State#state{block_next_put = true}};
handle_cast(runner_prompt, State) ->
    case State#state.runner_queue of
        [] ->
            %% Only the queue becoming empty can take the queue out of backlog
            %% state
            ok = aae_runner:runner_work(State#state.runner, queue_empty),
            {noreply, State#state{queue_backlog = false}};
        [WI|Tail] ->
            ok = aae_runner:runner_work(State#state.runner, WI),
            {noreply, State#state{runner_queue = Tail}}
    end.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% External functions
%%%============================================================================

-spec foldobjects_buildtrees(list(responsible_preflist()),
                                aae_util:log_levels()) -> {fun(), list()}.
%% @doc
%% Return an object fold fun for building hashtrees, with an initialised 
%% accumulator
foldobjects_buildtrees(IndexNs, LogLevels) ->
    InitMapFun  = 
        fun(IndexN) ->
            {IndexN, leveled_tictac:new_tree(IndexN, ?TREE_SIZE)}
        end,
    InitAcc = lists:map(InitMapFun, IndexNs),
    
    FoldObjectsFun = 
        fun(B, K, V, Acc) ->
            {preflist, IndexN} = lists:keyfind(preflist, 1, V),
            {hash, Hash} = lists:keyfind(hash, 1, V),
            BinK = aae_util:make_binarykey(B, K),
            BinExtractFun = fun(_BK, _V) -> {BinK, {is_hash, Hash}} end,
            case lists:keyfind(IndexN, 1, Acc) of 
                {IndexN, Tree} ->
                    Tree0 = 
                        leveled_tictac:add_kv(Tree, 
                                                {null}, {null},
                                                    % BinExtractfun will ignore
                                                    % this dummy key and value
                                                    % and substitute its own 
                                                    % pre-defined values to 
                                                    % generate segment ID and 
                                                    % hash
                                                BinExtractFun),
                    lists:keyreplace(IndexN, 1, Acc, {IndexN, Tree0});
                false ->
                    aae_util:log("AAE14", [IndexN], logs(), LogLevels),
                    Acc 
            end
        end,
    
    {FoldObjectsFun, InitAcc}.
    
wrapped_splitobjfun(ObjectSplitFun) ->
    fun(Obj) ->
        case ObjectSplitFun(Obj) of
            {Size, SibCount, IndexHash, Val} ->
                Val0 =
                    case Val of
                        null -> ?EMPTY_MD;
                        ValB when is_binary(ValB) -> ValB
                    end,
                {Size, SibCount, IndexHash, undefined, Val0};
            T ->
                T
        end
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================


-spec generate_returnfun(string(), fun((any()) -> ok)) ->
            fun(({error, term()}|{list(), list()}) -> ok).
generate_returnfun(GUID, ReturnFun) ->
    ReplaceFun = 
        fun({_IdxN, T, SegM}) ->
            aae_treecache:cache_replacedirtysegments(T, SegM, GUID)
        end,
    fun(Response) ->
        case Response of
            {error, Reason} ->
                ReturnFun({error, Reason});
            {KeyClockList, SubTree} ->
                ReturnFun(KeyClockList),
                lists:foreach(ReplaceFun, SubTree)
        end
    end.

-spec get_treecache(responsible_preflist(), controller_state()) -> pid().
%% @doc
%% Fetch the tree cache from state, creating a new tree cache if it isn't 
%% present
get_treecache(IndexN, State)->
    case lists:keyfind(IndexN, 1, State#state.tree_caches) of 
        {IndexN, TreeCache0} ->
            TreeCache0;
        false ->
            LogLevels = State#state.log_levels,
            aae_util:log("AAE09", [IndexN], logs(), LogLevels),
            {true, NC} = cache(new, IndexN, State#state.root_path, LogLevels),
            NC
    end.

-spec resolve_clock(binary(), binary(), pid(), list()) -> version_vector().
%% @doc
%% Get the Keystore to return the current clock or none if the key is not 
%% present
resolve_clock(Bucket, Key, Store, PutQueue) ->
    QR = lists:foldl(fun check_queuefun/2, {Bucket, Key, false}, PutQueue),
    case QR of 
        {Bucket, Key, false} ->
            aae_keystore:store_fetchclock(Store, Bucket, Key);
        {Bucket, Key, Clock} ->
            Clock
    end.

check_queuefun(ObjectSpec, {Bucket, Key, false}) ->
    case aae_keystore:check_objectspec(Bucket, Key, ObjectSpec) of
        {ok, null} ->
            {Bucket, Key, none};
        {ok, Value} ->
            Clock = aae_keystore:value(parallel, 
                                        {clock, null}, 
                                        {Bucket, Key, Value}),
            {Bucket, Key, Clock};
        false ->
            {Bucket, Key, false}
    end;
check_queuefun(_ObjectSpec, {Bucket, Key, Value}) ->
    {Bucket, Key, Value}.

-spec maybe_flush_puts(pid(), list(), boolean(), boolean()) -> ok.
%% @doc
%% Flush all the puts into the store.  The Puts have been queued with the most
%% recent PUT at the head.  
maybe_flush_puts(Store, ObjSpecL, true, Blocking) ->
    aae_keystore:store_mput(Store, ObjSpecL, Blocking);
maybe_flush_puts(_Store, _ObjSpecL, false, _Blocking) ->
    ok.


-spec flush_load(pid(), list()) -> ok.
%% @doc
%% Flush all the puts into the store.  The Puts have been queued with the most
%% recent PUT at the head.  Loading is used when the store is being rebuilt
flush_load(Store, ObjSpecL) ->
    aae_keystore:store_mload(Store, ObjSpecL).

-spec cache(new|open, responsible_preflist(), list(), aae_util:log_levels())
                                                        -> {boolean(), pid()}.
%% @doc
%% Start a new tree cache, return a boolean along with the Pid to indicate 
%% if the opening of the cache was clean (i.e. the cache had been saved and 
%% checksummed correctly when last saved)
cache(Startup, IndexN, RootPath, LogLevels) ->
    TreeRP = filename:join(RootPath, ?TREE_PATH),
    case Startup of 
        new ->
            {ok, NC} = aae_treecache:cache_new(TreeRP, IndexN, LogLevels),
            {true, NC};
        open ->
            aae_treecache:cache_open(TreeRP, IndexN, LogLevels)
    end.


-spec schedule_rebuild(erlang:timestamp()|never, rebuild_schedule()) 
                                                        -> erlang:timestamp().
%% @doc
%% Set a rebuild time based on the last rebuild time and the rebuild schedule
schedule_rebuild(never, Schedule) ->
    schedule_rebuild(os:timestamp(), Schedule);
schedule_rebuild({MegaSecs, Secs, MicroSecs}, {MinHours, JitterSeconds}) ->
    SlotCount = min(1024, max(JitterSeconds, 1)),
    SlotSize = JitterSeconds div SlotCount,
    P = self(),
    Slot = erlang:phash2({P, MicroSecs}, SlotCount),
    NewSecs = 
        MegaSecs * ?MEGA 
            + Secs 
            + MinHours * 3600 + SlotSize * Slot,
    {NewSecs div ?MEGA, NewSecs rem ?MEGA, MicroSecs}.


-spec generate_objectspec(binary(), binary(), leveled_tictac:segment48(), 
                            tuple(),
                            binary(), version_vector(), integer()|none, 
                            fun()) -> tuple().
%% @doc                            
%% Generate an object specification for a parallel key store
generate_objectspec(Bucket, Key, SegmentID, _IndexN,
                        _BinaryObj, none, _CurrentHash, 
                        _SplitFun) ->
    SegTree_int = aae_keystore:generate_treesegment(SegmentID),
    aae_keystore:define_delobjectspec(Bucket, Key, SegTree_int);
generate_objectspec(Bucket, Key, SegmentID, IndexN,
                        BinaryObj, CurrentVV, CurrentHash, 
                        SplitFun) ->
    SegTree_int = aae_keystore:generate_treesegment(SegmentID),
    Value = aae_keystore:generate_value(IndexN, 
                                        SegTree_int,
                                        CurrentVV, 
                                        CurrentHash, 
                                        SplitFun(BinaryObj)),
    aae_keystore:define_addobjectspec(Bucket, Key, Value).


-spec handle_unexpected_key(binary(), binary(), tuple(),
                            list(tuple()), aae_util:log_levels()) -> ok.
%% @doc
%% Log out that an unexpected key has been seen
handle_unexpected_key(Bucket, Key, IndexN, TreeCaches, LogLevels) ->
    RespPreflists = lists:map(fun({RP, _TC}) ->  RP end, TreeCaches),
    aae_util:log("AAE03", [Bucket, Key, IndexN, RespPreflists],
                    logs(), LogLevels).

-spec hash_clocks(version_vector(), version_vector()) 
                                                    -> {integer(), integer()}.
%% @doc
%% Has the version vectors 
hash_clocks(CurrentVV, PrevVV) ->
    {hash_clock(CurrentVV), hash_clock(PrevVV)}.

hash_clock(none) ->
    0;
hash_clock(Clock) ->
    erlang:phash2(lists:sort(Clock)).

-spec wait_on_sync(atom(), atom(), pid(), tuple()|atom(), pos_integer())
                                                                    -> any().
%% @doc
%% Wait on a sync call until timeout - but don't crash on the timeout
wait_on_sync(Mod, Fun, Pid, Call, Timeout) ->
    try Mod:Fun(Pid, Call, Timeout)
    catch exit:{timeout, _} -> timeout
    end.

-spec preflist_wrapper_fun(
        fun((term(), term(), term(), term()) -> term()),
        list(responsible_preflist())) -> 
        fun((term(), term(), term(), term()) -> term()).
preflist_wrapper_fun(FoldObjectsFun, IndexNs) ->
    fun(B, K, V, Acc) ->
        {preflist, PL} = lists:keyfind(preflist, 1, V),
        case lists:member(PL, IndexNs) of
            true ->
                FoldObjectsFun(B, K, V, Acc);
            false ->
                Acc
        end
    end.


%%%============================================================================
%%% log definitions
%%%============================================================================

-spec logs() -> list(tuple()).
%% @doc
%% Define log lines for this module
logs() ->
    [{"AAE01", 
            {warn, "AAE Key Store rebuild required on startup due to " 
                    ++ "mismatch between vnode store state ~w "
                    ++ "and AAE key store state of ~w "
                    ++ "maybe restart with node excluded from coverage "
                    ++ "queries to improve AAE operation until rebuild "
                    ++ "is complete"}},
        {"AAE02",
            {info, "Native KeyStore type ~w startup request"}},
        {"AAE03",
            {debug, "Unexpected Bucket ~w Key ~w passed with IndexN ~w "
                    "that does not match any of ~w"}},
        {"AAE04",
            {warn, "Misrouted request for IndexN ~w"}},
        

        {"AAE06",
            {info, "Received rebuild trees request for IndexNs ~w"}},
        {"AAE07",
            {info, "Dispatching test fold"}},
        {"AAE08",
            {info, "Spawned worker receiving test fold"}},
        {"AAE09",
            {info, "Change in IndexNs detected at rebuild - new IndexN ~w"}},
        {"AAE10",
            {info, "AAE controller started with IndexNs ~w and StoreType ~w"}},
        {"AAE11",
            {info, "Next rebuild scheduled for ~w"}},
        {"AAE12",
            {info, "Received rebuild store for parallel store ~w"}},
        {"AAE13",
            {info, "Completed tree rebuild"}},
        {"AAE14",
            {debug, "Mismatch finding unexpected IndexN in fold of ~w"}},
        {"AAE15",
            {info, "Ping showed time difference of ~w ms"}},
        {"AAE16",
            {info, "Keystore ~w when tree rebuild requested"}}
    
    ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-define(TEST_DEFAULT_PARTITION, {0, 3}).
-define(ALTERNATIVE_PARTITION, {1, 3}).
-define(TEST_MINHOURS, 1).
-define(TEST_JITTERSECONDS, 300).

rebuild_notempty_test() ->
    RootPath = "test/notemptycntrllr/",
    aae_util:clean_subdir(RootPath),
    {ok, Cntrl0} = start_wrap(false, RootPath, leveled_so),
    NRB0 = aae_nextrebuild(Cntrl0),
    ?assertMatch(true, NRB0 < os:timestamp()),
    
    % Shutdown was with rebuild due - so should not reset the rebuild to
    % future 
    ok = aae_close(Cntrl0),
    {ok, Cntrl1} = start_wrap(false, RootPath, leveled_so),
    NRB1 = aae_nextrebuild(Cntrl1),
    ?assertMatch(true, NRB1 < os:timestamp()),

    ok = aae_put(Cntrl1, ?TEST_DEFAULT_PARTITION, 
                <<"B">>, <<"K">>, [{a, 1}], none, <<>>),
    NRB1 = aae_nextrebuild(Cntrl1),
    ok = aae_put(Cntrl1, ?ALTERNATIVE_PARTITION, 
                <<"B">>, <<"K0">>, [{a, 1}], none, <<>>),
    NRB2 = aae_nextrebuild(Cntrl1),
    ?assertMatch(true, NRB2 > NRB1),
    ?assertMatch(true, NRB2 < os:timestamp()),
    % The rebuild time should have been reset 
    ok = aae_close(Cntrl1),
    aae_util:clean_subdir(RootPath).

rebuild_onempty_test() ->
    RootPath = "test/emptycntrllr/",
    aae_util:clean_subdir(RootPath),
    {ok, Cntrl0} = start_wrap(true, RootPath, leveled_so),
    NRB0 = aae_nextrebuild(Cntrl0),
    ?assertMatch(false, NRB0 < os:timestamp()),
    
    % Shutdown and startup 
    ok = aae_close(Cntrl0),
    {ok, Cntrl1} = start_wrap(true, RootPath, leveled_so),
    NRB1 = aae_nextrebuild(Cntrl1),
    ?assertMatch(false, NRB1 < os:timestamp()),
    
    % Shutdown then startup with wrong ISEmpty state
    ok = aae_close(Cntrl1),
    {ok, Cntrl2} = start_wrap(false, RootPath, leveled_so),
    NRB2 = aae_nextrebuild(Cntrl2),
    ?assertMatch(true, NRB2 < os:timestamp()),
    
    ok = aae_close(Cntrl2),
    aae_util:clean_subdir(RootPath).


shutdown_parallel_rebuild_test() ->
    Start = 
        calendar:datetime_to_gregorian_seconds(
            calendar:now_to_datetime(os:timestamp())),
    RootPath = "test/shutdownpllrbld/",
    aae_util:clean_subdir(RootPath),
    {ok, Cntrl0} = start_wrap(true, RootPath, leveled_so),
    ok = aae_put(Cntrl0, 
                    ?TEST_DEFAULT_PARTITION, 
                    <<"B">>, <<"K">>, [{a, 1}], [], <<>>),
    NR_TS0 = calendar:now_to_datetime(aae_nextrebuild(Cntrl0)),
    GS_TS0 = calendar:datetime_to_gregorian_seconds(NR_TS0),
    ?assertMatch(true, GS_TS0 > (?TEST_MINHOURS * 3600 + Start)),
    ok = aae_close(Cntrl0),

    TreePath = 
        filename:join(RootPath, ?TREE_PATH) ++ "/",
    aae_util:clean_subdir(TreePath),

    {ok, Cntrl1} = start_wrap(false, RootPath, leveled_so),
    ok = aae_rebuildtrees(Cntrl1, 
                            [?TEST_DEFAULT_PARTITION], 
                            null, workerfun(fun(ok) -> ok end), 
                            true),
    NR_TS1 = calendar:now_to_datetime(aae_nextrebuild(Cntrl1)),
    GS_TS1 = calendar:datetime_to_gregorian_seconds(NR_TS1),
    ?assertMatch(true, GS_TS1 > (?TEST_MINHOURS * 3600 + Start)),

    ok = aae_close(Cntrl1),
    aae_util:clean_subdir(RootPath).


overload_runner_test_() ->
    {timeout, 60, fun overloadrunner_tester/0}.

shudown_parallel_test_() ->
    {timeout, 60, fun shutdown_parallel_tester/0}.

wrong_indexn_test_() ->
    {timeout, 60, fun wrong_indexn_tester/0}.

rebuildso_test_() ->
    {timeout, 60, fun() -> basic_cache_rebuild_tester(leveled_so) end}.

rebuildko_test_() ->
    {timeout, 60, fun() -> basic_cache_rebuild_tester(leveled_ko) end}.

vary_indexnso_test_() ->
    {timeout, 60, fun() -> varyindexn_cache_rebuild_tester(leveled_so) end}.

vary_indexnlo_test_() ->
    {timeout, 60, fun() -> varyindexn_cache_rebuild_tester(leveled_ko) end}.


overloadrunner_tester() ->
    RootPath = "test/overloadrunner/",
    aae_util:clean_subdir(RootPath),
    {ok, Cntrl0} = start_wrap(false, RootPath, leveled_so),
    ok = aae_put(Cntrl0, {1, 3}, <<"B">>, <<"K">>, [{a, 1}], [], <<>>),
    BinaryKey1 = aae_util:make_binarykey(<<"B">>, <<"K">>),
    SegID1 = 
        leveled_tictac:get_segment(leveled_tictac:keyto_segment32(BinaryKey1), 
                                    ?TREE_SIZE),
    
    RPid = self(),
    ReturnFun = fun(R) -> RPid ! {result, R} end,

    FetchFun =
        fun(_N) ->
            ok = aae_fetchclocks(Cntrl0, [{1, 3}], [SegID1], ReturnFun, null)
        end,
    CatchFun =
        fun(_N) ->
            Result0 = start_receiver(),
            io:format("Result0 of ~w~n", [Result0]),
            ?assertMatch([{<<"B">>,<<"K">>,[{a,1}]}], Result0)
        end,
    lists:foreach(FetchFun, lists:seq(1, ?MAX_RUNNER_QUEUEDEPTH + 1)),
    lists:foreach(CatchFun, lists:seq(1, ?MAX_RUNNER_QUEUEDEPTH)),
    OverloadResult = start_receiver(),
    ?assertMatch({error, query_backlog}, OverloadResult),

    lists:foreach(FetchFun, lists:seq(1, ?MAX_RUNNER_QUEUEDEPTH)),
    lists:foreach(CatchFun, lists:seq(1, ?MAX_RUNNER_QUEUEDEPTH)),

    ok = aae_close(Cntrl0),
    aae_util:clean_subdir(RootPath).


shutdown_parallel_tester() ->
    RootPath = "test/shutdownpll/",
    aae_util:clean_subdir(RootPath),
    {ok, Cntrl0} = start_wrap(false, RootPath, leveled_so),
    ok = aae_put(Cntrl0, {1, 3}, <<"B">>, <<"K">>, [{a, 1}], [], <<>>),
    BinaryKey1 = aae_util:make_binarykey(<<"B">>, <<"K">>),
    SegmentID1 = 
        leveled_tictac:get_segment(leveled_tictac:keyto_segment32(BinaryKey1), 
                                    ?TREE_SIZE),
    
    RPid = self(),
    ReturnFun = fun(R) -> RPid ! {result, R} end,
    ok = aae_fetchclocks(Cntrl0, [{1, 3}], [SegmentID1], ReturnFun, null),
    Result0 = start_receiver(),
    io:format("Result0 of ~w~n", [Result0]),
    ?assertMatch([{<<"B">>,<<"K">>,[{a,1}]}], Result0),
    
    % at this close the PUT has been flushed because of the fold
    ok = aae_close(Cntrl0),

    {ok, Cntrl1} = start_wrap(true, RootPath, leveled_so),
    
    ok = aae_fetchclocks(Cntrl1, [{1, 3}], [SegmentID1], ReturnFun, null),
    Result1 = start_receiver(),
    io:format("Result1 of ~w~n", [Result1]),
    ?assertMatch([{<<"B">>,<<"K">>,[{a,1}]}], Result1),

    ok = aae_put(Cntrl1, {1, 3}, <<"B">>, <<"K0">>, [{b, 1}], [], <<>>),
    BinaryKey2 = aae_util:make_binarykey(<<"B">>, <<"K0">>),
    SegmentID2 = 
        leveled_tictac:get_segment(leveled_tictac:keyto_segment32(BinaryKey2), 
                                    ?TREE_SIZE),

    % Don't fold - so the PUT must be flushed by the close
    ok = aae_close(Cntrl1),
    {ok, Cntrl2} = start_wrap(true, RootPath, leveled_so),

    ok = aae_fetchclocks(Cntrl2,[{1, 3}], [SegmentID1, SegmentID2], 
                            ReturnFun, null),
    Result2 = start_receiver(),
    io:format("Result2 of ~w~n", [Result2]),
    ExpResult2 = [{<<"B">>,<<"K">>,[{a,1}]}, {<<"B">>, <<"K0">>, [{b, 1}]}],
    ?assertMatch(ExpResult2, lists:usort(Result2)),

    ok = aae_close(Cntrl2),
    aae_util:clean_subdir(RootPath).


wrong_indexn_tester() ->
    RootPath = "test/emptycntrllr/",
    aae_util:clean_subdir(RootPath),

    RPid = self(),
    ReturnFun = fun(R) -> RPid ! {result, R} end,

    {ok, Cntrl0} = start_wrap(true, RootPath, leveled_so),
    NRB0 = aae_controller:aae_nextrebuild(Cntrl0),
    ?assertMatch(false, NRB0 < os:timestamp()),
    
    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, ZeroB0}] = start_receiver(),
    ?assertMatch(<<0:131072/integer>>, ZeroB0),

    ok = aae_fetchroot(Cntrl0, [{1, 3}], ReturnFun),
    [{{1, 3}, F0}] = start_receiver(),
    ?assertMatch(?EMPTY, F0),
    
    io:format("Put entry - wrong index~n"),
    ok = aae_put(Cntrl0, {1, 3}, <<"B">>, <<"K">>, [{a, 1}], [], <<>>),
    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, Root1}] = start_receiver(),
    ?assertMatch(<<0:131072/integer>>, Root1),
    
    ok = aae_fetchroot(Cntrl0, [{1, 3}], ReturnFun),
    [{{1, 3}, F1}] = start_receiver(),
    ?assertMatch(?EMPTY, F1),
    
    io:format("Put entry - correct index same key~n"),
    ok = aae_put(Cntrl0, {0, 3}, <<"B">>, <<"K">>, [{c, 1}], [], <<>>),
    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, Root2}] = start_receiver(),
    ?assertMatch(false, <<0:131072/integer>> == Root2),

    ok = aae_fetchroot(Cntrl0, [{1, 3}], ReturnFun),
    [{{1, 3}, F2}] = start_receiver(),
    ?assertMatch(?EMPTY, F2),
    
    BranchIDL = leveled_tictac:find_dirtysegments(Root1, Root2),
    ?assertMatch(1, length(BranchIDL)),
    [BranchID] = BranchIDL, 
    
    ok = aae_fetchbranches(Cntrl0, [{0, 3}], BranchIDL, ReturnFun),
    [{{0,3}, [{BranchID, Branch3}]}] = start_receiver(),
    ?assertMatch(false, <<0:131072/integer>> == Branch3),

    SegIDL = leveled_tictac:find_dirtysegments(Branch3, <<0:8192>>),
    ?assertMatch(1, length(SegIDL)),
    [SubSegID] = SegIDL,
    SegID = 256 * BranchID + SubSegID,
    {BB, KB} = {<<"B">>, <<"K">>},
    ExpSegID = 
        leveled_tictac:keyto_segment32(<<BB/binary, KB/binary>>)
            band (1024 * 1024 - 1),
    ?assertMatch(ExpSegID, SegID),
    io:format("SegID ~w ExpSegID ~w~n", [SegID, ExpSegID]),
    
    ok = aae_fetchclocks(Cntrl0, [{0, 3}], [SegID], ReturnFun, null),
    KC4 = start_receiver(),
    % Should find new key
    ?assertMatch([{<<"B">>, <<"K">>, [{c, 1}]}], KC4),

    ok = aae_fetchclocks(Cntrl0, [{1, 3}], [SegID], ReturnFun, null),
    KC5 = start_receiver(),
    % Shouldn't find old key - has been replaced by new key
    ?assertMatch([], KC5),
    
    ok = aae_close(Cntrl0),
    aae_util:clean_subdir(RootPath).


basic_cache_rebuild_tester(StoreType) ->
    RootPath = "test/emptycntrllr/",
    aae_util:clean_subdir(RootPath),

    RPid = self(),
    ReturnFun = fun(R) -> RPid ! {result, R} end,

    Preflists = [{0, 3}, {100, 3}, {200, 3}],
    {ok, Cntrl0} = start_wrap(true, RootPath, Preflists, StoreType),
    NRB0 = aae_controller:aae_nextrebuild(Cntrl0),
    ?assertMatch(false, NRB0 < os:timestamp()),

    PKL = put_keys(Cntrl0, Preflists, [], 5000),
    {RepL, Rest0} = lists:split(1000, PKL),
    {RemL, Rest1} = lists:split(1000, Rest0),
    RKL = replace_keys(Cntrl0, RepL, []),
    ok = remove_keys(Cntrl0, RemL),
    _KVL = lists:sort(RKL ++ Rest1),

    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, Root0}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{100, 3}], ReturnFun),
    [{{100,3}, Root1}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{200, 3}], ReturnFun),
    [{{200,3}, Root2}] = start_receiver(),

    ok = aae_rebuildtrees(Cntrl0, 
                            Preflists, 
                            null,
                            workerfun(ReturnFun),
                            false),
    ok = aae_fetchclocks(Cntrl0,
                            Preflists, lists:seq(1, 16),
                            fun(_R) -> ok end,
                            null),
    timer:sleep(2100),
        % Must wait the prompt for the fetch to happen (and a touch)
    ok = start_receiver(),

    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, RB_Root0}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{100, 3}], ReturnFun),
    [{{100,3}, RB_Root1}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{200, 3}], ReturnFun),
    [{{200,3}, RB_Root2}] = start_receiver(),

    SegIDL = leveled_tictac:find_dirtysegments(Root0, RB_Root0),
    io:format("Count of dirty segments in IndexN 0 ~w~n", [length(SegIDL)]),
    ?assertMatch(0, length(SegIDL)),

    ?assertMatch(Root0, RB_Root0),
    ?assertMatch(Root1, RB_Root1),
    ?assertMatch(Root2, RB_Root2),
    
    ok = aae_destroy(Cntrl0),
    aae_util:clean_subdir(RootPath).


varyindexn_cache_rebuild_tester(StoreType) ->
    RootPath = "test/emptycntrllr/",
    aae_util:clean_subdir(RootPath),

    RPid = self(),
    ReturnFun = fun(R) -> RPid ! {result, R} end,

    Preflists = [{0, 3}, {100, 3}, {200, 3}],
    {ok, Cntrl0} = start_wrap(true, RootPath, Preflists, StoreType),
    skipped = aae_rebuildtrees(Cntrl0, 
                                Preflists, 
                                null,
                                workerfun(ReturnFun),
                                true),
    NRB0 = aae_controller:aae_nextrebuild(Cntrl0),
    ?assertMatch(false, NRB0 < os:timestamp()),

    % Note now adding a preflist to the keys being loaded.
    % There housl be no tree cache for these keys, but they should be added
    % to the Key store
    UpdPreflists = [{300, 3}|Preflists],
    PKL = put_keys(Cntrl0, UpdPreflists, [], 5000),
    {RepL, Rest0} = lists:split(1000, PKL),
    {RemL, Rest1} = lists:split(1000, Rest0),
    RKL = replace_keys(Cntrl0, RepL, []),
    ok = remove_keys(Cntrl0, RemL),
    _KVL = lists:sort(RKL ++ Rest1),

    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, Root0}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{100, 3}], ReturnFun),
    [{{100,3}, Root1}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{200, 3}], ReturnFun),
    [{{200,3}, Root2}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{300, 3}], ReturnFun),
    [{{300,3}, Root3}] = start_receiver(),
    ?assertMatch(?EMPTY, Root3),

    ok = aae_rebuildtrees(Cntrl0, 
                            UpdPreflists, 
                            null,
                            workerfun(ReturnFun),
                            false),
    ok = start_receiver(),

    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, RB1_Root0}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{100, 3}], ReturnFun),
    [{{100,3}, RB1_Root1}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{200, 3}], ReturnFun),
    [{{200,3}, RB1_Root2}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{300, 3}], ReturnFun),
    [{{300,3}, RB1_Root3}] = start_receiver(),
    
    ?assertMatch(true, is_binary(RB1_Root3)),

    SegIDL = leveled_tictac:find_dirtysegments(Root0, RB1_Root0),
    io:format("Count of dirty segments in IndexN 0 ~w~n", [length(SegIDL)]),
    ?assertMatch(0, length(SegIDL)),

    ?assertMatch(Root0, RB1_Root0),
    ?assertMatch(Root1, RB1_Root1),
    ?assertMatch(Root2, RB1_Root2),

    ok = aae_rebuildtrees(Cntrl0, 
                            Preflists, 
                            null,
                            workerfun(ReturnFun),
                            false),
    ok = start_receiver(),

    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, RB2_Root0}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{100, 3}], ReturnFun),
    [{{100,3}, RB2_Root1}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{200, 3}], ReturnFun),
    [{{200,3}, RB2_Root2}] = start_receiver(),
    ok = aae_fetchroot(Cntrl0, [{300, 3}], ReturnFun),
    [{{300,3}, RB2_Root3}] = start_receiver(),
    ?assertMatch(?EMPTY, RB2_Root3),

    SegIDL = leveled_tictac:find_dirtysegments(Root0, RB1_Root0),
    io:format("Count of dirty segments in IndexN 0 ~w~n", [length(SegIDL)]),
    ?assertMatch(0, length(SegIDL)),

    ?assertMatch(Root0, RB2_Root0),
    ?assertMatch(Root1, RB2_Root1),
    ?assertMatch(Root2, RB2_Root2),
    
    ok = aae_fetchbranches(Cntrl0, [{300, 3}], [1], ReturnFun),
    [{{300, 3}, RB2_Branch3}] = start_receiver(),
    ?assertMatch([{1, ?EMPTY}], RB2_Branch3),

    ok = aae_close(Cntrl0),
    aae_util:clean_subdir(RootPath).


coverage_cheat_test() ->
    {noreply, _State0} = handle_info(null, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).



%%%============================================================================
%%% Test Utils
%%%============================================================================


start_wrap(IsEmpty, RootPath, StoreType) ->
    start_wrap(IsEmpty, RootPath, [?TEST_DEFAULT_PARTITION], StoreType).

start_wrap(IsEmpty, RootPath, RPL, StoreType) ->
    F = fun(_X) -> {0, 1, 0, null} end,
    aae_start({parallel, StoreType}, 
                IsEmpty, 
                {?TEST_MINHOURS, ?TEST_JITTERSECONDS}, 
                RPL, RootPath, F).


put_keys(Cntrl, _Preflists, KeyList, 0) ->
    ok = aae_ping(Cntrl, os:timestamp(), {sync, 10000}),
    KeyList;
put_keys(Cntrl, Preflists, KeyList, Count) ->
    Preflist = lists:nth(leveled_rand:uniform(length(Preflists)), Preflists),
    Bucket = integer_to_binary(Count rem 5),  
    Key = list_to_binary(string:right(integer_to_list(Count), 6, $0)),
    VersionVector = add_randomincrement([]),
    ok = aae_put(Cntrl, 
                    Preflist, 
                    Bucket, 
                    Key, 
                    VersionVector, 
                    none, 
                    <<>>),
    put_keys(Cntrl, 
                Preflists, 
                [{Bucket, Key, VersionVector, Preflist}|KeyList], 
                Count - 1).
    
replace_keys(_Cntrl, [], OutList) ->
    OutList;
replace_keys(Cntrl, [{B, K, C, PL}|Rest], OutList) ->
    NewC = add_randomincrement(C),
    ok = aae_put(Cntrl, PL, B, K, NewC, C, <<>>),
    replace_keys(Cntrl, Rest, [{B, K, NewC, PL}|OutList]).

remove_keys(_Cntrl, []) ->
    ok;
remove_keys(Cntrl, [{B, K, C, PL}|Rest]) ->
    ok = aae_put(Cntrl, PL, B, K, none, C, <<>>),
    remove_keys(Cntrl, Rest).

add_randomincrement(Clock) ->
    RandIncr = leveled_rand:uniform(100),
    RandNode = lists:nth(leveled_rand:uniform(9), 
                            ["a", "b", "c", "d", "e", "f", "g", "h", "i"]),
    UpdClock = 
        case lists:keytake(RandNode, 1, Clock) of 
            false ->
                [{RandNode, RandIncr}|Clock];
            {value, {RandNode, Incr0}, Rest} ->
                [{RandNode, Incr0 + RandIncr}|Rest]
        end,
    lists:usort(UpdClock).

workerfun(ReturnFun) ->
    WorkerPid = spawn(?MODULE, rebuild_worker, [ReturnFun]),
    fun(FoldFun, FinishFun) ->
        aae_util:log("AAE07", [], logs()),
        WorkerPid! {fold, FoldFun, FinishFun}
    end.

start_receiver() ->
    receive
        {result, Reply} ->
            Reply 
    end.

rebuild_worker(ReturnFun) ->
    receive
        {fold, FoldFun, FinishFun} ->
            aae_util:log("AAE08", [], logs()),
            FinishFun(FoldFun()),
            ReturnFun(ok)
    end.

returnfun_test() ->
    CheckFun =
        fun(ReturnTuple) -> ?assertMatch(error, element(1, ReturnTuple)) end,
    ReturnFun = generate_returnfun("ABCD", CheckFun),
    ok = ReturnFun({error, queue_backlog}).

wrap_splitfun_test() ->
    SplitObjFun = 
        fun(_Obj) -> {1000, 2, 0, term_to_binary(null)} end,
    WrappedFun = wrapped_splitobjfun(SplitObjFun),
    SplitObj = WrappedFun(null),
    ?assertMatch(true, is_tuple(SplitObj)),
    ?assertMatch(5, tuple_size(SplitObj)),
    ?assertMatch(undefined, element(4, SplitObj)). 

-endif.

