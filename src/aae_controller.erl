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
            aae_nextrebuild/1,
            aae_put/7,
            aae_close/1,
            aae_fetchroot/3,
            aae_mergeroot/3,
            aae_fetchbranches/4,
            aae_mergebranches/4,
            aae_fetchclocks/5,
            aae_rebuildtrees/5,
            aae_rebuildstore/2,
            aae_fold/5]).

-export([foldobjects_buildtrees/1,
            hash_clocks/2]).

-export([rebuild_worker/1]).

-include_lib("eunit/include/eunit.hrl").

-define(STORE_PATH, "keystore/").
-define(TREE_PATH, "aaetree/").
-define(MEGA, 1000000).
-define(BATCH_LENGTH, 128).
-define(DEFAULT_REBUILD_SCHEDULE, {1, 300}).


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
                runner :: pid()|undefined}).

-record(options, {keystore_type :: keystore_type(),
                    store_isempty :: boolean(),
                    rebuild_schedule :: rebuild_schedule(),
                    index_ns :: list(responsible_preflist()),
                    object_splitfun,
                    root_path :: list()}).

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
        :: {integer(), integer()}.
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
    

-export_type([responsible_preflist/0,
                keystore_type/0,
                rebuild_schedule/0,
                version_vector/0]).



%%%============================================================================
%%% API
%%%============================================================================

-spec aae_start(keystore_type(), 
                boolean(), 
                rebuild_schedule(),
                list(responsible_preflist()), 
                list(),
                fun()) -> {ok, pid()}.
%% @doc
%% Start an AAE controller 
%% The ObjectsplitFun must take a vnode object in a binary form and output 
%% {Size, SibCount, IndexHash, _Head}
aae_start(KeyStoreT, IsEmpty, RebuildSch, Preflists, RootPath, ObjSplitFun) ->
    AAEopts =
        #options{keystore_type = KeyStoreT,
                    store_isempty = IsEmpty,
                    rebuild_schedule = RebuildSch,
                    index_ns = Preflists,
                    root_path = RootPath,
                    object_splitfun = ObjSplitFun},
    gen_server:start(?MODULE, [AAEopts], []).


-spec aae_nextrebuild(pid()) -> erlang:timestamp().
%% @doc
%% When is the next keystore rebuild process scheduled for
aae_nextrebuild(Pid) ->
    gen_server:call(Pid, rebuild_time).

-spec aae_put(pid(), responsible_preflist(), 
                            binary(), binary(), 
                            version_vector(), version_vector(),
                            binary()) -> ok.
%% @doc
%% Put a change into the AAE system - updating the TicTac tree, and the 
%% KeyStore where a parallel Keystore is used.
aae_put(Pid, IndexN, Bucket, Key, CurrentVV, PrevVV, BinaryObj) ->
    gen_server:cast(Pid, 
                    {put, IndexN, Bucket, Key, CurrentVV, PrevVV, BinaryObj}).


-spec aae_fetchroot(pid(), list(responsible_preflist()), fun()) -> ok.
%% @doc
%% Fetch the roots of AAE tree caches for a list of IndexNs returning an
%% indexed list of results using ReturnFun - with a result of `false` in the 
%% special case where no TreeCache exists for that preflist
%%
%% The tree cache could be doing a rbeuild, and so may not respond immediately
%% and hence the use of cast/ReturnFun.  Assume that ReturnFun would be a wrap
%% around reply/2 in riak_kv_vnode.
aae_fetchroot(Pid, IndexNs, ReturnFun) ->
    gen_server:cast(Pid, {fetch_root, IndexNs, ReturnFun}).

-spec aae_mergeroot(pid(), list(responsible_preflist()), fun()) -> ok.
%% @doc
%% As with aae_fetch root, but now the reply will be just a single root rather
%% than an indexed list.  The response will now always be a binary - an empty 
%% one where the response is false.
aae_mergeroot(Pid, IndexNs, ReturnFun) ->
    MergeFoldFun =
        fun({_IndexN, Root}, RootAcc) ->
            case Root of 
                false -> RootAcc;
                R -> aae_exchange:merge_root(R, RootAcc)
            end
        end,
    WrappedReturnFun = 
        fun(Result) ->
            MergedResult = lists:foldl(MergeFoldFun, <<>>, Result),
            ReturnFun(MergedResult)
        end,
    gen_server:cast(Pid, {fetch_root, IndexNs, WrappedReturnFun}).

-spec aae_fetchbranches(pid(), 
                        list(responsible_preflist()), list(integer()), 
                        fun()) -> ok.
%% @doc
%% Fetch the branches of AAE tree caches for a list of IndexNs returning an
%% indexed list of results using ReturnFun - with a result of `false` in the 
%% special case where no TreeCache exists for that preflist
aae_fetchbranches(Pid, IndexNs, BranchIDs, ReturnFun) ->
    gen_server:cast(Pid, {fetch_branches, IndexNs, BranchIDs, ReturnFun}).
    


-spec aae_mergebranches(pid(), 
                        list(responsible_preflist()), list(integer()), 
                        fun()) -> ok.
%% @doc
%% As with fetch branches but the results are merged before sending
aae_mergebranches(Pid, IndexNs, BranchIDs, ReturnFun) ->
    MergeFoldFun =
        fun({_IndexN, Branches}, BranchesAcc) ->
            Branches0 = 
                case Branches of 
                    false -> [];
                    Bs -> Bs
                end,
            aae_exchange:merge_branches(Branches0, BranchesAcc)
        end,
    WrappedReturnFun = 
        fun(Result) ->
            MergedResult = lists:foldl(MergeFoldFun, [], Result),
            ReturnFun(MergedResult)
        end,
    gen_server:cast(Pid, 
                    {fetch_branches, IndexNs, BranchIDs, WrappedReturnFun}).
    

-spec aae_fetchclocks(pid(), 
                        list(responsible_preflist()), list(integer()), 
                        fun(), null|fun()) -> ok.
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
    gen_server:call(Pid, 
                    {fetch_clocks, IndexNs, SegmentIDs, ReturnFun, PrefLFun}).

-spec aae_fold(pid(), aae_keystore:fold_limiter(), fun(), any(), 
                        list(aae_keystore:value_element())) -> {async, fun()}.
%% @doc
%% Return a folder to fold over the keys in the aae_keystore (or native 
%% keystore if in native mode)
aae_fold(Pid, Limiter, FoldObjectsFun, InitAcc, Elements) ->
    gen_server:call(Pid, {fold, Limiter, FoldObjectsFun, InitAcc, Elements}).

-spec aae_close(pid()) -> ok.
%% @doc
%% Closedown the AAE controller.
aae_close(Pid) ->
    gen_server:call(Pid, close, 30000).


-spec aae_rebuildtrees(pid(), 
                        list(responsible_preflist()), fun()|null, fun(),
                        boolean()) -> ok|skipped. 
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
    gen_server:call(Pid, {rebuild_trees, 
                            IndexNs, PreflistFun, WorkerFun, 
                            OnlyIfBroken}).


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
    gen_server:call(Pid, {rebuild_store, SplitObjectFun}).



%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    RootPath = Opts#options.root_path,
    RebuildSchedule = Opts#options.rebuild_schedule,
    % Start the KeyStore
    % Need to update the state to reflect the potential need to rebuild the 
    % key store if the shutdown was not clean as expected
    {ok, State0} = 
        case Opts#options.keystore_type of 
            {parallel, StoreType} ->
                StoreRP = filename:join([RootPath, StoreType, ?STORE_PATH]),
                {ok, {LastRebuild, IsEmpty}, Pid} =
                    aae_keystore:store_parallelstart(StoreRP, StoreType),
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
                                        logs()),
                        {ok, #state{key_store = Pid, 
                                        next_rebuild = os:timestamp(), 
                                        rebuild_schedule = RebuildSchedule,
                                        reliable = false,
                                        parallel_keystore = true}}
                end;
            {native, StoreType, BackendPid} ->
                aae_util:log("AAE02", [StoreType], logs()),
                StoreRP = filename:join([RootPath, StoreType, ?STORE_PATH]),
                {ok, {LastRebuild, _IsE}, KeyStorePid} =
                    aae_keystore:store_nativestart(StoreRP, 
                                                    StoreType, 
                                                    BackendPid),
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
                        cache(new, IndexN, RootPath);
                    false ->
                        cache(open, IndexN, RootPath)
                end,
            {IsRestored and AllRestored, [{IndexN, Cache}|Caches]}
        end,
    
    {AllTreesOK, TreeCaches} = 
        lists:foldl(StartCacheFun, {true, []}, Opts#options.index_ns),
    
    % Start clock runner
    {ok, Runner} = aae_runner:runner_start(),

    aae_util:log("AAE10", 
                    [Opts#options.index_ns, Opts#options.keystore_type], 
                    logs()),
    {ok, State0#state{object_splitfun = Opts#options.object_splitfun,
                        index_ns = Opts#options.index_ns,
                        tree_caches = TreeCaches,
                        broken_trees = not AllTreesOK,
                        root_path = RootPath,
                        runner = Runner}}.


handle_call(rebuild_time, _From, State) ->  
    {reply, State#state.next_rebuild, State};
handle_call(close, _From, State) ->
    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore),
    ok = aae_keystore:store_close(State#state.key_store),
    CloseTCFun = 
        fun({_IndexN, TreeCache}) ->
            ok = aae_treecache:cache_close(TreeCache)
        end,
    lists:foreach(CloseTCFun, State#state.tree_caches),
    ok = aae_runner:runner_stop(State#state.runner),
    {stop, normal, ok, State};
handle_call({rebuild_trees, IndexNs, PreflistFun, WorkerFun, OnlyIfBroken}, 
                _From, State) ->
    DontRebuild = OnlyIfBroken and not State#state.broken_trees,
    case DontRebuild of
        true ->
            {reply, skipped, State};
        false ->
            aae_util:log("AAE06", [IndexNs], logs()),
            SW = os:timestamp(),
            % Before the fold flush all the PUTs (if a parallel store)
            ok = maybe_flush_puts(State#state.key_store, 
                                    State#state.objectspecs_queue,
                                    State#state.parallel_keystore),
            
            % Setup a fold over the store
            {FoldFun, InitAcc} = foldobjects_buildtrees(IndexNs),
            {async, Folder} = 
                aae_keystore:store_fold(State#state.key_store, 
                                        all, 
                                        FoldFun, InitAcc, 
                                        [{preflist, PreflistFun}, 
                                            {hash, null}]),
            
            % Handle the current list of responsible preflists for this vnode 
            % having changed since the last call to start or rebuild the 
            % cache trees
            SetupCacheFun = 
                fun(IndexN, TreeCachesAcc) ->
                    TreeCache1 = get_treecache(IndexN, State),
                    ok = aae_treecache:cache_startload(TreeCache1),
                    [{IndexN, TreeCache1}|TreeCachesAcc]
                end,
            TreeCaches = lists:foldl(SetupCacheFun, [], IndexNs),

            % Produce a Finishfun to be called at the end of the Folder with
            % the input as the results.  This should call rebuild_complete on
            % each Tree cache in turn
            FinishTreeFun =
                fun({FoldIndexN, FoldTree}) ->
                    {FoldIndexN, TreeCache} = 
                        lists:keyfind(FoldIndexN, 1, TreeCaches),
                    aae_treecache:cache_completeload(TreeCache, FoldTree) 
                end,
            FinishFun = 
                fun(FoldTreeCaches) ->
                    lists:foreach(FinishTreeFun, FoldTreeCaches),
                    aae_util:log_timer("AAE13", [], SW, logs())
                end,

            WorkerFun(Folder, FinishFun),

            % The IndexNs and TreeCaches supported by the controller must now
            % be updated to match the lasted provided list of responsible
            % preflists
            %
            % Also should schedule the next rebuild time to the future based on
            % now as the last rebuild time (we assume the rebuild of the trees
            % will be successful, and a rebuild of the store has just been
            % completed)
            %
            % Reschedule will not be required if this was an OnlyIfBroken
            % rebuild (which would normally follow restart not a store
            % rebuild) and the store is parallel.  This might otherwise
            % reschedule an outstanding requirement to rebuild the store.
            RescheduleRequired = 
                not (OnlyIfBroken and State#state.parallel_keystore),
            case RescheduleRequired of
                true ->
                    RebuildTS = 
                        schedule_rebuild(os:timestamp(), 
                                            State#state.rebuild_schedule),
                    aae_util:log("AAE11", [RebuildTS], logs()),
                    {reply, 
                        ok, 
                        State#state{tree_caches = TreeCaches, 
                                        index_ns = IndexNs, 
                                        next_rebuild = RebuildTS,
                                        broken_trees = false}};
                false ->
                    {reply,
                        ok,
                        State#state{tree_caches = TreeCaches, 
                                        index_ns = IndexNs,
                                        broken_trees = false}}
            end
    end;
handle_call({rebuild_store, SplitObjFun}, _From, State)->
    aae_util:log("AAE12", [State#state.parallel_keystore], logs()),
    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore),
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
handle_call({fold, Limiter, FoldObjectsFun, InitAcc, Elements}, 
                                                            _From, State) ->
    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore),
    R = aae_keystore:store_fold(State#state.key_store, 
                                Limiter, 
                                FoldObjectsFun, 
                                InitAcc,
                                Elements),
    {reply, R, State};
handle_call({fetch_clocks, IndexNs, SegmentIDs, ReturnFun, PreflFun},
                                                            _From, State) ->
    
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
            {preflist, PL} = lists:keyfind(preflist, 1, V),
            case lists:member(PL, IndexNs) of   
                true ->
                    {clock, VC} = lists:keyfind(clock, 1, V),
                    {aae_segment, SegID} = lists:keyfind(aae_segment, 1, V),
                    {hash, H} = lists:keyfind(hash, 1, V),
                    {PL, T, SegMap} = lists:keyfind(PL, 1, SubTreeAcc),
                    {SegID, HashAcc} = lists:keyfind(SegID, 1, SegMap),
                    BinK = aae_util:make_binarykey(B, K),
                    {_, HashToAdd} = 
                        leveled_tictac:tictac_hash(BinK, {is_hash, H}),
                    UpdHash = HashAcc bxor HashToAdd,
                    SegMap0 = 
                        lists:keyreplace(SegID, 1, SegMap, {SegID, UpdHash}),
                    SubTreeAcc0 = lists:keyreplace(PL, 1, SubTreeAcc, 
                                                    {PL, T, SegMap0}),
                    {[{B, K, VC}|Acc], SubTreeAcc0};
                false ->
                    {Acc, SubTreeAcc} 
            end 
        end,
    
    ReturnFun0 = 
        fun({KeyClockList, SubTree}) ->
            ReturnFun(KeyClockList),
            ReplaceFun = 
                fun({_IdxN, T, SegM}) ->
                    aae_treecache:cache_replacedirtysegments(T, SegM, GUID)
                end,
            lists:foreach(ReplaceFun, SubTree)
        end,
    SizeFun =
        fun({KeyClockList, _SubTree}) ->
            length(KeyClockList)
        end,

    ok = maybe_flush_puts(State#state.key_store, 
                            State#state.objectspecs_queue,
                            State#state.parallel_keystore),
    {async, Folder} = 
        aae_keystore:store_fold(State#state.key_store, 
                                {segments, SegmentIDs}, 
                                FoldObjFun, 
                                {[], InitMap},
                                [{preflist, PreflFun}, 
                                    {clock, null},
                                    {aae_segment, null},
                                    {hash, null}]),
    aae_runner:runner_clockfold(State#state.runner, 
                                Folder, 
                                ReturnFun0, 
                                SizeFun),
    {reply, ok, State}.

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
    case lists:keyfind(IndexN, 1, TreeCaches) of 
        false ->
            % Note that this will eventually end up in the Tree Cache if in 
            % the future the IndexN combination is added to the list of 
            % responsible preflists
            handle_unexpected_key(Bucket, Key, IndexN, TreeCaches);
        {IndexN, TreeCache} ->
            ok = aae_treecache:cache_alter(TreeCache, BinaryKey, CH, OH)
    end,

    % Batch up an update to the Key Store
    %
    % If we receive an unexpected key - still include it in the Key Store,
    % perhaps a new bucket has been configured withe a new IndexN.  When 
    % the next cache rebuild happens, the latest IndexNs will be passed in and
    % then the unexpected key will be included in the cache
    case State#state.parallel_keystore of 
        true ->
            SegmentID = leveled_tictac:keyto_segment48(BinaryKey),
            ObjSpec =  generate_objectspec(Bucket, Key, SegmentID, IndexN,
                                            BinaryObj, 
                                            Clock, CH, 
                                            State#state.object_splitfun),
            UpdSpecL = [ObjSpec|State#state.objectspecs_queue],
            case length(UpdSpecL) >= ?BATCH_LENGTH of 
                true ->
                    % Push to the KeyStore as batch is now at least full
                    maybe_flush_puts(State#state.key_store, UpdSpecL, true),
                    {noreply, State#state{objectspecs_queue = []}};
                false ->
                    {noreply, State#state{objectspecs_queue = UpdSpecL}}
            end;
        false ->
            {noreply, State}
    end;
handle_cast({fetch_root, IndexNs, ReturnFun}, State) ->
    FetchRootFun = 
        fun(IndexN) ->
            Root = 
                case lists:keyfind(IndexN, 1, State#state.tree_caches) of 
                    {IndexN, TreeCache} ->
                        aae_treecache:cache_root(TreeCache);
                    false ->
                        aae_util:log("AAE04", [IndexN], logs()),
                        false
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
                        aae_util:log("AAE04", [IndexN], logs()),
                        false
                end,
            {IndexN, Leaves}
        end,
    Result = lists:map(FetchBranchFun, IndexNs),
    ReturnFun(Result),
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% External functions
%%%============================================================================

-spec foldobjects_buildtrees(list(responsible_preflist())) -> {fun(), list()}.
%% @doc
%% Return an object fold fun for building hashtrees, with an initialised 
%% accumulator
foldobjects_buildtrees(IndexNs) ->
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
            BinExtractFun = 
                fun(_BK, _V) -> 
                    {BinK, {is_hash, Hash}} end,
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
                    Acc 
            end
        end,
    
    {FoldObjectsFun, InitAcc}.
    


%%%============================================================================
%%% Internal functions
%%%============================================================================


-spec get_treecache(responsible_preflist(), controller_state()) -> pid().
%% @doc
%% Fetch the tree cache from state, creating a new tree cache if it isn't 
%% present
get_treecache(IndexN, State)->
    case lists:keyfind(IndexN, 1, State#state.tree_caches) of 
        {IndexN, TreeCache0} ->
            TreeCache0;
        false ->
            aae_util:log("AAE09", [IndexN], logs()),
            {true, NC} = cache(new, IndexN, State#state.root_path),
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

-spec maybe_flush_puts(pid(), list(), boolean()) -> ok.
%% @doc
%% Flush all the puts into the store.  The Puts have been queued with the most
%% recent PUT at the head.  
maybe_flush_puts(Store, ObjSpecL, true) ->
    aae_keystore:store_mput(Store, ObjSpecL);
maybe_flush_puts(_Store, _ObjSpecL, false) ->
    ok.


-spec flush_load(pid(), list()) -> ok.
%% @doc
%% Flush all the puts into the store.  The Puts have been queued with the most
%% recent PUT at the head.  Loading is used when the store is being rebuilt
flush_load(Store, ObjSpecL) ->
    aae_keystore:store_mload(Store, ObjSpecL).

-spec cache(new|open, responsible_preflist(), list()) -> {boolean(), pid()}.
%% @doc
%% Start a new tree cache, return a boolean along with the Pid to indicate 
%% if the opening of the cache was clean (i.e. the cache had been saved and 
%% checksummed correctly when last saved)
cache(Startup, IndexN, RootPath) ->
    TreeRP = filename:join(RootPath, ?TREE_PATH),
    case Startup of 
        new ->
            {ok, NC} = aae_treecache:cache_new(TreeRP, IndexN),
            {true, NC};
        open ->
            aae_treecache:cache_open(TreeRP, IndexN)
    end.


-spec schedule_rebuild(erlang:timestamp()|never, rebuild_schedule()) 
                                                        -> erlang:timestamp().
%% @doc
%% Set a rebuild time based on the last rebuild time and the rebuild schedule
schedule_rebuild(never, Schedule) ->
    schedule_rebuild(os:timestamp(), Schedule);
schedule_rebuild({MegaSecs, Secs, MicroSecs}, {MinHours, JitterSeconds}) ->
    NewSecs = 
        MegaSecs * ?MEGA 
            + Secs 
            + MinHours * 3600 + leveled_rand:uniform(JitterSeconds),
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
    aae_keystore:define_objectspec(remove, SegTree_int, Bucket, Key, null);
generate_objectspec(Bucket, Key, SegmentID, IndexN,
                        BinaryObj, CurrentVV, CurrentHash, 
                        SplitFun) ->
    SegTree_int = aae_keystore:generate_treesegment(SegmentID),
    KSV = aae_keystore:generate_value(IndexN, 
                                        SegTree_int,
                                        CurrentVV, 
                                        CurrentHash, 
                                        SplitFun(BinaryObj)),
    aae_keystore:define_objectspec(add, SegTree_int, Bucket, Key, KSV).


-spec handle_unexpected_key(binary(), binary(), tuple(), list(tuple())) -> ok.
%% @doc
%% Log out that an unexpected key has been seen
handle_unexpected_key(Bucket, Key, IndexN, TreeCaches) ->
    RespPreflists = lists:map(fun({RP, _TC}) ->  RP end, TreeCaches),
    aae_util:log("AAE03", [Bucket, Key, IndexN, RespPreflists], logs()).

-spec hash_clocks(version_vector(), version_vector()) 
                                                    -> {integer(), integer()}.
%% @doc
%% Has the version vectors 
hash_clocks(CurrentVV, PrevVV) ->
    {hash_clock(CurrentVV), hash_clock(PrevVV)}.

hash_clock(none) ->
    0;
hash_clock(Clock) ->
    erlang:phash2(Clock).

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
            {info, "Completed tree rebuild"}}
    
    ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-define(TEST_DEFAULT_PARTITION, {0, 3}).
-define(TEST_MINHOURS, 1).

rebuild_notempty_test() ->
    RootPath = "test/notemptycntrllr/",
    aae_util:clean_subdir(RootPath),
    {ok, Cntrl0} = start_wrap(false, RootPath, leveled_so),
    NRB0 = aae_controller:aae_nextrebuild(Cntrl0),
    ?assertMatch(true, NRB0 < os:timestamp()),
    
    % Shutdown was with rebuild due - so should not reset the rebuild to
    % future 
    ok = aae_close(Cntrl0),
    {ok, Cntrl1} = start_wrap(false, RootPath, leveled_so),
    NRB1 = aae_controller:aae_nextrebuild(Cntrl1),
    ?assertMatch(true, NRB1 < os:timestamp()),
    
    aae_util:clean_subdir(RootPath).

rebuild_onempty_test() ->
    RootPath = "test/emptycntrllr/",
    aae_util:clean_subdir(RootPath),
    {ok, Cntrl0} = start_wrap(true, RootPath, leveled_so),
    NRB0 = aae_controller:aae_nextrebuild(Cntrl0),
    ?assertMatch(false, NRB0 < os:timestamp()),
    
    % Shutdown and startup 
    ok = aae_close(Cntrl0),
    {ok, Cntrl1} = start_wrap(true, RootPath, leveled_so),
    NRB1 = aae_controller:aae_nextrebuild(Cntrl1),
    ?assertMatch(false, NRB1 < os:timestamp()),
    
    % Shutdown then startup with wrong ISEmpty state
    ok = aae_close(Cntrl1),
    {ok, Cntrl2} = start_wrap(false, RootPath, leveled_so),
    NRB2 = aae_controller:aae_nextrebuild(Cntrl2),
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



shutdown_parallel_test() ->
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


wrong_indexn_test() ->
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
    ?assertMatch(false, F0),
    
    io:format("Put entry - wrong index~n"),
    ok = aae_put(Cntrl0, {1, 3}, <<"B">>, <<"K">>, [{a, 1}], [], <<>>),
    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, Root1}] = start_receiver(),
    ?assertMatch(<<0:131072/integer>>, Root1),
    
    ok = aae_fetchroot(Cntrl0, [{1, 3}], ReturnFun),
    [{{1, 3}, F1}] = start_receiver(),
    ?assertMatch(false, F1),
    
    io:format("Put entry - correct index same key~n"),
    ok = aae_put(Cntrl0, {0, 3}, <<"B">>, <<"K">>, [{c, 1}], [], <<>>),
    ok = aae_fetchroot(Cntrl0, [{0, 3}], ReturnFun),
    [{{0,3}, Root2}] = start_receiver(),
    ?assertMatch(false, <<0:131072/integer>> == Root2),

    ok = aae_fetchroot(Cntrl0, [{1, 3}], ReturnFun),
    [{{1, 3}, F2}] = start_receiver(),
    ?assertMatch(false, F2),
    
    BranchIDL = leveled_tictac:find_dirtysegments(Root1, Root2),
    ?assertMatch(1, length(BranchIDL)),
    [BranchID] = BranchIDL, 
    
    ok = aae_fetchbranches(Cntrl0, [{0, 3}], BranchIDL, ReturnFun),
    [{{0,3}, [{BranchID, Branch3}]}] = start_receiver(),
    ?assertMatch(false,<<0:131072/integer>> == Branch3),

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


basic_cache_rebuild_so_test() ->
    basic_cache_rebuild_tester(leveled_so).

basic_cache_rebuild_ko_test() ->
    basic_cache_rebuild_tester(leveled_ko).

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
    
    ok = aae_close(Cntrl0),
    aae_util:clean_subdir(RootPath).


varyindexn_cache_rebuild_so_test() ->
    varyindexn_cache_rebuild_tester(leveled_so).

varyindexn_cache_rebuild_ko_test() ->
    varyindexn_cache_rebuild_tester(leveled_ko).

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
    ?assertMatch(false, Root3),

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
    ?assertMatch(false, RB2_Root3),

    SegIDL = leveled_tictac:find_dirtysegments(Root0, RB1_Root0),
    io:format("Count of dirty segments in IndexN 0 ~w~n", [length(SegIDL)]),
    ?assertMatch(0, length(SegIDL)),

    ?assertMatch(Root0, RB2_Root0),
    ?assertMatch(Root1, RB2_Root1),
    ?assertMatch(Root2, RB2_Root2),
    
    ok = aae_fetchbranches(Cntrl0, [{300, 3}], [1], ReturnFun),
    [{{300, 3}, RB2_Branch3}] = start_receiver(),
    ?assertMatch(false, RB2_Branch3),

    ok = aae_close(Cntrl0),
    aae_util:clean_subdir(RootPath).


coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).



%%%============================================================================
%%% Test Utils
%%%============================================================================


start_wrap(IsEmpty, RootPath, StoreType) ->
    start_wrap(IsEmpty, RootPath, [?TEST_DEFAULT_PARTITION], StoreType).

start_wrap(IsEmpty, RootPath, RPL, StoreType) ->
    F = fun(_X) -> {0, 1, 0, null} end,
    aae_start({parallel, StoreType}, IsEmpty, {?TEST_MINHOURS, 300}, RPL, RootPath, F).


put_keys(_Cntrl, _Preflists, KeyList, 0) ->
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

-endif.


