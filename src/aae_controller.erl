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
            aae_close/2,
            aae_fetchroot/3,
            aae_fetchbranches/4,
            aae_rebuildcaches/4]).

-export([foldobjects_buildtrees/3]).

-include_lib("eunit/include/eunit.hrl").

-define(STORE_PATH, "_keystore/").
-define(TREE_PATH, "aaetree/").
-define(MEGA, 1000000).
-define(BATCH_LENGTH, 32).


-record(state, {key_store :: pid(),
                tree_caches :: tree_caches(),
                index_ns :: list(responsible_preflist()),
                initiate_node_worker_fun,
                object_splitfun,
                reliable = false :: boolean(),
                next_rebuild :: erlang:timestamp(),
                prompt_cacherebuild :: boolean(),
                parallel_keystore :: boolean(),
                objectspecs_queue = [] :: list(),
                root_path :: list()}).

-record(options, {keystore_type :: keystore_type(),
                    startup_storestate :: startup_storestate(),
                    rebuild_schedule :: rebuild_schedule(),
                    index_ns :: list(responsible_preflist()),
                    object_splitfun,
                    root_path :: list()}).

-type responsible_preflist() :: {integer(), integer()}. 
        % The responsible preflist is a reference to the partition associated 
        % with an AAE requirement.  The preflist is a reference to the id of 
        % the head partition in preflist, and and n-val.
-type tree_caches() 
        :: list({responsible_preflist(), pid()}).
        % A map between the responsible_preflist reference and the tree_cache 
        % for that preflist
-type keystore_type() 
        :: {parallel, aae_keystore:supported_stores()}|{native, pid()}.
        % Key Store can be native (no separate AAE store required) or
        % parallel when a seperate Key Store is needed for AAE.  The Type
        % for parallel stores must be a supported KV store by the aae_keystore
        % module 
-type startup_storestate() 
        :: {boolean(), list()|none}.
        % On startup the current state of the store is passed for the aae 
        % controller to determine if the startup of the aae_controller has 
        % placed the system in a 'reliable' and consistent state between the
        % vnode store and the parallel key store.  The first element is the 
        % is_empty status of the vnode backend, and the second element is a 
        % GUID which was stored in the backend prior to the last controlled 
        % close.
-type rebuild_schedule() 
        :: {integer(), integer()}.
        % A rebuild schedule, the first integer being the minimum number of 
        % hours to wait between rebuilds.  The second integer is a number of 
        % seconds by which to jitter the rebuild.  The actual rebuild will be 
        % scheduled by adding a random integer number of seconds (between 0 
        % and the jitter value) to the minimum time
-type version_vector()
        :: list(tuple())|none.


%%%============================================================================
%%% API
%%%============================================================================

-spec aae_start(keystore_type(), 
                startup_storestate(), 
                rebuild_schedule(),
                list(responsible_preflist()), 
                list(),
                fun()) -> {ok, pid()}.
%% @doc
%% Start an AAE controller 
aae_start(KeyStoreT, StartupD, RebuildSch, Preflists, RootPath, ObjSplitFun) ->
    AAEopts =
        #options{keystore_type = KeyStoreT,
                    startup_storestate = StartupD,
                    rebuild_schedule = RebuildSch,
                    index_ns = Preflists,
                    object_splitfun = ObjSplitFun,
                    root_path = RootPath},
    gen_server:start(?MODULE, [AAEopts], []).


-spec aae_nextrebuild(pid()) -> erlang:timestamp().
%% @doc
%% When is the next keystore rebuild process scheduled for
aae_nextrebuild(Pid) ->
    gen_server:call(Pid, rebuild_time).

-spec aae_put(pid(), responsible_preflist(), 
                            binary(), binary(), 
                            list(), list(), binary()) -> ok.
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

-spec aae_fetchbranches(pid(), 
                        list(responsible_preflist()), list(integer()), 
                        fun()) -> ok.
%% @doc
%% Fetch the branches of AAE tree caches for a list of IndexNs returning an
%% indexed list of results using ReturnFun - with a result of `false` in the 
%% special case where no TreeCache exists for that preflist
aae_fetchbranches(Pid, IndexNs, BranchIDs, ReturnFun) ->
    gen_server:cast(Pid, {fetch_branches, IndexNs, BranchIDs, ReturnFun}).

-spec aae_close(pid(), list()) -> ok.
%% @doc
%% Closedown the AAE controlle claosing and saving state.  Tag the closed state 
%% with the ShutdownGUID releases on close from the vnode store so that on 
%% startup the system can be tested for consistency between parallel stores.
aae_close(Pid, ShutdownGUID) ->
    gen_server:call(Pid, {close, ShutdownGUID}, 30000).


-spec aae_rebuildcaches(pid(), list(responsible_preflist()), 
                        fun()|parallel_keystore, fun()) -> ok.
%% @doc
%% Rebuild the tree caches.  
%% 
%% This should occur if and only if a rebuild of the cache is pending.  The 
%% IndexNs supported should be passed in (it is not assumed that the rebuild
%% will be on the same list of responsible_preflists as the original start).
%%
%% If the keystore is in native mode this will use a passed in folder function 
%% as well as worker function.  The folder function will be on a snapshot 
%% taken by the vnode manager as part of the atomic unit of work which has 
%% made this call.  The worker function will trigger a worker from a pool to
%% perfom the fold which should also take a finish function that will be used
%% to call cache_completeload for eahc IndexN when the fold is finished.
%%
%% If parallel_keystore is passed, the rebuild will be run against the 
%% parallel store so te controller should take the snapshot and prepare the
%% fold function.
aae_rebuildcaches(Pid, IndexNs, FoldFun, WorkerFun) ->
    gen_server:call(Pid, {rebuild_treecaches, IndexNs, FoldFun, WorkerFun}).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    RootPath = Opts#options.root_path,
    % Start the KeyStore
    % Need to update the state to reflect the potential need to rebuild the 
    % key store if the shutdown was not clean as expected
    {ok, State0} = 
        case Opts#options.keystore_type of 
            {parallel, StoreType} ->
                StoreRP = 
                    filename:join([RootPath, StoreType, ?STORE_PATH]),
                {ok, {LastRebuild, ShutdownGUID, IsEmpty}, Pid} =
                    aae_keystore:store_parallelstart(StoreRP, StoreType),
                case Opts#options.startup_storestate of 
                    {IsEmpty, ShutdownGUID} ->
                        RebuildTS = 
                            schedule_rebuild(LastRebuild, 
                                                Opts#options.rebuild_schedule),
                        {ok, #state{key_store = Pid, 
                                        next_rebuild = RebuildTS, 
                                        reliable = true,
                                        parallel_keystore = true}};
                    StoreState ->
                        aae_util:log("AAE01", 
                                        [StoreState, {IsEmpty, ShutdownGUID}],
                                        logs()),
                        {ok, #state{key_store = Pid, 
                                        next_rebuild = os:timestamp(), 
                                        reliable = false,
                                        parallel_keystore = true}}
                end;
            KeyStoreType ->
                aae_util:log("AAE02", [KeyStoreType], logs())
        end,

    % Start the TreeCaches
    % Trust any cache that is neatly shutdown, and ignore any cache if the 
    % vnode store is empty.  If caches are not started cleanly as expected 
    % then the prompt_cacherebuild should trigger for them to be rebuilt from
    % the AAE KeyStore (if that itself is not pending a rebuild)  
    {ExpectEmpty, _} = Opts#options.startup_storestate,
    TreeRP = filename:join(RootPath, ?TREE_PATH),
    StartCacheFun = 
        fun(IndexN, {AllRestored, Caches}) ->
            {Index, N} = IndexN,
            PartitionID = Index + N,
            {IsRestored, Cache} = 
                case ExpectEmpty of 
                    true ->
                        {ok, NewCache} = 
                            aae_treecache:cache_new(TreeRP, PartitionID),
                        {true, NewCache};
                    false ->
                        aae_treecache:cache_open(TreeRP, PartitionID)
                end,
            {IsRestored and AllRestored, [{IndexN, Cache}|Caches]}
        end,
    {AllTreesOK, TreeCaches} = 
        lists:foldl(StartCacheFun, {true, []}, Opts#options.index_ns),
    {ok, State0#state{object_splitfun = Opts#options.object_splitfun,
                        index_ns = Opts#options.index_ns,
                        tree_caches = TreeCaches,
                        prompt_cacherebuild = not AllTreesOK,
                        root_path = RootPath}}.


handle_call(rebuild_time, _From, State) ->  
    {reply, State#state.next_rebuild, State};
handle_call({close, ShutdownGUID}, _From, State) ->
    ok = aae_keystore:store_close(State#state.key_store, ShutdownGUID),
    CloseTCFun = 
        fun({_IndexN, TreeCache}) ->
            ok = aae_treecache:cache_close(TreeCache)
        end,
    lists:foreach(CloseTCFun, State#state.tree_caches),
    {stop, normal, ok, State};
handle_call({rebuild_treecaches, IndexNs, parallel_keystore, WorkerFun}, 
                                                            _From, State) ->
    % Before the fold flush all the PUTs 
    ok =  aae_keystore:store_mput(State#state.key_store, 
                                    State#state.objectspecs_queue),
    
    % Setup a fold over the store
    IndexNFun = fun(_B, _K, V) -> aae_keystore:value_preflist(V) end,
    ExtractHashFun = fun aae_keystore:value_hash/1,
    {FoldFun, InitAcc} = 
        foldobjects_buildtrees(IndexNs, IndexNFun, ExtractHashFun),
    {async, Folder} = 
        aae_keystore:store_fold(State#state.key_store, all, FoldFun, InitAcc),
    
    % Handle the current list of responsible preflists for this vnode 
    % having changed since the last call to start or rebuild the cache trees
    NewIndexNs = lists:subtract(IndexNs, State#state.index_ns),
    NewTreeCaches = 
        lists:map(fun(NewIdxN) -> 
                            {ok, TCP} = 
                                new_cache(NewIdxN, State#state.root_path),
                            {NewIdxN, TCP}
                        end, 
                    NewIndexNs),
    TreeCaches = NewTreeCaches ++ State#state.tree_caches,

    % Produce a Finishfun to eb called at the end of the Folder with the 
    % input as the results.  this should call rebuild_complete on each Tree
    % cache in turn
    FinishTreeFun =
        fun({FoldIndexN, FoldTree}) ->
            case lists:keyfind(FoldIndexN, 1, TreeCaches) of 
                {FoldIndexN, TreeCache} ->
                    aae_treecache:cache_completeload(TreeCache, FoldTree);
                false ->
                    % Maybe an n-val was added, then since the n-val 
                    % was changed
                    aae_util:log("AAE05", [FoldIndexN], logs())
            end 
        end,
    FinishFun = 
        fun(FoldTreeCaches) ->
            lists:foreach(FinishTreeFun, FoldTreeCaches)
        end,

    WorkerFun(Folder, FinishFun),

    % The IndexNs and TreeCaches supported by the controller must now be 
    % updated to match the lasted provided list of responsible preflists
    {reply, 
        ok, 
        State#state{tree_caches = TreeCaches, index_ns = IndexNs}};
handle_call(_Msg, _From, State) ->
    {reply, not_implemented, State}.

handle_cast({put, IndexN, Bucket, Key, Clock, PrevClock, BinaryObj}, State) ->
    % Setup
    TreeCaches = State#state.tree_caches,
    BinaryKey = make_binarykey(Bucket, Key),
    {CH, OH} = hash_clocks(Clock, PrevClock),
    
    % Update the TreeCache associated with the Key (should a cache exist for
    % that store)
    case lists:keyfind(IndexN, 1, TreeCaches) of 
        false ->
            % Note that this will eventually end up in the Tree Cache if in 
            % the future the IndexN combination is added to the list of 
            % responsible preflists
            handle_unexpected_key(Bucket, Key, IndexN, TreeCaches);
        {IndexN, TreeCache} ->
            % TODO:
            % Need to handle a PrevClock of unidentified - in this case there
            % will need to be a read before write against the Key Store.
            % Note that this should only occur for LWW with Bitcask (or with 
            % another Backend with the 2i capability removed)
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
            SegmentID = leveled_tictac:keyto_segment32(BinaryKey),
            ObjSpec =  generate_objectspec(Bucket, Key, SegmentID, IndexN,
                                            BinaryObj, 
                                            Clock, CH, 
                                            State#state.object_splitfun),
            UpdSpecL = [ObjSpec|State#state.objectspecs_queue],
            case length(UpdSpecL) >= ?BATCH_LENGTH of 
                true ->
                    % Push to the KeyStore as batch is now at least full
                    ok =  aae_keystore:store_mput(State#state.key_store, 
                                                    UpdSpecL),
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

-spec foldobjects_buildtrees(list(responsible_preflist()), fun(), fun()) 
                                                        -> {fun(), list()}.
%% @doc
%% Return an object fold fun for building hashtrees, with an initialised 
%% accumulator
foldobjects_buildtrees(IndexNs, IndexNFun, ExtractHashFun) ->
    InitMapFun  = 
        fun(IndexN) ->
            {IndexN, leveled_tictac:new_tree(IndexN, ?TREE_SIZE)}
        end,
    InitAcc = lists:map(InitMapFun, IndexNs),
    
    FoldObjectsFun = 
        fun(B, K, V, Acc) ->
            IndexN = IndexNFun(B, K, V),
            BinK = <<B/binary, K/binary>>,
            BinExtractFun = 
                fun(_BK, _V) -> {BinK, {is_hash, ExtractHashFun(V)}} end,
            case lists:keyfind(IndexN, 1, Acc) of 
                {IndexN, Tree} ->
                    Tree0 = leveled_tictac:add_kv(Tree, K, V, BinExtractFun),
                    lists:keyreplace(IndexN, 1, Acc, {IndexN, Tree0});
                false ->
                    Acc 
            end
        end,
    
    {FoldObjectsFun, InitAcc}.
    


%%%============================================================================
%%% Internal functions
%%%============================================================================


-spec new_cache(responsible_preflist(), list()) -> {ok, pid()}.
%% @doc
%% Start a new tree cache
new_cache(IndexN, RootPath) ->
    TreeRP = filename:join(RootPath, ?TREE_PATH),
    {Index, N} = IndexN,
    PartitionID = Index + N,
    aae_treecache:cache_new(TreeRP, PartitionID).

-spec schedule_rebuild(erlang:timestanmp(), {integer(), integer()}) 
                                                        -> erlang:timestamp().
%% @doc
%% Set a rebuild time based on the last rebuild time and the rebuild schedule
schedule_rebuild({MegaSecs, Secs, MicroSecs}, {MinHours, JitterSeconds}) ->
    NewSecs = 
        MegaSecs * ?MEGA 
            + Secs 
            + MinHours * 3600 + random:uniform(JitterSeconds),
    {NewSecs div ?MEGA, NewSecs rem ?MEGA, MicroSecs}.


-spec generate_objectspec(binary(), binary(), integer(), tuple(),
                            binary(), version_vector(), integer()|none, 
                            fun()) -> tuple().
%% @doc                            
%% Generate an object specification for a parallel key store
generate_objectspec(Bucket, Key, SegmentID, _IndexN,
                        _BinaryObj, none, _CurrentHash, 
                        _SplitFun) ->
    aae_keystore:define_objectspec(remove, 
                                    SegmentID, Bucket, Key, 
                                    null);
generate_objectspec(Bucket, Key, SegmentID, IndexN,
                        BinaryObj, CurrentVV, CurrentHash, 
                        SplitFun) ->
    KSV = aae_keystore:generate_value(IndexN, 
                                        CurrentVV, 
                                        CurrentHash, 
                                        SplitFun(BinaryObj)),
    aae_keystore:define_objectspec(add, 
                                    SegmentID, 
                                    Bucket, Key, 
                                    KSV).


-spec handle_unexpected_key(binary(), binary(), tuple(), list(tuple())) -> ok.
%% @doc
%% Log out that an unexpected key has been seen
handle_unexpected_key(Bucket, Key, IndexN, TreeCaches) ->
    RespPreflists = lists:map(fun({RP, _TC}) ->  RP end, TreeCaches),
    aae_util:log("AAE03", [Bucket, Key, IndexN, RespPreflists], logs()).

-spec make_binarykey(binary(), binary()) -> binary().
%% @doc
%% Convert Bucket and Key into a single binary 
make_binarykey(Bucket, Key) ->
    term_to_binary({Bucket, Key}).

-spec hash_clocks(version_vector(), version_vector()) 
                                                    -> {integer(), integer()}.
%% @doc
%% Has the version vectors 
hash_clocks(CurrentVV, PrevVV) ->
    {erlang:phash2(CurrentVV), erlang:phash2(PrevVV)}.

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
            {error, "Unexpected KeyStore type information passed ~w"}},
        {"AAE03",
            {debug, "Unexpected Bucket ~w Key ~w passed with IndexN ~w "
                    "that does not match any of ~w"}},
        {"AAE04",
            {warn, "Misrouted request for IndexN ~w"}},
        {"AAE05",
            {info, "New IndexN ~w found in cache rebuild"}}
    
    ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-endif.


