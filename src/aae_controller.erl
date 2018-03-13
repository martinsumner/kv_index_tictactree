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
            aae_rebuild/1,
            aae_put/7,
            aae_close/2,
            aae_fetchroot/3,
            aae_fetchbranches/4]).
            
-include_lib("eunit/include/eunit.hrl").

-define(STORE_PATH, "_keystore/").
-define(TREE_PATH, "_aaetree/").
-define(MEGA, 1000000).
-define(BATCH_LENGTH, 32).


-record(state, {key_store :: pid(),
                tree_caches :: tree_caches(),
                initiate_node_worker_fun,
                object_splitfun,
                reliable = false :: boolean(),
                next_rebuild :: erlang:timestamp(),
                parallel_keystore :: boolean(),
                objectspecs_queue = [] :: list()}).

-record(options, {keystore_type :: keystore_type(),
                    startup_storestate :: startup_storestate(),
                    rebuild_schedule :: rebuild_schedule(),
                    index_ns :: list(responsible_preflist()),
                    object_splitfun,
                    root_path :: list()}).

-type responsible_preflist() :: {binary(), integer()}. 
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


-spec aae_rebuild(pid()) -> erlang:timestamp().
%% @doc
%% When is the next keystore rebuild process scheduled for
aae_rebuild(Pid) ->
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

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    RootPath = Opts#options.root_path,
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
    {ok, State0#state{object_splitfun = Opts#options.object_splitfun}}.

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
handle_call(_Msg, _From, State) ->
    {reply, not_implemented, State}.

handle_cast({put, IndexN, Bucket, Key, CurrentVV, PrevVV, BinaryObj}, State) ->
    TreeCaches = State#state.tree_caches,
    case lists:keyfind(IndexN, 1, TreeCaches) of 
        false ->
            handle_unexpected_key(Bucket, Key, IndexN, TreeCaches),
            {noreply, State};
        {IndexN, TreeCache} ->
            BinaryKey = make_binarykey(Bucket, Key),
            {CH, OH} = hash_vectors(CurrentVV, PrevVV),
            ok = aae_treecache:cache_alter(TreeCache, BinaryKey, CH, OH),
            case State#state.parallel_keystore of 
                true ->
                    SegmentID = leveled_tictac:keyto_segment32(BinaryKey),
                    ObjSpec = 
                        generate_objectspec(Bucket, Key, SegmentID, IndexN,
                                                BinaryObj, 
                                                CurrentVV, CH, 
                                                State#state.object_splitfun),
                    UpdSpecL = [ObjSpec|State#state.objectspecs_queue],
                    case length(UpdSpecL) >= ?BATCH_LENGTH of 
                        true ->
                            ok = 
                                aae_keystore:store_mput(State#state.key_store, 
                                                        UpdSpecL),
                            {noreply, 
                                State#state{objectspecs_queue = []}};
                        false ->
                            {noreply, 
                                State#state{objectspecs_queue = UpdSpecL}}
                    end;
                false ->
                    {noreply, 
                        State}
            end     
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
%%% Internal functions
%%%============================================================================

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

-spec hash_vectors(version_vector(), version_vector()) 
                                                    -> {integer(), integer()}.
%% @doc
%% Has the version vectors 
hash_vectors(CurrentVV, PrevVV) ->
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
            {warn, "Unexpected Bucket ~w Key ~w passed with IndexN ~w "
                    "that does not match any of ~w"}},
        {"AAE04",
            {warn, "Misrouted request for IndexN ~w"}}
    
    ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-endif.


