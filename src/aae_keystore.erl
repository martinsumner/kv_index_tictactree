%% -------- Overview ---------
%%
%% The KeyStore can be run in two modes:
%%
%% - parallel - in this case the store is separate to the main vnode store
%% and is managed directly by this process.  The Keystore duplicates 
%% information within the vnode backend.  When creating a parallel store 
%% the store must first transition through the building state
%%
%% - native - in this case the store uses a reference back to the vnode
%% itself, and resolves queries by querying the vnode.  Only a Riak with a 
%% leveled backend can be run in native mode.  In antive mode there is no 
%% duplication of information wihtin the key store (which is empty)

-module(aae_keystore).

-behaviour(gen_fsm).

-ifdef(fsm_deprecated).
-compile({nowarn_deprecated_function, 
            [{gen_fsm, start_link, 3},
                {gen_fsm, send_event, 2},
                {gen_fsm, sync_send_event, 2},
                {gen_fsm, sync_send_event, 3},
                {gen_fsm, sync_send_all_state_event, 2},
                {gen_fsm, send_all_state_event, 2}
                ]}).
-endif.

-include("include/aae.hrl").

-export([init/1,
            handle_sync_event/4,
            handle_event/3,
            handle_info/3,
            terminate/3,
            code_change/4]).

-export([loading/2, 
            loading/3,
            parallel/2,
            parallel/3,
            native/2,
            native/3]).

-export([store_parallelstart/3,
            store_nativestart/4,
            store_startupdata/1,
            store_close/1,
            store_destroy/1,
            store_mput/2,
            store_mload/2,
            store_prompt/2,
            store_fold/8,
            store_fetchclock/3,
            store_bucketlist/1,
            store_loglevel/2]).

-export([define_addobjectspec/3,
            define_delobjectspec/3,
            check_objectspec/3,
            generate_value/5,
            generate_treesegment/1,
            value/3]).

-record(state, {vnode :: pid()|undefined,
                store :: pid()|undefined,
                id = key_store :: any(),
                store_type :: parallel_stores()|native_stores(),
                change_queue = [] :: list(),
                change_queue_counter = 0 :: integer(),
                load_counter = 0 :: integer(),
                current_guid :: list()|undefined,
                root_path :: list()|undefined,
                last_rebuild :: os:timestamp()|never,
                load_store :: pid()|undefined,
                load_guid :: list()|undefined,
                backend_opts = [] :: list(),
                trim_count = 0 :: integer(),
                log_levels :: aae_util:log_levels()|undefined}).

-record(manifest, {current_guid :: list()|undefined, 
                    pending_guid :: list()|undefined, 
                    last_rebuild = never :: erlang:timestamp()|never, 
                    shutdown_guid = none :: list()|none}).

-record(objectspec, {op :: add|remove,
                        segment_id :: integer(),
                        bucket :: binary(),
                        key :: binary(),
                        last_mod_dates :: undefined|list(os:timestamp()),
                        value = null :: tuple()|null}).

-include_lib("eunit/include/eunit.hrl").

-define(LEVELED_BACKEND_OPTS, [{max_pencillercachesize, 32000},
                                    {cache_size, 4000},
                                    {sync_strategy, none},
                                    {max_journalsize, 10000000},
                                    {compression_method, native},
                                    {compression_point, on_compact}]).
-define(CHANGEQ_LOGFREQ, 10000).
-define(STATE_BUCKET, <<"state">>).
-define(MANIFEST_FN, "keystore"). 
    % filename for Keystore manifes
-define(COMLPETE_EXT, ".man"). 
    % file extension to be used once manifest write is complete
-define(PENDING_EXT, ".pnd").
    % file extension to be used once manifest write is pending
-define(VALUE_VERSION, 2).
-define(MAYBE_TRIM, 500).
-define(NULL_SUBKEY, <<>>).
-define(CHECK_NATIVE_PRESENCE, true). 
    % Things will be faster if false, but there will be data loss scenarios
    % which won't be detected - most noticeably the loss of a Journal file
    % without correlated loss from the Ledger
-define(NOCHECK_PRESENCE, false). 
    % needs to be false if it is a parallel store, and may be false if query
    % is not part of the internal AAE process
-define(SNAP_PREFOLD, true).
    % Allow for the snapshot to be taken before the fold function is returned
    % so the fold will represent the state of the store when the request was
    % processed, and not when the fold was run
-define(USE_SET_FOR_SPEED, 64).

-type bucket() :: binary()|{binary(),binary()}.
-type key() :: binary().
-type parallel_stores() :: leveled_so|leveled_ko. 
    % Stores supported for parallel running
-type native_stores() :: leveled_nko.
    % Vnode backends which can support aae_keystore running in native mode
-type manifest() :: #manifest{}.
    % Saves state of what store is currently active
-type objectspec() :: #objectspec{}.
    % Object specification required by the store within mputs
-type range_limiter()
    :: all|{buckets, list(bucket())}|{key_range, bucket(), key(), key()}.
    % Limit the scope of the fold, to specific segments or to specific 
    % buckets (and otherwise state all).
-type segment_limiter()
    :: all|{segments, list(integer()), small|medium|large}.
-type modified_limiter()
    :: all|{integer(), integer()}.
    % A range of last modified dates to use in the fold
-type count_limiter()
    :: pos_integer()|false.
    % A Limit on the count of objects returned.  If there is a leveled_so
    % backend to the keystore it is not possible to continue if the count is
    % reached
    % The count will be ignored if it is a leveled_so backend (as no
    % continuation is possible if too_many_results is hit)
-type segment_checker() ::
    all|{list, list(non_neg_integer())}|{sets, sets:set(non_neg_integer())}.
    %% After expanding the segment list, convert into a refined checker which
    %% may be a set if the segment_limiter is beyond a size optimal for lists
    %% membership checks
-type rebuild_prompts()
    :: rebuild_start|rebuild_complete.
    % Prompts to be used when rebuilding the key store (note this is rebuild
    % of the key store not just the aae trees)
-type metadata() 
    :: binary().
    % Metadata should be a binary which can be unwrapped using term_to_binary
-type value_v2()
    :: {2, 
        {tuple(), aae_controller:version_vector(), 
            non_neg_integer(), non_neg_integer(), non_neg_integer(), 
            non_neg_integer(), non_neg_integer(),
            list(erlang:timestamp())|undefined, metadata()}}.
-type value() ::
    value_v2().
    % Value v1 has been an dgone pre-release, so no backwards compatability has
    % been maintained

-type value_element() 
    :: {preflist|clock|hash|size|sibcount|indexhash|aae_segment|lmd|md, 
        fun()|null}.
    % A request for a value element to be returned from a value, and the
    % function to apply to the f(Bucket, Key) for elements where the item
    % needs to be calculated (like IndexN in native stores)


-export_type([parallel_stores/0,
                native_stores/0,
                manifest/0,
                objectspec/0,
                value_element/0,
                range_limiter/0,
                segment_limiter/0,
                modified_limiter/0,
                count_limiter/0,
                rebuild_prompts/0,
                bucket/0,
                key/0]).


%%%============================================================================
%%% API
%%%============================================================================

-spec store_parallelstart(list(),
                            parallel_stores(),
                            aae_util:log_levels()|undefined) -> 
                {ok, {os:timestamp()|never, boolean()}, pid()}.
%% @doc
%% Start a store to be run in parallel mode
store_parallelstart(Path, leveled_so, LogLevels) ->
    MinLevel = aae_util:min_loglevel(LogLevels),
    Opts = 
        [{root_path, Path}, 
            {native, {false, leveled_so}}, 
            {backend_opts, [{log_level, MinLevel}|?LEVELED_BACKEND_OPTS]},
            {log_levels, LogLevels}],
    {ok, Pid} = gen_fsm:start_link(?MODULE, [Opts], []),
    store_startupdata(Pid);
store_parallelstart(Path, leveled_ko, LogLevels) ->
    MinLevel = aae_util:min_loglevel(LogLevels),
    Opts = 
        [{root_path, Path}, 
            {native, {false, leveled_ko}}, 
            {backend_opts, [{log_level, MinLevel}|?LEVELED_BACKEND_OPTS]},
            {log_levels, LogLevels}],
    {ok, Pid} = gen_fsm:start_link(?MODULE, [Opts], []),
    store_startupdata(Pid).

-spec store_nativestart(list(), native_stores(), pid(),
                        aae_util:log_levels()|undefined) ->
                {ok, {os:timestamp()|never, boolean()}, pid()}.
%% @doc
%% Start a keystore in native mode.  In native mode the store is just a pass
%% through for queries - and there will be no puts
store_nativestart(Path, NativeStoreType, BackendPid, LogLevels) ->
    Opts = 
        [{root_path, Path},
            {native, {true, NativeStoreType, BackendPid}},
            {log_levels, LogLevels}],
    {ok, Pid} = gen_fsm:start_link(?MODULE, [Opts], []),
    store_startupdata(Pid).


-spec store_startupdata(pid()) ->
                {ok, {os:timestamp()|never, boolean()}, pid()}.
%% @doc
%% Get the startup metadata from the store
store_startupdata(Pid) ->
    {LastRebuild, IsEmpty} = gen_fsm:sync_send_event(Pid, startup_metadata),
    {ok, {LastRebuild, IsEmpty}, Pid}.


-spec store_close(pid()) -> ok.
%% @doc
%% Close the store neatly.  If a GUID is past it will be returned when the 
%% Store is opened.  This can be used to ensure that the backend and the AAE
%% KeyStore both closed neatly (by making the storing of the GUID in the 
%% vnode backend the lasta ct before closing), if both vnode and aae backends
%% report the same GUID at startup.
%%
%% Startup should always delete the Shutdown GUID in both stores.
store_close(Pid) ->
    gen_fsm:sync_send_event(Pid, close, 10000).


-spec store_destroy(pid()) -> ok.
%% @doc
%% Close the store and clear the data if parallel
store_destroy(Pid) ->
    gen_fsm:sync_send_event(Pid, destroy, 30000).

-spec store_mput(pid(), list()) -> ok.
%% @doc
%% Put multiple objectspecs into the store.  The object specs should be of the
%% form {ObjectOp, Segment, Bucket, Key, Value}.
%% ObjectOp :: add|remove
%% Segment :: binary() [<<SegmentID:32/integer>>]
%% Bucket/Key :: binary() 
%% Value :: {Version, ...} - Tuples may chnage between different version
store_mput(Pid, ObjectSpecs) ->
    gen_fsm:send_event(Pid, {mput, ObjectSpecs}).

-spec store_mload(pid(), list(objectspec())) -> ok.
%% @doc
%% Put multiple objectspecs into the store.  The object specs should be of the
%% form {ObjectOp, Segment, Bucket, Key, Value}.
%% ObjectOp :: add|remove
%% Segment :: binary() [<<SegmentID:32/integer>>]
%% Bucket/Key :: binary() 
%% Value :: {Version, ...} - Tuples may chnage between different version
%%
%% Load requests are only expected whilst loading, and are pushed to the store
%% while put requests are cached
store_mload(Pid, ObjectSpecs) ->
    gen_fsm:send_event(Pid, {mload, ObjectSpecs}).

-spec store_prompt(pid(), rebuild_prompts())  -> ok.
%% @doc
%% Prompt the store to either commence a rebuild, or complete a rebuild.
%% Commencing a rebuild should happen between a snapshot for a fold being 
%% taken, and the fold commencing with no new updates to be received between
%% the snapshot and the prompt.  The complete prompt should be made after 
%% the fold is complete
%%
%% Once the prompt has been processed - the prompt will be forwarded by 
%% calling NotifyFun(Prompt) -> ok.
store_prompt(Pid, Prompt) ->
    gen_fsm:send_event(Pid, {prompt, Prompt}).


-spec store_fold(pid(), 
                    range_limiter(),
                    segment_limiter(),
                    modified_limiter(),
                    count_limiter(),
                    fun(), any(), 
                    list(value_element())) -> any()|{async, fun()}.
%% @doc
%% Return a fold function to asynchronously run a fold over a snapshot of the
%% store
store_fold(Pid, RLimiter, SLimiter, LMDLimiter, MaxObjectCount, 
            FoldObjectsFun, InitAcc, Elements) ->
    gen_fsm:sync_send_event(Pid, 
                            {fold, 
                                RLimiter, SLimiter, LMDLimiter, MaxObjectCount,
                                FoldObjectsFun, InitAcc,
                                Elements},
                            infinity).


-spec store_fetchclock(pid(), binary(), binary())
                                            -> aae_controller:version_vector().
%% @doc
%% Return the clock of a given Bucket and Key
store_fetchclock(Pid, Bucket, Key) ->
    gen_fsm:sync_send_event(Pid, {fetch_clock, Bucket, Key}, infinity).


-spec store_bucketlist(pid()) -> fun(() -> list(bucket())).
%% @doc
%% List all the buckets in the keystore
store_bucketlist(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, bucket_list).

-spec store_loglevel(pid(), aae_util:log_levels()) -> ok.
%% @doc
%% Alter the log level at runtime
store_loglevel(Pid, LogLevels) ->
    gen_fsm:send_all_state_event(Pid, {log_level, LogLevels}).

%%%============================================================================
%%% gen_fsm callbacks
%%%============================================================================

init([Opts]) ->
    RootPath = aae_util:get_opt(root_path, Opts),
    LogLevels = aae_util:get_opt(log_levels, Opts),
    Manifest0 =
        case open_manifest(RootPath, LogLevels) of 
            false ->
                GUID = leveled_util:generate_uuid(),
                #manifest{current_guid = GUID};
            {ok, M} ->
                M 
        end,

    case aae_util:get_opt(native, Opts) of 
        {false, StoreType} ->
            Manifest1 = clear_pendingpath(Manifest0, RootPath),
            LastRebuild = Manifest1#manifest.last_rebuild,
            BackendOpts0 = aae_util:get_opt(backend_opts, Opts),
            {ok, Store} = open_store(StoreType, 
                                        BackendOpts0, 
                                        RootPath,
                                        Manifest1#manifest.current_guid),
            
            ok = store_manifest(RootPath, Manifest1, LogLevels),
            {ok, 
                parallel, 
                #state{store = Store, 
                        store_type = StoreType,
                        root_path = RootPath,
                        current_guid = Manifest0#manifest.current_guid,
                        last_rebuild = LastRebuild,
                        backend_opts = BackendOpts0,
                        log_levels = LogLevels}};
        {true, StoreType, BackendPid} ->
            {ok, 
                native,
                #state{store = BackendPid,
                        store_type = StoreType,
                        root_path = RootPath,
                        current_guid = Manifest0#manifest.current_guid,
                        last_rebuild = Manifest0#manifest.last_rebuild,
                        log_levels = LogLevels}}
    end.

loading({fold, Range, Segments, LMD, Count, FoldFun, InitAcc, Elements},
                                                            _From, State) ->
    Result = do_fold(State#state.store_type, State#state.store,
                        Range, Segments, LMD, Count,
                        FoldFun, InitAcc, Elements),
    {reply, Result, loading, State};
loading({fetch_clock, Bucket, Key}, _From, State) ->
    VV = do_fetchclock(State#state.store_type, State#state.store, Bucket, Key),
    {reply, VV, loading, State};
loading(Shutdown, _From, State) when Shutdown == close; Shutdown == destroy ->
    ok = delete_store(State#state.store_type, State#state.load_store),
    ok = close_store(State#state.store_type, State#state.store, Shutdown),
    {stop, normal, ok, State}.

parallel({fold, Range, Segments, LMD, Count, FoldFun, InitAcc, Elements},
                                                            _From, State) ->
    Result = do_fold(State#state.store_type, State#state.store,
                    Range, Segments, LMD, Count, FoldFun, InitAcc, Elements),
    {reply, Result, parallel, State};
parallel({fetch_clock, Bucket, Key}, _From, State) ->
    VV = do_fetchclock(State#state.store_type, State#state.store, Bucket, Key),
    {reply, VV, parallel, State};
parallel(startup_metadata, _From, State) ->
    IsEmpty = is_empty(State#state.store_type, State#state.store),
    {reply, 
        {State#state.last_rebuild, IsEmpty}, 
        parallel, 
        State};
parallel(Shutdown, _From, State) when Shutdown == close; Shutdown == destroy ->
    ok = close_store(State#state.store_type, State#state.store, Shutdown),
    {stop, normal, ok, State}.


native({fold, Range, SegFilter, LMD, Count, FoldFun, InitAcc, Elements},
                                                            _From, State) ->
    Result = do_fold(State#state.store_type, State#state.store,
                            Range, SegFilter, LMD, Count,
                            FoldFun, InitAcc, Elements),
    {reply, Result, native, State};
native(startup_metadata, _From, State) ->
    {reply, 
        {State#state.last_rebuild, false}, 
        native,
        State};
native(Shutdown, _From, State) when Shutdown == close; Shutdown == destroy ->
    {stop, normal, ok, State}.


loading({mput, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.store, ObjectSpecs),
    ChangeQueue1 = [ObjectSpecs|State#state.change_queue],
    ObjectCount0 = State#state.change_queue_counter,
    ObjectCount1 = State#state.change_queue_counter + length(ObjectSpecs),
    ToLog = 
        ObjectCount1 div ?CHANGEQ_LOGFREQ > ObjectCount0 div ?CHANGEQ_LOGFREQ,
    case ToLog of 
        true ->
            aae_util:log("KS001",
                            [State#state.id, ObjectCount1],
                            logs(),
                            State#state.log_levels);
        false ->
            ok
    end,
    {next_state, loading, State#state{change_queue_counter = ObjectCount1, 
                                        change_queue = ChangeQueue1}};
loading({mload, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.load_store, ObjectSpecs),
    LoadCount0 = State#state.load_counter,
    LoadCount1 = State#state.load_counter + length(ObjectSpecs),
    ToLog = 
        LoadCount1 div ?CHANGEQ_LOGFREQ > LoadCount0 div ?CHANGEQ_LOGFREQ,
    case ToLog of 
        true ->
            aae_util:log("KS004",
                            [State#state.id, LoadCount1],
                            logs(),
                            State#state.log_levels);
        false ->
            ok
    end,
    {next_state, loading, State#state{load_counter = LoadCount1}};
loading({prompt, rebuild_complete}, State) ->
    StoreType = State#state.store_type,
    LoadStore = State#state.load_store,
    GUID = State#state.load_guid,
    aae_util:log("KS007",
                    [rebuild_complete, GUID],
                    logs(),
                    State#state.log_levels),
    LoadFun = fun(OS) -> do_load(StoreType, LoadStore, OS) end,
    lists:foreach(LoadFun, lists:reverse(State#state.change_queue)),
    LastRebuild = os:timestamp(),
    ok = store_manifest(State#state.root_path, 
                        #manifest{current_guid = GUID,
                                    last_rebuild = LastRebuild},
                        State#state.log_levels),
    ok = delete_store(StoreType, State#state.store),
    {next_state, 
        parallel, 
        State#state{change_queue = [], 
                    change_queue_counter = 0,
                    load_counter = 0,
                    store = LoadStore,
                    current_guid = GUID,
                    last_rebuild = LastRebuild}}.

parallel({mput, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.store, ObjectSpecs),
    TrimCount = State#state.trim_count + 1,
    ok = maybe_trim(State#state.store_type, TrimCount, State#state.store),
    {next_state, parallel, State#state{trim_count = TrimCount}};
parallel({prompt, rebuild_start}, State) ->
    GUID = leveled_util:generate_uuid(),
    aae_util:log("KS007",
                    [rebuild_start, GUID],
                    logs(),
                    State#state.log_levels),
    {ok, Store} =  open_store(State#state.store_type, 
                                State#state.backend_opts, 
                                State#state.root_path, 
                                GUID),
    ok = store_manifest(State#state.root_path, 
                        #manifest{current_guid = State#state.current_guid,
                                    pending_guid = GUID},
                        State#state.log_levels),
    {next_state, loading, State#state{load_store = Store, load_guid = GUID}}.

native({prompt, rebuild_start}, State) ->
    GUID = leveled_util:generate_uuid(),
    aae_util:log("KS007",
                    [rebuild_start, GUID],
                    logs(),
                    State#state.log_levels),
    
    {next_state, native, State#state{current_guid = GUID}};
native({prompt, rebuild_complete}, State) ->
    GUID = State#state.current_guid,
    aae_util:log("KS007",
                    [rebuild_complete, GUID],
                    logs(),
                    State#state.log_levels),
    LastRebuild = os:timestamp(),
    ok = store_manifest(State#state.root_path, 
                        #manifest{current_guid = GUID,
                                    last_rebuild = LastRebuild},
                        State#state.log_levels),
    {next_state, native, State#state{last_rebuild = LastRebuild}}.



handle_sync_event(bucket_list, _From, StateName, State) ->
    Folder = bucket_list(State#state.store_type, State#state.store),
    {reply, Folder, StateName, State};
handle_sync_event(current_status, _From, StateName, State) ->
    {reply, {StateName, State#state.current_guid}, StateName, State}.

handle_event({log_level, LogLevels}, StateName, State) ->
    MinLevel = aae_util:min_loglevel(LogLevels),
    BackendOpts = [{log_level, MinLevel}|?LEVELED_BACKEND_OPTS],
        % Change of log level will take place after next store is started
    {next_state,
        StateName,
        State#state{log_levels = LogLevels, backend_opts = BackendOpts}}.

handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

terminate(normal, native, _State) ->
    ok;
terminate(normal, _StateName, State) ->
    store_manifest(State#state.root_path, 
                    #manifest{current_guid = State#state.current_guid,
                                last_rebuild = State#state.last_rebuild},
                    State#state.log_levels).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.



%%%============================================================================
%%% Key Codec
%%%============================================================================

-spec define_addobjectspec(bucket(), key(), value()) -> objectspec().
%% @doc
%% Create an ObjectSpec for adding to the backend store
define_addobjectspec(Bucket, Key, V) ->
    #objectspec{op = add, 
                segment_id = 
                    value(parallel, {aae_segment, null}, {Bucket, Key, V}),
                last_mod_dates =
                    value(parallel, {lmd, null}, {Bucket, Key, V}),
                bucket = Bucket, 
                key = Key, 
                value = V}.

-spec define_delobjectspec(bucket(), key(), non_neg_integer())
                                                            -> objectspec().
%% @doc
%% Create an object spec to remove an object from the backend store
define_delobjectspec(Bucket, Key, Segment) ->
    #objectspec{op = remove, 
                segment_id = Segment,
                bucket = Bucket, 
                key = Key, 
                value = null}.

-spec check_objectspec(binary(), binary(), objectspec()) 
                                                -> {ok, tuple()|null}|false.
%% @doc
%% Check to see if an objetc spec matches the bucket and key, and if so 
%% return {ok, Value} (or false if not).
check_objectspec(Bucket, Key, ObjSpec) ->
    case {ObjSpec#objectspec.bucket, ObjSpec#objectspec.key} of
        {Bucket, Key} ->
            {ok, ObjSpec#objectspec.value};
        _ ->
            false
    end.


-spec generate_treesegment(leveled_tictac:segment48()) -> integer().
%% @doc
%% Get actual SegmentID for tree
generate_treesegment(SegmentID) ->
    Seg32_int = leveled_tictac:keyto_segment32(SegmentID),
    leveled_tictac:get_segment(Seg32_int, ?TREE_SIZE).

-spec generate_value(tuple(), integer(), 
                        aae_controller:version_vector(), integer(), 
                        {integer(), integer(), integer(), 
                            list(os:timestamp())|undefined, metadata()})
                                                        -> value_v2().
%% @doc
%% Create a value based on the current active version number
%% Currently the "head" is ignored.  This may be changed to support other 
%% coverage fold features in the future.  Other items
%% PreflistID - {Partition, NVal} - although this is largeish, compression 
%% should minimise the disk footprint
%% Clock - A Riak dotted version vector of {Actor, Seqn}
%% Hash - In this case a hash of the VClock
%% Size - The byte size of the binary Riak object (uncompressed)
%% SibCount - the count of siblings for the object (on this vnode only)
%% IndexHash - A Hash of all the index fields and terms for this object
generate_value(PreflistID, SegTS_int, Clock, Hash, 
                            {Size, SibCount, IndexHash, SibLMD, SibMD}) ->
    {?VALUE_VERSION, 
        {PreflistID, Clock, Hash, Size, SibCount, IndexHash, SegTS_int,
            SibLMD, SibMD}}.

%% Some helper functions for accessing individual value elements by version, 
%% intended to make it easier to uplift the version of the value format at a
%% later date
%%
%% These value functions will change between native and parallel stores


-spec value(parallel|native, {atom(), fun()|null}, {bucket(), key(), value()})
                                                                    -> any().
%% @doc
%% Return the value referenced by the secend input from the tuple containing
%% {Bucket, Key, Value}.  For the parallel store this should be a straight 
%% element fetch.  For the native store this will require the binary to be 
%% processed.  A function can be passed in to handle the scenario where this
%% is not pre-defined
value(parallel, {preflist, _F}, {_B, _K, {2, ValueItems}}) ->
    element(1, ValueItems);
value(parallel, {clock, _F}, {_B, _K, {2, ValueItems}}) ->
    element(2, ValueItems);
value(parallel, {hash, _F}, {_B, _K, {2, ValueItems}}) ->
    element(3, ValueItems);
value(parallel, {size, _F}, {_B, _K, {2, ValueItems}}) ->
    element(4, ValueItems);
value(parallel, {sibcount, _F}, {_B, _K, {2, ValueItems}}) ->
    element(5, ValueItems);
value(parallel, {indexhash, _F}, {_B, _K, {2, ValueItems}}) ->
    element(6, ValueItems);
value(parallel, {aae_segment, _F}, {_B, _K, {2, ValueItems}}) ->
    element(7, ValueItems);
value(parallel, {lmd, _F}, {_B, _K, {2, ValueItems}}) ->
    element(8, ValueItems);
value(parallel, {md, _F}, {_B, _K, {2, ValueItems}}) ->
    element(9, ValueItems);
value(native, {clock, _F}, {_B, _K, V}) ->
    element(1, summary_from_native(V));
value(native, {hash, _F}, {_B, _K, V}) ->
    {VC, _Sz, _SC, _MB} = summary_from_native(V),
    element(1, aae_controller:hash_clocks(VC, none));
value(native, {size, _F}, {_B, _K, V}) ->
    element(2, summary_from_native(V));
value(native, {sibcount, _F}, {_B, _K, V}) ->
    element(3, summary_from_native(V));
value(native, {aae_segment, null}, {B, K, _V}) ->
    BinaryKey = aae_util:make_binarykey(B, K),
    SegmentID = leveled_tictac:keyto_segment48(BinaryKey),
    aae_keystore:generate_treesegment(SegmentID);
value(native, {md, null}, {_B, _K, V}) ->
    element(4, summary_from_native(V));
value(native, {_NotExposed, F}, {B, K, _V}) ->
    % preflist, aae_segment, index_hash
    F(B, K).


-spec summary_from_native(binary()) 
                        -> {aae_controller:version_vector(), 
                            non_neg_integer(), non_neg_integer(),
                            binary()}.
%% @doc
%% Extract only summary information from the binary - the vector, the object 
%% size and the sibling count
summary_from_native(<<131, _Rest/binary>>=ObjBin) ->  
    {proxy_object, HeadBin, ObjSize, _Fetcher} 
        = binary_to_term(ObjBin),
    summary_from_native(HeadBin, ObjSize).

-spec summary_from_native(binary(), integer()) 
                        -> {aae_controller:version_vector(),
                            non_neg_integer(), non_neg_integer(),
                            binary()}.
%% @doc 
%% Split a version 1 binary, as found in the native store
summary_from_native(ObjBin, ObjSize) ->
    <<?MAGIC:8/integer, 
        1:8/integer, 
        VclockLen:32/integer, VclockBin:VclockLen/binary, 
        SibCount:32/integer, 
        SibBin/binary>> = ObjBin,
    {binary_to_term(VclockBin), ObjSize, SibCount, SibBin}.


-spec convert_segmentlimiter(segment_limiter()) ->
                            {segment_checker(), false|list(non_neg_integer())}.
%% @doc
%% Convert the passed in segment list to something that us usable by
%% member_check/2
convert_segmentlimiter(all) ->
    {all, false};
convert_segmentlimiter({segments, SegList, TreeSize}) ->
    SegList0 =
        leveled_tictac:adjust_segmentmatch_list(SegList, TreeSize, ?TREE_SIZE),
    case length(SegList0) > ?USE_SET_FOR_SPEED of
        true ->
            {{sets, sets:from_list(SegList0)}, SegList};
        false ->
            {{list, SegList0}, SegList}
    end.

-spec member_check(non_neg_integer(), segment_checker()) -> boolean().
%% @doc
%% Check to see if a segment is a member of a segment list (post conversion)
member_check(_Hash, all) ->
    true;
member_check(Hash, {list, HashList}) ->
    lists:member(Hash, HashList);
member_check(Hash, {sets, HashSet}) ->
    sets:is_element(Hash, HashSet).


%%%============================================================================
%%% Store functions
%%%============================================================================

-spec maybe_trim(parallel_stores(), integer(), pid()) -> ok.
%% @doc
%% Every Trim count, trim the store
maybe_trim(StoreType, TrimCount, Store) ->
    case TrimCount rem ?MAYBE_TRIM of
        0 ->
            case StoreType of
                leveled_so ->
                    leveled_bookie:book_trimjournal(Store);
                leveled_ko ->
                    leveled_bookie:book_trimjournal(Store)
            end;
        _ ->
            ok
    end.
            

-spec fold_elements_fun(fun(), list(value_element()), native|parallel) 
                                                                    -> fun().
%% @doc
%% Add a filter to the Fold Objects Fun so that the passed in fun sees a tuple
%% whose elements are the requested elements passed in, rather than the actual
%% value
fold_elements_fun(FoldObjectsFun, Elements, StoreType) ->
    fun(B, K, V, Acc) ->
        ValueMapFun =
            fun({E, F}) ->
                {E, value(StoreType, {E, F}, {B, K, V})}
            end,
        ElementValues = lists:map(ValueMapFun, Elements),
        FoldObjectsFun(B, K, ElementValues, Acc)
    end.

-spec is_empty(parallel_stores(), pid()) -> boolean().
%% @doc
%% Check to see if the store is empty
is_empty(leveled_so, Store) ->
    leveled_bookie:book_isempty(Store, ?HEAD_TAG);
is_empty(leveled_ko, Store) ->
    leveled_bookie:book_isempty(Store, ?HEAD_TAG).

-spec close_store(parallel_stores(), pid(), close|destroy) -> ok.
%% @doc
%% Wait for store to close
close_store(PS, Store, close) when PS == leveled_so; PS == leveled_ko ->
    leveled_bookie:book_close(Store);
close_store(PS, Store, destroy) when PS == leveled_so; PS == leveled_ko ->
    leveled_bookie:book_destroy(Store).

-spec open_store(parallel_stores(), list(), list(), list()) -> {ok, pid()}.
%% @doc
%% Open a parallel backend key store
open_store(leveled_so, BackendOpts0, RootPath, GUID) ->
    BackendOpts1 = add_path_toopts(BackendOpts0, RootPath, GUID),
    BackendOpts = [{head_only, no_lookup}|BackendOpts1],
    leveled_bookie:book_start(BackendOpts);
open_store(leveled_ko, BackendOpts0, RootPath, GUID) ->
    BackendOpts1 = add_path_toopts(BackendOpts0, RootPath, GUID),
    BackendOpts = [{head_only, with_lookup}|BackendOpts1],
    leveled_bookie:book_start(BackendOpts).


add_path_toopts(BackendOpts, RootPath, GUID) ->
    Path = filename:join(RootPath, GUID),
    ok = filelib:ensure_dir(Path),
    [{root_path, Path}|BackendOpts].


-spec delete_store(parallel_stores(), pid()) -> ok.
%% @doc
%% delete the store - as it has been replaced by a rebuild
delete_store(leveled_so, Store) ->
    leveled_bookie:book_destroy(Store);
delete_store(leveled_ko, Store) ->
    leveled_bookie:book_destroy(Store).

-spec do_load(parallel_stores(), pid(), list(tuple())) -> ok.
%% @doc
%% Load a batch of object specifications into the store
do_load(leveled_so, Store, ObjectSpecs) ->
    leveled_bookie:book_mput(Store, dedup_map(leveled_so, ObjectSpecs)),
    ok;
do_load(leveled_ko, Store, ObjectSpecs) ->
    leveled_bookie:book_mput(Store, dedup_map(leveled_ko, ObjectSpecs)),
    ok.

-spec dedup_map(parallel_stores(), list(objectspec())) -> list(tuple()).
%% @doc
%% Map the spec records into tuples as required by the backend store type, 
%% and dedup assuming the left-most record is the most recent Bucket/Key
dedup_map(leveled_so, ObjectSpecs) ->
    SegmentOrderedMapFun = 
        fun(ObjSpec) ->
            SegTS_int = ObjSpec#objectspec.segment_id,
            {ObjSpec#objectspec.op,
                v1,
                <<SegTS_int:24/integer>>, 
                term_to_binary({ObjSpec#objectspec.bucket, 
                                ObjSpec#objectspec.key}), 
                ?NULL_SUBKEY,
                ObjSpec#objectspec.last_mod_dates,
                ObjSpec#objectspec.value}
        end,
    lists:ukeysort(4, lists:map(SegmentOrderedMapFun, ObjectSpecs));
dedup_map(leveled_ko, ObjectSpecs) ->
    FoldFun =
        fun(ObjSpec, {Acc, Members}) ->
            B = ObjSpec#objectspec.bucket,
            K = ObjSpec#objectspec.key,
            case lists:member({B, K}, Members) of
                true ->
                    {Acc, Members};
                false ->
                    UpdSpec = {ObjSpec#objectspec.op,
                                v1,
                                B, K, ?NULL_SUBKEY,
                                ObjSpec#objectspec.last_mod_dates,
                                ObjSpec#objectspec.value},
                    {[UpdSpec|Acc], [{B, K}|Members]}
            end
        end,
    {UpdSpecL, _MemberL} = lists:foldl(FoldFun, {[], []}, ObjectSpecs),
    UpdSpecL.
            

-spec do_fetchclock(parallel_stores(), pid(), binary(), binary()) 
                                            -> aae_controller:version_vector().
%% @doc
%% Fetch an indicvidual clock for an individual key.  This will be done by 
%% direct fetch for key-ordered backends, and by fold for segment_ordered 
%% backends
do_fetchclock(leveled_so, Store, Bucket, Key) ->
    Seg = leveled_tictac:keyto_segment48(aae_util:make_binarykey(Bucket, Key)),
    Seg0 = generate_treesegment(Seg),
    do_fetchclock(leveled_so, Store, Bucket, Key, Seg0);
do_fetchclock(leveled_ko, Store, Bucket, Key) ->
    case leveled_bookie:book_headonly(Store, Bucket, Key, ?NULL_SUBKEY) of
        not_found ->
            none;
        {ok, V} ->
            value(parallel, {clock, null}, {Bucket, Key, V})
    end.

-spec do_fetchclock(leveled_so, pid(), binary(), binary(), integer())
                                            -> aae_controller:version_vector().
%% @doc
%% Specific function to allow fetch_clock from leveled segment_ordered backend.
%% This function is split-out from do_fetchclock/4 to make unit testing of this
%% scenario easier.
do_fetchclock(leveled_so, Store, Bucket, Key, Seg) ->
    FoldFun  = 
        fun(B, K, V, Acc) ->
            case {B, K} of 
                {Bucket, Key} ->
                    {clock, Clock} = lists:keyfind(clock, 1, V),
                    Clock;
                _ ->
                    Acc
            end
        end,
    InitAcc = none,
    {async, Folder} = 
        do_fold(leveled_so, Store, all,
                        {segments, [Seg], ?TREE_SIZE},
                        all, false,
                        FoldFun, InitAcc, [{clock, null}]),
    Folder().


-spec bucket_list(parallel_stores(), pid()) -> {async, fun()}.
%% @doc
%% List buckets in backend - using fast skipping method native to leveled if
%% the backend is key-ordered.
bucket_list(leveled_so, Store) ->
    FoldFun = 
        fun(B, _K, _V, Acc) ->
            case lists:member(B, Acc) of
                true ->
                    Acc;
                false ->
                    lists:usort([B|Acc])
            end
        end,
    do_fold(leveled_so, Store, all, all, all, false, FoldFun, [], []);
bucket_list(leveled_ko, Store) ->
    FoldFun =  fun(Bucket, Acc) -> Acc ++ [Bucket] end,
    leveled_bookie:book_bucketlist(Store, ?HEAD_TAG, {FoldFun, []}, all);
bucket_list(leveled_nko, Store) ->
    FoldFun =  fun(Bucket, Acc) -> Acc ++ [Bucket] end,
    leveled_bookie:book_bucketlist(Store, ?RIAK_TAG, {FoldFun, []}, all).


-spec do_fold(parallel_stores(), pid(), 
                        range_limiter(), segment_limiter(),
                        modified_limiter(), count_limiter(),
                        fun(), any(),
                        list(value_element())) -> {async, fun()}.
%% @doc
%% Fold over the store applying FoldObjectsFun to each object and the 
%% accumulator.  The store can be limited by a list of segments or a list
%% of buckets, or can be set to all to fold over the whole store.
%%
%% Folds should always be async - they should return (async, Folder) for 
%% Folder() to be called by the worker process.  The snapshot should have
%% been taken prior to the fold, and should not happen as part of the fold.
%% There should be no refresh of the iterator, the snapshot should last the
%% duration of the fold.
do_fold(leveled_so, Store, Range, SegFilter, LMDRange, MoC,
                                            FoldObjectsFun, InitAcc, Elements)
                                                        when is_integer(MoC) ->
    {async, Runner} = 
        do_fold(leveled_so, 
                Store, Range, SegFilter, LMDRange, false,
                FoldObjectsFun, InitAcc, Elements),
    {async, fun() -> {-1, Runner()} end};
do_fold(leveled_so, Store, Range, SegFilter, LMDRange, false,
                                            FoldFun, InitAcc, Elements) ->
    FoldFun0 = 
        fun(_S, {BKBin, _SK}, V, Acc) ->
            {B, K} = binary_to_term(BKBin),
            case range_check(Range, B, K) of
                true ->
                    FoldFun(B, K, V, Acc);
                false ->
                    Acc
            end
        end,
    FoldElementsFun = fold_elements_fun(FoldFun0, Elements, parallel),
    case SegFilter of
        all ->
            leveled_bookie:book_headfold(Store, 
                                            ?HEAD_TAG,
                                            all,
                                            {FoldElementsFun, InitAcc}, 
                                            ?NOCHECK_PRESENCE, 
                                            ?SNAP_PREFOLD, 
                                            false,
                                            modify_modifiedrange(LMDRange),
                                            false);
        {segments, SegList, TreeSize} ->
            SegList0 = 
                leveled_tictac:adjust_segmentmatch_list(SegList,
                                                        TreeSize,
                                                        ?TREE_SIZE),
            MapSegFun = 
                fun(S) -> 
                    S0 = leveled_tictac:get_segment(S, ?TREE_SIZE),
                    <<S0:24/integer>> 
                end,
            SegList1 = lists:map(MapSegFun, SegList0),
            leveled_bookie:book_headfold(Store, 
                                            ?HEAD_TAG,
                                            {bucket_list, SegList1},
                                            {FoldElementsFun, InitAcc}, 
                                            ?NOCHECK_PRESENCE, 
                                            ?SNAP_PREFOLD, 
                                            false,
                                            modify_modifiedrange(LMDRange),
                                            false)
    end;
do_fold(leveled_ko, Store, Range, SegFilter, LMDRange, MaxObjects,
                                                FoldFun, InitAcc, Elements) ->
    {SegChecker, SegList} = convert_segmentlimiter(SegFilter),
    Elements0 = lists:ukeysort(1, [{aae_segment, null}|Elements]),
    FoldFun0 =
        fun(B, {K, ?NULL_SUBKEY}, V, Acc) ->
            {aae_segment, SegID} = lists:keyfind(aae_segment, 1, V),
            case member_check(SegID, SegChecker) of
                true ->
                    FoldFun(B, K, V, Acc);
                false ->
                    Acc
            end
        end,
    FoldElementsFun = fold_elements_fun(FoldFun0, Elements0, parallel),
    ReformattedRange =
        case Range of
            {key_range, B0, SK, EK} ->
                {range, B0,  {{SK, ?NULL_SUBKEY}, {EK, ?NULL_SUBKEY}}};
            {buckets, BucketList} ->
                {bucket_list, BucketList};
            all ->
                all
        end,
    leveled_bookie:book_headfold(Store,
                                    ?HEAD_TAG, ReformattedRange,
                                    {FoldElementsFun, InitAcc}, 
                                    ?NOCHECK_PRESENCE, ?SNAP_PREFOLD, 
                                    SegList, modify_modifiedrange(LMDRange),
                                    MaxObjects);
do_fold(leveled_nko, Store, Range, SegFilter, LMDRange, MaxObjects,
                                        FoldFun, InitAcc, Elements) ->
    {SegChecker, SegList} = convert_segmentlimiter(SegFilter),
    Elements0 = lists:ukeysort(1, [{aae_segment, null}|Elements]),
    FoldFun0 =
        fun(B, K, V, Acc) ->
            {aae_segment, Seg} = lists:keyfind(aae_segment, 1, V),
            case member_check(Seg, SegChecker) of
                true ->
                    FoldFun(B, K, V, Acc);
                false ->
                    Acc
            end
        end,
    FoldElementsFun = fold_elements_fun(FoldFun0, Elements0, native),
    {ReformattedRange, CheckPresence} =
        case Range of
            {key_range, B0, SK, EK} ->
                {{range, B0, {SK, EK}}, ?NOCHECK_PRESENCE};
            {buckets, BucketList} ->
                {{bucket_list, BucketList}, ?NOCHECK_PRESENCE};
            all ->
                % Rebuilds should check presence
                {all, ?CHECK_NATIVE_PRESENCE}
        end,
    leveled_bookie:book_headfold(Store,
                                    ?RIAK_TAG, ReformattedRange,
                                    {FoldElementsFun, InitAcc}, 
                                    CheckPresence, ?SNAP_PREFOLD, 
                                    SegList, modify_modifiedrange(LMDRange),
                                    MaxObjects).


modify_modifiedrange(all) ->
    false;
modify_modifiedrange({ST, ET}) ->
    {ST, ET}.

range_check(all, _B, _K) ->
    true;
range_check({buckets, BucketList}, Bucket, _K) ->
    lists:member(Bucket, BucketList);
range_check({key_range, Bucket, StartKey, EndKey}, Bucket, Key) ->
    case {Key >= StartKey, Key =< EndKey} of
        {true, true} ->
            true;
        _ ->
            false
    end;
range_check(_Range, _B, _K) ->
    false.


-spec open_manifest(list(), aae_util:log_levels()|undefined)
                                                    -> {ok, manifest()}|false.
%% @doc
%% Open the manifest file, check the CRC and return the active folder 
%% reference
open_manifest(RootPath, LogLevels) ->
    FN = filename:join(RootPath, ?MANIFEST_FN ++ ?COMLPETE_EXT),
    case aae_util:safe_open(FN) of
        {ok, BinaryContents} ->
            M = binary_to_term(BinaryContents),
            aae_util:log("KS005",
                            [M#manifest.current_guid],
                            logs(),
                            LogLevels),
            {ok, M};
        {error, Reason} ->
            aae_util:log("KS002",
                            [RootPath, Reason],
                            logs(),
                            LogLevels),
            false
    end.
            
-spec store_manifest(list(), manifest(), aae_util:log_levels()|undefined)
                                                                        -> ok.
%% @doc
%% Store tham manifest file, adding a CRC, and ensuring it is readable before
%% returning
store_manifest(RootPath, Manifest, LogLevels) ->
    aae_util:log("KS003", [Manifest#manifest.current_guid], logs(), LogLevels),
    ManBin = term_to_binary(Manifest),
    CRC32 = erlang:crc32(ManBin),
    PFN = filename:join(RootPath, ?MANIFEST_FN ++ ?PENDING_EXT),
    CFN = filename:join(RootPath, ?MANIFEST_FN ++ ?COMLPETE_EXT),
    ok = filelib:ensure_dir(PFN),
    ok = file:write_file(PFN, <<CRC32:32/integer, ManBin/binary>>, [raw]),
    ok = file:rename(PFN, CFN),
    {ok, <<CRC32:32/integer, ManBin/binary>>} = file:read_file(CFN),
    ok.


-spec clear_pendingpath(manifest(), list()) -> manifest().
%% @doc
%% Clear any Keystore that had been partially loaded at the last shutdown
clear_pendingpath(Manifest, RootPath) ->
    case Manifest#manifest.pending_guid of 
        undefined ->
            Manifest;
        GUID ->
            PendingPath = filename:join(RootPath, GUID),
            aae_util:log("KS006", [PendingPath], logs()),
            Manifest#manifest{pending_guid = undefined}
    end.



%%%============================================================================
%%% log definitions
%%%============================================================================

-spec logs() -> list(tuple()).
%% @doc
%% Define log lines for this module
logs() ->
    [{"KS001", 
            {info, "Key Store loading with id=~w has reached " 
                    ++ "deferred count=~w"}},
        {"KS002",
            {warn, "No valid manifest found for AAE keystore at ~s "
                    ++ "reason ~s"}},
        {"KS003",
            {info, "Storing manifest with current GUID ~s"}},
        {"KS004", 
            {info, "Key Store building with id=~w has reached " 
                    ++ "loaded count=~w"}},
        {"KS005",
            {info, "Clean opening of manifest with current GUID ~s"}},
        {"KS006",
            {warn, "Pending store is garbage and should be deleted at ~s"}},
        {"KS007",
            {info, "Rebuild prompt ~w with GUID ~s"}}

        ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


store_fold(Pid, RLimiter, SLimiter, FoldObjectsFun, InitAcc, Elements) ->
    store_fold(Pid, RLimiter, SLimiter, all, false, 
                FoldObjectsFun, InitAcc, Elements).

-spec store_currentstatus(pid()) -> {atom(), list()}.
%% @doc
%% Included for test functions only - get the manifest
store_currentstatus(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, current_status).


leveled_so_emptybuildandclose_test() ->
    empty_buildandclose_tester(leveled_so).

leveled_ko_emptybuildandclose_test() ->
    empty_buildandclose_tester(leveled_ko).

empty_buildandclose_tester(StoreType) ->
    RootPath = "test/keystore0/",
    aae_util:clean_subdir(RootPath),
    {ok, {never, true}, Store0} =
        store_parallelstart(RootPath, StoreType, undefined),
    
    {ok, Manifest0} = open_manifest(RootPath, undefined),
    {parallel, CurrentGUID} = store_currentstatus(Store0),
    ?assertMatch(CurrentGUID, Manifest0#manifest.current_guid),
    ?assertMatch(none, Manifest0#manifest.shutdown_guid),
    
    ok = store_close(Store0),
    {ok, Manifest1} = open_manifest(RootPath, undefined),
    ?assertMatch(CurrentGUID, Manifest1#manifest.current_guid),
    
    {ok, {never, true}, Store1} =
        store_parallelstart(RootPath, StoreType, undefined),
    ?assertMatch({parallel, CurrentGUID}, store_currentstatus(Store1)),
    ok = store_close(Store1),
    
    aae_util:clean_subdir(RootPath).

bad_manifest_test() ->
    RootPath = "test/keystore1/",
    ok = filelib:ensure_dir(RootPath),
    aae_util:clean_subdir(RootPath),
    ?assertMatch(false, open_manifest(RootPath, undefined)),
    Manifest = #manifest{current_guid = "aaa-111"},
    ok = store_manifest(RootPath, Manifest, undefined),
    ?assertMatch({ok, Manifest}, open_manifest(RootPath, [])),
    ManifestFN = filename:join(RootPath, ?MANIFEST_FN ++ ?COMLPETE_EXT),
    {ok, Bin0} = file:read_file(ManifestFN),
    Bin1 = aae_util:flip_byte(Bin0, 0, byte_size(Bin0)),
    ok = file:delete(ManifestFN),
    ok = file:write_file(ManifestFN, Bin1),
    ?assertMatch(false, open_manifest(RootPath, undefined)),
    ok = file:delete(ManifestFN),
    ok = file:write_file(ManifestFN, Bin0),
    ?assertMatch({ok, Manifest}, open_manifest(RootPath, [])),
    aae_util:clean_subdir(RootPath).

empty_manifest_test() ->
    RootPath = "test/emptyman1/",
    ok = filelib:ensure_dir(RootPath),
    aae_util:clean_subdir(RootPath),
    ManifestFN = filename:join(RootPath, ?MANIFEST_FN ++ ?COMLPETE_EXT),
    ok = file:write_file(ManifestFN, <<>>),
    ?assertMatch(false, open_manifest(RootPath, undefined)),
    aae_util:clean_subdir(RootPath).


leveled_so_load_test() ->
    load_tester(leveled_so).

leveled_ko_load_test() ->
    load_tester(leveled_ko).

load_tester(StoreType) ->
    RootPath = "test/keystore2/",
    ok = filelib:ensure_dir(RootPath),
    aae_util:clean_subdir(RootPath),

    GenerateKeyFun = aae_util:test_key_generator(v1),

    InitialKeys = lists:map(GenerateKeyFun, lists:seq(1, 100)),
    AlternateKeys = lists:map(GenerateKeyFun, lists:seq(61, 80)),
    RemoveKeys = lists:map(GenerateKeyFun, lists:seq(81, 100)),

    FinalState = 
        lists:map(fun({K, V}) -> {<<"B1">>, K, element(1, V)} end, 
                    lists:sublist(InitialKeys, 60) ++ AlternateKeys),
    

    ObjectSpecs = 
        generate_objectspecs(add, <<"B1">>, InitialKeys) ++
        generate_objectspecs(add, <<"B1">>, AlternateKeys) ++
        generate_objectspecs(remove, <<"B1">>, RemoveKeys),
    
    {L1, R1} = lists:split(32, ObjectSpecs),
    {L2, R2} = lists:split(32, R1),
    {L3, R3} = lists:split(32, R2),
    {L4, L5} = lists:split(32, R3),

    {ok, {never, true}, Store0} =
        store_parallelstart(RootPath, StoreType, undefined),
    ok = store_mput(Store0, L1),
    ok = store_mput(Store0, L2),
    ok = store_mput(Store0, L3),

    FoldObjectsFun =
        fun(B, K, V, Acc) ->
            {clock, VC} = lists:keyfind(clock, 1, V),
            [{B, K, VC}|Acc]
        end,
    
    {async, Folder0} = 
        store_fold(Store0, all, all, FoldObjectsFun, [], [{clock, null}]),
    Res0 = lists:usort(Folder0()),
    ?assertMatch(96, length(Res0)),
    
    ok = store_prompt(Store0, rebuild_start),

    % Need to prove fetch_clock in loading state, otherwise not covered
    [{FirstKey, FirstValue}|_RestIK] = InitialKeys,
    FirstClock = element(1, FirstValue),
    ?assertMatch(FirstClock, store_fetchclock(Store0, <<"B1">>, FirstKey)),

    {async, Folder1} = 
        store_fold(Store0, all, all, FoldObjectsFun, [], [{clock, null}]),
    Res1 = lists:usort(Folder1()),
    ?assertMatch(Res0, Res1),

    ok = store_mput(Store0, L4),

    % 4 adds, 20 alterations, 8 removes -> 92 entries
    {async, Folder2} = 
        store_fold(Store0, all, all, FoldObjectsFun, [], [{clock, null}]),
    Res2 = lists:usort(Folder2()),
    ?assertMatch(92, length(Res2)),

    ok = store_mload(Store0, L1),
    ok = store_mload(Store0, L2),
    ok = store_mload(Store0, L3),

    {async, Folder3} = 
        store_fold(Store0, all, all, FoldObjectsFun, [], [{clock, null}]),
    Res3 = lists:usort(Folder3()),
    ?assertMatch(Res2, Res3),

    RebuildCompleteTime = os:timestamp(),

    ok = store_prompt(Store0, rebuild_complete),

    {async, Folder4} = 
        store_fold(Store0, all, all, FoldObjectsFun, [], [{clock, null}]),
    Res4 = lists:usort(Folder4()),
    ?assertMatch(Res2, Res4),

    ok = store_mput(Store0, L5),

    % Removes now complete so only 80 entries
    {async, Folder5} = 
        store_fold(Store0, all, all, FoldObjectsFun, [], [{clock, null}]),
    Res5 = lists:usort(Folder5()),
    ?assertMatch(FinalState, Res5),

    ok = store_close(Store0),

    {ok, {LastRebuildTime, false}, Store1} 
        = store_parallelstart(RootPath, StoreType, undefined),
    
    ?assertMatch(true, LastRebuildTime > RebuildCompleteTime),
   
    {async, Folder6} = 
        store_fold(Store1, all, all, FoldObjectsFun, [], [{clock, null}]),
    Res6 = lists:usort(Folder6()),
    ?assertMatch(Res5, Res6),

    {async, Folder7} = 
        store_fold(Store1, 
                    {buckets, [<<"B1">>]},
                    all,
                    FoldObjectsFun, [], 
                    [{clock, null}]),
    Res7 = lists:usort(Folder7()),
    ?assertMatch(Res5, Res7),

    {async, Folder8} = 
        store_fold(Store1, 
                    {buckets, [<<"B2">>]}, 
                    all,
                    FoldObjectsFun, [], 
                    [{clock, null}]),
    Res8 = lists:usort(Folder8()),
    ?assertMatch([], Res8),

    [{B1, K1, _V1}|_Rest] = FinalState,
    {BL, KL, _VL} = lists:last(FinalState),
    SegList = [aae_util:get_segmentid(B1, K1), aae_util:get_segmentid(BL, KL)],
    CheckSegFun = 
        fun({B, K, V}, Acc) ->
            S = aae_util:get_segmentid(B, K),
            case lists:member(S, SegList) of 
                true ->
                    [{B, K, V}|Acc];
                false ->
                    Acc
            end
        end,
    FinalStateSL = lists:usort(lists:foldl(CheckSegFun, [], FinalState)),
    io:format("FinalStateSL: ~w~n", [FinalStateSL]),

    {async, Folder9} = 
        store_fold(Store1,
                    all,
                    {segments, SegList, large}, 
                    FoldObjectsFun, [], 
                    [{clock, null}]),
    Res9 = lists:usort(Folder9()),
    ?assertMatch(FinalStateSL, Res9),

    ok = store_close(Store1),
    aae_util:clean_subdir(RootPath).


split_lists(L, SplitSize, Acc) when length(L) =< SplitSize ->
    [L|Acc];
split_lists(L, SplitSize, Acc) ->
    {LL, RL} = lists:split(SplitSize, L),
    split_lists(RL, SplitSize, [LL|Acc]).


fetch_clock_test() ->
    RootPath = "test/keystore4/",
    ok = filelib:ensure_dir(RootPath),
    aae_util:clean_subdir(RootPath),
    % When fetching clocks we may have multiple matches on a segment ID
    % Want to prove that here.  Rather than trying to force a hash collision
    % on the segment ID, we will just generate false segment IDs
    ObjSplitFun = fun(static) -> {0, 1, 0, null} end,
    WrappedFun = aae_controller:wrapped_splitobjfun(ObjSplitFun),
    GenVal =
        fun(Clock) ->
            generate_value({1, 3}, 1, Clock, 0, WrappedFun(static))
        end,
    Spc1 = define_addobjectspec(<<"B1">>, <<"K2">>, GenVal([{"a", 1}])),
    Spc2 = define_addobjectspec(<<"B1">>, <<"K3">>, GenVal([{"b", 1}])),
    Spc3 = define_addobjectspec(<<"B2">>, <<"K1">>, GenVal([{"c", 1}])),
    Spc4 = define_addobjectspec(<<"B1">>, <<"K1">>, GenVal([{"d", 1}])),

    {ok, Store0} = open_store(leveled_so, 
                                ?LEVELED_BACKEND_OPTS, 
                                RootPath,
                                leveled_util:generate_uuid()),

    do_load(leveled_so, Store0, [Spc1, Spc2, Spc3, Spc4]),

    ?assertMatch([{"a", 1}], 
                    do_fetchclock(leveled_so, Store0, <<"B1">>, <<"K2">>, 1)),
    ?assertMatch([{"b", 1}], 
                    do_fetchclock(leveled_so, Store0, <<"B1">>, <<"K3">>, 1)),
    ?assertMatch([{"c", 1}],
                    do_fetchclock(leveled_so, Store0, <<"B2">>, <<"K1">>, 1)),
    ?assertMatch([{"d", 1}],
                    do_fetchclock(leveled_so, Store0, <<"B1">>, <<"K1">>, 1)),
    
    ok = close_store(leveled_so, Store0, destroy),
    aae_util:clean_subdir(RootPath).
    



so_big_load_test_() ->
    {timeout, 60, fun so_big_load_tester/0}.

ko_big_load_test_() ->
    {timeout, 60, fun ko_big_load_tester/0}.

so_big_load_tester() ->
    big_load_tester(leveled_so).

ko_big_load_tester() ->
    big_load_tester(leveled_ko).

big_load_tester(StoreType) ->
    RootPath = "test/keystore3/",
    KeyCount = 2 * ?CHANGEQ_LOGFREQ,
    ok = filelib:ensure_dir(RootPath),
    aae_util:clean_subdir(RootPath),

    GenerateKeyFun = aae_util:test_key_generator(v1),

    {ok, {never, true}, Store0} =
        store_parallelstart(RootPath, StoreType, undefined),

    InitialKeys = 
        lists:map(GenerateKeyFun, lists:seq(1, KeyCount)),
    InitObjectSpecs = generate_objectspecs(add, <<"B1">>, InitialKeys),
    SubLists = timed_bulk_put(Store0, InitObjectSpecs, StoreType),

    FoldObjectsFun =
        fun(_B, _K, _V, Acc) ->
            Acc + 1
        end,
    timed_fold(Store0, KeyCount, FoldObjectsFun, StoreType, false),

    ok = store_prompt(Store0, rebuild_start),
    ok = lists:foreach(fun(L) -> store_mload(Store0, L) end, SubLists),

    AdditionalKeys = 
        lists:map(GenerateKeyFun, lists:seq(KeyCount + 1, 2 * KeyCount)),
    AdditionalObjectSpecs = 
        generate_objectspecs(add, <<"B1">>, AdditionalKeys),
    _ = timed_bulk_put(Store0, AdditionalObjectSpecs, StoreType),

    timed_fold(Store0, KeyCount, FoldObjectsFun, StoreType, true),

    ok = store_prompt(Store0, rebuild_complete),

    {async, Folder2} = store_fold(Store0, all, all, FoldObjectsFun, 0, []),
    ?assertMatch(KeyCount, Folder2() - KeyCount),

    ok = store_close(Store0),

    {ok, {RebuildTS, false}, Store1} 
        = store_parallelstart(RootPath, StoreType, undefined),
    ?assertMatch(true, RebuildTS < os:timestamp()),
    
    timed_fold(Store1, KeyCount, FoldObjectsFun, StoreType, true),

    ok = store_prompt(Store1, rebuild_start),
    OpenWhenPendingSavedFun = 
        fun(_X, {Complete, M}) ->
            case Complete of 
                true ->
                    {true, M};
                false ->
                    timer:sleep(100),
                    {ok, PMan0} = open_manifest(RootPath, undefined),
                    {not (PMan0#manifest.pending_guid == undefined), PMan0}
            end
        end,
    {true, PendingManifest} = 
        lists:foldl(OpenWhenPendingSavedFun, {false, null}, lists:seq(1,10)),

    ok = store_close(Store1),
    ?assertMatch(false, undefined == PendingManifest#manifest.pending_guid),
    M0 = clear_pendingpath(PendingManifest, RootPath),
    ?assertMatch(undefined, M0#manifest.pending_guid),

    {ok, {RebuildTS2, false}, Store2} 
        = store_parallelstart(RootPath, StoreType, undefined),
    ?assertMatch(RebuildTS, RebuildTS2),

    timed_fold(Store2, KeyCount, FoldObjectsFun, StoreType, true),

    ok = store_close(Store2),
    aae_util:clean_subdir(RootPath).


timed_fold(Store, KeyCount, FoldObjectsFun, StoreType, DoubleCount) ->
    SW = os:timestamp(),
    {async, Folder} = store_fold(Store, all, all, FoldObjectsFun, 0, []),
    case DoubleCount of 
        true ->
            ?assertMatch(KeyCount, Folder() - KeyCount);
        false ->
            ?assertMatch(KeyCount, Folder())
    end,
    io:format("Fold took ~w for StoreType ~w~n", 
                [timer:now_diff(os:timestamp(), SW), StoreType]).

timed_bulk_put(Store, ObjectSpecs, StoreType) ->
    SubLists = split_lists(ObjectSpecs, 32, []),
    % Add a duplicate entry to one of the sub-lists
    [FirstList|RestLists] = SubLists,
    [FirstSpec|_RestSpecs] = FirstList,
    FirstList0 = FirstList ++ [FirstSpec],
    SubLists0 = [FirstList0|RestLists],
    SW = os:timestamp(),
    ok = lists:foreach(fun(L) -> store_mput(Store, L) end, SubLists0),
    io:format("Load took ~w for StoreType ~w~n", 
                [timer:now_diff(os:timestamp(), SW), StoreType]),
    % Return the sublists without the duplicate
    SubLists.

coverage_cheat_test() ->
    State = #state{store_type = leveled_so},
    {next_state, native, _State} = handle_info(null, native, State),
    {ok, native, _State} = code_change(null, native, State, null).

dumb_value_test() ->
    V = generate_value({0, 3}, 0, [{a, 1}], erlang:phash2(<<>>), 
                        {100, 1, 0, undefined, term_to_binary([])}),
    ?assertMatch(100, value(parallel, {size, null}, {<<"B">>, <<"K">>, V})),
    ?assertMatch(1, value(parallel, {sibcount, null}, {<<"B">>, <<"K">>, V})),
    ?assertMatch(0, value(parallel, {indexhash, null}, {<<"B">>, <<"K">>, V})).

generate_objectspecs(Op, B, KeyList) ->
    FoldFun = 
        fun({K, V}) ->
            {Clock, Hash, Size, SibCount} = V,
            Seg = 
                leveled_tictac:keyto_segment48(aae_util:make_binarykey(B, K)),
            Seg0 = generate_treesegment(Seg),
            Value = generate_value({0, 0}, Seg0, Clock, Hash, 
                                    {Size, SibCount, 0, 
                                        undefined, term_to_binary([])}),
            
            case Op of
                add ->
                    define_addobjectspec(B, K, Value);
                remove ->
                    define_delobjectspec(B, K, Seg0)
            end
        end,
    lists:map(FoldFun, KeyList).
            


-endif.