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

-export([store_parallelstart/2,
            store_startupdata/1,
            store_close/2,
            store_mput/2,
            store_mload/2,
            store_prompt/2,
            store_fold/4]).

-export([define_objectspec/5,
            generate_value/5,
            generate_treesegment/1,
            value_preflist/1,
            value_clock/1,
            value_hash/1,
            value_size/1,
            value_sibcount/1,
            value_indexhash/1,
            value_aaesegment/1]).

-record(state, {vnode :: pid(),
                store :: pid(),
                id = "KeyStore" :: any(),
                store_type :: supported_stores(),
                change_queue = [] :: list(),
                change_queue_counter = 0 :: integer(),
                load_counter = 0 :: integer(),
                current_guid :: list(),
                root_path :: list(),
                last_rebuild :: os:timestamp()|never,
                load_store :: pid(),
                load_guid :: list(),
                backend_opts :: list(),
                shutdown_guid = none :: list()|none}).

-record(manifest, {current_guid :: list(), 
                    pending_guid :: list(), 
                    last_rebuild = never :: erlang:timestamp()|never, 
                    shutdown_guid = none :: list()|none}).

-record(objectspec, {op :: add|remove,
                        segment_id :: integer(),
                        bucket :: binary(),
                        key :: binary(),
                        value = null :: tuple()|null}).

-include_lib("eunit/include/eunit.hrl").

-define(LEVELED_BACKEND_OPTS, [{max_pencillercachesize, 16000},
                                    {sync_strategy, none},
                                    {max_journalsize, 1000000}]).
-define(CHANGEQ_LOGFREQ, 10000).
-define(HEAD_TAG, h). 
    % Used in leveled as a Tag for head-only objects
-define(STATE_BUCKET, <<"state">>).
-define(MANIFEST_FN, "keystore"). 
    % filename for Keystore manifes
-define(COMLPETE_EXT, ".man"). 
    % file extension to be used once manifest write is complete
-define(PENDING_EXT, ".pnd").
    % file extension to be used once manifest write is pending
-define(VALUE_VERSION, 1).

-type supported_stores() :: leveled_so|leveled_ko. 
    % Stores supported for parallel running
-type manifest() :: #manifest{}.
    % Saves state of what store is currently active
-type objectspec() :: #objectspec{}.

%%%============================================================================
%%% API
%%%============================================================================

-spec store_parallelstart(list(), supported_stores()) -> 
                {ok, {os:timestamp()|never, list()|none, boolean()}, pid()}.
%% @doc
%% Start a store to be run in parallel mode
store_parallelstart(Path, leveled_so) ->
    Opts = 
        [{root_path, Path}, 
            {native, {false, leveled_so}}, 
            {backend_opts, ?LEVELED_BACKEND_OPTS}],
    {ok, Pid} = gen_fsm:start(?MODULE, [Opts], []),
    store_startupdata(Pid);
store_parallelstart(Path, leveled_ko) ->
    Opts = 
        [{root_path, Path}, 
            {native, {false, leveled_ko}}, 
            {backend_opts, ?LEVELED_BACKEND_OPTS}],
    {ok, Pid} = gen_fsm:start(?MODULE, [Opts], []),
    store_startupdata(Pid).

-spec store_startupdata(pid()) ->
                {ok, {os:timestamp()|never, list()|none, boolean()}, pid()}.
%% @doc
%% Get the startup metadata from the store
store_startupdata(Pid) ->
    {LastRebuild, ShutdownGUID, IsEmpty} 
        = gen_fsm:sync_send_event(Pid, startup_metadata),
    {ok, {LastRebuild, ShutdownGUID, IsEmpty}, Pid}.


-spec store_close(pid(), list()|none) -> ok.
%% @doc
%% Close the store neatly.  If a GUID is past it will be returned when the 
%% Store is opened.  This can be used to ensure that the backend and the AAE
%% KeyStore both closed neatly (by making the storing of the GUID in the 
%% vnode backend the lasta ct before closing), if both vnode and aae backends
%% report the same GUID at startup.
%%
%% Startup should always delete the Shutdown GUID in both stores.
store_close(Pid, ShutdownGUID) ->
    gen_fsm:sync_send_event(Pid, {close, ShutdownGUID}, 10000).

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

-spec store_mload(pid(), list()) -> ok.
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

-spec store_prompt(pid(), rebuild_start|rebuild_complete) -> ok.
%% @doc
%% Prompt the store to either commence a rebuild, or complete a rebuild.
%% Commencing a rebuild should happen between a snapshot for a fold being 
%% taken, and the fold commencing with no new updates to be received between
%% the snapshot and the prompt.  The complete prompt should be made after 
%% the fold is complete
store_prompt(Pid, Prompt) ->
    gen_fsm:send_event(Pid, {prompt, Prompt}).


-spec store_fold(pid(), tuple()|all, fun(), any()) -> any()|{async, fun()}.
%% @doc
%% Return a fold function to asynchronously run a fold over a snapshot of the
%% store
store_fold(Pid, Limiter, FoldObjectsFun, InitAcc) ->
    gen_fsm:sync_send_event(Pid, 
                            {fold, Limiter, FoldObjectsFun, InitAcc}, 
                            infinity).


%%%============================================================================
%%% gen_fsm callbacks
%%%============================================================================

init([Opts]) ->
    case aae_util:get_opt(native, Opts) of 
        % true ->
        %    {stop, not_yet_implemented};
        {false, StoreType} ->
            RootPath = aae_util:get_opt(root_path, Opts),
            Manifest0 =
                case open_manifest(RootPath) of 
                    false ->
                        GUID = leveled_codec:generate_uuid(),
                        #manifest{current_guid = GUID};
                    {ok, M} ->
                        M 
                end,
            
            Manifest1 = clear_pendingpath(Manifest0, RootPath),
                
            BackendOpts0 = aae_util:get_opt(backend_opts, Opts),
            {ok, Store} = open_store(StoreType, 
                                        BackendOpts0, 
                                        RootPath,
                                        Manifest1#manifest.current_guid),
            
            LastRebuild = 
                % Need to determine when the store was last rebuilt, if it 
                % can't be determined that the store has been safely rebuilt, 
                % then an "immediate" rebuild should be triggered
                case Manifest1#manifest.shutdown_guid of 
                    none ->
                        never;
                    _GUID ->
                        Manifest1#manifest.last_rebuild
                end,
            Manifest2 = 
                Manifest1#manifest{shutdown_guid = none, 
                                    last_rebuild = LastRebuild},
            ok = store_manifest(RootPath, Manifest2),
            {ok, 
                parallel, 
                #state{store = Store, 
                        store_type = StoreType,
                        root_path = RootPath,
                        current_guid = Manifest0#manifest.current_guid,
                        shutdown_guid = Manifest0#manifest.shutdown_guid,
                        last_rebuild = LastRebuild,
                        backend_opts = BackendOpts0}}
    end.

loading({fold, Limiter, FoldObjectsFun, InitAcc}, _From, State) ->
    Result = do_fold(State#state.store_type, State#state.store,
                        Limiter, FoldObjectsFun, InitAcc),
    {reply, Result, loading, State};
loading({close, ShutdownGUID}, _From, State) ->
    ok = delete_store(State#state.store_type, State#state.load_store),
    ok = close_store(State#state.store_type, State#state.store),
    {stop, normal, ok, State#state{shutdown_guid = ShutdownGUID}}.

parallel({fold, Limiter, FoldObjectsFun, InitAcc}, _From, State) ->
    Result = do_fold(State#state.store_type, State#state.store,
                        Limiter, FoldObjectsFun, InitAcc),
    {reply, Result, parallel, State};
parallel({close, ShutdownGUID}, _From, State) ->
    ok = close_store(State#state.store_type, State#state.store),
    {stop, normal, ok, State#state{shutdown_guid = ShutdownGUID}};
parallel(startup_metadata, _From, State) ->
    IsEmpty = is_empty(State#state.store_type, State#state.store),
    {reply, 
        {State#state.last_rebuild, State#state.shutdown_guid, IsEmpty}, 
        parallel, 
        State#state{shutdown_guid = none}}.

native(_Msg, _From, State) ->
    {reply, ok, native, State}.


loading({mput, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.store, ObjectSpecs),
    ChangeQueue1 = [ObjectSpecs|State#state.change_queue],
    ObjectCount0 = State#state.change_queue_counter,
    ObjectCount1 = State#state.change_queue_counter + length(ObjectSpecs),
    ToLog = 
        ObjectCount1 div ?CHANGEQ_LOGFREQ > ObjectCount0 div ?CHANGEQ_LOGFREQ,
    case ToLog of 
        true ->
            aae_util:log("KS001", [State#state.id, ObjectCount1], logs());
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
            aae_util:log("KS004", [State#state.id, LoadCount1], logs());
        false ->
            ok
    end,
    {next_state, loading, State#state{load_counter = LoadCount1}};
loading({prompt, rebuild_complete}, State) ->
    StoreType = State#state.store_type,
    LoadStore = State#state.load_store,
    GUID = State#state.load_guid,
    LoadFun = fun(OS) -> do_load(StoreType, LoadStore, OS) end,
    lists:foreach(LoadFun, lists:reverse(State#state.change_queue)),
    ok = store_manifest(State#state.root_path, 
                        #manifest{current_guid = GUID}),
    ok = delete_store(StoreType, State#state.store),
    {next_state, 
        parallel, 
        State#state{change_queue = [], 
                    change_queue_counter = 0,
                    load_counter = 0,
                    store = LoadStore,
                    current_guid = GUID}}.

parallel({mput, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.store, ObjectSpecs),
    {next_state, parallel, State};
parallel({prompt, rebuild_start}, State) ->
    GUID = leveled_codec:generate_uuid(),
    {ok, Store} =  open_store(State#state.store_type, 
                                State#state.backend_opts, 
                                State#state.root_path, 
                                GUID),
    ok = store_manifest(State#state.root_path, 
                        #manifest{current_guid = State#state.current_guid,
                                    pending_guid = GUID}),
    {next_state, loading, State#state{load_store = Store, load_guid = GUID}}.

native(_Msg, State) ->
    {next_state, native, State}.


handle_sync_event(current_status, _From, StateName, State) ->
    {reply, {StateName, State#state.current_guid}, StateName, State}.

handle_event(_Msg, StateName, State) ->
    {next_state, StateName, State}.

handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

terminate(normal, _StateName, State) ->
    store_manifest(State#state.root_path, 
                    #manifest{current_guid = State#state.current_guid, 
                                shutdown_guid = State#state.shutdown_guid}).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.



%%%============================================================================
%%% Key Codec
%%%============================================================================

-spec define_objectspec(add|remove, 
                        integer(), binary(), binary(), tuple()|null) 
                                                            -> objectspec().
%% @doc
%% Create an ObjectSpec for adding to the backend store
define_objectspec(Op, SegTree_int, Bucket, Key, Value) ->
    #objectspec{op = Op, 
                segment_id = SegTree_int,
                bucket = Bucket, 
                key = Key, 
                value = Value}.


-spec generate_treesegment({integer(), integer()}) -> integer().
%% @doc
%% Get actual SegmentID for tree
generate_treesegment(SegmentID) ->
    Seg32_int = leveled_tictac:keyto_segment32(SegmentID),
    leveled_tictac:get_segment(Seg32_int, ?TREE_SIZE).

-spec generate_value(tuple(), integer(), 
                        aae_controller:version_vector(), integer(), 
                        {integer(), integer(), integer(), any()}) -> tuple().
%% @doc
%% Create a value based on the current active version number
%% Currently the "head" is ignored.  This may eb changed to support other 
%% coverage fold features in the future.  Other items
%% PreflistID - {Partition, NVal} - although this is largeish, compression 
%% should minimise the disk footprint
%% Clock - A Riak dotted version vector of {Actor, Seqn}
%% Hash - In this case a hash of the VClock
%% Size - The byte size of the binary Riak object (uncompressed)
%% SibCount - the count of siblings for the object (on this vnode only)
%% IndexHash - A Hash of all the index fields and terms for this object
generate_value(PreflistID, SegTS_int, Clock, Hash, 
                                        {Size, SibCount, IndexHash, _Head}) ->
    {?VALUE_VERSION, 
        {PreflistID, Clock, Hash, Size, SibCount, IndexHash, SegTS_int}}.

%% Some helper functions for accessing individual value elements by version, 
%% intended to make it easier to uplift the version of the value format at a
%% later date

value_preflist({1, ValueItems}) ->
    element(1, ValueItems).

value_clock({1, ValueItems}) ->
    element(2, ValueItems).

value_hash({1, ValueItems}) ->
    element(3, ValueItems).

value_size({1, ValueItems}) ->
    element(4, ValueItems).

value_sibcount({1, ValueItems}) ->
    element(5, ValueItems).
    
value_indexhash({1, ValueItems}) ->
    element(6, ValueItems).

value_aaesegment({1, ValueItems}) ->
    element(7, ValueItems).


%%%============================================================================
%%% Store functions
%%%============================================================================

-spec is_empty(supported_stores(), pid()) -> boolean().
%% @doc
%% Check to see if the store is empty
is_empty(leveled_so, Store) ->
    leveled_bookie:book_isempty(Store, ?HEAD_TAG);
is_empty(leveled_ko, Store) ->
    leveled_bookie:book_isempty(Store, ?HEAD_TAG).

-spec close_store(supported_stores(), pid()) -> ok.
%% @doc
%% Wait for store to close
close_store(leveled_so, Store) ->
    leveled_bookie:book_close(Store);
close_store(leveled_ko, Store) ->
    leveled_bookie:book_close(Store).

-spec open_store(supported_stores(), list(), list(), list()) -> {ok, pid()}.
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


-spec delete_store(supported_stores(), pid()) -> ok.
%% @doc
%% delete the store - as it has been replaced by a rebuild
delete_store(leveled_so, Store) ->
    leveled_bookie:book_destroy(Store);
delete_store(leveled_ko, Store) ->
    leveled_bookie:book_destroy(Store).

-spec do_load(supported_stores(), pid(), list(tuple())) -> ok.
%% @doc
%% Load a batch of object specifications into the store
do_load(leveled_so, Store, ObjectSpecs) ->
    leveled_bookie:book_mput(Store, dedup_map(leveled_so, ObjectSpecs)),
    ok;
do_load(leveled_ko, Store, ObjectSpecs) ->
    leveled_bookie:book_mput(Store, dedup_map(leveled_ko, ObjectSpecs)),
    ok.

-spec dedup_map(supported_stores(), list(objectspec())) -> list(tuple()).
%% @doc
%% Map the spec records into tuples as required by the backend store type, 
%% and dedup assuming the left-most record is the most recent Bucket/Key
dedup_map(leveled_so, ObjectSpecs) ->
    SegmentOrderedMapFun = 
        fun(ObjSpec) ->
            SegTS_int = ObjSpec#objectspec.segment_id,
            {ObjSpec#objectspec.op, 
                <<SegTS_int:24/integer>>, % needs to be binary not bitstring  
                term_to_binary({ObjSpec#objectspec.bucket, 
                                ObjSpec#objectspec.key}), 
                null, 
                ObjSpec#objectspec.value}
        end,
    lists:ukeysort(3, lists:map(SegmentOrderedMapFun, ObjectSpecs));
dedup_map(leveled_ko, ObjectSpecs) ->
    FoldFun =
        fun(ObjSpec, {Acc, Members}) ->
            B = ObjSpec#objectspec.bucket,
            K = ObjSpec#objectspec.key,
            case lists:member({B, K}, Members) of
                true ->
                    {Acc, Members};
                false ->
                    UpdSpec = {ObjSpec#objectspec.op, B, K, null, 
                                ObjSpec#objectspec.value},
                    {[UpdSpec|Acc], [{B, K}|Members]}
            end
        end,
    {UpdSpecL, _MemberL} = lists:foldl(FoldFun, {[], []}, ObjectSpecs),
    UpdSpecL.
            

-spec do_fold(supported_stores(), pid(), tuple()|all, fun(), any()) 
                                                            -> {async, fun()}.
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
do_fold(leveled_so, Store, {segments, SegList}, FoldObjectsFun, InitAcc) ->
    Query = 
        populate_so_query(SegList, 
                            {fun(_S, BKBin, V, Acc) ->
                                    {B, K} = binary_to_term(BKBin),
                                    FoldObjectsFun(B, K, V, Acc)
                                end,
                                InitAcc}),
    leveled_bookie:book_returnfolder(Store, Query);
do_fold(leveled_ko, Store, {segments, SegList}, FoldObjectsFun, InitAcc) ->
    FoldFun = 
        fun(B, K, V, Acc) ->
            case lists:member(value_aaesegment(V), SegList) of 
                true ->
                    FoldObjectsFun(B, K, V, Acc);
                false ->
                    Acc 
            end
        end,
    Query =
        {foldheads_allkeys, 
            ?HEAD_TAG,
            {FoldFun, InitAcc},
            false, true, SegList},
    leveled_bookie:book_returnfolder(Store, Query);
do_fold(leveled_so, Store, {buckets, BucketList}, FoldObjectsFun, InitAcc) ->
    Query = 
        populate_so_query(all, 
                            {fun(_S, BKBin, V, Acc) ->
                                    {B, K} = binary_to_term(BKBin),
                                    case lists:member(B, BucketList) of 
                                        true ->
                                            FoldObjectsFun(B, K, V, Acc);
                                        false ->
                                            Acc
                                    end
                                end,
                                InitAcc}),
    leveled_bookie:book_returnfolder(Store, Query);
do_fold(leveled_ko, Store, {buckets, BucketList}, FoldObjectsFun, InitAcc) ->
    Query = 
        {foldheads_bybucket,
            ?HEAD_TAG, 
            BucketList, bucket_list,
            {FoldObjectsFun, InitAcc},
            false, true, false},
    leveled_bookie:book_returnfolder(Store, Query);
do_fold(leveled_so, Store, all, FoldObjectsFun, InitAcc) ->
    Query = 
        populate_so_query(all,
                            {fun(_S, BKBin, V, Acc) -> 
                                    {B, K} = binary_to_term(BKBin),
                                    FoldObjectsFun(B, K, V, Acc)
                                end, 
                                InitAcc}),
    leveled_bookie:book_returnfolder(Store, Query);
do_fold(leveled_ko, Store, all, FoldObjectsFun, InitAcc) ->
    Query =
        {foldheads_allkeys, 
            ?HEAD_TAG,
            {FoldObjectsFun, InitAcc},
            false, true, false},
    leveled_bookie:book_returnfolder(Store, Query).


-spec populate_so_query(all|list(), tuple()) -> tuple().
%% @doc
%% Pupulate query template
populate_so_query(all, FoldFun) ->
    {foldheads_allkeys,
            ?HEAD_TAG,
            FoldFun,
            false, true, false};
populate_so_query(SegList, FoldFun) ->
    MapSegFun = 
        fun(S) -> 
            S0 = leveled_tictac:get_segment(S, ?TREE_SIZE),
            <<S0:24/integer>> 
        end,
    SegList0 = lists:map(MapSegFun, SegList),
    {foldheads_bybucket,
            ?HEAD_TAG, 
            SegList0, bucket_list,
            FoldFun,
            false, true, false}.



-spec open_manifest(list()) -> {ok, manifest()}|false.
%% @doc
%% Open the manifest file, check the CRC and return the active folder 
%% reference
open_manifest(RootPath) ->
    FN = filename:join(RootPath, ?MANIFEST_FN ++ ?COMLPETE_EXT),
    case filelib:is_file(FN) of 
        true ->
            {ok, <<CRC32:32/integer, Manifest/binary>>} = file:read_file(FN),
            case erlang:crc32(Manifest) of 
                CRC32 ->
                    M = binary_to_term(Manifest),
                    aae_util:log("KS005", [M#manifest.current_guid], logs()),
                    {ok, M};
                _ ->
                    aae_util:log("KS002", [RootPath, "crc32"], logs()),
                    false
            end;
        false ->
            aae_util:log("KS002", [RootPath, "missing"], logs()),
            false
    end.
            
-spec store_manifest(list(), manifest()) -> ok.
%% @doc
%% Store tham manifest file, adding a CRC, and ensuring it is readable before
%% returning
store_manifest(RootPath, Manifest) ->
    aae_util:log("KS003", [Manifest#manifest.current_guid], logs()),
    ManBin = term_to_binary(Manifest),
    CRC32 = erlang:crc32(ManBin),
    PFN = filename:join(RootPath, ?MANIFEST_FN ++ ?PENDING_EXT),
    CFN = filename:join(RootPath, ?MANIFEST_FN ++ ?COMLPETE_EXT),
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
            {warn, "Pending store is garbage and should be deleted at ~s"}}

        ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


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
    {ok, {never, none, true}, Store0} 
        = store_parallelstart(RootPath, StoreType),
    
    {ok, Manifest0} = open_manifest(RootPath),
    {parallel, CurrentGUID} = store_currentstatus(Store0),
    ?assertMatch(CurrentGUID, Manifest0#manifest.current_guid),
    ?assertMatch(none, Manifest0#manifest.shutdown_guid),
    
    ok = store_close(Store0, ShutdownGUID = leveled_codec:generate_uuid()),
    {ok, Manifest1} = open_manifest(RootPath),
    ?assertMatch(CurrentGUID, Manifest1#manifest.current_guid),
    ?assertMatch(ShutdownGUID, Manifest1#manifest.shutdown_guid),
    
    {ok, {never, ShutdownGUID, true}, Store1} 
        = store_parallelstart(RootPath, StoreType),
    ?assertMatch({parallel, CurrentGUID}, store_currentstatus(Store1)),
    ok = store_close(Store1, none),
    
    aae_util:clean_subdir(RootPath).

bad_manifest_test() ->
    RootPath = "test/keystore1/",
    ok = filelib:ensure_dir(RootPath),
    aae_util:clean_subdir(RootPath),
    ?assertMatch(false, open_manifest(RootPath)),
    Manifest = #manifest{current_guid = "aaa-111"},
    ok = store_manifest(RootPath, Manifest),
    ?assertMatch({ok, Manifest}, open_manifest(RootPath)),
    ManifestFN = filename:join(RootPath, ?MANIFEST_FN ++ ?COMLPETE_EXT),
    {ok, Bin0} = file:read_file(ManifestFN),
    Bin1 = aae_util:flip_byte(Bin0, 0, byte_size(Bin0)),
    ok = file:delete(ManifestFN),
    ok = file:write_file(ManifestFN, Bin1),
    ?assertMatch(false, open_manifest(RootPath)),
    ok = file:delete(ManifestFN),
    ok = file:write_file(ManifestFN, Bin0),
    ?assertMatch({ok, Manifest}, open_manifest(RootPath)),
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

    {ok, {never, none, true}, Store0} 
        = store_parallelstart(RootPath, StoreType),
    ok = store_mput(Store0, L1),
    ok = store_mput(Store0, L2),
    ok = store_mput(Store0, L3),

    FoldObjectsFun =
        fun(B, K, V, Acc) ->
            [{B, K, value_clock(V)}|Acc]
        end,
    
    {async, Folder0} = store_fold(Store0, all, FoldObjectsFun, []),
    Res0 = lists:usort(Folder0()),
    ?assertMatch(96, length(Res0)),
    
    ok = store_prompt(Store0, rebuild_start),

    {async, Folder1} = store_fold(Store0, all, FoldObjectsFun, []),
    Res1 = lists:usort(Folder1()),
    ?assertMatch(Res0, Res1),

    ok = store_mput(Store0, L4),

    % 4 adds, 20 alterations, 8 removes -> 92 entries
    {async, Folder2} = store_fold(Store0, all, FoldObjectsFun, []),
    Res2 = lists:usort(Folder2()),
    ?assertMatch(92, length(Res2)),

    ok = store_mload(Store0, L1),
    ok = store_mload(Store0, L2),
    ok = store_mload(Store0, L3),

    {async, Folder3} = store_fold(Store0, all, FoldObjectsFun, []),
    Res3 = lists:usort(Folder3()),
    ?assertMatch(Res2, Res3),

    ok = store_prompt(Store0, rebuild_complete),

    {async, Folder4} = store_fold(Store0, all, FoldObjectsFun, []),
    Res4 = lists:usort(Folder4()),
    ?assertMatch(Res2, Res4),

    ok = store_mput(Store0, L5),

    % Removes now complete so only 80 entries
    {async, Folder5} = store_fold(Store0, all, FoldObjectsFun, []),
    Res5 = lists:usort(Folder5()),
    ?assertMatch(FinalState, Res5),

    ok = store_close(Store0, none),

    {ok, {never, none, false}, Store1} 
        = store_parallelstart(RootPath, StoreType),
   
    {async, Folder6} = store_fold(Store1, all, FoldObjectsFun, []),
    Res6 = lists:usort(Folder6()),
    ?assertMatch(Res5, Res6),

    {async, Folder7} = 
        store_fold(Store1, {buckets, [<<"B1">>]}, FoldObjectsFun, []),
    Res7 = lists:usort(Folder7()),
    ?assertMatch(Res5, Res7),

    {async, Folder8} = 
        store_fold(Store1, {buckets, [<<"B2">>]}, FoldObjectsFun, []),
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
        store_fold(Store1, {segments, SegList}, FoldObjectsFun, []),
    Res9 = lists:usort(Folder9()),
    ?assertMatch(FinalStateSL, Res9),

    ok = store_close(Store1, none),
    aae_util:clean_subdir(RootPath).


split_lists(L, SplitSize, Acc) when length(L) =< SplitSize ->
    [L|Acc];
split_lists(L, SplitSize, Acc) ->
    {LL, RL} = lists:split(SplitSize, L),
    split_lists(RL, SplitSize, [LL|Acc]).


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

    {ok, {never, none, true}, Store0} 
        = store_parallelstart(RootPath, StoreType),

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

    {async, Folder2} = store_fold(Store0, all, FoldObjectsFun, 0),
    ?assertMatch(KeyCount, Folder2() - KeyCount),

    ok = store_close(Store0, ShutdownGUID0 = leveled_codec:generate_uuid()),

    {ok, {never, ShutdownGUID0, false}, Store1} 
        = store_parallelstart(RootPath, StoreType),
    
    timed_fold(Store1, KeyCount, FoldObjectsFun, StoreType, true),

    ok = store_prompt(Store1, rebuild_start),
    OpenWhenPendingSavedFun = 
        fun(_X, {Complete, M}) ->
            case Complete of 
                true ->
                    {true, M};
                false ->
                    timer:sleep(100),
                    {ok, PMan0} = open_manifest(RootPath),
                    {not (PMan0#manifest.pending_guid == undefined), PMan0}
            end
        end,
    {true, PendingManifest} = 
        lists:foldl(OpenWhenPendingSavedFun, {false, null}, lists:seq(1,10)),

    ok = store_close(Store1, ShutdownGUID1 = leveled_codec:generate_uuid()),
    ?assertMatch(false, undefined == PendingManifest#manifest.pending_guid),
    M0 = clear_pendingpath(PendingManifest, RootPath),
    ?assertMatch(undefined, M0#manifest.pending_guid),

    {ok, {never, ShutdownGUID1, false}, Store2} 
        = store_parallelstart(RootPath, StoreType),
    
    timed_fold(Store2, KeyCount, FoldObjectsFun, StoreType, true),

    ok = store_close(Store2, none),
    aae_util:clean_subdir(RootPath).


timed_fold(Store, KeyCount, FoldObjectsFun, StoreType, DoubleCount) ->
    SW = os:timestamp(),
    {async, Folder} = store_fold(Store, all, FoldObjectsFun, 0),
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
    {reply, ok, native, _State} = native(null, self(), #state{}),
    {next_state, native, _State} = native(null, #state{}),
    {next_state, native, _State} = handle_event(null, native, #state{}),
    {next_state, native, _State} = handle_info(null, native, #state{}),
    {ok, native, _State} = code_change(null, native, #state{}, null).

dumb_value_test() ->
    V = generate_value({0, 3}, 0, {a, 1}, erlang:phash2(<<>>), 
                        {100, 1, 0, null}),
    ?assertMatch(100, value_size(V)),
    ?assertMatch(1, value_sibcount(V)),
    ?assertMatch(0, value_indexhash(V)).

generate_objectspecs(Op, B, KeyList) ->
    FoldFun = 
        fun({K, V}) ->
            {Clock, Hash, Size, SibCount} = V,
            Seg = 
                leveled_tictac:keyto_segment48(aae_util:make_binarykey(B, K)),
            Seg0 = generate_treesegment(Seg),
            Value = generate_value({0, 0}, Seg0, Clock, Hash, 
                                    {Size, SibCount, 0, null}),
            
            define_objectspec(Op, Seg0, B, K, Value)
        end,
    lists:map(FoldFun, KeyList).
            


-endif.