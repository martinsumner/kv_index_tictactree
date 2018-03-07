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
            store_isempty/1,
            store_close/1,
            store_mput/2,
            store_mload/2,
            store_prompt/2,
            store_fold/4]).

-export([define_objectspec/5,
            generate_value/5,
            value_clock/1,
            value_hash/1,
            value_size/1,
            value_sibcount/1]).

-record(state, {vnode :: pid(),
                store :: pid(),
                id = "KeyStore" :: any(),
                store_type :: supported_stores(),
                change_queue = [] :: list(),
                change_queue_counter = 0 :: integer(),
                current_guid :: list(),
                root_path :: list(),
                last_rebuild :: os:timestamp()|never,
                load_store :: pid(),
                load_guid :: list(),
                backend_opts :: list()}).

-record(manifest, {current_guid :: list(), 
                    pending_guid :: list(), 
                    last_rebuild = never :: erlang:timestamp()|never, 
                    safe_shutdown  = false :: boolean()}).

-include_lib("eunit/include/eunit.hrl").

-define(BACKEND_OPTS, [{max_pencillercachesize, 16000},
                        {sync_strategy, none},
                        {head_only, no_lookup},
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

-type supported_stores() :: leveled. 
    % Stores supported for parallel running
-type manifest() :: #manifest{}.
    % Saves state of what store is currently active

%%%============================================================================
%%% API
%%%============================================================================

-spec store_parallelstart(list(), supported_stores()) -> 
                                        {ok, os:timestamp()|never, pid()}.
%% @doc
%% Start a store to be run in parallel mode
store_parallelstart(Path, leveled) ->
    Opts = 
        [{root_path, Path}, 
            {native, {false, leveled}}, 
            {backend_opts, ?BACKEND_OPTS}],
    {ok, Pid} = gen_fsm:start(?MODULE, [Opts], []),
    LastRebuild = gen_fsm:sync_send_event(Pid, last_rebuild, 2000),
    {ok, LastRebuild, Pid}.

-spec store_isempty(pid()) -> boolean().
%% @doc
%% Is the key store empty
store_isempty(Pid) ->
    gen_fsm:sync_send_event(Pid, is_empty, 2000).

-spec store_close(pid()) -> ok.
%% @doc
%% Close the store neatly
store_close(Pid) ->
    gen_fsm:sync_send_event(Pid, close, 10000).

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
        true ->
            {stop, not_yet_implemented};
        {false, leveled} ->
            RootPath = aae_util:get_opt(root_path, Opts),
            Manifest0 =
                case open_manifest(RootPath) of 
                    false ->
                        GUID = leveled_codec:generate_uuid(),
                        #manifest{current_guid = GUID};
                    {ok, M} ->
                        M 
                end,
            
            Manifest1 = clear_pendingpath(Manifest0),
                
            BackendOpts0 = aae_util:get_opt(backend_opts, Opts),
            {ok, Store} = open_store(leveled, 
                                        BackendOpts0, 
                                        RootPath,
                                        Manifest1#manifest.current_guid),
            
            LastRebuild = 
                % Need to determine when the store was last rebuilt, if it 
                % can't be determined that the store has been safely rebuilt, 
                % then an "immediate" rebuild should be triggered
                case Manifest1#manifest.safe_shutdown of 
                    true ->
                        Manifest1#manifest.last_rebuild;
                    _ ->
                        never
                end,
            Manifest2 = 
                Manifest1#manifest{safe_shutdown = false, 
                                    last_rebuild = LastRebuild},
            ok = store_manifest(RootPath, Manifest2),
            {ok, 
                parallel, 
                #state{store = Store, 
                        store_type = leveled,
                        root_path = RootPath,
                        current_guid = Manifest2#manifest.current_guid,
                        last_rebuild = LastRebuild,
                        backend_opts = BackendOpts0}}
    end.

loading(is_empty, _From, State) ->
    {reply, 
        is_empty(State#state.store_type, State#state.store), 
        loading, 
        State};
loading({fold, Limiter, FoldObjectsFun, InitAcc}, _From, State) ->
    Result = do_fold(State#state.store_type, State#state.store,
                        Limiter, FoldObjectsFun, InitAcc),
    {reply, Result, loading, State};
loading(close, _From, State) ->
    ok = delete_store(State#state.store_type, State#state.load_store),
    ok = close_store(State#state.store_type, State#state.store),
    {stop, normal, ok, State}.

parallel({fold, Limiter, FoldObjectsFun, InitAcc}, _From, State) ->
    Result = do_fold(State#state.store_type, State#state.store,
                        Limiter, FoldObjectsFun, InitAcc),
    {reply, Result, parallel, State};
parallel(close, _From, State) ->
    ok = close_store(State#state.store_type, State#state.store),
    {stop, normal, ok, State};
parallel(last_rebuild, _From, State) ->
    {reply, State#state.last_rebuild, parallel, State}.

native(_Msg, _From, State) ->
    {reply, ok, native, State}.


loading({mput, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.store, ObjectSpecs),
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
    ChangeQueue1 = [ObjectSpecs|State#state.change_queue],
    {next_state, 
        loading, 
            State#state{change_queue_counter = ObjectCount1, 
                        change_queue = ChangeQueue1}};
loading({mload, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.load_store, ObjectSpecs),
    {next_state, loading, State};
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
                    store = LoadStore,
                    current_guid = GUID}}.

parallel({mput, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.store, ObjectSpecs),
    {next_state, parallel, State};
parallel({prompt, rebuild_start}, State) ->
    GUID = leveled_codec:generate_uuid(),
    {ok, Store} =  open_store(leveled, 
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
    ok = store_manifest(State#state.root_path, 
                        #manifest{current_guid = State#state.current_guid, 
                                    safe_shutdown = true}),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.



%%%============================================================================
%%% Key Codec
%%%============================================================================

define_objectspec(Op, SegmentID, Bucket, Key, Value) ->
    {Op, <<SegmentID:24/integer>>, {Bucket, Key}, null, Value}.

generate_value(Clock, Hash, Size, SibCount, _Head) ->
    {?VALUE_VERSION, {Clock, Hash, Size, SibCount}}.

value_clock({1, ValueItems}) ->
    element(1, ValueItems).

value_hash({1, ValueItems}) ->
    element(2, ValueItems).

value_size({1, ValueItems}) ->
    element(3, ValueItems).

value_sibcount({1, ValueItems}) ->
    element(4, ValueItems).


%%%============================================================================
%%% Store functions
%%%============================================================================

-spec is_empty(supported_stores(), pid()) -> boolean().
%% @doc
%% Check to see if the store is empty
is_empty(leveled, Store) ->
    FoldBucketsFun = fun(B, Acc) -> sets:add_element(B, Acc) end,
    ListBucketQ = {binary_bucketlist,
                    ?HEAD_TAG,
                    {FoldBucketsFun, sets:new()}},
    {async, Folder} = leveled_bookie:book_returnfolder(Store, ListBucketQ),
    BSet = Folder(),
    case sets:size(BSet) of
        0 ->
            true;
        _ ->
            false
    end.

-spec close_store(supported_stores(), pid()) -> ok.
%% @doc
%% Wait for store to close
close_store(leveled, Store) ->
    leveled_bookie:book_close(Store).

-spec open_store(supported_stores(), list(), list(), list()) -> {ok, pid()}.
%% @doc
%% Open a parallel backend key store
open_store(leveled, BackendOpts0, RootPath, GUID) ->
    Path = filename:join(RootPath, GUID),
    ok = filelib:ensure_dir(Path),
    BackendOpts = [{root_path, Path}|BackendOpts0],
    leveled_bookie:book_start(BackendOpts).

-spec delete_store(supported_stores(), pid()) -> ok.
%% @doc
%% delete the store - as it has been replaced by a rebuild
delete_store(leveled, Store) ->
    leveled_bookie:book_destroy(Store).

-spec do_load(supported_stores(), pid(), list(tuple())) -> ok.
%% @doc
%% Load a batch of object specifications into the store
do_load(leveled, Store, ObjectSpecs) ->
    leveled_bookie:book_mput(Store, ObjectSpecs),
    ok.

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
do_fold(leveled, Store, {segments, SegList}, FoldObjectsFun, InitAcc) ->
    Query = 
        populate_query(leveled,
                        SegList, 
                        {fun(_S, {B, K}, V, Acc) ->
                                FoldObjectsFun({B, K, V}, Acc)
                            end,
                            InitAcc}),
    leveled_bookie:book_returnfolder(Store, Query);
do_fold(leveled, Store, {buckets, BucketList}, FoldObjectsFun, InitAcc) ->
    Query = 
        populate_query(leveled, 
                        all, 
                        {fun(_S, {B, K}, V, Acc) ->
                                case lists:member(B, BucketList) of 
                                    true ->
                                        FoldObjectsFun({B, K, V}, Acc);
                                    false ->
                                        Acc
                                end
                            end,
                            InitAcc}),
    leveled_bookie:book_returnfolder(Store, Query);
do_fold(leveled, Store, all, FoldObjectsFun, InitAcc) ->
    Query = 
        populate_query(leveled,
                        all,
                        {fun(_S, {B, K}, V, Acc) -> 
                                FoldObjectsFun({B, K, V}, Acc)
                            end, 
                            InitAcc}),
    leveled_bookie:book_returnfolder(Store, Query).


-spec populate_query(supported_stores(), all|list(), tuple()) -> tuple().
%% @doc
%% Pupulate query template
populate_query(leveled, all, FoldFun) ->
    {foldheads_allkeys,
            ?HEAD_TAG,
            FoldFun,
            false, true, false};
populate_query(leveled, SegList, FoldFun) ->
    SegList0 = lists:map(fun(S) -> <<S:24/integer>> end, SegList),
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
                    {ok, binary_to_term(Manifest)};
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
    aae_util:log("KS003", [Manifest], logs()),
    ManBin = term_to_binary(Manifest),
    CRC32 = erlang:crc32(ManBin),
    PFN = filename:join(RootPath, ?MANIFEST_FN ++ ?PENDING_EXT),
    CFN = filename:join(RootPath, ?MANIFEST_FN ++ ?COMLPETE_EXT),
    ok = file:write_file(PFN, <<CRC32:32/integer, ManBin/binary>>, [raw]),
    ok = file:rename(PFN, CFN),
    {ok, <<CRC32:32/integer, ManBin/binary>>} = file:read_file(CFN),
    ok.


-spec clear_pendingpath(manifest()) -> manifest().
%% @doc
%% Clear any Keystore that had been aprtially loaded at the last shutdown
clear_pendingpath(Manifest) ->
    case Manifest#manifest.pending_guid of 
        undefined ->
            Manifest;
        PendingPath ->
            case filelib:is_dir(PendingPath) of
                true ->
                    ok = file:del_dir(PendingPath);
                false ->
                    ok 
            end,
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
            {info, "Key Store building with id=~w has reached " 
                    ++ "deferred count=~w"}},
        {"KS002",
            {warn, "No valid manifest found for AAE keystore at ~s "
                    ++ "reason ~s"}},
        {"KS003",
            {info, "Storing manifest of ~w"}}].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


-spec store_currentstatus(pid()) -> {atom(), list()}.
%% @doc
%% Included for test functions only - get the manifest
store_currentstatus(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, current_status).


empty_buildandclose_test() ->
    RootPath = "test/keystore0/",
    aae_util:clean_subdir(RootPath),
    {ok, never, Store0} = store_parallelstart(RootPath, leveled),
    
    {ok, Manifest0} = open_manifest(RootPath),
    {parallel, CurrentGUID} = store_currentstatus(Store0),
    ?assertMatch(CurrentGUID, Manifest0#manifest.current_guid),
    ?assertMatch(false, Manifest0#manifest.safe_shutdown),
    
    ok = store_close(Store0),
    {ok, Manifest1} = open_manifest(RootPath),
    ?assertMatch(CurrentGUID, Manifest1#manifest.current_guid),
    ?assertMatch(true, Manifest1#manifest.safe_shutdown),
    
    {ok, never, Store1} = store_parallelstart(RootPath, leveled),
    ?assertMatch({parallel, CurrentGUID}, store_currentstatus(Store1)),
    ok = store_close(Store1),
    
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

load_test() ->
    RootPath = "test/keystore2/",
    ok = filelib:ensure_dir(RootPath),
    aae_util:clean_subdir(RootPath),

    GenerateKeyFun = aae_util:test_key_generator(v1),

    InitialKeys = lists:map(GenerateKeyFun, lists:seq(1,100)),
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

    {ok, never, Store0} = store_parallelstart(RootPath, leveled),
    ok = store_mput(Store0, L1),
    ok = store_mput(Store0, L2),
    ok = store_mput(Store0, L3),

    FoldObjectsFun =
        fun({B, K, V}, Acc) ->
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

    ok = store_close(Store0),

    {ok, never, Store1} = store_parallelstart(RootPath, leveled),
   
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

    {async, Folder9} = 
        store_fold(Store1, {segments, SegList}, FoldObjectsFun, []),
    Res9 = lists:usort(Folder9()),
    ?assertMatch(FinalStateSL, Res9),

    ok = store_close(Store1),
    aae_util:clean_subdir(RootPath).




generate_objectspecs(Op, Bucket, KeyList) ->
    FoldFun = 
        fun({K, V}) ->
            {Clock, Hash, Size, SibCount} = V,
            Value = generate_value(Clock, Hash, Size, SibCount, null),
            define_objectspec(Op, 
                                aae_util:get_segmentid(Bucket, K), 
                                Bucket, 
                                K, 
                                Value)
        end,
    lists:map(FoldFun, KeyList).
            


-endif.