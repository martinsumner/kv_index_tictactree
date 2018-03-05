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
            store_drop/1,
            store_close/1,
            store_mput/2,
            store_mload/2,
            store_build/1,
            store_fold/4]).


-record(state, {vnode :: pid(),
                store :: pid(),
                id = "KeyStore" :: any(),
                store_type :: supported_stores(),
                change_queue = [] :: list(),
                change_queue_counter = 0 :: integer()}).

-include_lib("eunit/include/eunit.hrl").

-define(BACKEND_OPTS, [{max_pencillercachesize, 16000},
                        {sync_strategy, none},
                        {head_only, no_lookup},
                        {max_journalsize, 1000000}]).
-define(CHANGEQ_LOGFREQ, 10000).
-define(HEAD_TAG, h). % Used in leveled as a Tag for head-only objects

-type supported_stores() :: leveled. 

% -type keystore_state() :: #state{}.


%%%============================================================================
%%% API
%%%============================================================================

-spec store_parallelstart(list(), supported_stores()) -> {ok, pid()}.
%% @doc
%% Start a store to be run in parallel mode
store_parallelstart(Path, StoreType) ->
    Opts = 
        [{root_path, Path}, 
            {native, {false, StoreType}}, 
            {backend_opts, ?BACKEND_OPTS}],
    gen_fsm:start(?MODULE, [Opts], []).

-spec store_isempty(pid()) -> boolean().
%% @doc
%% Is the key store empty
store_isempty(Pid) ->
    gen_fsm:sync_send_event(Pid, is_empty, 2000).

-spec store_drop(pid()) -> ok.
%% @doc
%% Drop the store, and remove from disk
store_drop(Pid) ->
    gen_fsm:send_event(Pid, drop).

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

-spec store_build(pid()) -> ok.
%% @doc
%% When load is complete, send a signal to finish the build of the store
store_build(Pid) ->
    gen_fsm:send_event(Pid, build).

-spec store_fold(pid(), tuple(), fun(), any()) -> any()|{async, fun()}.
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
            BackendOpts = 
                [{root_path, RootPath}|aae_util:get_opt(backend_opts, Opts)],
            {ok, Store} = leveled_bookie:book_start(BackendOpts),

            {ok, loading, #state{store = Store, store_type = leveled}}
    end.

loading(is_empty, _From, State) ->
    {reply, 
        is_empty(State#state.store_type, State#state.store), 
        loading, 
        State};
loading(close, _From, State) ->
    ok = delete_store(State#state.store_type, State#state.store),
    {stop, normal, ok, State}.

parallel({fold, Limiter, FoldObjectsFun, InitAcc}, _From, State) ->
    Result = do_fold(State#state.store_type, State#state.store,
                        Limiter, FoldObjectsFun, InitAcc),
    {reply, Result, parallel, State};
parallel(close, _From, State) ->
    ok = close_store(State#state.store_type, State#state.store),
    {stop, normal, ok, State}.

native(_Msg, _From, State) ->
    {reply, ok, native, State}.


loading(drop, State) ->
    ok = delete_store(State#state.store_type, State#state.store),
    {stop, normal, State};
loading({mput, ObjectSpecs}, State) ->
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
    ok = do_load(State#state.store_type, State#state.store, ObjectSpecs),
    {next_state, loading, State};
loading(build, State) ->
    LoadFun = 
        fun(OS) -> 
            do_load(State#state.store_type, State#state.store, OS)
        end,
    lists:foreach(LoadFun, lists:reverse(State#state.change_queue)),
    {next_state, 
        parallel, 
        State#state{change_queue = [], change_queue_counter = 0}}.

parallel(drop, State) ->
    ok = delete_store(State#state.store_type, State#state.store),
    {stop, normal, State};
parallel({mput, ObjectSpecs}, State) ->
    ok = do_load(State#state.store_type, State#state.store, ObjectSpecs),
    {next_state, parallel, State}.

native(_Msg, State) ->
    {next_state, native, State}.


handle_sync_event(_Msg, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_event(_Msg, StateName, State) ->
    {next_state, StateName, State}.

handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

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


-spec do_fold(supported_stores(), pid(), tuple(), fun(), any()) 
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
                        {FoldObjectsFun, InitAcc}),
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
    {foldheads_bybucket,
            ?HEAD_TAG, SegList,
            FoldFun,
            false, true, false}.


%%%============================================================================
%%% log definitions
%%%============================================================================

-spec logs() -> list(tuple()).
%% @doc
%% Define log lines for this module
logs() ->
    [{"KS001", 
        {info, "Key Store building with id=~w has reached deferred count=~w"}}].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-endif.