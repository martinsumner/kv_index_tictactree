%% -------- Overview ---------
%%
%% A simplified mock of riak_kv_vnode for testing


-module(mock_kv_vnode).

-behaviour(gen_server).

-export([init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3]).

-export([open/4,
            put/4,
            push/6,
            backend_delete/4,
            exchange_message/4,
            rebuild/2,
            rehash/4,
            rebuild_complete/2,
            fold_aae/6,
            close/1]).

-export([extractclock_from_riakhead/1,
            from_aae_binary/1,
            new_v1/2,
            workerfun/1,
            rebuild_worker/1]).

-record(r_content, {
                    metadata,
                    value :: term()
                    }).

-record(r_object, {
                    bucket,
                    key,
                    contents :: [#r_content{}],
                    vclock = [],
                    updatemetadata=dict:store(clean, true, dict:new()),
                    updatevalue :: term()}).

-record(options, {aae :: parallel|native,
                    index_ns :: list(tuple()),
                    root_path :: list(),
                    preflist_fun = null :: preflist_fun()}).


-record(state, {root_path :: list(),
                index_ns :: list(tuple()),
                aae_controller :: pid(),
                vnode_store :: pid(),
                vnode_id :: binary(),
                vnode_sqn = 1 :: integer(),
                preflist_fun = null :: preflist_fun(),
                aae_rebuild = false :: boolean()}).

-include_lib("eunit/include/eunit.hrl").

-define(RIAK_TAG, o_rkv).
-define(REBUILD_SCHEDULE, {1, 60}).
-define(LASTMOD_LEN, 29). 
-define(V1_VERS, 1).
-define(MAGIC, 53).  
-define(EMPTY_VTAG_BIN, <<"e">>).


-type r_object() :: #r_object{}.
-type preflist_fun() :: null|fun().

%%%============================================================================
%%% API
%%%============================================================================

-spec open(list(), atom(), list(tuple()), fun()|null) -> {ok, pid()}.
%% @doc
%% Open a mock vnode
open(Path, AAEType, IndexNs, PreflistFun) ->
    gen_server:start(?MODULE, 
                        [#options{aae = AAEType, 
                                    index_ns = IndexNs, 
                                    root_path = Path,
                                    preflist_fun = PreflistFun}], 
                        []).

-spec put(pid(), r_object(), tuple(), list(pid())) -> ok.
%% @doc
%% Put a new object in the store, updating AAE - and co-ordinating
put(Vnode, Object, IndexN, OtherVnodes) ->
    gen_server:call(Vnode, {put, Object, IndexN, OtherVnodes}).

-spec push(pid(), binary(), binary(), list(tuple()), binary(), tuple()) -> ok.
%% @doc
%% Push a new object in the store, updating AAE
push(Vnode, Bucket, Key, UpdClock, ObjectBin, IndexN) ->
    gen_server:cast(Vnode, {push, Bucket, Key, UpdClock, ObjectBin, IndexN}).

-spec backend_delete(pid(), binary(), binary(), tuple()) -> ok.
%% @doc
%% Delete an object from the backend
backend_delete(Vnode, Bucket, Key, IndexN) ->
    gen_server:call(Vnode, {delete, Bucket, Key, IndexN}).

-spec rebuild(pid(), boolean()) -> {erlang:timestamp(), boolean()}.
%% @doc
%% Prompt for the next rebuild time, using ForceRebuild=true to override that
%% time and trigger a rebuild.  As well as the next rebuild time the response
%$ includes if a rebuild is currently in progress
rebuild(Vnode, ForceRebuild) ->
    gen_server:call(Vnode, {rebuild, ForceRebuild}).

-spec rebuild_complete(pid(), store|tree) -> ok.
%% @doc
%% Prompt for the rebuild of the tree
rebuild_complete(Vnode, Stage) ->
    gen_server:cast(Vnode, {rebuild_complete, Stage}).

-spec rehash(pid(), binary(), binary(), tuple()) -> ok.
%% @doc
%% Prompt a given key to be rehashed
rehash(Vnode, Bucket, Key, IndexN) ->
    gen_server:call(Vnode, {rehash, Bucket, Key, IndexN}).

-spec fold_aae(pid(), 
                aae_keystore:range_limiter(), aae_keystore:segment_limiter(),
                fun(), any(), 
                list(aae_keystore:value_element())) -> {async, fun()}.
%% @doc
%% Fold over the heads in the aae store (which may be the key store when 
%% running in native mode)
fold_aae(Vnode, Range, Segments, FoldObjectsFun, InitAcc, Elements) ->
    gen_server:call(Vnode, 
                    {fold_aae, 
                        Range, Segments,
                        FoldObjectsFun, InitAcc, Elements}).

-spec exchange_message(pid(), tuple()|atom(), list(tuple()), atom()) -> ok.
%% @doc
%% Handle a message from an AAE exchange
exchange_message(Vnode, Msg, IndexNs, ReturnFun) ->
    gen_server:call(Vnode, {aae, Msg, IndexNs, ReturnFun}).


-spec close(pid()) -> ok.
%% @doc
%% Close the vnode, and any aae controller
close(Vnode) ->
    gen_server:call(Vnode, close).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    % Start the vnode backend
    % Get the shutdown GUID
    % Delete the shutdown GUID
    % Check is_empty
    % Start the aae_controller 
    % Report back OK
    RP = Opts#options.root_path,
    {ok, VnSt} = 
        leveled_bookie:book_start(RP, 4000, 100000000, none),
    IsEmpty = leveled_bookie:book_isempty(VnSt, ?RIAK_TAG),
    KeyStoreType = 
        case Opts#options.aae of 
            native ->
                {native, leveled_nko, VnSt};
            parallel ->
                {parallel, leveled_so}
        end,
    {ok, AAECntrl} = 
        aae_controller:aae_start(KeyStoreType, 
                                    IsEmpty, 
                                    ?REBUILD_SCHEDULE, 
                                    Opts#options.index_ns, 
                                    RP, 
                                    fun from_aae_binary/1),
    {ok, #state{root_path = RP,
                vnode_store = VnSt,
                index_ns = Opts#options.index_ns,
                aae_controller = AAECntrl,
                vnode_id = list_to_binary(leveled_util:generate_uuid()),
                preflist_fun = Opts#options.preflist_fun}}.

handle_call({put, Object, IndexN, OtherVnodes}, _From, State) ->
    % Get Bucket and Key from object
    % Do head request
    % Compare clock, update clock
    % Send update to other stores
    % Update AAE
    % Report back OK
    Bucket = Object#r_object.bucket,
    Key = Object#r_object.key,

    {UpdClock, PrevClock} = 
        case leveled_bookie:book_head(State#state.vnode_store, 
                                        Bucket, Key, ?RIAK_TAG) of
            not_found ->
                {[{State#state.vnode_id, State#state.vnode_sqn}],
                    none};
            {ok, Head} ->
                Clock0 = 
                    extractclock_from_riakhead(Head),
                Clock1 = 
                    [{State#state.vnode_id, State#state.vnode_sqn}|Clock0],
                {lists:ukeysort(1, Clock1), Clock0}
        end,
    ObjectBin = new_v1(UpdClock, Object#r_object.contents),
    VVEBin = to_aae_binary(ObjectBin),
    leveled_bookie:book_put(State#state.vnode_store, 
                                Bucket, 
                                Key, 
                                ObjectBin, 
                                [], 
                                ?RIAK_TAG),
    
    ok = aae_controller:aae_put(State#state.aae_controller, 
                                IndexN, 
                                Bucket, Key, 
                                UpdClock, PrevClock, 
                                VVEBin),
    
    lists:foreach(fun(VN) -> 
                        push(VN, Bucket, Key, UpdClock, ObjectBin, IndexN)
                    end, 
                    OtherVnodes),

    {reply, ok, State#state{vnode_sqn = State#state.vnode_sqn + 1}};
handle_call({delete, Bucket, Key, IndexN}, _From, State) ->
    PrevClock = 
        case leveled_bookie:book_head(State#state.vnode_store, 
                                        Bucket, Key, ?RIAK_TAG) of
            not_found ->
                none;
            {ok, Head} ->
                extractclock_from_riakhead(Head)
        end,
    leveled_bookie:book_put(State#state.vnode_store, 
                            Bucket, Key, delete, [], ?RIAK_TAG),
    ok = aae_controller:aae_put(State#state.aae_controller, 
                                IndexN, 
                                Bucket, Key, 
                                none, PrevClock, 
                                <<>>),
    {reply, ok, State};
handle_call({rebuild, true}, _From, State) ->
    % To rebuild the store an Object SplitFun will be required if is is a 
    % parallel store, which will depend on the preflist_fun.
    NRT = aae_controller:aae_nextrebuild(State#state.aae_controller),

    SplitFun = 
        fun(B, K, V) ->
            PreflistFun = State#state.preflist_fun,
            IndexN = PreflistFun(B, K),
            Clock = extractclock_from_riakhead(V),
            {IndexN, Clock}
        end,
    Vnode = self(),
    ReturnFun = 
        fun(ok) ->
            ok = rebuild_complete(Vnode, store)
        end,

    case aae_controller:aae_rebuildstore(State#state.aae_controller, 
                                            SplitFun) of
        ok ->
            % This store is rebuilt already (i.e. it is native), so nothing to
            % do here other than prompt the status change
            ReturnFun(ok);
        {ok, FoldFun, FinishFun} ->
            Worker = workerfun(ReturnFun),
            % Now need to get a fold query to run over the vnode store to 
            % rebuild the parallel store.  The aae_controller has provided 
            % the object fold fun which should load the parallel store, and
            % the finish fun which should tell the controller the fold is 
            % complete and prompt the finishing of the rebuild activity
            {async, Runner} = 
                leveled_bookie:book_headfold(State#state.vnode_store,
                                                ?RIAK_TAG, 
                                                {FoldFun, []}, 
                                                true, true, false),
            Worker(Runner, FinishFun) % dispatch the work to the worker
    end,
    {reply, {NRT, true}, State#state{aae_rebuild = true}};
handle_call({rebuild, false}, _From, State) ->
    % Check next rebuild
    % Reply with next rebuild TS - and the status to indicate an ongoing 
    % rebuild
    NRT = aae_controller:aae_nextrebuild(State#state.aae_controller),
    {reply, {NRT, State#state.aae_rebuild}, State};
handle_call({rehash, Bucket, Key, IndexN}, _From, State) ->
    case leveled_bookie:book_head(State#state.vnode_store, 
                                        Bucket, Key, ?RIAK_TAG) of
        not_found ->
            ok = aae_controller:aae_put(State#state.aae_controller, 
                                        IndexN, 
                                        Bucket, Key, 
                                        none, undefined, 
                                        <<>>);
        {ok, Head} ->
            C0 = extractclock_from_riakhead(Head),
            ok = aae_controller:aae_put(State#state.aae_controller, 
                                        IndexN, 
                                        Bucket, Key, 
                                        C0, undefined, 
                                        to_aae_binary(Head))
    end,
    {reply, ok, State};
handle_call({aae, Msg, IndexNs, ReturnFun}, _From, State) ->
    case Msg of 
        fetch_root ->
            aae_controller:aae_mergeroot(State#state.aae_controller, 
                                            IndexNs, 
                                            ReturnFun);
        {fetch_branches, BranchIDs} ->
            aae_controller:aae_mergebranches(State#state.aae_controller, 
                                                IndexNs, 
                                                BranchIDs, 
                                                ReturnFun);
        {fetch_clocks, SegmentIDs} ->
            aae_controller:aae_fetchclocks(State#state.aae_controller,
                                                IndexNs,
                                                SegmentIDs,
                                                ReturnFun,
                                                State#state.preflist_fun)
    end,
    {reply, ok, State};
handle_call({fold_aae, Range, Segments, FoldFun, InitAcc, Elements}, 
                                                        _From, State) ->
    R = aae_controller:aae_fold(State#state.aae_controller, 
                                Range,
                                Segments, 
                                FoldFun, InitAcc, 
                                Elements),
    {reply, R, State};
handle_call(close, _From, State) ->
    ok = aae_controller:aae_close(State#state.aae_controller),
    {stop, normal, ok, State}.

handle_cast({push, Bucket, Key, UpdClock, ObjectBin, IndexN}, State) ->
    % As PUT, but don't increment vclock, replace regardless of current state
    PrevClock = 
        case leveled_bookie:book_head(State#state.vnode_store, 
                                        Bucket, Key, ?RIAK_TAG) of
            not_found ->
                none;
            {ok, Head} ->
                extractclock_from_riakhead(Head)
        end,
    leveled_bookie:book_put(State#state.vnode_store, 
                                Bucket, 
                                Key, 
                                ObjectBin, 
                                [], 
                                ?RIAK_TAG),
    
    ok = aae_controller:aae_put(State#state.aae_controller, 
                                IndexN, 
                                Bucket, Key, 
                                UpdClock, PrevClock, 
                                to_aae_binary(ObjectBin)),

    {noreply, State};
handle_cast({rebuild_complete, store}, State) ->
    % Trigger a rebuild of the tree.  Will require a non-null preflist_fun 
    % if the store is native (as the native store will not store the IndexN, 
    % and so a recalculation will be required)
    Vnode = self(),
    ReturnFun = 
        fun(ok) ->
            ok = rebuild_complete(Vnode, tree)
        end,
    
    Worker = workerfun(ReturnFun),

    ok = aae_controller:aae_rebuildtrees(State#state.aae_controller, 
                                            State#state.index_ns,
                                            State#state.preflist_fun,
                                            Worker,
                                            false),

    {noreply, State#state{aae_rebuild = true}};
handle_cast({rebuild_complete, tree}, State) ->
    {noreply, State#state{aae_rebuild = false}}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% External functions
%%%============================================================================

-spec extractclock_from_riakhead(binary()) -> list(tuple()).
%% @doc
%% Extract the vector clock from a riak binary object (without doing a full
%% binary to objetc conversion)
extractclock_from_riakhead(<<?MAGIC:8/integer, ?V1_VERS:8/integer, 
                            VclockLen:32/integer, VclockBin:VclockLen/binary,
                            _Rest/binary>>) ->
    lists:usort(binary_to_term(VclockBin));
extractclock_from_riakhead(RiakHead) ->
    {proxy_object, HeadBin, _Size, _F} = binary_to_term(RiakHead),
    extractclock_from_riakhead(HeadBin).


%% V1 Riak Object Binary Encoding
%% -type binobj_header()     :: <<53:8, Version:8, VClockLen:32, VClockBin/binary,
%%                                SibCount:32>>.
%% -type binobj_flags()      :: <<Deleted:1, 0:7/bitstring>>.
%% -type binobj_umeta_pair() :: <<KeyLen:32, Key/binary, ValueLen:32, Value/binary>>.
%% -type binobj_meta()       :: <<LastMod:LastModLen, VTag:128, binobj_flags(),
%%                                [binobj_umeta_pair()]>>.
%% -type binobj_value()      :: <<ValueLen:32, ValueBin/binary, MetaLen:32,
%%                                [binobj_meta()]>>.
%% -type binobj()            :: <<binobj_header(), [binobj_value()]>>.
new_v1(Vclock, Siblings) ->
    VclockBin = term_to_binary(Vclock),
    VclockLen = byte_size(VclockBin),
    SibCount = length(Siblings),
    SibsBin = bin_contents(Siblings),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, 
                VclockLen:32/integer, VclockBin/binary, 
                SibCount:32/integer, SibsBin/binary>>.

bin_content(#r_content{metadata=Meta0, value=Val}) ->
    TypeTag = 1,
    ValBin = encode_maybe_binary(Val, TypeTag),
    ValLen = byte_size(ValBin),
    MetaBin = meta_bin(Meta0),
    MetaLen = byte_size(MetaBin),
    <<ValLen:32/integer, ValBin:ValLen/binary, 
        MetaLen:32/integer, MetaBin:MetaLen/binary>>.

encode_maybe_binary(Value, TypeTag) when is_binary(Value) ->
    <<TypeTag, Value/binary>>.

bin_contents(Contents) ->
    F = fun(Content, Acc) ->
                <<Acc/binary, (bin_content(Content))/binary>>
        end,
    lists:foldl(F, <<>>, Contents).

meta_bin(MetaData) ->
    {Mega,Secs,Micro} = os:timestamp(),
    LastModBin = <<Mega:32/integer, Secs:32/integer, Micro:32/integer>>,
    Deleted = <<0>>,
    RestBin = term_to_binary(MetaData),
    VTagBin = ?EMPTY_VTAG_BIN,
    VTagLen = byte_size(VTagBin),
    <<LastModBin/binary, VTagLen:8/integer, VTagBin:VTagLen/binary,
      Deleted:1/binary-unit:8, RestBin/binary>>.


workerfun(ReturnFun) ->
    WorkerPid = spawn(?MODULE, rebuild_worker, [ReturnFun]),
    fun(FoldFun, FinishFun) ->
        WorkerPid! {fold, FoldFun, FinishFun}
    end.

rebuild_worker(ReturnFun) ->
    receive
        {fold, FoldFun, FinishFun} ->
            FinishFun(FoldFun()),
            ReturnFun(ok)
    end.

from_aae_binary(AAEBin) ->
    <<ObjectSize:32/integer, SibCount:32/integer, IndexHash:32/integer, 
        HeadOnly/binary>> = AAEBin,
    {ObjectSize, SibCount, IndexHash, HeadOnly}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

to_aae_binary(ObjectBin) ->
    ObjectSize = byte_size(ObjectBin),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, 
        VclockLen:32/integer, _VclockBin:VclockLen/binary, 
        SibCount:32/integer, SibsBin/binary>> = ObjectBin,

    IndexHash = erlang:phash2([]), % faking here

    HeadOnly = strip_metabinary(SibCount, SibsBin, <<>>),
    <<ObjectSize:32/integer, SibCount:32/integer, IndexHash:32/integer, 
        HeadOnly/binary>>.


strip_metabinary(0, <<>>, MetaBinAcc) ->
    MetaBinAcc;
strip_metabinary(SibCount, SibBin, MetaBinAcc) ->
    <<ValLen:32/integer, _ValBin:ValLen/binary, 
        MetaLen:32/integer, MetaBin:MetaLen/binary, Rest/binary>> = SibBin,
    strip_metabinary(SibCount - 1, 
                        Rest, 
                        <<MetaBinAcc/binary, 
                            MetaLen:32/integer, 
                            MetaBin:MetaLen/binary>>).


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-endif.


