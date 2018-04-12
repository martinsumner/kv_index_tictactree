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

-export([open/3,
            put/4,
            push/6,
            exchange_message/3,
            rebuild/2,
            close/1]).

-export([riak_extract_metadata/1,
            new_v1/2]).

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
                    root_path :: list()}).


-record(state, {root_path :: list(),
                index_ns :: list(tuple()),
                aae_controller :: pid(),
                vnode_store :: pid(),
                vnode_id :: binary(),
                vnode_sqn = 1 :: integer()}).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET_SDG, <<"MD">>).
-define(KEY_SDG, <<"SHUDOWN_GUID">>).
-define(TAG_SDG, o).
-define(RIAK_TAG, o_rkv).
-define(REBUILD_SCHEDULE, {1, 60}).
-define(LASTMOD_LEN, 29). 
-define(V1_VERS, 1).
-define(MAGIC, 53).  
-define(EMPTY_VTAG_BIN, <<"e">>).


-type r_object() :: #r_object{}.

%%%============================================================================
%%% API
%%%============================================================================

-spec open(list(), atom(), list(tuple())) -> {ok, pid()}.
%% @doc
%% Open a mock vnode
open(Path, AAEType, IndexNs) ->
    gen_server:start(?MODULE, 
                        [#options{aae = AAEType, 
                                    index_ns = IndexNs, 
                                    root_path = Path}], 
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

-spec rebuild(pid(), boolean()) -> erlang:timestamp().
%% @doc
%% Prompt for the next rebuild time, using ForceRebuild=true to oevrride that
%% time
rebuild(Vnode, ForceRebuild) ->
    gen_server:call(Vnode, {rebuild, ForceRebuild}).


-spec exchange_message(pid(), tuple()|atom(), list(tuple())) -> ok.
%% @doc
%% Handle a message from an AAE exchange
exchange_message(Vnode, Msg, IndexNs) ->
    gen_server:call(Vnode, {aae, Msg, IndexNs}).


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
    ShutdownGUID = 
        case leveled_bookie:book_get(VnSt, ?BUCKET_SDG, ?KEY_SDG, ?TAG_SDG) of
            not_found ->
                none;
            {ok, Value} when is_list(Value) ->
                ok = leveled_bookie:book_delete(VnSt, 
                                                ?BUCKET_SDG, 
                                                ?KEY_SDG, 
                                                []),
                Value
        end,
    IsEmpty = leveled_bookie:book_isempty(VnSt, ?RIAK_TAG),
    KeyStoreType = 
        case Opts#options.aae of 
            native ->
                {native, leveled_ko, VnSt};
            parallel ->
                {parallel, leveled_so}
        end,
    {ok, AAECntrl} = 
        aae_controller:aae_start(KeyStoreType, 
                                    {IsEmpty, ShutdownGUID}, 
                                    ?REBUILD_SCHEDULE, 
                                    Opts#options.index_ns, 
                                    RP, 
                                    fun riak_extract_metadata/1),
    {ok, #state{root_path = RP,
                vnode_store = VnSt,
                index_ns = Opts#options.index_ns,
                aae_controller = AAECntrl,
                vnode_id = list_to_binary(leveled_codec:generate_uuid())}}.

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
                {VclockBin, _SMB, _LMs} 
                    = leveled_codec:riak_metadata_frombinary(Head),
                Clock0 = 
                    term_to_binary(VclockBin),
                Clock1 = 
                    [{State#state.vnode_id, State#state.vnode_sqn}|Clock0],
                {lists:ukeysort(Clock1, 1), Clock0}
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
handle_call({rebuild, _ForceRebuild}, _From, State) ->
    % Check next rebuild
    % Reply with next rebuild TS
    % If force rebuild, then trigger rebuild
    {reply, ok, State};
handle_call({aae, Msg, IndexNs}, From, State) ->
    ReturnFun = 
        fun(Reply) ->
            gen_server:reply(From, Reply)
        end,
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
                                                ReturnFun)
    end,
    {reply, ok, State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({push, Bucket, Key, UpdClock, ObjectBin, IndexN}, State) ->
    % As PUT, but don't increment vclock, replace regardless of current state
    PrevClock = 
        case leveled_bookie:book_head(State#state.vnode_store, 
                                        Bucket, Key, ?RIAK_TAG) of
            not_found ->
                none;
            {ok, Head} ->
                {VclockBin, _SMB, _LMs} 
                    = leveled_codec:riak_metadata_frombinary(Head),
                term_to_binary(VclockBin)
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

-spec riak_extract_metadata(binary()) 
                                -> {integer(), integer(), integer(), any()}.
%% @doc
%% The vnode should produce a special version of the object binary which with 
%% this function cna be quickly unpacked.
%%
%% If the backend supports can rely on a special PUT that spits back the 
%% metadata - so this doesn't have to be unpacked spearately by the backend.
riak_extract_metadata(ObjBin) ->
    <<ObjSize:32/integer, 
        SibCount:32/integer,
        IndexHash:32/integer, 
        HeadLen:32/integer,
        Head:HeadLen/binary>> = ObjBin,
    {ObjSize, SibCount, IndexHash, binary_to_term(Head)}.


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
    HeadSize = byte_size(HeadOnly),
    <<ObjectSize:32/integer, SibCount:32/integer, IndexHash:32/integer, 
        HeadSize:32/integer, HeadOnly/binary>>.


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


