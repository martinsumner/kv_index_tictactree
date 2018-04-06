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
            push/3,
            rebuild/2,
            close/1]).

-record(r_content, {
                    metadata,
                    value :: term()
                    }).

-record(r_object, {
                    bucket,
                    key,
                    contents :: [#r_content{}],
                    vclock,
                    updatemetadata=dict:store(clean, true, dict:new()),
                    updatevalue :: term()}).

-record(options, {aae :: parallel|native,
                    index_ns :: list(tuple()),
                    root_path :: list()}).


-record(state, {root_path :: list(),
                index_ns :: list(tuple()),
                vnode_id :: list(),
                vnode_sqn = 1 :: integer()}).

-include_lib("eunit/include/eunit.hrl").


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

-spec push(pid(), r_object(), tuple()) -> ok.
%% @doc
%% Push a new object in the store, updating AAE
push(Vnode, Object, IndexN) ->
    gen_server:cast(Vnode, {push, Object, IndexN}).

-spec rebuild(pid(), boolean()) -> erlang:timestamp().
%% @doc
%% Prompt for the next rebuild time, using ForceRebuild=true to oevrride that
%% time
rebuild(Vnode, ForceRebuild) ->
    gen_server:call(Vnode, {rebuild, ForceRebuild}).


-spec close(pid()) -> ok.
%% @doc
%% Cloe the vnode, and any aae controller
close(Vnode) ->
    gen_server:call(Vnode, close).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([_Opts]) ->
    % Start the vnode backend
    % Get the shutdown GUID
    % Delete the shutdown GUID
    % Check is_empty
    % Start the aae_controller 
    % Report back OK
    {ok, #state{}}.

handle_call({put, _Object, _IndexN, _OtherVnodes}, _From, State) ->
    % Get Bucket and Key from object
    % Do head request
    % Compare clock, update clock
    % Send update to other stores
    % Reoprt back OK
    {reply, ok, State};
handle_call({rebuild, _ForceRebuild}, _From, State) ->
    % Check next rebuild
    % Reply with next rebuild TS
    % If force rebuild, then trigger rebuild
    {reply, ok, State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({push, _Object, _IndexN}, State) ->
    % As PUT, but vnode check may form a sibling 
    {ok, State}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% External functions
%%%============================================================================




%%%============================================================================
%%% Internal functions
%%%============================================================================




%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-endif.


