%% -------- Overview ---------
%%
%% An AAE controller, that should receive updates from a KV store along with
%% requests for:
%% - Tictac trees, and parts thereof
%% - Keys and clocks by portion of the tree
%% - Snapshots of the KeyStore to support async object folds
%% - Periodic requests to rebuild
%% - Requests to startup and shutdown


-module(aae_controller).

-behaviour(gen_fsm).
-include("include/aae.hrl").

-export([init/1,
            handle_sync_event/4,
            handle_event/3,
            handle_info/3,
            terminate/3,
            code_change/4]).

-export([starting/3,
            replacing_store/3,
            replacing_store/2,
            replacing_tree/2,
            replacing_tree/3,
            steady/3,
            steady/2]).
            
-include_lib("eunit/include/eunit.hrl").


-record(state, {key_store :: key_store(),
                tree_caches :: tree_caches(),
                rebuild_disklog :: pid(),
                vnode :: pid()}).


% -type controller_state() :: #state{}.
-type key_store() :: {boolean(), pid()|none}.
-type tree_caches() :: list({tuple(), pid()}).


%%%============================================================================
%%% API
%%%============================================================================





%%%============================================================================
%%% gen_fsm callbacks
%%%============================================================================

init([_Opts]) ->
    {ok, starting, #state{}}.

starting(_Msg, _From, State) ->
    {reply, ok, replacing_store, State}.

replacing_store(_Msg, _From, State) ->
    {reply, ok, replacing_tree, State}.

replacing_tree(_Msg, _From, State) ->
    {reply, ok, steady, State}.

steady(_Msg, _From, State) ->
    {reply, ok, steady, State}.



replacing_store(_Msg, State) ->
    {next_state, replacing_tree, State}.

replacing_tree(_Msg, State) ->
    {next_state, steady, State}.

steady(_Msg, State) ->
    {next_state, steady, State}.


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
%%% Internal functions
%%%============================================================================









