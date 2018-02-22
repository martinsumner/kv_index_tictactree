%% -------- Overview ---------
%%

-module(aae_keystore).

-behaviour(gen_server).

-include("include/aae.hrl").


-export([
            init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3]).


-record(state, {}).

-include_lib("eunit/include/eunit.hrl").

% -type keystore_state() :: #state{}.


%%%============================================================================
%%% API
%%%============================================================================



%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([_Opts]) ->
    {ok, #state{}}. 
    

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
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
