%% -------- Overview ---------
%%
%% Runner used for fetch_clock queries on this AAE vnode

-module(aae_runner).

-behaviour(gen_server).
-include("include/aae.hrl").

-export([init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3]).

-export([runner_start/0, 
            runner_clockfold/3]).

-record(state, {result_size = 0 :: integer(),
                query_count = 0 :: integer(),
                query_time  = 0 :: integer()}).

-define(LOG_FREQUENCY, 100).

%%%============================================================================
%%% API
%%%============================================================================


%% @doc
%% Start an AAE runner to manage folds 
runner_start() ->
    gen_server:start(?MODULE, [], []).

%% @doc
%% Pass some work to a runner
runner_clockfold(Runner, Folder, ReturnFun) ->
    gen_server:cast(Runner, {work, Folder, ReturnFun}).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call(_Msg, _From, State) ->
    {reply, not_implemented, State}.

handle_cast({work, Folder, ReturnFun}, State) ->
    SW = os:timestamp(),
    Results = Folder(),

    RS0 = State#state.result_size + length(Results),
    QT0 = State#state.query_time + timer:now_diff(os:timestamp(), SW),
    QC0 = State#state.query_count + 1,
    {RS1, QT1, QC1} = maybe_log(RS0, QT0, QC0, ?LOG_FREQUENCY),

    ReturnFun(Results),

    {noreply, State#state{result_size = RS1, 
                            query_time = QT1, 
                            query_count = QC1}}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(normal, State) ->
    _ = maybe_log(State#state.result_size, 
                    State#state.query_time, 
                    State#state.query_count, 
                    1),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================


maybe_log(RS_Acc, QT_Acc, QC_Acc, LogFreq) when QC_Acc < LogFreq ->
    {RS_Acc, QT_Acc, QC_Acc};
maybe_log(RS_Acc, QT_Acc, QC_Acc, _LogFreq) ->
    aae_util:log("R0001", [RS_Acc, QT_Acc, QC_Acc], logs()),
    {0, 0, 0}.


%%%============================================================================
%%% log definitions
%%%============================================================================

-spec logs() -> list(tuple()).
%% @doc
%% Define log lines for this module
logs() ->
    [{"R0001", 
            {info, "AAE fetch clock runner has seen results=~w " ++ 
                    "query_time=~w for a query_count=~w queries"}}
    
    ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-endif.


