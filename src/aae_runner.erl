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

-export([runner_start/1, 
            runner_clockfold/4,
            runner_stop/1]).

-include_lib("eunit/include/eunit.hrl").

-record(state, {result_size = 0 :: integer(),
                query_count = 0 :: integer(),
                query_time  = 0 :: integer(),
                log_levels :: aae_util:log_levels()|undefined}).

-define(LOG_FREQUENCY, 100).

%%%============================================================================
%%% API
%%%============================================================================


-spec runner_start(aae_util:log_levels()|undefined) -> {ok, pid()}.
%% @doc
%% Start an AAE runner to manage folds 
runner_start(LogLevels) ->
    gen_server:start(?MODULE, [LogLevels], []).

%% @doc
%% Pass some work to a runner
runner_clockfold(Runner, Folder, ReturnFun, SizeFun) ->
    gen_server:cast(Runner, {work, Folder, ReturnFun, SizeFun}).
%% @doc
%% Close the runner
runner_stop(Runner) ->
    gen_server:call(Runner, close).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([LogLevels]) ->
    {ok, #state{log_levels = LogLevels}}.

handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({work, Folder, ReturnFun, SizeFun}, State) ->
    SW = os:timestamp(),
    Results = Folder(),

    RS0 = State#state.result_size + SizeFun(Results),
    QT0 = State#state.query_time + timer:now_diff(os:timestamp(), SW),
    QC0 = State#state.query_count + 1,
    {RS1, QT1, QC1} =
        maybe_log(RS0, QT0, QC0, ?LOG_FREQUENCY, State#state.log_levels),

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
                    1,
                    State#state.log_levels),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================


maybe_log(RS_Acc, QT_Acc, QC_Acc, LogFreq, _LogLs) when QC_Acc < LogFreq ->
    {RS_Acc, QT_Acc, QC_Acc};
maybe_log(RS_Acc, QT_Acc, QC_Acc, _LogFreq, LogLs) ->
    aae_util:log("R0001", [RS_Acc, QT_Acc, QC_Acc], logs(), LogLs),
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


coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).



-endif.


