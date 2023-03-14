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
            runner_work/2,
            runner_stop/1]).

-record(state, {result_size = 0 :: integer(),
                query_count = 0 :: integer(),
                query_time  = 0 :: integer(),
                aae_controller :: pid()|undefined,
                log_levels :: aae_util:log_levels()|undefined}).

-define(LOG_FREQUENCY, 10).

-define(PROMPT_MILLISECONDS, 2000).

%%%============================================================================
%%% API
%%%============================================================================


-spec runner_start(aae_util:log_levels()|undefined) -> {ok, pid()}.
%% @doc
%% Start an AAE runner to manage folds 
runner_start(LogLevels) ->
    gen_server:start_link(?MODULE, [LogLevels, self()], []).

-spec runner_work(pid(), aae_controller:runner_work()|queue_empty) -> ok.
%% @doc
%% Be cast some work
runner_work(Runner, Work) ->
    gen_server:cast(Runner, Work).

-spec runner_stop(pid()) -> ok.
%% @doc
%% Close the runner
runner_stop(Runner) ->
    gen_server:call(Runner, close, 30000).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([LogLevels, Controller]) ->
    {ok,
        #state{log_levels = LogLevels, aae_controller = Controller},
        ?PROMPT_MILLISECONDS}.

handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(queue_empty, State) ->
    {noreply, State, ?PROMPT_MILLISECONDS};
handle_cast({work, Folder, ReturnFun, SizeFun}, State) ->
    SW = os:timestamp(),
    State0 =
        try Folder() of
            query_backlog ->
                aae_util:log("R0002", [], logs(), State#state.log_levels),
                ReturnFun({error, query_backlog}),
                State;
            Results ->
                QueryTime = timer:now_diff(os:timestamp(), SW),
                aae_util:log("R0003", [QueryTime], logs(),
                                State#state.log_levels),
                RS0 = State#state.result_size + SizeFun(Results),
                QT0 = State#state.query_time + QueryTime,
                QC0 = State#state.query_count + 1,
                {RS1, QT1, QC1} =
                    maybe_log(RS0, QT0, QC0,
                                ?LOG_FREQUENCY, State#state.log_levels),

                ReturnFun(Results),

                State#state{result_size = RS1,
                            query_time = QT1,
                            query_count = QC1}
        catch
            Error:Pattern ->
                aae_util:log("R0005", [Error, Pattern], logs(),
                                State#state.log_levels),
                ReturnFun({error, Error}),
                State
        end,
    {noreply, State0, 0}.

handle_info(timeout, State) ->
    aae_util:log("R0004", [], logs(), State#state.log_levels),
    ok = aae_controller:aae_runnerprompt(State#state.aae_controller),
    {noreply, State}.

terminate(_Reason, State) ->
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
                    "query_time=~w for a query_count=~w queries"}},
        {"R0002",
            {info, "Query backlog resulted in dummy fold"}},
        {"R0003",
            {debug, "Query complete in time ~w"}},
        {"R0004",
            {debug, "Prompting controller"}},
        {"R0005",
            {warn, "Query lead to error ~w pattern ~w"}}
    
    ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

runner_fail_test() ->
    {ok, R} = runner_start(undefined),
    TestProcess = self(),
    CheckFun =
        fun(ReturnTuple) ->
            ?assertMatch(error, element(1, ReturnTuple)),
            TestProcess ! error
        end,
    ReturnFun = aae_controller:generate_returnfun("ABCD", CheckFun),
    FoldFun = fun() -> throw(noproc) end,
    SizeFun = fun(_Results) -> 0 end,
    runner_work(R, {work, FoldFun, ReturnFun, SizeFun}),
    error = start_receiver(),
    ok = runner_stop(R).
    
start_receiver() ->
    receive
        error ->
            error 
    end.


coverage_cheat_test() ->
    {ok, _State1} = code_change(null, #state{}, null).

-endif.


