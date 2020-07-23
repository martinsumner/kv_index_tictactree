-module(timeout_test).
-behaviour(gen_server).
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
            code_change/3, terminate/2]).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop() ->  gen_server:call(?MODULE, stop).

init([]) -> {ok, []}.

handle_call({test}, _From, State) -> {reply, timer:sleep(1000), State};
handle_call(stop, _From, State) -> {stop, normal, ok, State}.

handle_cast(_Request, State) -> {noreply, State}.
handle_info(_, State) -> {noreply, State}.
code_change(_Old, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

-ifdef(TEST).

wait_on_sync_test() ->
    {ok, P} = start_link(),
    ?assertMatch(timeout,
        aae_controller:wait_on_sync(gen_server, call, P, {test}, 100)),
    ?assertMatch(ok,
        aae_controller:wait_on_sync(gen_server, call, P, {test}, 2000)),
    stop().

-endif.