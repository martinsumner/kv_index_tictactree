%%% @author Thomas Arts <thomas@SpaceGrey.local>
%%% @copyright (C) 2019, Thomas Arts
%%% @doc
%%%
%%% @end
%%% Created :  5 Feb 2019 by Thomas Arts <thomas@SpaceGrey.local>

-module(aae_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile([export_all, nowarn_export_all]).
-compile({nowarn_deprecated_function, [{erlang, now, 0}]}).

%% -- State and state functions ----------------------------------------------
initial_state() ->
    #{aae_controllers => 
          [{"a", #{is_empty => true,
                  store => []}}, 
           {"b", #{is_empty => true,
                  store => []}}], %% list of controllers, each unique map
      history => 
          [] %% {Bucket, Key, VClock, LastModified}
      }.  

%% -- Generators -------------------------------------------------------------

pos() ->
    ?LET(N, nat(), N+1).

timestamp(_Obj) ->
    1.


%% -- Common pre-/post-conditions --------------------------------------------
command_precondition_common(_S, _Cmd) ->
    true.

precondition_common(_S, _Call) ->
    true.

postcondition_common(_S, _Call, _Res) ->
    true.

%% -- Operations -------------------------------------------------------------

object_split(Object) ->
     {_Size, _SiblingCount, _IndexHash, _LastMod, _UserData} = binary_to_term(Object).

%% --- Operation: init ---
start_pre(S) ->
    unstarted_controllers(S) =/= [].

start_args(S) ->
    ?LET({Path, _}, elements(unstarted_controllers(S)),
         [ Path, 
           {parallel, leveled_ko}, 
           bool(), 
           elements([{1, 1}, {0, 3600}]), %% if hours is set to 1 it means we cannot trigger a rebuild in a test
           [{0, 3}, {1, 3}, {2,3}],   %% behaviour is not different for less
           {var, dir}
         ]).

start(Path, KeyStoreType, IsEmpty, RebuildSchedule, PrefLists, RootPath) ->
    case catch aae_controller:aae_start(KeyStoreType, IsEmpty, RebuildSchedule, PrefLists, 
                                         filename:join(RootPath, Path), fun object_split/1) of
        {ok, Pid} -> Pid;
        Other -> Other
    end.

start_next(S, Value, [Path, _KeyStoreType, IsEmpty, _RebuildSchedule, PrefLists, _RootPath]) ->
    Controllers = maps:get(aae_controllers, S),
    {_, Map} = lists:keyfind(Path, 1, Controllers),
    RebuildIsDue = (not IsEmpty andalso maps:get(is_empty, Map)),
    S#{aae_controllers => 
           lists:keyreplace(Path, 1, Controllers, {Path, Map#{aae_controller => Value,
                                                              rebuild_due => RebuildIsDue,
                                                              preflists => PrefLists}})}. 

start_post(_S, _Args, Res) ->
    is_pid(Res).

start_features(_S, [_Path, _KeyStoreType, IsEmpty, RebuildSchedule, _PrefLists, _RootPath], _Res) ->
    [ {start, {schedule, RebuildSchedule}}, {start, {is_empty, IsEmpty}} ].


%% --- Operation: stop ---
stop_pre(S) ->
    started_controllers(S) =/= [].

stop_args(S) ->
    ?LET({Path, M}, elements(started_controllers(S)),
         [Path, maps:get(aae_controller, M)]).

stop_pre(S, [Path, Pid]) ->
    {_, M} = lists:keyfind(Path, 1, maps:get(aae_controllers, S)),
    Pid == maps:get(aae_controller, M).  %% for shrinking

stop(_, Pid) ->
    catch aae_controller:aae_close(Pid).

stop_next(S, _Value, [Path, _Pid]) ->
    Controllers = maps:get(aae_controllers, S),
    {_, M} = lists:keyfind(Path, 1, Controllers),
    S#{aae_controllers => 
           lists:keyreplace(Path, 1, Controllers, {Path,  maps:without([aae_controller], M)})}.

stop_post(_S, [_, _Pid], Res) ->
    eq(Res, ok).

%% --- Operation: next_rebuild ---
nextrebuild_pre(S) ->
    started_controllers(S) =/= [].

nextrebuild_args(S) ->
    ?LET({Path, M}, elements(started_controllers(S)),
         [Path, maps:get(aae_controller, M)]).

nextrebuild_pre(S, [Path, Pid]) ->
    Controllers = maps:get(aae_controllers, S),
    {_, M} = lists:keyfind(Path, 1, Controllers),
    Pid == maps:get(aae_controller, M).  %% for shrinking

%% If we expected to be due, it should be due.
nextrebuild(_, Pid) ->
    TS = aae_controller:aae_nextrebuild(Pid),
    os:timestamp() > TS.

nextrebuild_post(S, [Path, _Pid], Res) ->
    Controllers = maps:get(aae_controllers, S),
    {_, M} = lists:keyfind(Path, 1, Controllers),
    not maps:get(rebuild_due, M) orelse Res.
            

nextrebuild_features(_S, [_, _Pid], Res) ->
    [ {rebuild, Res} ].


%% --- ... more operations

%% -- Property ---------------------------------------------------------------
prop_aae() ->
    Dir = "./aae_data",
    eqc:dont_print_counterexample( 
    ?FORALL(Cmds, commands(?MODULE),
    begin
        os:cmd("rm -rf " ++ Dir),
        {H, S, Res} = run_commands(Cmds, [{dir, Dir}]),
        %% io:format("State = ~p~n", [S]),
        [ aae_controller:aae_close(maps:get(aae_controller, M)) || {_, M} <- started_controllers(S) ],
        CallFeatures = call_features(H),
        check_command_names(Cmds,
            measure(length, commands_length(Cmds),
            featuresort(CallFeatures,
            features(CallFeatures,
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                Res == ok)))))
    end)).

featuresort([], Prop) ->
    Prop;
featuresort([{Key, Feature} | Features], Prop) ->
    Same = [ F || {K, F} <- Features, K == Key],
    Different =  [ {K, F} || {K, F} <- Features, K =/= Key],
    aggregate(with_title(Key), [Feature | Same], 
       featuresort(Different, Prop)).


bugs() -> bugs(10).

bugs(N) -> bugs(N, []).

bugs(Time, Bugs) ->
    more_bugs(eqc:testing_time(Time, prop_aae()), 20, Bugs).


%%% ---- state functions

unstarted_controllers(S) ->
    Controllers = maps:get(aae_controllers, S, []),
    lists:filter(fun({_, M}) -> not maps:is_key(aae_controller, M) end, Controllers).

started_controllers(S) ->
    Controllers = maps:get(aae_controllers, S, []),
    lists:filter(fun({_, M}) -> maps:is_key(aae_controller, M) end, Controllers).

