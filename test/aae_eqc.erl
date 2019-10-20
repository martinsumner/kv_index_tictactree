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

-define(LOG_LEVELS, [error, critical]).
-define(EXCHANGE_PAUSE_MS, 10).

%% -- State and state functions ----------------------------------------------
initial_state() ->
    #{aae_controllers => 
          [{"a", #{store => []}}, 
           {"b", #{store => []}}], %% list of controllers, each unique map
      history => 
          [] %% {Bucket, Key, VClock, LastModified}
      }.  

%% -- Generators -------------------------------------------------------------

pos() ->
    ?LET(N, nat(), N+1).

timestamp(_Obj) ->
    1.

gen_vclock() ->
    ?LET(Names, non_empty(sublist(names())),
         [ {Name, nat()} || Name <- Names]).

gen_vclock(VClockGen) ->
    ?LET(VClock, VClockGen,
    ?LET({{K, C}, P}, {elements(VClock), pos()},
         lists:keyreplace(K, 1, VClock, {K, C + P}))).

names() ->
    [a, b, c, d, e, f].

%% Cannot be atoms!
%% key() type specified: should be binary().
gen_bucket() -> 
    elements([<<"bucket1">>, <<"bucket2">>, <<"bucket3">>]).

gen_key() ->
    binary(16).

gen_bkcm(S) ->
    ?LET({B, K}, frequency([{length(maps:get(history, S, [])), ?LAZY(elements([F || {F, _, _} <-maps:get(history, S)]))},
                            {10, {gen_bucket(), gen_key()}}]), 
         case lists:keyfind({B, K}, 1, maps:get(history, S, [])) of
             false ->
                 {B, K, none, gen_vclock(), gen_last_modified()};
             {_, PrevClock, _LastModifed} ->
                 {B, K, undefined, gen_vclock(PrevClock), gen_last_modified()}
         end).

gen_last_modified() ->
    [{1549, choose(448000, 448100), 0}].


%% generate a new store
gen_store([], Store2) ->
    Store2;
gen_store([{{B, K}, C1, LM1} | Store1], Store2) ->
    case lists:keyfind({B, K}, 1, Store2) of
        false ->
            [ {{B, K}, C1, LM1} | gen_store(Store1, Store2) ]; 
        {_, C2, _} ->
            [ {{B, K}, gen_vclock(elements([C1, C2])), gen_last_modified()} | 
              gen_store(Store1, lists:keydelete({B,K}, 1, Store2))]
    end.
    

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
    ?LET({Path, M}, elements(unstarted_controllers(S)),
         [ Path, 
           {parallel, leveled_ko}, 
           maps:get(store, M, []) == [], 
           elements([{1, 1}, {0, 3600}]), %% if hours is set to 1 it means we cannot trigger a rebuild in a test
           [{0, 3}, {1, 3}, {2,3}],   %% behaviour is not different for less
           {var, dir}
         ]).

start_pre(S, [Path, _KeyStoreType, _IsEmpty, _RebuildSchedule, _PrefLists, _RootPath]) ->
    Controllers = maps:get(aae_controllers, S, []),
    case lists:keyfind(Path, 1, Controllers) of
        false ->
            %% Controller has not been started yet
            true;
        {_, M} ->
            %% Check whether the controller is already started
            not maps:is_key(aae_controller, M)
    end.

start(Path, KeyStoreType, IsEmpty, RebuildSchedule, PrefLists, RootPath) ->
    case catch aae_controller:aae_start(KeyStoreType, IsEmpty, RebuildSchedule, PrefLists, 
                                         filename:join(RootPath, Path),
                                         fun object_split/1,
                                         ?LOG_LEVELS) of
        {ok, Pid} -> Pid;
        Other -> Other
    end.

start_next(S, Value, [Path, _KeyStoreType, IsEmpty, _RebuildSchedule, PrefLists, _RootPath]) ->
    Controllers = maps:get(aae_controllers, S),
    {_, Map} = lists:keyfind(Path, 1, Controllers),
    RebuildIsDue = (not IsEmpty andalso maps:get(store, Map, []) == []),
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
    [ {nextrebuild, Res} ].


%%--- Operation: put ---
put_pre(S) ->
    started_controllers(S) =/= [].

put_args(S) ->
    ?LET({{Path, M}, {B, K, PClock, VClock, LastMod}}, {elements(started_controllers(S)), gen_bkcm(S)},
         [Path, maps:get(aae_controller, M), 
          maps:get(preflists, M), B, K, VClock, PClock, {pos(), pos(), 0, LastMod, []}]).

put_pre(_S, [_Path, _Pid, _PrefLists, _Bucket, _Key, _CurrentClock, _PrevClock, _MetaData]) ->
    true.

put(_Path, Pid, PrefLists, Bucket, Key, CurrentClock, PrevClock, MetaData) ->
    PrefList = lists:nth((erlang:phash2({Bucket, Key}) rem length(PrefLists)) + 1, PrefLists),
    aae_controller:aae_put(Pid, PrefList, Bucket, Key, CurrentClock, PrevClock, term_to_binary(MetaData)).

put_next(S, _Value, [Path, _Pid, _PrefLists, Bucket, Key, CurrentClock, _PrevClock, {_, _, _, LastMod, _}]) ->
    Controllers = maps:get(aae_controllers, S),
    {_, M} = lists:keyfind(Path, 1, Controllers),
    S#{aae_controllers => 
           lists:keyreplace(Path, 1, Controllers, 
                            {Path, M#{store =>
                                          [ {{B, K}, C, L} || {{B, K}, C, L} <- maps:get(store, M), {Bucket, Key} =/= {B, K}] ++
                                          [ {{Bucket, Key}, CurrentClock, LastMod} ] 
                                          }}),
       history =>
           maps:get(history, S, []) ++ [{{Bucket, Key}, CurrentClock, LastMod}]
      }.


put_post(_S, [_Path, _Pid, _PrefLists, _Bucket, _Key, _CurrentClock, _PrevClock, _MetaData], Res) ->
    eq(Res, ok).

put_features(_S, [_Path, _Pid, _PrefLists, _Bucket, _Key, _CurrentClock, PrevClock, _MetaData], _Res) ->
    [ {put, PrevClock} ].


%% --- Operation: exchange ---
exchange_pre(S) ->
    length(started_controllers(S)) >= 2.

exchange_args(S) ->
    Controllers = started_controllers(S),
    ?LET({Path1, M1}, elements(Controllers),
    ?LET({Path2, M2}, elements(Controllers), %% possibly minus the already selected one
         [ Path1, Path2,
           [maps:get(aae_controller, M1), maps:get(preflists, M1)], %% BlueList
           [maps:get(aae_controller, M2), maps:get(preflists, M2)]  %% PinkList
         ])).

exchange_pre(S, [Path1, Path2, _Blue, _Pink]) ->
    lists:keymember(Path1, 1, started_controllers(S)) andalso 
        lists:keymember(Path2, 1, started_controllers(S)).

exchange(_, _, [BluePid, BluePrefLists], [PinkPid, PinkPrefLists]) ->
    BlueList =  [{testutil:exchange_sendfun(BluePid), BluePrefLists}],
    PinkList =  [{testutil:exchange_sendfun(PinkPid), PinkPrefLists}],
    QuickCheck = self(),
    {ok, Pid, _UUID} = aae_exchange:start(full, BlueList, PinkList, 
                                          fun(KeyList) -> QuickCheck ! {self(), repair, KeyList} end, %% do not repair at all 
                                          fun(Result) -> QuickCheck ! {self(), reply, Result} end,
                                          none,
                                          [{transition_pause_ms, ?EXCHANGE_PAUSE_MS},
                                            {log_levels, ?LOG_LEVELS}]),
    receive
        {Pid, reply, {root_compare, 0}} ->            
            {root_compare, 0};
        {Pid, reply, Other} ->
            receive
                {Pid, repair, KeyList} ->
                    {repair, Other, KeyList}
            after 5000 -> timeout
            end
    after 5000 -> timeout
    end.

exchange_post(S, [Path1, Path2, _Blue, _Pink], Res) ->
    {_, M1} = lists:keyfind(Path1, 1, maps:get(aae_controllers, S, [])),
    {_, M2} = lists:keyfind(Path2, 1, maps:get(aae_controllers, S, [])),
    BlueStore = lists:usort(maps:get(store, M1, [])), 
    PinkStore = lists:usort(maps:get(store, M2, [])),
    MatchBlueFun =
        fun({{B, K}, C, _L}, Acc) ->
            case lists:keyfind({B, K}, 1, PinkStore) of
                false ->
                    [{{B, K}, {C, none}}|Acc];
                {{B, K}, C, _} ->
                    Acc;
                {{B, K}, NC, _} ->
                    [{{B, K}, {C, NC}}|Acc]
            end
        end,
    MatchPinkFun =
        fun({{B, K}, C, _L}, Acc) ->
            case lists:keyfind({B, K}, 1, BlueStore) of
                false ->
                    [{{B, K}, {none, C}}|Acc];
                _ ->
                    Acc
            end
        end,
    Acc0 = lists:foldl(MatchBlueFun, [], BlueStore),
    Expected = lists:usort(lists:foldl(MatchPinkFun, Acc0, PinkStore)),
    case Res of
        {root_compare, 0} -> 
            eq(0, length(Expected));
        {repair, {clock_compare, N}, KeyList} -> 
            N == length(KeyList) 
                andalso eq(lists:sort(KeyList), Expected);
        _ ->
            eq(Res, Expected)  %% will print the difference
    end.

exchange_features(_S, [_Path1, _Path2, _Blue, _Pink], Res) ->
    case Res of
        {root_compare, 0} -> 
            root_compare;
        {repair, {clock_compare, N}, _KeyList} -> 
            {clock_compare, N};
        _ ->
            Res
    end.



%% --- Operation: sync ---
sync_pre(S) ->
    length(started_controllers(S)) >= 2.

sync_args(S) ->
    Controllers = started_controllers(S),
    ?LET({Path1, M1}, elements(Controllers),
    ?LET({Path2, M2}, elements(Controllers -- [{Path1, M1}]),
         [ Path1, Path2, 
           maps:get(preflists, M1), maps:get(preflists, M2),
           maps:get(aae_controller, M1), maps:get(aae_controller, M2),
           gen_store(maps:get(store, M1), maps:get(store, M2)) ])).


sync_pre(S, [Path1, Path2, _, _, _, _, _Store]) ->
    lists:keymember(Path1, 1, started_controllers(S)) andalso 
        lists:keymember(Path2, 1, started_controllers(S)).


sync(_Path1, _Path2, _PrefLists1, _PrefLists2, _Pid1, _Pid2, []) ->
    ok;
sync(Path1, Path2, PrefLists1, PrefLists2, Pid1, Pid2, [{{B, K}, VC, LastMod}|Store]) ->
    %% TODO: add meta data to the state and extract it again
    put(Path1, Pid1, PrefLists1, B, K, VC, undefined, {1, 1, 0, LastMod, []}),
    put(Path2, Pid2, PrefLists2, B, K, VC, undefined, {1, 1, 0, LastMod, []}),
    sync(Path1, Path2, PrefLists1, PrefLists2, Pid1, Pid2, Store).

sync_next(S, _Value, [Path1, Path2, _PrefLists1, _PrefLists2, _Pid1, _Pid2, Store]) ->
    Controllers = maps:get(aae_controllers, S),
    {_, M1} = lists:keyfind(Path1, 1, Controllers),
    {_, M2} = lists:keyfind(Path2, 1, Controllers),
    S#{aae_controllers => 
           lists:keyreplace(Path1, 1, 
                            lists:keyreplace(Path2, 1, Controllers,
                            {Path2, M2#{store => Store}}),
                            {Path1, M1#{store => Store}}),
       history =>
           maps:get(history, S, []) ++ Store
      }.




%% --- ... more operations

%% -- Property ---------------------------------------------------------------
prop_aae() ->
    Dir = "./aae_data",
    eqc:dont_print_counterexample( 
    ?FORALL(Cmds, commands(?MODULE),
    begin
        os:cmd("rm -rf " ++ Dir),
        {H, S, Res} = run_commands(Cmds, [{dir, Dir}]),
        [ aae_controller:aae_close(maps:get(aae_controller, M)) || {_, M} <- started_controllers(S) ],
        CallFeatures = call_features(H),
        check_command_names(Cmds,
            measure(length, commands_length(Cmds),
            aggregate(with_title('Features'), CallFeatures,
            aggregate_feats(all_command_names(), CallFeatures,
            features(CallFeatures,
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                Res == ok))))))
    end)).

aggregate_feats([], _, Prop) -> Prop;
aggregate_feats([atoms | Kinds], Features, Prop) ->
    {Atoms, Rest} = lists:partition(fun is_atom/1, Features),
    aggregate(with_title(atoms), Atoms, aggregate_feats(Kinds, Rest, Prop));
aggregate_feats([Tag | Kinds], Features, Prop) ->
    {Tuples, Rest} = lists:partition(fun(X) -> is_tuple(X) andalso element(1, X) == Tag end, Features),
    aggregate(with_title(Tag), [ Arg || {_, Arg} <- Tuples ], aggregate_feats(Kinds, Rest, Prop)).


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

