-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([dual_store_compare/1]).

all() -> [dual_store_compare].

-define(ROOT_PATH, "test/").

dual_store_compare(_Config) ->
    % Setup to AAE controllers, each representing the same data.  One store
    % will be split into two three preflists, the other into two.  The 
    % preflists will be mapped as follows:
    % {2, 0} <-> {3, 0}
    % {2, 1} <-> {3, 1} & {3, 2}
    RootPath = reset_filestructure(),
    VnodePath1 = filename:join(RootPath, "vnode1/"),
    VnodePath2 = filename:join(RootPath, "vnode2/"),
    SplitF = fun(_X) -> {leveled_rand:uniform(1000), 1, 0, null} end,
    RPid = self(),
    ReturnFun = fun(R) -> RPid ! {result, R} end,

    {ok, Cntrl1} = 
        aae_controller:aae_start({parallel, leveled_so}, 
                                    {true, none}, 
                                    {1, 300}, 
                                    [{2, 0}, {2, 1}], 
                                    VnodePath1, 
                                    SplitF),
    {ok, Cntrl2} = 
        aae_controller:aae_start({parallel, leveled_so}, 
                                    {true, none}, 
                                    {1, 300}, 
                                    [{3, 0}, {3, 1}, {3, 2}], 
                                    VnodePath2, 
                                    SplitF),
    
    BKVList = gen_keys([], 10000),
    ok = put_keys(Cntrl1, 2, BKVList),
    ok = put_keys(Cntrl2, 3, BKVList),
    
    % Confirm all partitions are aligned as expected using direct access to 
    % the controller

    ok = aae_controller:aae_mergeroot(Cntrl1, 
                                        [{2, 0}, {2, 1}], 
                                        ReturnFun),
    Root1A = start_receiver(),
    ok = aae_controller:aae_mergeroot(Cntrl2, 
                                        [{3, 0}, {3, 1}, {3, 2}], 
                                        ReturnFun),
    Root2A = start_receiver(),
    true = Root1A == Root2A,

    ok = aae_controller:aae_fetchroot(Cntrl1, 
                                        [{2, 0}], 
                                        ReturnFun),
    [{{2, 0}, Root1B}] = start_receiver(),
    ok = aae_controller:aae_fetchroot(Cntrl2, 
                                        [{3, 0}], 
                                        ReturnFun),
    [{{3, 0}, Root2B}] = start_receiver(),
    true = Root1B == Root2B,

    ok = aae_controller:aae_mergeroot(Cntrl1, 
                                        [{2, 1}], 
                                        ReturnFun),
    Root1C = start_receiver(),
    ok = aae_controller:aae_mergeroot(Cntrl2, 
                                        [{3, 1}, {3, 2}], 
                                        ReturnFun),
    Root2C = start_receiver(),
    true = Root1C == Root2C,
    


    % Confirm no dependencies when using different matching AAE exchanges
    RepairFun = fun(_KL) -> null end,  

    {ok, _P, GUID} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}],
                                [{exchange_sendfun(Cntrl2), [{3, 0}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID]),
    ExchangeState = start_receiver(),
    true = ExchangeState == root_compare,

    % Create a discrepancy and discove rit through exchange



    % Shutdown and tidy up
    ok = aae_controller:aae_close(Cntrl1, none),
    ok = aae_controller:aae_close(Cntrl2, none),
    RootPath = reset_filestructure().



reset_filestructure() ->
    reset_filestructure(0, ?ROOT_PATH).
    
reset_filestructure(Wait, RootPath) ->
    io:format("Waiting ~w ms to give a chance for all file closes " ++
                 "to complete~n", [Wait]),
    timer:sleep(Wait),
    clear_all(RootPath),
    RootPath.

clear_all(RootPath) ->
    ok = filelib:ensure_dir(RootPath),
    {ok, FNs} = file:list_dir(RootPath),
    FoldFun =
        fun(FN) ->
            case filelib:is_file(FN) of 
                true ->
                    file:delete(filename:join(RootPath, FN));
                false ->
                    case filelib:is_dir(FN) of 
                        true ->
                            clear_all(filename:join(RootPath, FN));
                        false ->
                            % Root Path
                            ok
                    end
            end
        end,
    lists:foreach(FoldFun, FNs).


gen_keys(KeyList, 0) ->
    KeyList;
gen_keys(KeyList, Count) ->
    Bucket = integer_to_binary(Count rem 5),  
    Key = list_to_binary(string:right(integer_to_list(Count), 6, $0)),
    VersionVector = add_randomincrement([]),
    gen_keys([{Bucket, Key, VersionVector}|KeyList], 
                Count - 1).

put_keys(_Cntrl, _Nval, []) ->
    ok;
put_keys(Cntrl, Nval, [{Bucket, Key, VersionVector}|Tail]) ->
    ok = aae_controller:aae_put(Cntrl, 
                                calc_preflist(Key, Nval), 
                                Bucket, 
                                Key, 
                                VersionVector, 
                                none, 
                                <<>>),
    put_keys(Cntrl, Nval, Tail).

add_randomincrement(Clock) ->
    RandIncr = leveled_rand:uniform(100),
    RandNode = lists:nth(leveled_rand:uniform(9), 
                            ["a", "b", "c", "d", "e", "f", "g", "h", "i"]),
    UpdClock = 
        case lists:keytake(RandNode, 1, Clock) of 
            false ->
                [{RandNode, RandIncr}|Clock];
            {value, {RandNode, Incr0}, Rest} ->
                [{RandNode, Incr0 + RandIncr}|Rest]
        end,
    lists:usort(UpdClock).

calc_preflist(Key, 2) ->
    case erlang:phash2(Key) band 3 of 
        0 ->
            {2, 0};
        _ ->
            {2, 1}
    end;
calc_preflist(Key, 3) ->
    case erlang:phash2(Key) band 3 of 
        0 ->
            {3, 0};
        1 ->
            {3, 1};
        _ ->
            {3, 2}
    end.

start_receiver() ->
    receive
        {result, Reply} ->
            Reply 
    end.


exchange_sendfun(Cntrl) ->
    SendFun = 
        fun(Msg, Preflists, Colour) ->
            RPid = self(),
            ReturnFun = 
                fun(R) -> 
                    io:format("Preparing reply to ~w for colour ~w~n", 
                                [RPid, Colour]),
                    aae_exchange:reply(RPid, R, Colour)
                end,
            case Msg of 
                fetch_root ->
                    io:format("fetch_root sent to ~w to return to ~w~n", 
                                [Cntrl, RPid]),
                    aae_controller:aae_mergeroot(Cntrl, Preflists, ReturnFun)
            end
        end,
    SendFun.

