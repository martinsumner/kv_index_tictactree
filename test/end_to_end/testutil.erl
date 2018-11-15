-module(testutil).

-export([gen_keys/2,
            gen_keys/3,
            put_keys/3,
            put_keys/4,
            remove_keys/3,
            gen_riakobjects/3]).
-export([calc_preflist/2]).
-export([start_receiver/0, 
            exchange_sendfun/1,
            exchange_vnodesendfun/1,
            repair_fun/3]).
-export([reset_filestructure/0,
            reset_filestructure/2]).

-include("testutil.hrl").

-define(ROOT_PATH, "test/").

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
            FFP = filename:join(RootPath, FN),
            case filelib:is_dir(FFP) of 
                true ->
                    clear_all(FFP ++ "/");
                false ->
                    case filelib:is_file(FFP) of 
                        true ->
                            file:delete(FFP);
                        false ->
                            ok 
                    end
            end
        end,
    lists:foreach(FoldFun, FNs).

gen_keys(KeyList, Count) ->
    gen_keys(KeyList, Count, 0).

gen_keys(KeyList, Count, Floor) when Count == Floor ->
    KeyList;
gen_keys(KeyList, Count, Floor) ->
    Bucket = integer_to_binary(Count rem 5),  
    Key = list_to_binary(string:right(integer_to_list(Count), 6, $0)),
    VersionVector = add_randomincrement([]),
    gen_keys([{Bucket, Key, VersionVector}|KeyList], 
                Count - 1,
                Floor).

put_keys(Cntrl, NVal, KL) ->
    put_keys(Cntrl, NVal, KL, none).

put_keys(_Cntrl, _Nval, [], _PrevVV) ->
    ok;
put_keys(Cntrl, Nval, [{Bucket, Key, VersionVector}|Tail], PrevVV) ->
    ok = aae_controller:aae_put(Cntrl, 
                                calc_preflist(Key, Nval), 
                                Bucket, 
                                Key, 
                                VersionVector, 
                                PrevVV, 
                                {[os:timestamp()],
                                    term_to_binary([{clock, VersionVector}])}),
    put_keys(Cntrl, Nval, Tail, PrevVV).

remove_keys(_Cntrl, _Nval, []) ->
    ok;
remove_keys(Cntrl, Nval, [{Bucket, Key, _VV}|Tail]) ->
    ok = aae_controller:aae_put(Cntrl, 
                                calc_preflist(Key, Nval), 
                                Bucket, 
                                Key, 
                                none, 
                                undefined, 
                                <<>>),
    remove_keys(Cntrl, Nval, Tail).


gen_riakobjects(0, ObjectList, _TupleBuckets) ->
    ObjectList;
gen_riakobjects(Count, ObjectList, TupleBuckets) ->
    Bucket = 
        case TupleBuckets of
            true ->
                {?BUCKET_TYPE, integer_to_binary(Count rem 5)};
            false ->
                integer_to_binary(Count rem 5)
        end,
    Key = list_to_binary(string:right(integer_to_list(Count), 6, $0)),
    Value = leveled_rand:rand_bytes(512),
    MD = [{last_modified_date, os:timestamp()}, 
            {random, leveled_rand:uniform(3)}],
    Obj = #r_object{bucket = Bucket,
                    key = Key,
                    contents = [#r_content{metadata = MD, value = Value}]},
    gen_riakobjects(Count - 1, [Obj|ObjectList], TupleBuckets).


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
                    aae_exchange:reply(RPid, R, Colour)
                end,
            case Msg of 
                fetch_root ->
                    aae_controller:aae_mergeroot(Cntrl, 
                                                    Preflists, 
                                                    ReturnFun);
                {fetch_branches, BranchIDs} ->
                    aae_controller:aae_mergebranches(Cntrl, 
                                                        Preflists, 
                                                        BranchIDs, 
                                                        ReturnFun);
                {fetch_clocks, SegmentIDs} ->
                    aae_controller:aae_fetchclocks(Cntrl,
                                                        Preflists,
                                                        SegmentIDs,
                                                        ReturnFun,
                                                        null)
            end
        end,
    SendFun.

exchange_vnodesendfun(VN) ->
    fun(Msg, Preflists, Colour) ->
        RPid = self(),
        ReturnFun = 
            fun(R) -> 
                aae_exchange:reply(RPid, R, Colour)
            end,
        mock_kv_vnode:exchange_message(VN, Msg, Preflists, ReturnFun)
    end.


repair_fun(SourceList, Cntrl, NVal) ->
    Lookup = lists:map(fun({B, K, V}) -> {{B, K}, V} end, SourceList),
    RepairFun = 
        fun(BucketKeyL) ->
            FoldFun =
                fun({{B0, K0}, _VCDelta}, Acc) -> 
                    {{B0, K0}, V0} = lists:keyfind({B0, K0}, 1, Lookup),
                    [{B0, K0, V0}|Acc]
                end,
            KVL = lists:foldl(FoldFun, [], BucketKeyL),
            ok = put_keys(Cntrl, NVal, KVL)
        end,
    RepairFun.
