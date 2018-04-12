-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([dual_store_compare_medium_so/1,
            dual_store_compare_medium_ko/1,
            dual_store_compare_large_so/1,
            dual_store_compare_large_ko/1,
            mock_vnode_medium/1]).

all() -> [dual_store_compare_medium_so,
            dual_store_compare_medium_ko,
            dual_store_compare_large_so,
            dual_store_compare_large_ko,
            mock_vnode_medium].

-define(ROOT_PATH, "test/").

-record(r_content, {
                    metadata,
                    value :: term()
                    }).

-record(r_object, {
                    bucket,
                    key,
                    contents :: [#r_content{}],
                    vclock = [],
                    updatemetadata=dict:store(clean, true, dict:new()),
                    updatevalue :: term()}).


dual_store_compare_medium_so(_Config) ->
    dual_store_compare_tester(10000, leveled_so).

dual_store_compare_medium_ko(_Config) ->
    dual_store_compare_tester(10000, leveled_ko).

dual_store_compare_large_so(_Config) ->
    dual_store_compare_tester(100000, leveled_so).

dual_store_compare_large_ko(_Config) ->
    dual_store_compare_tester(100000, leveled_ko).


dual_store_compare_tester(InitialKeyCount, StoreType) ->
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
        aae_controller:aae_start({parallel, StoreType}, 
                                    {true, none}, 
                                    {1, 300}, 
                                    [{2, 0}, {2, 1}], 
                                    VnodePath1, 
                                    SplitF),
    {ok, Cntrl2} = 
        aae_controller:aae_start({parallel, StoreType}, 
                                    {true, none}, 
                                    {1, 300}, 
                                    [{3, 0}, {3, 1}, {3, 2}], 
                                    VnodePath2, 
                                    SplitF),
    
    BKVList = gen_keys([], InitialKeyCount),
    ok = put_keys(Cntrl1, 2, BKVList, none),
    ok = put_keys(Cntrl2, 3, BKVList, none),

    % Change some of the keys - cheat by using undefined rather than replace 
    % properly

    BKVListR = gen_keys([], InitialKeyCount div 10),
    ok = put_keys(Cntrl1, 2, BKVListR, undefined),
    ok = put_keys(Cntrl2, 3, BKVListR, undefined),
    
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

    {ok, _P1, GUID1} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}],
                                [{exchange_sendfun(Cntrl2), [{3, 0}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID1]),
    {ExchangeState1, 0} = start_receiver(),
    true = ExchangeState1 == root_compare,

    {ok, _P2, GUID2} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), [{3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID2]),
    {ExchangeState2, 0} = start_receiver(),
    true = ExchangeState2 == root_compare,

    {ok, _P3, GUID3} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2, 0}, {2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3]),
    {ExchangeState3, 0} = start_receiver(),
    true = ExchangeState3 == root_compare,

    {ok, _P4, GUID4} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID4]),
    {ExchangeState4, 0} = start_receiver(),
    true = ExchangeState4 == root_compare,


    % Create a discrepancy and discover it through exchange
    BKVListN = gen_keys([], InitialKeyCount + 10, InitialKeyCount),
    _SL = lists:foldl(fun({B, K, _V}, Acc) -> 
                            BK = aae_util:make_binarykey(B, K),
                            Seg = leveled_tictac:keyto_segment48(BK),
                            Seg0 = aae_keystore:generate_treesegment(Seg),
                            io:format("Generate new key B ~w K ~w " ++ 
                                        "for Segment ~w ~w ~w partition ~w ~w~n",
                                        [B, K, Seg0,  Seg0 bsr 8, Seg0 band 255, 
                                        calc_preflist(K, 2), calc_preflist(K, 3)]),
                            [Seg0|Acc]
                        end,
                        [],
                        BKVListN),
    ok = put_keys(Cntrl1, 2, BKVListN),

    {ok, _P6, GUID6} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID6]),
    {ExchangeState6, 10} = start_receiver(),
    true = ExchangeState6 == clock_compare,

    % Same again, but request a missing partition, and should get same result

    {ok, _P6a, GUID6a} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}, {3, 3}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID6a]),
    {ExchangeState6a, 10} = start_receiver(),
    true = ExchangeState6a == clock_compare,

    % Nothing repaired last time.  The deltas are all new keys though, so
    % We can repair by adding them in to the other vnode

    RepairFun0 = repair_fun(BKVListN, Cntrl2, 3),
    {ok, _P7, GUID7} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun0,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID7]),
    {ExchangeState7, 10} = start_receiver(),
    true = ExchangeState7 == clock_compare,
    
    {ok, _P8, GUID8} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID8]),
    {ExchangeState8, 0} = start_receiver(),
    true = ExchangeState8 == root_compare,



    % Shutdown and tidy up
    ok = aae_controller:aae_close(Cntrl1, none),
    ok = aae_controller:aae_close(Cntrl2, none),
    RootPath = reset_filestructure().



mock_vnode_medium(_Config) ->
    mock_vnode_tester(10000).


mock_vnode_tester(InitialKeyCount) ->
    RootPath = reset_filestructure(),
    MockPathN = filename:join(RootPath, "mock_native/"),
    MockPathP = filename:join(RootPath, "mock_parallel/"),

    IndexNs = [{1, 3}, {2, 3}, {3, 3}],
    PreflistFun = 
        fun(_B, K) ->
            Idx = erlang:phash2(K) rem length(IndexNs),
            lists:nth(Idx + 1, IndexNs)
        end,

    {ok, VNN} = mock_kv_vnode:open(MockPathN, native, IndexNs, PreflistFun),
    {ok, VNP} = mock_kv_vnode:open(MockPathP, parallel, IndexNs, null),

    RPid = self(),
    RepairFun = fun(_KL) -> null end,  
    ReturnFun = fun(R) -> RPid ! {result, R} end,

    % Exchange between empty vnodes
    {ok, _P0, GUID0} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID0]),
    {ExchangeState0, 0} = start_receiver(),
    true = ExchangeState0 == root_compare,

    ObjList = gen_riakobjects(InitialKeyCount, []),

    PutFun = 
        fun(Store1, Store2) ->
            fun(Object) ->
                PL = PreflistFun(null, Object#r_object.key),
                mock_kv_vnode:put(Store1, Object, PL, [Store2])
            end
        end,
    
    PutFun1 = PutFun(VNN, VNP),
    PutFun2 = PutFun(VNP, VNN),
    {OL1, OL2A} = lists:split(InitialKeyCount div 2, ObjList),
    {[RogueObj1, RogueObj2], OL2} = lists:split(2, OL2A),
        % Keep some rogue objects to cause failures, by not putting them
        % correctly into both vnodes
    ok = lists:foreach(PutFun1, OL1),
    ok = lists:foreach(PutFun2, OL2),
    
    % Exchange between equivalent vnodes
    {ok, _P1, GUID1} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID1]),
    {ExchangeState1, 0} = start_receiver(),
    true = ExchangeState1 == root_compare,

    % Make change to one vnode only (the parallel one)
    Idx1 = erlang:phash2(RogueObj1#r_object.key) rem length(IndexNs),
    mock_kv_vnode:put(VNP, RogueObj1, lists:nth(Idx1 + 1, IndexNs), []),

    % Exchange between nodes to expose difference
    {ok, _P2, GUID2} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID2]),
    {ExchangeState2, 1} = start_receiver(),
    true = ExchangeState2 == clock_compare,

    % Make change to one vnode only (the native one)
    Idx2 = erlang:phash2(RogueObj2#r_object.key) rem length(IndexNs),
    mock_kv_vnode:put(VNN, RogueObj2, lists:nth(Idx2 + 1, IndexNs), []),

    % Exchange between nodes to expose differences (one in VNN, one in VNP)
    {ok, _P3, GUID3} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3]),
    {ExchangeState3, 2} = start_receiver(),
    true = ExchangeState3 == clock_compare,

    ok = mock_kv_vnode:close(VNN),
    ok = mock_kv_vnode:close(VNP),
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
                                <<>>),
    put_keys(Cntrl, Nval, Tail).


gen_riakobjects(0, ObjectList) ->
    ObjectList;
gen_riakobjects(Count, ObjectList) ->
    Bucket = integer_to_binary(Count rem 5),  
    Key = list_to_binary(string:right(integer_to_list(Count), 6, $0)),
    Value = leveled_rand:rand_bytes(512),
    Obj = #r_object{bucket = Bucket,
                    key = Key,
                    contents = [#r_content{value = Value}]},
    gen_riakobjects(Count - 1, [Obj|ObjectList]).



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
                    io:format("Preparing reply to ~w for msg ~w colour ~w~n", 
                                [RPid, Msg, Colour]),
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
                io:format("Preparing reply to ~w for msg ~w colour ~w~n", 
                            [RPid, Msg, Colour]),
                aae_exchange:reply(RPid, R, Colour)
            end,
        mock_kv_vnode:exchange_message(VN, Msg, Preflists, ReturnFun)
    end.


repair_fun(SourceList, Cntrl, NVal) ->
    Lookup = lists:map(fun({B, K, V}) -> {{B, K}, V} end, SourceList),
    RepairFun = 
        fun(BucketKeyL) ->
            FoldFun =
                fun(BucketKeyT, Acc) -> 
                    {{B0, K0}, V0} = lists:keyfind(BucketKeyT, 1, Lookup),
                    [{B0, K0, V0}|Acc]
                end,
            KVL = lists:foldl(FoldFun, [], BucketKeyL),
            ok = put_keys(Cntrl, NVal, KVL)
        end,
    RepairFun.
