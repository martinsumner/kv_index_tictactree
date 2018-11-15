-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([dual_store_compare_medium_so/1,
            dual_store_compare_medium_ko/1,
            dual_store_compare_large_so/1,
            dual_store_compare_large_ko/1]).

all() -> [dual_store_compare_medium_so,
            dual_store_compare_medium_ko,
            dual_store_compare_large_so,
            dual_store_compare_large_ko
        ].
    

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
    %
    % Think of these preflists in terms of needless partitions for test 
    % purposes.  Alhtough this is a comparison between 2 'nodes', it is 
    % more like a comparison between 2 clusters where n=1, there is 1 
    % vnode, but data is still partitioned into either 2 or 3 partitions.
    % Don't rtry and make sense of this in term of a ring - the 
    % mock_vnode_coverage_fold tests have a more Riak ring-like setup.

    RootPath = testutil:reset_filestructure(),
    VnodePath1 = filename:join(RootPath, "vnode1/"),
    VnodePath2 = filename:join(RootPath, "vnode2/"),
    SplitF = fun(_X) -> {leveled_rand:uniform(1000), 1, 0, null} end,
    RPid = self(),
    ReturnFun = fun(R) -> RPid ! {result, R} end,
    RepairFun = fun(_KL) -> null end,  

    {ok, Cntrl1} = 
        aae_controller:aae_start({parallel, StoreType}, 
                                    true, 
                                    {1, 300}, 
                                    [{2, 0}, {2, 1}], 
                                    VnodePath1, 
                                    SplitF),
    {ok, Cntrl2} = 
        aae_controller:aae_start({parallel, StoreType}, 
                                    true, 
                                    {1, 300}, 
                                    [{3, 0}, {3, 1}, {3, 2}], 
                                    VnodePath2, 
                                    SplitF),
    
    initial_load(InitialKeyCount, Cntrl1, Cntrl2),

    SW1 = os:timestamp(),

    ok = aae_controller:aae_mergeroot(Cntrl1, 
                                        [{2, 0}, {2, 1}], 
                                        ReturnFun),
    Root1A = testutil:start_receiver(),
    ok = aae_controller:aae_mergeroot(Cntrl2, 
                                        [{3, 0}, {3, 1}, {3, 2}], 
                                        ReturnFun),
    Root2A = testutil:start_receiver(),
    true = Root1A == Root2A,

    ok = aae_controller:aae_fetchroot(Cntrl1, 
                                        [{2, 0}], 
                                        ReturnFun),
    [{{2, 0}, Root1B}] = testutil:start_receiver(),
    ok = aae_controller:aae_fetchroot(Cntrl2, 
                                        [{3, 0}], 
                                        ReturnFun),
    [{{3, 0}, Root2B}] = testutil:start_receiver(),
    true = Root1B == Root2B,

    ok = aae_controller:aae_mergeroot(Cntrl1, 
                                        [{2, 1}], 
                                        ReturnFun),
    Root1C = testutil:start_receiver(),
    ok = aae_controller:aae_mergeroot(Cntrl2, 
                                        [{3, 1}, {3, 2}], 
                                        ReturnFun),
    Root2C = testutil:start_receiver(),
    true = Root1C == Root2C,
    
    io:format("Direct partition compare complete in ~w ms~n", 
                [timer:now_diff(os:timestamp(), SW1)/1000]),

    % Now do a comparison based based on some key range queries:
    SW2 = os:timestamp(),
    Bucket = integer_to_binary(1),
    StartKey = list_to_binary(string:right(integer_to_list(10), 6, $0)),
    EndKey = list_to_binary(string:right(integer_to_list(50), 6, $0)),
    Elements = [{sibcount, null}],
    SCFoldFun = 
        fun(FB, FK, FV, {FAccKL, FAccSc}) ->
            {sibcount, FSc} = lists:keyfind(sibcount, 1, FV),
            true = FB == Bucket,
            true = FK >= StartKey,
            true = FK < EndKey,
            {[FK|FAccKL], FAccSc + FSc}
        end,
    SCInitAcc = {[], 0},

    {async, SCFolder1} = 
        aae_controller:aae_fold(Cntrl1, 
                                {key_range, Bucket, StartKey, EndKey},
                                all,
                                SCFoldFun, 
                                SCInitAcc,
                                Elements),
    {async, SCFolder2} = 
        aae_controller:aae_fold(Cntrl2, 
                                {key_range, Bucket, StartKey, EndKey},
                                all,
                                SCFoldFun, 
                                SCInitAcc,
                                Elements),
    SCF1 = SCFolder1(),
    SCF2 = SCFolder2(),

    true = SCF1 == SCF2,
    true = element(2, SCF1) == 8,
    true = length(element(1, SCF1)) == 8,
    io:format("Comparison through key range folder in ~w ms with results ~w~n", 
                [timer:now_diff(os:timestamp(), SW2)/1000, SCF1]),


    % Confirm no differences when using different matching AAE exchanges
    SW3 = os:timestamp(),

    {ok, _P1, GUID1} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}],
                                [{exchange_sendfun(Cntrl2), [{3, 0}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID1]),
    {ExchangeState1, 0} = testutil:start_receiver(),
    true = ExchangeState1 == root_compare,

    {ok, _P2, GUID2} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), [{3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID2]),
    {ExchangeState2, 0} = testutil:start_receiver(),
    true = ExchangeState2 == root_compare,

    {ok, _P3, GUID3} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2, 0}, {2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3]),
    {ExchangeState3, 0} = testutil:start_receiver(),
    true = ExchangeState3 == root_compare,

    {ok, _P4, GUID4} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID4]),
    {ExchangeState4, 0} = testutil:start_receiver(),
    true = ExchangeState4 == root_compare,

    BKVListN = create_discrepancy(Cntrl1, InitialKeyCount),

    {ok, _P6, GUID6} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID6]),
    {ExchangeState6, 10} = testutil:start_receiver(),
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
    {ExchangeState6a, 10} = testutil:start_receiver(),
    true = ExchangeState6a == clock_compare,

    % Nothing repaired last time.  The deltas are all new keys though, so
    % We can repair by adding them in to the other vnode

    RepairFun0 = testutil:repair_fun(BKVListN, Cntrl2, 3),
    {ok, _P7, GUID7} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun0,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID7]),
    {ExchangeState7, 10} = testutil:start_receiver(),
    true = ExchangeState7 == clock_compare,
    
    {ok, _P8, GUID8} = 
        aae_exchange:start([{exchange_sendfun(Cntrl1), [{2,0}]}, 
                                    {exchange_sendfun(Cntrl1), [{2,1}]}],
                                [{exchange_sendfun(Cntrl2), 
                                    [{3, 0}, {3, 1}, {3, 2}]}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID8]),
    {ExchangeState8, 0} = testutil:start_receiver(),
    true = ExchangeState8 == root_compare,

    io:format("Comparison through exchange complete in ~w ms~n", 
                [timer:now_diff(os:timestamp(), SW3)/1000]),

    % Shutdown and tidy up
    ok = aae_controller:aae_close(Cntrl1),
    ok = aae_controller:aae_close(Cntrl2),
    RootPath = testutil:reset_filestructure().


initial_load(InitialKeyCount, Cntrl1, Cntrl2) ->

    SW0 = os:timestamp(),

    BKVListXS = testutil:gen_keys([], InitialKeyCount),
    {BKVList, _Discard} = lists:split(20, BKVListXS),
        % The first 20 keys discarded to create an overlap between the add
        % replace list
    ok = testutil:put_keys(Cntrl1, 2, BKVList, none),
    ok = testutil:put_keys(Cntrl2, 3, lists:reverse(BKVList), none),

    {BKVListRem, _Ignore} = lists:split(10, BKVList),
    ok = testutil:remove_keys(Cntrl1, 2, BKVListRem),
    ok = testutil:remove_keys(Cntrl2, 3, BKVListRem),

    % Change all of the keys - cheat by using undefined rather than replace 
    % properly

    BKVListR = testutil:gen_keys([], 100),
        % As 100 > 20 expect 20 of these keys to be new, so no clock will be
        % returned from fetch_clock, and 80 of these will be updates
    ok = testutil:put_keys(Cntrl1, 2, BKVListR, undefined),
    ok = testutil:put_keys(Cntrl2, 3, BKVListR, undefined),
    
    io:format("Initial put complete in ~w ms~n", 
                [timer:now_diff(os:timestamp(), SW0)/1000]).
    

create_discrepancy(Cntrl, InitialKeyCount) ->
    % Create a discrepancy and discover it through exchange
    BKVListN = testutil:gen_keys([], InitialKeyCount + 10, InitialKeyCount),
    _SL = lists:foldl(fun({B, K, _V}, Acc) -> 
                            BK = aae_util:make_binarykey(B, K),
                            Seg = leveled_tictac:keyto_segment48(BK),
                            Seg0 = aae_keystore:generate_treesegment(Seg),
                            io:format("Generate new key B ~w K ~w " ++ 
                                    "for Segment ~w ~w ~w partition ~w ~w~n",
                                    [B, K, Seg0,  Seg0 bsr 8, Seg0 band 255, 
                                        testutil:calc_preflist(K, 2), 
                                        testutil:calc_preflist(K, 3)]),
                            [Seg0|Acc]
                        end,
                        [],
                        BKVListN),
    ok = testutil:put_keys(Cntrl, 2, BKVListN),
    BKVListN.


exchange_sendfun(Cntrl) -> testutil:exchange_sendfun(Cntrl).