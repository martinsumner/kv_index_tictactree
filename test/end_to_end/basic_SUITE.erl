-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([dual_store_compare_medium_so/1,
            dual_store_compare_medium_ko/1,
            dual_store_compare_large_so/1,
            dual_store_compare_large_ko/1,
            mock_vnode_loadexchangeandrebuild/1,
            mock_vnode_coveragefold_nativemedium/1,
            mock_vnode_coveragefold_nativesmall/1,
            mock_vnode_coveragefold_parallelmedium/1]).

all() -> [dual_store_compare_medium_so,
            dual_store_compare_medium_ko,
            dual_store_compare_large_so,
            dual_store_compare_large_ko,
            mock_vnode_loadexchangeandrebuild,
            mock_vnode_coveragefold_nativemedium,
            mock_vnode_coveragefold_nativesmall,
            mock_vnode_coveragefold_parallelmedium
        ].

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
    %
    % Think of these preflists in terms of needless partitions for test 
    % purposes.  Alhtough this is a comparison between 2 'nodes', it is 
    % more like a comparison between 2 clusters where n=1, there is 1 
    % vnode, but data is still partitioned into either 2 or 3 partitions.
    % Don't rtry and make sense of this in term of a ring - the 
    % mock_vnode_coverage_fold tests have a more Riak ring-like setup.

    RootPath = reset_filestructure(),
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
    
    SW0 = os:timestamp(),

    BKVListXS = gen_keys([], InitialKeyCount),
    {BKVList, _Discard} = lists:split(20, BKVListXS),
        % The first 20 keys discarded to create an overlap between the add
        % replace list
    ok = put_keys(Cntrl1, 2, BKVList, none),
    ok = put_keys(Cntrl2, 3, lists:reverse(BKVList), none),

    {BKVListRem, _Ignore} = lists:split(10, BKVList),
    ok = remove_keys(Cntrl1, 2, BKVListRem),
    ok = remove_keys(Cntrl2, 3, BKVListRem),

    % Change all of the keys - cheat by using undefined rather than replace 
    % properly

    BKVListR = gen_keys([], 100),
        % As 100 > 20 expect 20 of these keys to be new, so no clock will be
        % returned from fetch_clock, and 80 of these will be updates
    ok = put_keys(Cntrl1, 2, BKVListR, undefined),
    ok = put_keys(Cntrl2, 3, BKVListR, undefined),
    
    io:format("Initial put complete in ~w ms~n", 
                [timer:now_diff(os:timestamp(), SW0)/1000]),
    SW1 = os:timestamp(),

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
    
    io:format("Direct partition compare complete in ~w ms~n", 
                [timer:now_diff(os:timestamp(), SW1)/1000]),
    SW2 = os:timestamp(),

    % Confirm no differences when using different matching AAE exchanges

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

    io:format("Comparison through exchange complete in ~w ms~n", 
                [timer:now_diff(os:timestamp(), SW2)/1000]),

    % Shutdown and tidy up
    ok = aae_controller:aae_close(Cntrl1),
    ok = aae_controller:aae_close(Cntrl2),
    RootPath = reset_filestructure().



mock_vnode_loadexchangeandrebuild(_Config) ->
    % Load up two vnodes with same data, with the data in each node split 
    % across 3 partitions (n=1).
    %
    % The purpose if to perform exchanges to first highlight no differences, 
    % and then once a difference is created, discover any difference 
    InitialKeyCount = 50000,
    RootPath = reset_filestructure(),
    MockPathN = filename:join(RootPath, "mock_native/"),
    MockPathP = filename:join(RootPath, "mock_parallel/"),

    IndexNs = [{1, 3}, {2, 3}, {3, 3}],
    PreflistFun = 
        fun(_B, K) ->
            Idx = erlang:phash2(K) rem length(IndexNs),
            lists:nth(Idx + 1, IndexNs)
        end,

    % Start up to two mock vnodes
    % - VNN is a native vnode (where the AAE process will not keep a parallel
    % key store)
    % - VNP is a parallel vnode (where a separate AAE key store is required 
    % to be kept in parallel)
    {ok, VNN} = mock_kv_vnode:open(MockPathN, native, IndexNs, PreflistFun),
    {ok, VNP} = mock_kv_vnode:open(MockPathP, parallel, IndexNs, PreflistFun),

    RPid = self(),
    RepairFun = 
        fun(KL) -> 
            lists:foreach(fun({{B, K}, _VCCompare}) -> 
                                io:format("Delta found in ~s ~s~n", 
                                            [binary_to_list(B), 
                                                binary_to_list(K)])
                            end,
                            KL) 
        end,  
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
    ReplaceList = gen_riakobjects(100, []), 
        % some objects to replace the first 100 objects
    DeleteList1 = lists:sublist(ObjList, 200, 100),
    DeleteList2 = 
        lists:sublist(ObjList, 400, 10) ++ 
        lists:sublist(ObjList, 500, 10) ++ 
        lists:sublist(ObjList, 600, 10),

    RehashList = lists:sublist(ObjList, 700, 10),

    PutFun = 
        fun(Store1, Store2) ->
            fun(Object) ->
                PL = PreflistFun(null, Object#r_object.key),
                mock_kv_vnode:put(Store1, Object, PL, [Store2])
            end
        end,
    DeleteFun =
        fun(Stores) ->
            fun(Object) ->
                PL = PreflistFun(null, Object#r_object.key),
                lists:foreach(
                    fun(Store) -> 
                        mock_kv_vnode:backend_delete(Store, 
                                                        Object#r_object.bucket,
                                                        Object#r_object.key,
                                                        PL)
                    end,
                    Stores)
            end
        end,
    RehashFun =
        fun(Stores) ->
            fun(Object) ->
                PL = PreflistFun(null, Object#r_object.key),
                lists:foreach(
                    fun(Store) -> 
                        mock_kv_vnode:rehash(Store, 
                                                Object#r_object.bucket,
                                                Object#r_object.key,
                                                PL)
                    end,
                    Stores)
            end
        end,
    
    % Load objects into both stores
    PutFun1 = PutFun(VNN, VNP),
    PutFun2 = PutFun(VNP, VNN),
    {OL1, OL2A} = lists:split(InitialKeyCount div 2, ObjList),
    {[RogueObj1, RogueObj2], OL2} = lists:split(2, OL2A),
        % Keep some rogue objects to cause failures, by not putting them
        % correctly into both vnodes.  These aren't loaded yet
    ok = lists:foreach(PutFun1, OL1),
    ok = lists:foreach(PutFun2, OL2),
    ok = lists:foreach(PutFun1, ReplaceList),
    ok = lists:foreach(DeleteFun([VNN, VNP]), DeleteList1),
    
    % Exchange between equivalent vnodes
    {ok, _P1, GUID1} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID1]),
    {ExchangeState1, 0} = start_receiver(),
    true = ExchangeState1 == root_compare,

    % Rehash some entries and confirm root_compare still matches, as 
    % rehash doesn't do anything
    ok = lists:foreach(RehashFun([VNN, VNP]), RehashList),
    {ok, _P1a, GUID1a} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID1a]),
    {ExchangeState1a, 0} = start_receiver(),
    true = ExchangeState1a == root_compare,

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

    {RebuildN, false} = mock_kv_vnode:rebuild(VNN, false),
    {RebuildP, false} = mock_kv_vnode:rebuild(VNP, false),
    % Discover Next rebuild times - should be in the future as both stores
    % were started empty, and hence without the need to rebuild
    io:format("Next rebuild vnn ~w vnp ~w~n", [RebuildN, RebuildP]),
    true = RebuildN > os:timestamp(),
    true = RebuildP > os:timestamp(),

    ok = mock_kv_vnode:close(VNN),
    ok = mock_kv_vnode:close(VNP),

    % Restart the vnodes, confirm next rebuilds are still in the future 
    % between startup and shutdown the next_rebuild will be rescheduled to
    % a different time, as the look at the last rebuild time and schedule 
    % forward from there.
    {ok, VNNa} = mock_kv_vnode:open(MockPathN, native, IndexNs, PreflistFun),
    {ok, VNPa} = mock_kv_vnode:open(MockPathP, parallel, IndexNs, PreflistFun),
    {RebuildNa, false} = mock_kv_vnode:rebuild(VNNa, false),
    {RebuildPa, false} = mock_kv_vnode:rebuild(VNPa, false),
    io:format("Next rebuild vnn ~w vnp ~w~n", [RebuildNa, RebuildPa]),
    true = RebuildNa > os:timestamp(),
    true = RebuildPa > os:timestamp(),

    % Exchange between nodes to expose differences (one in VNN, one in VNP)
    % Should still discover the same difference as when they were closed.
    {ok, _P3a, GUID3a} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3a]),
    {ExchangeState3a, 2} = start_receiver(),
    true = ExchangeState3a == clock_compare,

    % Prompts for a rebuild of both stores.  The rebuild is a rebuild of both
    % the store and the tree in the case of the parallel vnode, and just the
    % tree in the case of the native rebuild
    {RebuildNb, true} = mock_kv_vnode:rebuild(VNNa, true),
    
    true = RebuildNb > os:timestamp(), 
        % next rebuild was in the future, and is still scheduled as such
        % key thing that the ongoing rebuild status is now true (the second 
        % element of the rebuild response)

    % Now poll to check to see when the rebuild is complete
    wait_for_rebuild(VNNa),

    % Next rebuild times should now still be in the future
    {RebuildNc, false} = mock_kv_vnode:rebuild(VNNa, false),
    {RebuildPc, false} = mock_kv_vnode:rebuild(VNPa, false),
    true = RebuildPc == RebuildPa, % Should not have changed

    % Following a completed rebuild - the exchange should still work as 
    % before, spotting two differences
    {ok, _P3b, GUID3b} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3b]),
    {ExchangeState3b, 2} = start_receiver(),
    true = ExchangeState3b == clock_compare,

    {RebuildPb, true} = mock_kv_vnode:rebuild(VNPa, true),
    true = RebuildPb > os:timestamp(),

    % There should now be a rebuild in progress - but immediately check that 
    % an exchange will still work (spotting the same two differences as before
    % following a clock_compare)
    {ok, _P3c, GUID3c} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3c]),
    {ExchangeState3c, 2} = start_receiver(),
    true = ExchangeState3c == clock_compare,

    wait_for_rebuild(VNPa),
    {RebuildNd, false} = mock_kv_vnode:rebuild(VNNa, false),
    {RebuildPd, false} = mock_kv_vnode:rebuild(VNPa, false),
    true = RebuildNd == RebuildNc,
    true = RebuildPd > os:timestamp(),
    
    % Rebuild now complete - should get the same result for an exchange
    {ok, _P3d, GUID3d} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3d]),
    {ExchangeState3d, 2} = start_receiver(),
    true = ExchangeState3d == clock_compare,

    % Delete some keys - and see the size of the delta increase
    ok = lists:foreach(DeleteFun([VNPa]), DeleteList2),
    {ok, _P4a, GUID4a} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID4a]),
    {ExchangeState4a, 32} = start_receiver(),
    true = ExchangeState4a == clock_compare,
    % Balance the deletions
    ok = lists:foreach(DeleteFun([VNNa]), DeleteList2),
    {ok, _P4b, GUID4b} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                RepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID4b]),
    {ExchangeState4b, 2} = start_receiver(),
    true = ExchangeState4b == clock_compare,

    % Shutdown and clear down files
    ok = mock_kv_vnode:close(VNNa),
    ok = mock_kv_vnode:close(VNPa),
    RootPath = reset_filestructure().


wait_for_rebuild(Vnode) ->
    RebuildComplete = 
        lists:foldl(fun(Wait, Complete) ->
                            case Complete of 
                                true ->
                                    true;
                                false ->
                                    timer:sleep(Wait),
                                    {_TSN, RSN} = 
                                        mock_kv_vnode:rebuild(Vnode, false),
                                    % Waiting for rebuild status to be false 
                                    % on both vnodes, which would indicate
                                    % that both rebuilds have completed
                                    (not RSN)
                            end
                        end,
                        false,
                        [1000, 2000, 3000, 5000, 8000, 13000, 21000]),
    
    % Both rebuilds have completed
    true = RebuildComplete == true.


mock_vnode_coveragefold_nativemedium(_Config) ->
    mock_vnode_coveragefolder(native, 50000).

mock_vnode_coveragefold_nativesmall(_Config) ->
    mock_vnode_coveragefolder(native, 1000).

mock_vnode_coveragefold_parallelmedium(_Config) ->
    mock_vnode_coveragefolder(parallel, 50000).



mock_vnode_coveragefolder(Type, InitialKeyCount) ->
    % This should load a set of 4 vnodes, with the data partitioned across the
    % vnodes n=2 to provide for 2 different coverage plans.
    %
    % After the load, an exchange cna confirm consistency between the coverage 
    % plans.  Then run some folds to make sure that the folds produce the 
    % expected results 
    RootPath = reset_filestructure(),
    MockPathN1 = filename:join(RootPath, "mock_native1/"),
    MockPathN2 = filename:join(RootPath, "mock_native2/"),
    MockPathN3 = filename:join(RootPath, "mock_native3/"),
    MockPathN4 = filename:join(RootPath, "mock_native4/"),
    
    IndexNs = 
        [{1, 2}, {2, 2}, {3, 2}, {0, 2}],
    PreflistFun = 
        fun(_B, K) ->
            Idx = erlang:phash2(K) rem length(IndexNs),
            lists:nth(Idx + 1, IndexNs)
        end,

    % Open four vnodes to take two of the rpeflists each
    % - this isintended to replicate a ring-size=4, n-val=2 ring 
    {ok, VNN1} = 
        mock_kv_vnode:open(MockPathN1, Type, [{1, 2}, {0, 2}], PreflistFun),
    {ok, VNN2} = 
        mock_kv_vnode:open(MockPathN2, Type, [{2, 2}, {1, 2}], PreflistFun),
    {ok, VNN3} = 
        mock_kv_vnode:open(MockPathN3, Type, [{3, 2}, {2, 2}], PreflistFun),
    {ok, VNN4} = 
        mock_kv_vnode:open(MockPathN4, Type, [{0, 2}, {3, 2}], PreflistFun),

    % Mapping of preflists to [Primary, Secondary] vnodes
    RingN =
        [{{1, 2}, [VNN1, VNN2]}, {{2, 2}, [VNN2, VNN3]}, 
            {{3, 2}, [VNN3, VNN4]}, {{0, 2}, [VNN4, VNN1]}],

    % Add each key to the vnode at the head of the preflist, and then push the
    % change to the one at the tail.  
    PutFun = 
        fun(Ring) ->
            fun(Object) ->
                PL = PreflistFun(null, Object#r_object.key),
                {PL, [Primary, Secondary]} = lists:keyfind(PL, 1, Ring),
                mock_kv_vnode:put(Primary, Object, PL, [Secondary])
            end
        end,

    ObjList = gen_riakobjects(InitialKeyCount, []),
    ok = lists:foreach(PutFun(RingN), ObjList),

    % Provide two coverage plans, equivalent to normal ring coverage plans
    % with offset=0 and offset=1
    AllPrimariesMap =
        fun({IndexN, [Pri, _FB]}) ->
            {exchange_vnodesendfun(Pri), [IndexN]}
        end,
    AllSecondariesMap =
        fun({IndexN, [_Pri, FB]}) ->
            {exchange_vnodesendfun(FB), [IndexN]}
        end,
    AllPrimaries = lists:map(AllPrimariesMap, RingN),
    AllSecondaries = lists:map(AllSecondariesMap, RingN),

    RPid = self(),
    RepairFun = fun(_KL) -> null end,  
    ReturnFun = fun(R) -> RPid ! {result, R} end,

    {ok, _P1, GUID1} = 
        aae_exchange:start(AllPrimaries, AllSecondaries, RepairFun, ReturnFun),
    io:format("Exchange id ~s~n", [GUID1]),
    {ExchangeState1, 0} = start_receiver(),
    true = ExchangeState1 == root_compare,


    % Fold over a valid coverage plan to find siblings (there are none) 
    SibCountFoldFun =
        fun(B, K, V, {NoSibAcc, SibAcc}) ->
            {sibcount, SC} = lists:keyfind(sibcount, 1, V),
            case SC of 
                1 -> {NoSibAcc + 1, SibAcc};
                _ -> {NoSibAcc, [{B, K}|SibAcc]}
            end
        end,
    
    SWF1 = os:timestamp(),
    {async, Folder1} = 
        mock_kv_vnode:fold_aae(VNN1, all, SibCountFoldFun, 
                                {0, []}, [{sibcount, null}]),
    {async, Folder3} = 
        mock_kv_vnode:fold_aae(VNN3, all, SibCountFoldFun, 
                                Folder1(), [{sibcount, null}]),
    {SC1, SibL1} = Folder3(),
    io:format("Coverage fold took ~w with output ~w for store ~w~n", 
                [timer:now_diff(os:timestamp(), SWF1),SC1, Type]),
    true = SC1 == InitialKeyCount,
    true = [] == SibL1,

    SWF2 = os:timestamp(),
    BucketListA = [integer_to_binary(0), integer_to_binary(1)],
    {async, Folder2} = 
        mock_kv_vnode:fold_aae(VNN2, {buckets, BucketListA}, SibCountFoldFun, 
                                {0, []}, [{sibcount, null}]),
    {async, Folder4} = 
        mock_kv_vnode:fold_aae(VNN4, {buckets, BucketListA}, SibCountFoldFun, 
                                Folder2(), [{sibcount, null}]),
    {SC2, SibL2} = Folder4(),
    io:format("Coverage fold took ~w with output ~w for store ~w~n", 
                [timer:now_diff(os:timestamp(), SWF2),SC2, Type]),
    true = SC2 == 2 * (InitialKeyCount div 5),
    true = [] == SibL2,

    % A fold over two coverage plans to compare the list of {B, K, H, Sz} 
    % tuples found within the coverage plans
    HashSizeFoldFun =
        fun(B, K, V, Acc) ->
            {hash, H} = lists:keyfind(hash, 1, V),
            {size, Sz} = lists:keyfind(size, 1, V),
            [{B, K, H, Sz}|Acc]
        end,

    {async, Folder1HS} = 
        mock_kv_vnode:fold_aae(VNN1, all, HashSizeFoldFun, 
                                [], [{hash, null}, {size, null}]),
    {async, Folder3HS} = 
        mock_kv_vnode:fold_aae(VNN3, all, HashSizeFoldFun, 
                                Folder1HS(), [{hash, null}, {size, null}]),
    BKHSzL1 = Folder3HS(),
    true = length(BKHSzL1) == InitialKeyCount,

    {async, Folder2HS} = 
        mock_kv_vnode:fold_aae(VNN2, all, HashSizeFoldFun, 
                                [], [{hash, null}, {size, null}]),
    {async, Folder4HS} = 
        mock_kv_vnode:fold_aae(VNN4, all, HashSizeFoldFun, 
                                Folder2HS(), [{hash, null}, {size, null}]),
    BKHSzL2 = Folder4HS(),
    true = length(BKHSzL2) == InitialKeyCount,

    true = lists:usort(BKHSzL2) == lists:usort(BKHSzL1),

    ok = mock_kv_vnode:close(VNN1),
    ok = mock_kv_vnode:close(VNN2),
    ok = mock_kv_vnode:close(VNN3),
    ok = mock_kv_vnode:close(VNN4),
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
                                <<>>),
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
                fun({{B0, K0}, _VCDelta}, Acc) -> 
                    {{B0, K0}, V0} = lists:keyfind({B0, K0}, 1, Lookup),
                    [{B0, K0, V0}|Acc]
                end,
            KVL = lists:foldl(FoldFun, [], BucketKeyL),
            ok = put_keys(Cntrl, NVal, KVL)
        end,
    RepairFun.
