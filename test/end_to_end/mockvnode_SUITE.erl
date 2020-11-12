-module(mockvnode_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([coveragefold_nativemedium/1,
            coveragefold_nativesmall/1,
            coveragefold_parallelmedium/1,
            coveragefold_parallelmediumko/1,
            coveragefold_parallelsmall/1,
            loadexchangeandrebuild_stbucketko/1,
            loadexchangeandrebuild_tuplebucketko/1,
            loadexchangeandrebuild_stbucketso/1,
            loadexchangeandrebuild_tuplebucketso/1]).

all() -> [
            coveragefold_nativemedium,
            coveragefold_nativesmall,
            coveragefold_parallelmedium,
            coveragefold_parallelmediumko,
            coveragefold_parallelsmall,
            loadexchangeandrebuild_stbucketso,
            loadexchangeandrebuild_tuplebucketso,
            loadexchangeandrebuild_stbucketko,
            loadexchangeandrebuild_tuplebucketko
        ].


-include("testutil.hrl").


loadexchangeandrebuild_stbucketko(_Config) ->
    mock_vnode_loadexchangeandrebuild_tester(false, parallel_ko),
    mock_vnode_loadexchangeandrebuild_tester(false, parallel_ko).

loadexchangeandrebuild_stbucketso(_Config) ->
    mock_vnode_loadexchangeandrebuild_tester(false, parallel_so),
    mock_vnode_loadexchangeandrebuild_tester(false, parallel_so).

loadexchangeandrebuild_tuplebucketko(_Config) ->
    mock_vnode_loadexchangeandrebuild_tester(true, parallel_ko),
    mock_vnode_loadexchangeandrebuild_tester(true, parallel_ko).

loadexchangeandrebuild_tuplebucketso(_Config) ->
    mock_vnode_loadexchangeandrebuild_tester(true, parallel_so),
    mock_vnode_loadexchangeandrebuild_tester(true, parallel_so).

mock_vnode_loadexchangeandrebuild_tester(TupleBuckets, PType) ->
    % Load up two vnodes with same data, with the data in each node split 
    % across 3 partitions (n=1).
    %
    % The purpose if to perform exchanges to first highlight no differences, 
    % and then once a difference is created, discover any difference 
    _TestStartPoint = os:timestamp(),
    InitialKeyCount = 80000,
    RootPath = testutil:reset_filestructure(),
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
    {ok, VNP} = mock_kv_vnode:open(MockPathP, PType, IndexNs, PreflistFun),

    RPid = self(),
    LogNotRepairFun = 
        fun(KL) -> 
            lists:foreach(fun({{B, K}, VCCompare}) -> 
                                io:format("Delta found in ~w ~s ~w~n",
                                            [B,
                                                binary_to_list(K),
                                                VCCompare])
                            end,
                            KL) 
        end,
    NullRepairFun = fun(_KL) -> ok end, 
    ReturnFun = fun(R) -> RPid ! {result, R} end,

    io:format("Exchange between empty vnodes~n"),
    {ok, _P0, GUID0} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                LogNotRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID0]),
    {ExchangeState0, 0} = testutil:start_receiver(),
    true = ExchangeState0 == root_compare,

    io:format("Same exchange - now using tree compare~n"),
    GetBucketFun = 
        fun(I) ->
            case TupleBuckets of
                true ->
                    {?BUCKET_TYPE, integer_to_binary(I)};
                false ->
                    integer_to_binary(I)
            end
        end,
    Bucket1 = GetBucketFun(1),
    Bucket2 = GetBucketFun(2),
    Bucket3 = GetBucketFun(3), 
    Bucket4 = GetBucketFun(4),

    {ok, _TC_P0, TC_GUID0} = 
        aae_exchange:start(partial,
                                [{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                LogNotRepairFun,
                                ReturnFun,
                                {filter, Bucket3, all, 
                                    small, all, all, prehash},
                                [{transition_pause_ms, 100},
                                    {log_levels, [warn, error, critical]}]),
    io:format("Exchange id for tree compare ~s~n", [TC_GUID0]),
    {ExchangeStateTC0, 0} = testutil:start_receiver(),
    true = ExchangeStateTC0 == tree_compare,
    {ok, _TC_P1, TC_GUID1} = 
        aae_exchange:start(partial,
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                [{exchange_vnodesendfun(VNN), IndexNs}],
                                LogNotRepairFun,
                                ReturnFun,
                                {filter, Bucket3, all, 
                                    small, all, all, prehash},
                                [{transition_pause_ms, 100}]),
    io:format("Exchange id for tree compare ~s~n", [TC_GUID1]),
    {ExchangeStateTC1, 0} = testutil:start_receiver(),
    true = ExchangeStateTC1 == tree_compare,

    ObjList = testutil:gen_riakobjects(InitialKeyCount, [], TupleBuckets),
    ReplaceList = testutil:gen_riakobjects(100, [], TupleBuckets), 
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
    
    io:format("Load objects into both stores~n"),
    PutFun1 = PutFun(VNN, VNP),
    PutFun2 = PutFun(VNP, VNN),
    {OL1, OL2A} = lists:split(InitialKeyCount div 2, ObjList),
    {[RogueObjC1, RogueObjC2], OL2} = lists:split(2, OL2A),
        % Keep some rogue objects to cause failures, by not putting them
        % correctly into both vnodes.  These aren't loaded yet
    RogueObj1 = RogueObjC1#r_object{bucket = Bucket1},
    RogueObj2 = RogueObjC2#r_object{bucket = Bucket2},
    ok = lists:foreach(PutFun1, OL1),
    ok = lists:foreach(PutFun2, OL2),
    ok = lists:foreach(PutFun1, ReplaceList),
    ok = lists:foreach(DeleteFun([VNN, VNP]), DeleteList1),
    
    io:format("Exchange between equivalent vnodes~n"),
    {ok, _P1, GUID1} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                LogNotRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID1]),
    {ExchangeState1, 0} = testutil:start_receiver(),
    true = ExchangeState1 == root_compare,

    io:format("Rehash some entries and confirm root_compare " ++ 
                "still matches, as rehash doesn't do anything~n"),
    ok = lists:foreach(RehashFun([VNN, VNP]), RehashList),
    {ok, _P1a, GUID1a} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                LogNotRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID1a]),
    {ExchangeState1a, 0} = testutil:start_receiver(),
    true = ExchangeState1a == root_compare,

    io:format("Compare the two stores using an AAE fold - " ++
                "and prove that AAE fold is working as expected~n"),
    Bucket = 
        case TupleBuckets of
            true ->
                {?BUCKET_TYPE, integer_to_binary(3)};
            false ->
                 integer_to_binary(3)
        end,
    StartKey = list_to_binary(string:right(integer_to_list(10), 6, $0)),
    EndKey = list_to_binary(string:right(integer_to_list(50), 6, $0)),
    Elements = [{sibcount, null}, {clock, null}, {hash, null}],
    InitAcc = {[], 0},
    FoldKRFun = 
        fun(FB, FK, FEs, {KCHAcc, SCAcc}) ->
            true = FB == Bucket,
            true = FK >= StartKey,
            true = FK < EndKey,
            {clock, FC} = lists:keyfind(clock, 1, FEs),
            {hash, FH} = lists:keyfind(hash, 1, FEs),
            {sibcount, FSC} = lists:keyfind(sibcount, 1, FEs),
            {lists:usort([{FK, FC, FH}|KCHAcc]), SCAcc + FSC}
        end,
    
    {async, VNNF} = 
        mock_kv_vnode:fold_aae(VNN, 
                                {key_range, Bucket, StartKey, EndKey},
                                all,
                                FoldKRFun, 
                                InitAcc,
                                Elements),
    {async, VNPF} = 
        mock_kv_vnode:fold_aae(VNP, 
                                {key_range, Bucket, StartKey, EndKey},
                                all,
                                FoldKRFun, 
                                InitAcc,
                                Elements),
    
    {VNNF_KL, VNNF_SC} = VNNF(),
    {VNPF_KL, VNPF_SC} = VNPF(),
    true = VNNF_SC == 8,
    true = VNPF_SC == 8,
    true = lists:usort(VNNF_KL) == lists:usort(VNPF_KL),
    true = length(VNNF_KL) == 8,
    true = length(VNPF_KL) == 8,
    
    [{K1, C1, H1}|Rest] = VNNF_KL,
    [{K2, C2, H2}|_Rest] = Rest,
    BinaryKey1 = aae_util:make_binarykey(Bucket, K1),
    BinaryKey2 = aae_util:make_binarykey(Bucket, K2),
    SegmentID1 = 
        leveled_tictac:get_segment(
            element(1, leveled_tictac:tictac_hash(BinaryKey1, <<>>)), 
            small),
    SegmentID2 = 
        leveled_tictac:get_segment(
            element(1, leveled_tictac:tictac_hash(BinaryKey2, <<>>)), 
            small),
    io:format("Looking for Segment IDs K1 ~w ~w K2 ~w ~w~n",
                [K1, SegmentID1, K2, SegmentID2]),

    {async, VNNF_SL} = 
        mock_kv_vnode:fold_aae(VNN, 
                                {key_range, Bucket, StartKey, EndKey},
                                {segments, [SegmentID1, SegmentID2], small},
                                FoldKRFun, 
                                InitAcc,
                                Elements),
    {async, VNPF_SL} = 
        mock_kv_vnode:fold_aae(VNP, 
                                {key_range, Bucket, StartKey, EndKey},
                                {segments, [SegmentID1, SegmentID2], small},
                                FoldKRFun, 
                                InitAcc,
                                Elements),
    {[{K1, C1, H1}, {K2, C2, H2}], 2} = VNNF_SL(),
    {[{K1, C1, H1}, {K2, C2, H2}], 2} = VNPF_SL(),

    io:format("Make change to one vnode only (the parallel one)~n"),
    Idx1 = erlang:phash2(RogueObj1#r_object.key) rem length(IndexNs),
    mock_kv_vnode:put(VNP, RogueObj1, lists:nth(Idx1 + 1, IndexNs), []),

    io:format("Exchange between nodes to expose difference~n"),
    {ok, _P2, GUID2} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                NullRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID2]),
    {ExchangeState2, 1} = testutil:start_receiver(),
    true = ExchangeState2 == clock_compare,

    io:format("Make change to one vnode only (the native one)~n"),
    Idx2 = erlang:phash2(RogueObj2#r_object.key) rem length(IndexNs),
    mock_kv_vnode:put(VNN, RogueObj2, lists:nth(Idx2 + 1, IndexNs), []),

    io:format("Exchange between nodes to expose differences" ++ 
                "(one in VNN, one in VNP)~n"),
    {ok, _P3, GUID3} = 
        aae_exchange:start([{exchange_vnodesendfun(VNN), IndexNs}],
                                [{exchange_vnodesendfun(VNP), IndexNs}],
                                NullRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3]),
    {ExchangeState3, 2} = testutil:start_receiver(),
    true = ExchangeState3 == clock_compare,

    {RebuildN, false} = mock_kv_vnode:rebuild(VNN, false),
    {RebuildP, false} = mock_kv_vnode:rebuild(VNP, false),
    
    io:format("Discover Next rebuild times - should be in the future " ++
                "as both stores were started empty, and hence without " ++
                "the need to rebuild~n"),
    io:format("Next rebuild vnn ~w vnp ~w~n", [RebuildN, RebuildP]),
    true = RebuildN > os:timestamp(),
    true = RebuildP > os:timestamp(),

    ok = mock_kv_vnode:close(VNN),
    ok = mock_kv_vnode:close(VNP),

    io:format("Restart the vnodes, " ++ 
                "confirm next rebuilds are still in the future~n"),
    % Between startup and shutdown the next_rebuild will be rescheduled to
    % a different time, as the look at the last rebuild time and schedule 
    % forward from there.
    {ok, VNNa} = mock_kv_vnode:open(MockPathN, native, IndexNs, PreflistFun),
    {ok, VNPa} = mock_kv_vnode:open(MockPathP, PType, IndexNs, PreflistFun),
    {RebuildNa, false} = mock_kv_vnode:rebuild(VNNa, false),
    {RebuildPa, false} = mock_kv_vnode:rebuild(VNPa, false),
    io:format("Next rebuild vnn ~w vnp ~w~n", [RebuildNa, RebuildPa]),
    true = RebuildNa > os:timestamp(),
    true = RebuildPa > os:timestamp(),

    % Exchange between nodes to expose differences (one in VNN, one in VNP)
    io:format("Should still discover the same difference " ++ 
                "as when they were close~n"),
    {ok, _P3a, GUID3a} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                NullRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3a]),
    {ExchangeState3a, 2} = testutil:start_receiver(),
    true = ExchangeState3a == clock_compare,

    % Exchange with a one hour modified range - should see same differences
    io:format("Repeat exchange with 1 hour modified range~n"),
    MRH = convert_ts(os:timestamp()),
    {ok, _P3mr1, GUID3mr1} = 
        aae_exchange:start(full,
                                [{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                NullRepairFun,
                                ReturnFun,
                                {filter, all, all, large, all,
                                    {MRH - (60 * 60), MRH},
                                    pre_hash},
                                []),
    io:format("Exchange id ~s~n", [GUID3mr1]),
    {ExchangeState3mr1, 2} = testutil:start_receiver(),
    true = ExchangeState3mr1 == clock_compare,

    % Exchnage with an older modified range - see clock_compare but no
    % differences
    io:format("Repeat exchange, but with change outside of modified range~n"),
    {ok, _P3mr2, GUID3mr2} = 
        aae_exchange:start(full,
                                [{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                NullRepairFun,
                                ReturnFun,
                                {filter, all, all, large, all,
                                    {MRH - (2 * 60 * 60), MRH - (60 * 60)},
                                    pre_hash},
                                []),
    io:format("Exchange id ~s~n", [GUID3mr2]),
    {ExchangeState3mr2, 0} = testutil:start_receiver(),
    true = ExchangeState3mr2 == clock_compare,

    io:format("Prompts for a rebuild of both stores~n"),
    % The rebuild is a rebuild of both
    % the store and the tree in the case of the parallel vnode, and just the
    % tree in the case of the native rebuild
    {RebuildNb, true} = mock_kv_vnode:rebuild(VNNa, true),
    
    true = RebuildNb > os:timestamp(), 
        % next rebuild was in the future, and is still scheduled as such
        % key thing that the ongoing rebuild status is now true (the second 
        % element of the rebuild response)

    io:format("Now poll to check to see when the rebuild is complete~n"),
    wait_for_rebuild(VNNa),

    % Next rebuild times should now still be in the future
    {RebuildNc, false} = mock_kv_vnode:rebuild(VNNa, false),
    {RebuildPc, false} = mock_kv_vnode:rebuild(VNPa, false),
    true = RebuildPc == RebuildPa, % Should not have changed

    io:format("Following a completed rebuild - the exchange should still" ++
                " work as  before, spotting two differences~n"),
    {ok, _P3b, GUID3b} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                NullRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3b]),
    {ExchangeState3b, 2} = testutil:start_receiver(),
    true = ExchangeState3b == clock_compare,

    {RebuildPb, true} = mock_kv_vnode:rebuild(VNPa, true),
    true = RebuildPb > os:timestamp(),

    io:format("Rebuild in progress, exchange still working~n"),
    % There should now be a rebuild in progress - but immediately check that 
    % an exchange will still work (spotting the same two differences as before
    % following a clock_compare)
    {ok, _P3c, GUID3c} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                NullRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3c]),
    % This could receive {error, 0}.  On complete of rebuild the leveled
    % store is shutdown - and by design this closes all iterators.  So this
    % may crash if in the fetch_clock state
    {ExchangeState3c, 2} = 
        case testutil:start_receiver() of
            {error, 0} ->
                aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                    [{exchange_vnodesendfun(VNPa), IndexNs}],
                                    NullRepairFun,
                                    ReturnFun),
                % Retry in the case this exchange times out on the rebuild
                % completing faster than expected
                testutil:start_receiver();
            Other ->
                Other
        end,
    true = ExchangeState3c == clock_compare,

    io:format("Waiting for rebuild after exchange success~n"),
    wait_for_rebuild(VNPa),
    {RebuildNd, false} = mock_kv_vnode:rebuild(VNNa, false),
    {RebuildPd, false} = mock_kv_vnode:rebuild(VNPa, false),
    true = RebuildNd == RebuildNc,
    true = RebuildPd > os:timestamp(),
    
    io:format("Rebuild now complete - " ++ 
                "should get the same result for an exchange~n"),
    {ok, _P3d, GUID3d} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                NullRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID3d]),
    {ExchangeState3d, 2} = testutil:start_receiver(),
    true = ExchangeState3d == clock_compare,

    io:format("Delete some keys - and see the size of the delta increase~n"),
    ok = lists:foreach(DeleteFun([VNPa]), DeleteList2),
    {ok, _P4a, GUID4a} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                NullRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID4a]),
    {ExchangeState4a, 32} = testutil:start_receiver(),
    true = ExchangeState4a == clock_compare,
    % Balance the deletions
    ok = lists:foreach(DeleteFun([VNNa]), DeleteList2),
    {ok, _P4b, GUID4b} = 
        aae_exchange:start([{exchange_vnodesendfun(VNNa), IndexNs}],
                                [{exchange_vnodesendfun(VNPa), IndexNs}],
                                NullRepairFun,
                                ReturnFun),
    io:format("Exchange id ~s~n", [GUID4b]),
    {ExchangeState4b, 2} = testutil:start_receiver(),
    true = ExchangeState4b == clock_compare,

    io:format("Same exchange - now using tree compare~n"),
    CheckBucketList = [Bucket1, Bucket2],
    CheckBucketFun = 
        fun(CheckBucket, Acc) ->
            CBFilters = {filter, CheckBucket, all, small, all, all, prehash},
            {ok, _TCCB_P, TCCB_GUID} = 
                aae_exchange:start(partial,
                                    [{exchange_vnodesendfun(VNNa), IndexNs}],
                                    [{exchange_vnodesendfun(VNPa), IndexNs}],
                                    NullRepairFun,
                                    ReturnFun,
                                    CBFilters,
                                    []),
            io:format("Exchange id for tree compare ~s~n", [TCCB_GUID]),
            {ExchangeStateTCCB, CBN} = testutil:start_receiver(),
            true = ExchangeStateTCCB == clock_compare,
            io:format("~w differences found in bucket ~w~n", 
                        [CBN, CheckBucket]),
            Acc + CBN
        end,
    true = 2 == lists:foldl(CheckBucketFun, 0, CheckBucketList),


    io:format("Tree compare section - with large deltas~n"),
    % Next section is going to test tree_compare with large deltas, and
    % with a genuine repair fun (one which actually repairs).  Repairs
    % should happen in stages as the mismatched segment list will at first
    % be too large - so there will be a down selection in select_ids
    %
    % A delta between buckets is created both ways, with bucket3 out of
    % sync one way, and bucket 4 out of sync th eother way

    true = InitialKeyCount > 2000,
    RplObjListTC = testutil:gen_riakobjects(2000, [], TupleBuckets),
    FilterBucket3Fun = fun(RObj) -> RObj#r_object.bucket == Bucket3 end,
    FilterBucket4Fun = fun(RObj) -> RObj#r_object.bucket == Bucket4 end,
    RplObjListTC3 = lists:filter(FilterBucket3Fun, RplObjListTC),
        % Only have changes in Bucket 3
    RplObjListTC4 = lists:filter(FilterBucket4Fun, RplObjListTC),
        % Only have changes in Bucket 4

    SingleSidedPutFun =
        fun(MVN) ->
            fun(RObj) ->
                PL = PreflistFun(null, RObj#r_object.key),
                mock_kv_vnode:put(MVN, RObj, PL, [])
            end
        end,
    lists:foreach(SingleSidedPutFun(VNNa), RplObjListTC3),
    lists:foreach(SingleSidedPutFun(VNPa), RplObjListTC4),

    NoRepairCheckB3 = CheckBucketFun(Bucket3, 0),
    NoRepairCheckB4 = CheckBucketFun(Bucket4, 0),
    true = length(RplObjListTC3) > NoRepairCheckB3,
    true = length(RplObjListTC4) > NoRepairCheckB4,
        % this should be less than, as a subset of mismatched segments will
        % be passed to fetch clocks
    true = 0 < NoRepairCheckB3,
    true = 0 < NoRepairCheckB4,
       
    RepairListMapFun = fun(RObj) -> {RObj#r_object.key, RObj} end,
    RepairListTC3 = lists:map(RepairListMapFun, RplObjListTC3),
    RepairListTC4 = lists:map(RepairListMapFun, RplObjListTC4),
    GenuineRepairFun =
        fun(SourceVnode, TargetVnode, RepairList) ->
            fun(KL) -> 
                SubRepairFun = 
                    fun({{RepB, K}, _VCCompare}, Acc) -> 
                        case lists:keyfind(K, 1, RepairList) of
                            {K, RObj} ->
                                PL = PreflistFun(null, K),
                                ok = mock_kv_vnode:read_repair(SourceVnode,
                                                                RObj,
                                                                PL,
                                                                [TargetVnode]),
                                Acc + 1;
                            false ->
                                io:format("Missing from repair list ~w ~w~n",
                                            [RepB, K]),
                                Acc
                        end
                    end,
                Repaired = lists:foldl(SubRepairFun, 0, KL),
                io:format("~w keys repaired to vnode ~w~n",
                    [Repaired, TargetVnode]) 
            end
        end,
    RepairFunTC3 = GenuineRepairFun(VNNa, VNPa, RepairListTC3),
    RepairFunTC4 = GenuineRepairFun(VNPa, VNNa, RepairListTC4),

    RepairBucketFun = 
        fun(CheckBucket, TargettedRepairFun, KR, MR, Hash) ->
            CBFilters = {filter, CheckBucket, KR, small, all, MR, Hash},
            {ok, _TCCB_P, TCCB_GUID} = 
                aae_exchange:start(partial,
                                    [{exchange_vnodesendfun(VNNa), IndexNs}],
                                    [{exchange_vnodesendfun(VNPa), IndexNs}],
                                    TargettedRepairFun,
                                    ReturnFun,
                                    CBFilters,
                                    []),
            io:format("Exchange id for tree compare ~s~n", [TCCB_GUID]),
            testutil:start_receiver()
        end,
    
    FoldRepair3Fun = 
        fun(_I, Acc) ->
            case RepairBucketFun(Bucket3, RepairFunTC3, all, all, prehash) of
                {clock_compare, Count3} ->
                    Acc + Count3;
                {tree_compare, 0} ->
                    Acc
            end
        end,
    TotalRepairs3 = lists:foldl(FoldRepair3Fun, 0, lists:seq(1, 6)),
    io:format("Repaired ~w from list of length ~w~n",
                [TotalRepairs3, length(RepairListTC3)]),
    true = length(RepairListTC3) == TotalRepairs3,

    FoldRepair4Fun = 
        fun(_I, Acc) ->
            case RepairBucketFun(Bucket4, RepairFunTC4,
                                    all, all, {rehash, 5000}) of
                {clock_compare, Count4} ->
                    Acc + Count4;
                {tree_compare, 0} ->
                    Acc
            end
        end,
    TotalRepairs4 = lists:foldl(FoldRepair4Fun, 0, lists:seq(1, 6)),
    io:format("Repaired ~w from list of length ~w~n",
                [TotalRepairs4, length(RepairListTC4)]),
    true = length(RepairListTC4) == TotalRepairs4,

    io:format("Check with a key range~n"),
    % Testing with a modified range requires more
    % effort as the objects don't have a last modified date

    LimiterCheckBucketFun = 
        fun(LimiterFilters) ->
            {ok, _TCCB_P, TCCB_GUID} = 
                aae_exchange:start(partial,
                                    [{exchange_vnodesendfun(VNNa), IndexNs}],
                                    [{exchange_vnodesendfun(VNPa), IndexNs}],
                                    NullRepairFun,
                                    ReturnFun,
                                    LimiterFilters,
                                    [{transition_pause_ms, 100}]),
            io:format("Exchange id for tree compare ~s~n", [TCCB_GUID]),
            testutil:start_receiver()
        end,
    CheckFiltersB = 
        {filter, Bucket3, all, small, all, all, prehash},
        % verify no hangover going into the key range test
    true = {tree_compare, 0} == LimiterCheckBucketFun(CheckFiltersB),

    RplObjListKR = testutil:gen_riakobjects(1000, [], TupleBuckets),
    RplObjListKR3 = lists:filter(FilterBucket3Fun, RplObjListKR),

    lists:foreach(SingleSidedPutFun(VNNa), RplObjListKR3),

    RepairListKR3 = lists:map(RepairListMapFun, RplObjListKR3),
    RLKR3_SL = lists:sublist(lists:ukeysort(1, RepairListKR3), 50, 50),
    [{SK, _SObj}|_RestKRSL] = RLKR3_SL,
    {EK, _EObj} = lists:last(RLKR3_SL),
    io:format("StartKey ~s EndKey ~s in Range test~n", 
                [binary_to_list(SK), binary_to_list(EK)]),

    CheckFiltersKR = 
        {filter, Bucket3, {SK, EK}, small, all, all, prehash},
    true = {clock_compare, 50} == LimiterCheckBucketFun(CheckFiltersKR),

    RepairFunKR3 = GenuineRepairFun(VNNa, VNPa, RepairListKR3),

    FoldRepair3FunKR = 
        fun(_I, Acc) ->
            case RepairBucketFun(Bucket3, RepairFunKR3,
                                    {SK, EK},
                                    all, prehash) of
                {clock_compare, Count3} ->
                    Acc + Count3;
                {tree_compare, 0} ->
                    Acc
            end
        end,
    % Now repair those deltas - do the key range deltas first then the rest
    TotalRepairsKR3 = lists:foldl(FoldRepair3FunKR, 0, lists:seq(1, 3)),
    io:format("Total range repairs after key range test ~w~n",
                [TotalRepairsKR3]),
    true = 50 == TotalRepairsKR3,
    AllRepairsKR3 = lists:foldl(FoldRepair3Fun, 0, lists:seq(1, 5)),
    io:format("Total repairs after key range test ~w~n", [AllRepairsKR3]),
    true = length(RplObjListKR3) - 50 == AllRepairsKR3,


    io:format("Tests with a modified range~n"),
    % Some tests with a modified range.  Split a bunch of changes into two
    % lots.  Apply those two lots in two distinct time ranges.  Find the 
    % deltas by time range
    MDR_TS1 = os:timestamp(),
    timer:sleep(1000),
    RplObjListMRa = testutil:gen_riakobjects(500, [], TupleBuckets),
    RplObjListMRa3 = lists:filter(FilterBucket3Fun, RplObjListMRa),
    MDR_TS2 = os:timestamp(),
    timer:sleep(1000),
    MDR_TS3 = os:timestamp(),
    timer:sleep(1000),
    RplObjListMRb = testutil:gen_riakobjects(100, [], TupleBuckets),
    RplObjListMRb3 = lists:filter(FilterBucket3Fun, RplObjListMRb),
    MDR_TS4 = os:timestamp(),
    lists:foreach(SingleSidedPutFun(VNNa), RplObjListMRa3),
        % add some deltas
    lists:foreach(SingleSidedPutFun(VNPa), RplObjListMRb3),
        % update some of those deltas
        % updating the other vnode
    
    % check between TS3 and TS4 - should only see 'b' changes
    TS3_4_Range = {convert_ts(MDR_TS3), convert_ts(MDR_TS4)},
    CheckFiltersMRb = 
        {filter, Bucket3, all, small, all, TS3_4_Range, prehash},
        % verify no hangover going into the key range test
    TS3_4_Result = LimiterCheckBucketFun(CheckFiltersMRb),
    io:format("Exchange in second modified range resulted in ~w~n",
                [TS3_4_Result]),
    true = {clock_compare, length(RplObjListMRb3)} == TS3_4_Result,
    
    % check between TS1 and TS2 - should only see 'a' changes, but
    % not 'b' chnages as they have a higher last modified date
    TS1_2_Range = {convert_ts(MDR_TS1), convert_ts(MDR_TS2)},
    CheckFiltersMRa = 
        {filter, Bucket3, all, small, all, TS1_2_Range, prehash},
        % verify no hangover going into the key range test
    TS1_2_Result = LimiterCheckBucketFun(CheckFiltersMRa),
    io:format("Exchange in first modified range resulted in ~w~n",
                [TS1_2_Result]),
    true = {clock_compare, length(RplObjListMRa3)} == TS1_2_Result,
    % Important to relaise that as the second amendments were made
    % on the other side (VNPa) - VNPa will return no modified 
    % objects, and as VNNa so none of the second round of updates it
    % will see all of the firts round of changes soon.

    % Conflicting updates to (b) - but to be applied to VNN not VNP
    RplObjListMRc = testutil:gen_riakobjects(100, [], TupleBuckets),
    RplObjListMRc3 = lists:filter(FilterBucket3Fun, RplObjListMRc),
    lists:foreach(SingleSidedPutFun(VNNa), RplObjListMRc3),

    TS1_2_Result_ii = LimiterCheckBucketFun(CheckFiltersMRa),
    io:format("Exchange in first modified range resulted in ~w~n",
                [TS1_2_Result_ii]),
    true =
        {clock_compare, length(RplObjListMRa3) - length(RplObjListMRc3)}
            == TS1_2_Result_ii,

    RepairListMR3 = lists:map(RepairListMapFun, RplObjListMRa3),
    RepairFunMR3 = GenuineRepairFun(VNNa, VNPa, RepairListMR3),

    FoldRepair3FunMR = 
        fun(_I, Acc) ->
            case RepairBucketFun(Bucket3, RepairFunMR3,
                                    all, 
                                    {convert_ts(MDR_TS1), 
                                        convert_ts(os:timestamp())},
                                    prehash) of
                {clock_compare, Count3} ->
                    Acc + Count3;
                {tree_compare, 0} ->
                    Acc
            end
        end,
    % Now repair those deltas - still using the modified range filter, but
    % with the modified range being from TS1 to now
    TotalRepairsMR3 = lists:foldl(FoldRepair3FunMR, 0, lists:seq(1, 6)),
    io:format("Total range repairs after modified range test ~w~n",
                [TotalRepairsMR3]),
    true = length(RplObjListMRa3) == TotalRepairsMR3,

    % Shutdown and clear down files
    ok = mock_kv_vnode:close(VNNa),
    ok = mock_kv_vnode:close(VNPa),
    RootPath = testutil:reset_filestructure().


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


coveragefold_nativemedium(_Config) ->
    mock_vnode_coveragefolder(native, 50000, true).

coveragefold_nativesmall(_Config) ->
    mock_vnode_coveragefolder(native, 5000, false).

coveragefold_parallelsmall(_Config) ->
    mock_vnode_coveragefolder(parallel_so, 5000, true).

coveragefold_parallelmedium(_Config) ->
    mock_vnode_coveragefolder(parallel_so, 50000, false).

coveragefold_parallelmediumko(_Config) ->
    mock_vnode_coveragefolder(parallel_ko, 50000, false).

mock_vnode_coveragefolder(Type, InitialKeyCount, TupleBuckets) ->
    % This should load a set of 4 vnodes, with the data partitioned across the
    % vnodes n=2 to provide for 2 different coverage plans.
    %
    % After the load, an exchange can confirm consistency between the coverage 
    % plans.  Then run some folds to make sure that the folds produce the 
    % expected results 
    RootPath = testutil:reset_filestructure(),
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

    % Open four vnodes to take two of the preflists each
    % - this is intended to replicate a ring-size=4, n-val=2 ring 
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

    ObjList = testutil:gen_riakobjects(InitialKeyCount, [], TupleBuckets),
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
    {ExchangeState1, 0} = testutil:start_receiver(),
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
        mock_kv_vnode:fold_aae(VNN1, all, all, SibCountFoldFun, 
                                {0, []}, [{sibcount, null}]),
    {async, Folder3} = 
        mock_kv_vnode:fold_aae(VNN3, all, all, SibCountFoldFun, 
                                Folder1(), [{sibcount, null}]),
    {SC1, SibL1} = Folder3(),
    io:format("Coverage fold took ~w with output ~w for store ~w~n", 
                [timer:now_diff(os:timestamp(), SWF1),SC1, Type]),
    true = SC1 == InitialKeyCount,
    true = [] == SibL1,

    SWF2 = os:timestamp(),
    BucketListA = 
        case TupleBuckets of
            true ->
                [{?BUCKET_TYPE, integer_to_binary(0)},
                    {?BUCKET_TYPE, integer_to_binary(1)}];
            false ->
                [integer_to_binary(0), integer_to_binary(1)]
        end,
    {async, Folder2} = 
        mock_kv_vnode:fold_aae(VNN2, 
                                {buckets, BucketListA}, all, 
                                SibCountFoldFun, {0, []},
                                [{sibcount, null}]),
    {async, Folder4} = 
        mock_kv_vnode:fold_aae(VNN4, 
                                {buckets, BucketListA}, all,
                                SibCountFoldFun, Folder2(),
                                [{sibcount, null}]),
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
        mock_kv_vnode:fold_aae(VNN1, all, all, HashSizeFoldFun, 
                                [], [{hash, null}, {size, null}]),
    {async, Folder3HS} = 
        mock_kv_vnode:fold_aae(VNN3, all, all, HashSizeFoldFun, 
                                Folder1HS(), [{hash, null}, {size, null}]),
    BKHSzL1 = Folder3HS(),
    true = length(BKHSzL1) == InitialKeyCount,

    {async, Folder2HS} = 
        mock_kv_vnode:fold_aae(VNN2, all, all, HashSizeFoldFun, 
                                [], [{hash, null}, {size, null}]),
    {async, Folder4HS} = 
        mock_kv_vnode:fold_aae(VNN4, all, all, HashSizeFoldFun, 
                                Folder2HS(), [{hash, null}, {size, null}]),
    BKHSzL2 = Folder4HS(),
    true = length(BKHSzL2) == InitialKeyCount,

    true = lists:usort(BKHSzL2) == lists:usort(BKHSzL1),

    % Fold over a valid coverage plan to find siblings (there are none) 
    RandMetadataFoldFun =
        fun(_B, _K, V, RandAcc) ->
            {md, MD} = lists:keyfind(md, 1, V),
            [MD_Dict] = fold_metabin(MD, []),
                %% The fold needs to cope with the MD being in different
                %% formats between parallel and native stores.  Preference
                %% is to avoid this going forward - but this is the case
                %% given how ObjectSplitFun was initially implemented in Riak
            {random, X} = lists:keyfind(random, 1, MD_Dict),
            [X|RandAcc]
        end,

    {async, Folder2MDR} =
        mock_kv_vnode:fold_aae(VNN2, all, all, RandMetadataFoldFun, 
                                [], [{md, null}]),
    {async, Folder4MDR} = 
        mock_kv_vnode:fold_aae(VNN4, all, all, RandMetadataFoldFun, 
                                Folder2MDR(), [{md, null}]),

    CountFun = fun(X, Acc) -> setelement(X, Acc, element(X, Acc) + 1) end,
    MDRAcc = lists:foldl(CountFun, {0, 0, 0}, Folder4MDR()),
    MinVal = InitialKeyCount div 3 - InitialKeyCount div 6,
    {A, B, C} = MDRAcc,
    true = InitialKeyCount == A + B + C,
    true = (A > MinVal) and (B > MinVal) and (C > MinVal),

    {async, BucketListF1} = mock_kv_vnode:bucketlist_aae(VNN1),
    {async, BucketListF2} = mock_kv_vnode:bucketlist_aae(VNN2),
    {async, BucketListF3} = mock_kv_vnode:bucketlist_aae(VNN3),
    {async, BucketListF4} = mock_kv_vnode:bucketlist_aae(VNN4),
    DedupedBL = lists:usort(BucketListF1() ++ BucketListF2()
                            ++ BucketListF3() ++ BucketListF4()),
    true = 5 == length(DedupedBL),

    ok = mock_kv_vnode:close(VNN1),
    ok = mock_kv_vnode:close(VNN2),
    ok = mock_kv_vnode:close(VNN3),
    ok = mock_kv_vnode:close(VNN4),
    RootPath = testutil:reset_filestructure().


fold_metabin(<<>>, MDAcc) ->
    lists:reverse(MDAcc);
fold_metabin(<<0:32/integer,
                MetaLen:32/integer, MetaBin:MetaLen/binary,
                Rest/binary>>, MDAcc) ->
    <<_LastModBin:12/binary, VTagLen:8/integer, _VTagBin:VTagLen/binary,
      _Deleted:1/binary-unit:8, MetaDictBin/binary>> = MetaBin,
    fold_metabin(Rest, [binary_to_term(MetaDictBin)|MDAcc]);
fold_metabin(<<MetaLen:32/integer, MetaBin:MetaLen/binary,
                Rest/binary>>, MDAcc) ->
    <<_LastModBin:12/binary, VTagLen:8/integer, _VTagBin:VTagLen/binary,
      _Deleted:1/binary-unit:8, MetaDictBin/binary>> = MetaBin,
    fold_metabin(Rest, [binary_to_term(MetaDictBin)|MDAcc]).


exchange_vnodesendfun(MVN) -> testutil:exchange_vnodesendfun(MVN).

convert_ts({Tmeg, Tsec, _Tmcr}) -> Tmeg * 1000000 + Tsec.