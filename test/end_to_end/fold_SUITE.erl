-module(fold_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([
            aae_fold_keyorder/1,
            aae_fold_segmentorder/1]).

all() -> [
            aae_fold_keyorder,
            aae_fold_segmentorder
        ].


aae_fold_keyorder(_Config) ->
    aae_fold_tester(leveled_ko, 50000).

aae_fold_segmentorder(_Config) ->
    aae_fold_tester(leveled_so, 50000).


aae_fold_tester(ParallelStoreType, KeyCount) ->
    RootPath = testutil:reset_filestructure(),
    FoldPath1 = filename:join(RootPath, "folder1/"),
    SplitF = 
        fun(X) -> 
            {leveled_rand:uniform(1000), 1, 0, element(1, X), element(2, X)}
        end,
    
    {ok, Cntrl1} = 
        aae_controller:aae_start({parallel, ParallelStoreType}, 
                                    true, 
                                    {1, 300}, 
                                    [{2, 0}, {2, 1}], 
                                    FoldPath1, 
                                    SplitF),

    BKVListXS = testutil:gen_keys([], KeyCount),
    
    {SWLowMegaS, SWLowS, _SWLowMicroS} = os:timestamp(),
    timer:sleep(1000),
    ok = testutil:put_keys(Cntrl1, 2, BKVListXS, none),
    timer:sleep(1000),
    {SWHighMegaS, SWHighS, _SWHighMicroS} = os:timestamp(),
    BucketList = [integer_to_binary(1), integer_to_binary(3)],
    FoldElements = [{clock, null}, {md, null}],
    FoldFun =
        fun(B, _K, ElementList, {B1Count, B3Count}) ->
            {clock, FoldClock} = lists:keyfind(clock, 1, ElementList),
            {md, FoldMD} = lists:keyfind(md, 1, ElementList),
            case binary_to_term(FoldMD) of
                [{clock, FoldClock}] ->
                    case B of
                        <<"1">> ->
                            {B1Count + 1, B3Count};
                        <<"3">> ->
                            {B1Count, B3Count + 1}
                    end
            end
        end,
    InitAcc = {0, 0},
    {async, Runner1} = 
        aae_controller:aae_fold(Cntrl1, 
                                {buckets, BucketList},
                                all,
                                all,
                                false,
                                FoldFun,
                                InitAcc,
                                FoldElements),
    true = {KeyCount div 5, KeyCount div 5} == Runner1(),

    {async, Runner2} = 
        aae_controller:aae_fold(Cntrl1, 
                                {buckets, BucketList},
                                all,
                                {SWLowMegaS * 1000000 + SWLowS,
                                    SWHighMegaS * 1000000 + SWHighS},
                                false,
                                FoldFun,
                                InitAcc,
                                FoldElements),
    true = {KeyCount div 5, KeyCount div 5} == Runner2(),

    {async, Runner3} = 
        aae_controller:aae_fold(Cntrl1, 
                                {buckets, BucketList},
                                all,
                                {0, SWLowMegaS * 1000000 + SWLowS},
                                false,
                                FoldFun,
                                InitAcc,
                                FoldElements),
    
    {0, 0} = Runner3(),

    {async, Runner4} = 
        aae_controller:aae_fold(Cntrl1, 
                                {buckets, BucketList},
                                all,
                                {SWHighMegaS * 1000000 + SWHighS, 
                                    infinity},
                                false,
                                FoldFun,
                                InitAcc,
                                FoldElements),
    {0, 0} = Runner4(),
    
    {async, Runner5} = 
        aae_controller:aae_fold(Cntrl1, 
                                {buckets, BucketList},
                                all,
                                all,
                                2000,
                                FoldFun,
                                InitAcc,
                                FoldElements),
    case ParallelStoreType of
        leveled_ko ->
            {0, {2000, 0}} = Runner5();
        leveled_so ->
            true = 
                {-1, {KeyCount div 5, KeyCount div 5}} == Runner5()
    end,

    {async, Runner6} = 
        aae_controller:aae_fold(Cntrl1, 
                                {buckets, BucketList},
                                all,
                                {SWLowMegaS * 1000000 + SWLowS,
                                    SWHighMegaS * 1000000 + SWHighS},
                                2000,
                                FoldFun,
                                InitAcc,
                                FoldElements),
    case ParallelStoreType of
        leveled_ko ->
            {0, {2000, 0}} = Runner6();
        leveled_so ->
            true = 
                {-1, {KeyCount div 5, KeyCount div 5}} == Runner6()
    end,

    BKVSL = lists:sublist(BKVListXS, KeyCount - 1000, 128),
    SegMapFun =
        fun ({B, K, _VV}) ->
            BinK = aae_util:make_binarykey(B, K),
            Seg32 = leveled_tictac:keyto_segment32(BinK),
            leveled_tictac:get_segment(Seg32, small)
        end,
    SegList = lists:map(SegMapFun, BKVSL),
    BKVSL_ByBL =
        lists:filter(fun({B, _K, _V}) -> lists:member(B, BucketList) end,
                        BKVSL),
    FoldClocksElements = [{clock, null}],
    FoldClocksFun =
        fun(B, K, ElementList, Acc) ->
            {clock, FoldClock} = lists:keyfind(clock, 1, ElementList),
            [{B, K, FoldClock}|Acc]
        end,

    {async, Runner7} = 
        aae_controller:aae_fold(Cntrl1, 
                                {buckets, BucketList},
                                {segments, SegList, small},
                                all,
                                false,
                                FoldClocksFun,
                                [],
                                FoldClocksElements),
    
    FetchedClocks = Runner7(),
    io:format("Fetched ~w clocks with segment filter~n",
                [length(FetchedClocks)]),
    true = 
        [] == lists:subtract(BKVSL_ByBL, FetchedClocks),
        % Found all the Keys and clocks in the list
    true =
        (KeyCount div 64) > length(lists:subtract(FetchedClocks, BKVSL_ByBL)),
        % Didn't find "too many" others due to collisions on segment

    ok = aae_controller:aae_close(Cntrl1),
    RootPath = testutil:reset_filestructure().


