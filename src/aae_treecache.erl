%% -------- Overview ---------
%%

-module(aae_treecache).

-behaviour(gen_server).

-include("include/aae.hrl").


-export([
            init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3]).

-export([cache_open/3,
            cache_new/3,
            cache_alter/4,
            cache_root/1,
            cache_leaves/2,
            cache_markdirtysegments/3,
            cache_replacedirtysegments/3,
            cache_destroy/1,
            cache_startload/1,
            cache_completeload/2,
            cache_loglevel/2,
            cache_close/1]).

-include_lib("eunit/include/eunit.hrl").

-define(PENDING_EXT, ".pnd").
-define(FINAL_EXT, ".aae").
-define(START_SQN, 1).
-define(SYNC_TIMEOUT, 30000).

-record(state, {save_sqn = 0 :: integer(),
                is_restored = false :: boolean(),
                tree :: leveled_tictac:tictactree()|undefined,
                root_path :: list()|undefined,
                partition_id :: integer()|undefined,
                loading = false :: boolean(),
                dirty_segments = [] :: list(),
                active_fold :: string()|undefined,
                change_queue = [] :: list(),
                log_levels :: aae_util:log_levels()|undefined}).

-type partition_id() :: integer()|{integer(), integer()}.


%%%============================================================================
%%% API
%%%============================================================================

-spec cache_open(list(), partition_id(), aae_util:log_levels()|undefined)
                                                        -> {boolean(), pid()}.
%% @doc
%% Open a tree cache, using any previously saved one for this tree cache as a 
%% starting point.  Return is_empty boolean as true to indicate if a new cache 
%% was created, as well as the PID of this FSM
cache_open(RootPath, PartitionID, LogLevels) ->
    Opts = [{root_path, RootPath},
            {partition_id, PartitionID},
            {log_levels, LogLevels}],
    {ok, Pid} = gen_server:start_link(?MODULE, [Opts], []),
    IsRestored = gen_server:call(Pid, is_restored, infinity),
    {IsRestored, Pid}.

-spec cache_new(list(), partition_id(), aae_util:log_levels()|undefined)
                                                        -> {ok, pid()}.
%% @doc
%% Open a tree cache, without restoring from file
cache_new(RootPath, PartitionID, LogLevels) ->
    Opts = [{root_path, RootPath}, 
            {partition_id, PartitionID}, 
            {ignore_disk, true},
            {log_levels, LogLevels}],
    gen_server:start_link(?MODULE, [Opts], []).

-spec cache_destroy(pid()) -> ok.
%% @doc
%% Close a cache without saving
cache_destroy(AAECache) ->
    gen_server:cast(AAECache, destroy).

-spec cache_close(pid()) -> ok.
%% @doc
%% Close a cache with saving
cache_close(AAECache) ->
    gen_server:call(AAECache, close, ?SYNC_TIMEOUT).

-spec cache_alter(pid(), binary(), integer(), integer()) -> ok.
%% @doc
%% Change the hash tree to reflect an addition and removal of a hash value
cache_alter(AAECache, Key, CurrentHash, OldHash) -> 
    gen_server:cast(AAECache, {alter, Key, CurrentHash, OldHash}).

-spec cache_root(pid()) -> binary().
%% @doc
%% Fetch the root of the cache tree to compare
cache_root(Pid) -> 
    gen_server:call(Pid, fetch_root, infinity).

-spec cache_leaves(pid(), list(integer())) -> list().
%% @doc
%% Fetch the root of the cache tree to compare
cache_leaves(Pid, BranchIDs) -> 
    gen_server:call(Pid, {fetch_leaves, BranchIDs}, infinity).

-spec cache_markdirtysegments(pid(), list(integer()), string()) -> ok.
%% @doc
%% Mark dirty segments.  These segments are currently subject to a fetch_clocks
%% fold.  If they aren't touched until the fold is complete, the segment can be
%% safely replaced with the value in the fold.
%%
%% The FoldGUID is used to identify the request that prompted the marking.  
%% This becomes the active_fold, replacing any previous marking.  Dirty 
%% segments can only be replaced by the last active fold.  Need to avoid race
%% conditions between multiple dirtysegment markings (as well as updates 
%% clearing dirty segments)
cache_markdirtysegments(Pid, SegmentIDs, FoldGUID) ->
    gen_server:cast(Pid, {mark_dirtysegments, SegmentIDs, FoldGUID}).

-spec cache_replacedirtysegments(pid(), 
                                    list({integer(), integer()}), 
                                    string()) -> ok.
%% @doc
%% When a fold_clocks is complete, replace any dirty_segments which remain 
%% clean from other interventions
cache_replacedirtysegments(Pid, ReplacementSegments, FoldGUID) ->
    gen_server:cast(Pid, 
                    {replace_dirtysegments, ReplacementSegments, FoldGUID}).

-spec cache_startload(pid()) -> ok.
%% @doc
%% Sets the cache loading state to true, now as well as maintaining the 
%% current tree the cache should keep a queue of all the changes from this
%% point.
%%
%% Eventually cache_completeload should be called with a tree built from 
%% a loading process snapshotted at the startload point, and the changes can
%% all be applied
cache_startload(Pid) ->
    gen_server:cast(Pid, start_load).

-spec cache_completeload(pid(), leveled_tictac:tictactree()) -> ok.
%% @doc
%% Take a tree which has been produced from a fold of the KeyStore, and make 
%% this thenew tree
cache_completeload(Pid, LoadedTree) ->
    gen_server:cast(Pid, {complete_load, LoadedTree}).

-spec cache_loglevel(pid(), aae_util:log_levels()) -> ok.
%% @doc
%% Alter the log level at runtime
cache_loglevel(Pid, LogLevels) ->
    gen_server:cast(Pid, {log_levels, LogLevels}).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    PartitionID = aae_util:get_opt(partition_id, Opts),
    RootPath = aae_util:get_opt(root_path, Opts),
    IgnoreDisk = aae_util:get_opt(ignore_disk, Opts, false),
    LogLevels = aae_util:get_opt(log_levels, Opts),
    RootPath0 = filename:join(RootPath, flatten_id(PartitionID)) ++ "/",
    {StartTree, SaveSQN, IsRestored} = 
        case {open_from_disk(RootPath0, LogLevels), IgnoreDisk} of
            % Always run open_from_disk even if the result is to be ignored,
            % as any files present must still be cleared
            {{Tree, SQN}, false} when Tree =/= none ->
                {Tree, SQN, true};
            _ ->
                {leveled_tictac:new_tree(PartitionID, ?TREE_SIZE),
                    ?START_SQN, 
                    false}
        end,
    aae_util:log("C0005", [IsRestored, PartitionID], logs(), LogLevels),
    process_flag(trap_exit, true),
    {ok, #state{save_sqn = SaveSQN, 
                tree = StartTree, 
                is_restored = IsRestored,
                root_path = RootPath0,
                partition_id = PartitionID,
                log_levels = LogLevels}}.
    

handle_call(is_restored, _From, State) ->
    {reply, State#state.is_restored, State};
handle_call(fetch_root, _From, State) ->
    {reply, leveled_tictac:fetch_root(State#state.tree), State};
handle_call({fetch_leaves, BranchIDs}, _From, State) ->
    {reply, leveled_tictac:fetch_leaves(State#state.tree, BranchIDs), State};
handle_call(close, _From, State) ->
    save_to_disk(State#state.root_path, 
                    State#state.save_sqn, 
                    State#state.tree,
                    State#state.log_levels),
    {stop, normal, ok, State}.

handle_cast({alter, Key, CurrentHash, OldHash}, State) ->
    {Tree0, Segment} = leveled_tictac:add_kv(State#state.tree, 
                                                Key, 
                                                {CurrentHash, OldHash}, 
                                                fun binary_extractfun/2,
                                                true),
    State0 = 
        case State#state.loading of 
            true ->
                CQ = State#state.change_queue,
                State#state{change_queue = [{Key, CurrentHash, OldHash}|CQ]};
            false ->
                State 
        end,
    case State#state.dirty_segments of
        [] ->
            {noreply, State0#state{tree = Tree0}};
        DirtyList ->
            DirtyList0 = lists:delete(Segment, DirtyList),
            {noreply, State0#state{tree = Tree0, dirty_segments = DirtyList0}}
    end;
handle_cast(start_load, State=#state{loading=Loading}) 
                                                    when Loading == false ->
    {noreply, 
        State#state{loading = true, 
                    change_queue = [], dirty_segments = [], 
                    active_fold = undefined}};
handle_cast({complete_load, Tree}, State=#state{loading=Loading}) 
                                                    when Loading == true ->
    LoadFun = 
        fun({Key, CH, OH}, AccTree) ->
            leveled_tictac:add_kv(AccTree, 
                                    Key, 
                                    {CH, OH}, 
                                    fun binary_extractfun/2)
        end,
    Tree0 = lists:foldr(LoadFun, Tree, State#state.change_queue),
    aae_util:log("C0008",
                    [length(State#state.change_queue)],
                    logs(),
                    State#state.log_levels),
    {noreply, State#state{loading = false, change_queue = [], tree = Tree0}};
handle_cast({mark_dirtysegments, SegmentList, FoldGUID}, State) ->
    case State#state.loading of 
        true ->
            % don't mess about with dirty segments, loading anyway
            {noreply, State};
        false ->
            {noreply, State#state{dirty_segments = SegmentList, 
                                    active_fold = FoldGUID}}
    end;
handle_cast({replace_dirtysegments, SegmentMap, FoldGUID}, State) ->
    ChangeSegmentFoldFun =
        fun({SegID, NewHash}, TreeAcc) ->
            case lists:member(SegID, State#state.dirty_segments) of 
                true ->
                    aae_util:log("C0006", 
                                    [State#state.partition_id, SegID, NewHash],
                                    logs(),
                                    State#state.log_levels),
                    leveled_tictac:alter_segment(SegID, NewHash, TreeAcc);
                false ->
                    TreeAcc
            end
        end,
    case State#state.active_fold of 
        FoldGUID ->
            UpdTree = 
                lists:foldl(ChangeSegmentFoldFun, 
                            State#state.tree, 
                            SegmentMap),
            {noreply, State#state{tree = UpdTree}};
        _ ->
            {noreply, State}
    end;
handle_cast(destroy, State) ->
    aae_util:log("C0004",
                    [State#state.partition_id],
                    logs(),
                    State#state.log_levels),
    {stop, normal, State};
handle_cast({log_levels, LogLevels}, State) ->
    {noreply, State#state{log_levels = LogLevels}}.


handle_info(_Info, State) ->
    {stop, normal, State}.

terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec flatten_id(partition_id()) -> list().
%% @doc
%% Flatten partition ID to make a folder name
flatten_id({Index, N}) ->
    integer_to_list(Index) ++ "_" ++ integer_to_list(N);
flatten_id(ID) ->
    integer_to_list(ID).

-spec save_to_disk(list(),
                    integer(),
                    leveled_tictac:tictactree(),
                    aae_util:log_levels()|undefined) -> ok.
%% @doc
%% Save the TreeCache to disk, with a checksum so thatit can be 
%% validated on read.
save_to_disk(RootPath, SaveSQN, TreeCache, LogLevels) ->
    Serialised = term_to_binary(leveled_tictac:export_tree(TreeCache)),
    CRC32 = erlang:crc32(Serialised),
    ok = filelib:ensure_dir(RootPath),
    PendingName = integer_to_list(SaveSQN) ++ ?PENDING_EXT,
    aae_util:log("C0003", [RootPath, PendingName], logs(), LogLevels),
    ok = file:write_file(filename:join(RootPath, PendingName),
                            <<CRC32:32/integer, Serialised/binary>>,
                            [raw]),
    file:rename(filename:join(RootPath, PendingName), 
                    form_cache_filename(RootPath, SaveSQN)).

-spec open_from_disk(list(), aae_util:log_levels()|undefined)
                            -> {leveled_tictac:tictactree()|none, integer()}.
%% @doc
%% Open most recently saved TicTac tree cache file on disk, deleting all 
%% others both used and unused - to save an out of date tree from being used
%% following a subsequent crash
open_from_disk(RootPath, LogLevels) ->
    ok = filelib:ensure_dir(RootPath),
    {ok, Filenames} = file:list_dir(RootPath),
    FileFilterFun = 
        fun(FN, FinalFiles) ->
            case filename:extension(FN) of 
                ?PENDING_EXT ->
                    aae_util:log("C0001", [FN], logs(), LogLevels),
                    ok = file:delete(filename:join(RootPath, FN)),
                    FinalFiles;
                ?FINAL_EXT ->
                    BaseFN = 
                        filename:basename(filename:rootname(FN, ?FINAL_EXT)),
                    [list_to_integer(BaseFN)|FinalFiles];
                _ ->
                    FinalFiles
            end 
        end,
    SQNList = 
        lists:reverse(lists:sort(lists:foldl(FileFilterFun, [], Filenames))),
    case SQNList of 
        [] ->
            {none, 1};
        [HeadSQN|Tail] ->
            DeleteFun = 
                fun(SQN) -> 
                    ok = file:delete(form_cache_filename(RootPath, SQN)) 
                end,
            lists:foreach(DeleteFun, Tail), 
            FileToUse = form_cache_filename(RootPath, HeadSQN),
            case aae_util:safe_open(FileToUse) of
                {ok, STC} ->
                    ok = file:delete(FileToUse),
                    {leveled_tictac:import_tree(binary_to_term(STC)), 
                        HeadSQN +  1};
                {error, Reason} ->
                    aae_util:log("C0002", [FileToUse, Reason], logs(), LogLevels),
                    {none, 1}
            end
    end.


-spec form_cache_filename(list(), integer()) -> list().
%% @doc
%% Return the cache filename by combining the Root Path with the SQN
form_cache_filename(RootPath, SaveSQN) ->
    filename:join(RootPath, integer_to_list(SaveSQN) ++ ?FINAL_EXT).


-spec binary_extractfun(binary(), {integer(), integer()}) -> 
                                            {binary(), {is_hash, integer()}}.
%% @doc 
%% Function to calulate the hash change need to make an alter into a straight
%% add as the BinExtractfun in leveled_tictac
binary_extractfun(Key, {CurrentHash, OldHash}) ->
    % TODO: Should move this function to leveled_tictac
    % - requires secret knowledge of implementation to perform
    % alter
    RemoveH = 
        case {CurrentHash, OldHash} of 
            {0, _} ->
                % Remove - treat like adding back in
                % the tictac will bxor this with the key - so don't need to
                % bxor this here again
                OldHash;
            {_, 0} ->
                % Add 
                0;
            _ ->
                % Alter - need to account for hashing with key
                % to remove the original
                {_SegmentHash, AltHash}
                    = leveled_tictac:keyto_doublesegment32(Key),
                OldHash bxor AltHash
        end,
    {Key, {is_hash, CurrentHash bxor RemoveH}}.

%%%============================================================================
%%% log definitions
%%%============================================================================

-spec logs() -> list(tuple()).
%% @doc
%% Define log lines for this module
logs() ->
    [{"C0001", {info, "Pending filename ~s found and will delete"}},
        {"C0002", {warn, "File ~w opened with error=~w so will be ignored"}},
        {"C0003", {info, "Saving tree cache to path ~s and filename ~s"}},
        {"C0004", {info, "Destroying tree cache for partition ~w"}},
        {"C0005", {info, "Starting cache with is_restored=~w and IndexN of ~w"}},
        {"C0006", {debug, "Altering segment for PartitionID=~w ID=~w Hash=~w"}},
        {"C0007", {warn, "Treecache exiting after trapping exit from Pid=~w"}},
        {"C0008", {info, "Complete load of tree with length of change_queue=~w"}}].

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

setup_savedcaches(RootPath) ->
    Tree0 = leveled_tictac:new_tree(test),
    Tree1 = leveled_tictac:add_kv(Tree0, 
                                    {<<"K1">>}, {<<"V1">>}, 
                                    fun({K}, {V}) -> {K, V} end),
    Tree2 = leveled_tictac:add_kv(Tree1, 
                                    {<<"K2">>}, {<<"V2">>}, 
                                    fun({K}, {V}) -> {K, V} end),
    ok = save_to_disk(RootPath, 1, Tree1, undefined),
    ok = save_to_disk(RootPath, 2, Tree2, undefined),
    Tree2.

clean_saveopen_test() ->
    % Check that pending files ar eignored, and that the highest SQN that is
    % not pending is the one opened
    RootPath = "test/cache0/",
    aae_util:clean_subdir(RootPath),
    Tree2 = setup_savedcaches(RootPath),
    NextFN = filename:join(RootPath, integer_to_list(3) ++ ?PENDING_EXT),
    ok = file:write_file(NextFN, <<"delete">>),
    UnrelatedFN = filename:join(RootPath, "alt.file"),
    ok = file:write_file(UnrelatedFN, <<"no_delete">>),

    {Tree3, SaveSQN} = open_from_disk(RootPath, undefined),
    ?assertMatch(3, SaveSQN),
    ?assertMatch([], leveled_tictac:find_dirtyleaves(Tree2, Tree3)),
    ?assertMatch({none, 1}, open_from_disk(RootPath, undefined)),
    
    ?assertMatch({ok, <<"no_delete">>}, file:read_file(UnrelatedFN)),
    ?assertMatch({error, enoent}, file:read_file(NextFN)),
    aae_util:clean_subdir(RootPath).


clear_old_cache_test() ->
    RootPath = "test/oldcache0/",
    PartitionID = 1,
    RP0 = filename:join(RootPath, integer_to_list(PartitionID)) ++ "/",
    aae_util:clean_subdir(RP0),
    _Tree2 = setup_savedcaches(RP0),
    {ok, FN0s} = file:list_dir(RP0),
    ?assertMatch(2, length(FN0s)),
    {ok, Cpid} = cache_new(RootPath, 1, undefined),
    {ok, FN1s} = file:list_dir(RP0),
    ?assertMatch(0, length(FN1s)),
    ok = cache_close(Cpid),
    {ok, FN2s} = file:list_dir(RP0),
    ?assertMatch(1, length(FN2s)),
    aae_util:clean_subdir(RootPath).


corrupt_save_test_() ->
    {timeout, 60, fun corrupt_save_tester/0}.

corrupt_save_tester() ->
    % If any byte is corrupted on disk - then the result should be a failure 
    % to open and the TreeCache reverting to empty
    RootPath = "test/cachecs/",
    aae_util:clean_subdir(RootPath),
    _Tree2 = setup_savedcaches(RootPath),
    BestFN = form_cache_filename(RootPath, 2),
    {ok, LatestCache} = file:read_file(BestFN),
    FlipByteFun =
        fun(Offset) ->
            aae_util:flip_byte(LatestCache, 1, Offset)
        end,
    BrokenCaches = 
        lists:map(FlipByteFun, lists:seq(1, byte_size(LatestCache) - 1)),
    BrokenCacheCheckFun =
        fun(BrokenCache) ->
            ok = file:write_file(BestFN, BrokenCache),
            R = open_from_disk(RootPath, undefined),
            ?assertMatch({none, 1}, R)
        end,
    ok = lists:foreach(BrokenCacheCheckFun, BrokenCaches),
    aae_util:clean_subdir(RootPath).


simple_test() ->
    RootPath = "test/cache1/",
    PartitionID = 99,
    aae_util:clean_subdir(RootPath ++ "/" ++ integer_to_list(PartitionID)),
    
    GenerateKeyFun = aae_util:test_key_generator(hash),

    InitialKeys = lists:map(GenerateKeyFun, lists:seq(1,100)),
    AlternateKeys = lists:map(GenerateKeyFun, lists:seq(61, 80)),
    RemoveKeys = lists:map(GenerateKeyFun, lists:seq(81, 100)),

    {ok, AAECache0} = cache_new(RootPath, PartitionID, undefined),
    
    {AddFun, AlterFun, RemoveFun} = test_setup_funs(InitialKeys),
    
    lists:foreach(AddFun(AAECache0), InitialKeys),
    
    ok = cache_close(AAECache0),

    {true, AAECache1} = cache_open(RootPath, PartitionID, undefined),
    
    lists:foreach(AlterFun(AAECache1), AlternateKeys),
    lists:foreach(RemoveFun(AAECache1), RemoveKeys),

    %% Now build the equivalent outside of the process
    %% Accouting up-fron for the removals and the alterations
    KHL0 = lists:sublist(InitialKeys, 60) ++ AlternateKeys,
    DirectAddFun =
        fun({K, H}, TreeAcc) ->
            leveled_tictac:add_kv(TreeAcc, 
                                    K, H, 
                                    fun(Key, Value) -> 
                                        {Key, {is_hash, Value}} 
                                    end)
        end,
    CompareTree = 
        lists:foldl(DirectAddFun, 
                        leveled_tictac:new_tree(raw, ?TREE_SIZE), 
                        KHL0),
    CompareRoot = leveled_tictac:fetch_root(CompareTree),
    Root = cache_root(AAECache1),
    ?assertMatch(Root, CompareRoot),


    ok = cache_destroy(AAECache1).


replace_test() ->
    RootPath = "test/cache1/",
    PartitionID = 99,
    aae_util:clean_subdir(RootPath ++ "/" ++ integer_to_list(PartitionID)),
    GenerateKeyFun = aae_util:test_key_generator(hash),

    InitialKeys = lists:map(GenerateKeyFun, lists:seq(1,100)),
    AlternateKeys = lists:map(GenerateKeyFun, lists:seq(61, 80)),
    RemoveKeys = lists:map(GenerateKeyFun, lists:seq(81, 100)),
    
    {ok, AAECache0} = cache_new(RootPath, PartitionID, undefined),
    
    {AddFun, AlterFun, RemoveFun} = test_setup_funs(InitialKeys),
    
    lists:foreach(AddFun(AAECache0), InitialKeys),
    ok = cache_startload(AAECache0),

    lists:foreach(AlterFun(AAECache0), AlternateKeys),
    lists:foreach(RemoveFun(AAECache0), RemoveKeys),

    %% Now build the equivalent outside of the process
    %% Accouting up-fron for the removals and the alterations
    KHL0 = lists:sublist(InitialKeys, 60) ++ AlternateKeys,
    DirectAddFun =
        fun({K, H}, TreeAcc) ->
            leveled_tictac:add_kv(TreeAcc, 
                                    K, H, 
                                    fun(Key, Value) -> 
                                        {Key, {is_hash, Value}} 
                                    end)
        end,
    CompareTree = 
        lists:foldl(DirectAddFun, 
                        leveled_tictac:new_tree(raw, ?TREE_SIZE), 
                        KHL0),
    
    %% The load tree is a tree as would have been produced by a fold over a 
    %% snapshot taken at the time all the initial keys added.
    %%
    %% If we now complete the load using this tree, the comparison should 
    %% still match.  The cache should be replaced by one playing the stored
    %% alterations ont the load tree. 

    LoadTree = 
        lists:foldl(DirectAddFun, 
                        leveled_tictac:new_tree(raw, ?TREE_SIZE), 
                        InitialKeys),
    
    ok = cache_completeload(AAECache0, LoadTree),
    
    CompareRoot = leveled_tictac:fetch_root(CompareTree),
    Root = cache_root(AAECache0),
    ?assertMatch(Root, CompareRoot),


    ok = cache_destroy(AAECache0).


dirty_segment_test() ->
    % Segments based on
    GetSegFun = 
        fun(BinaryKey) ->
            SegmentID = leveled_tictac:keyto_segment48(BinaryKey),
            aae_keystore:generate_treesegment(SegmentID)
        end,
    % Have clashes with keys of integer_to_binary/1 and integers - 
    % [4241217,2576207,2363385]
    RootPath = "test/dirtysegment/",
    PartitionID = 99,
    aae_util:clean_subdir(RootPath ++ "/" ++ integer_to_list(PartitionID)),

    {ok, AAECache0} = cache_new(RootPath, PartitionID, undefined),
    AddFun = 
        fun(I) ->
            K = integer_to_binary(I),
            H = erlang:phash2(leveled_rand:uniform(100000)),
            cache_alter(AAECache0, K, H, 0)
        end,

    lists:foreach(AddFun, lists:seq(2350000, 2380000)),

    K0 = integer_to_binary(2363385),
    K1 = integer_to_binary(2576207),
    K2 = integer_to_binary(4241217),
    S0 = GetSegFun(K0),
    S1 = GetSegFun(K1),
    S2 = GetSegFun(K2),
    ?assertMatch(true, S0 == S1),
    ?assertMatch(true, S0 == S2),
    BranchID = S0 bsr 8,
    LeafID = S0 band 255,
    
    Leaf0 = get_leaf(AAECache0, BranchID, LeafID),

    ?assertMatch(false, Leaf0 == 0),

    H1 = erlang:phash2(leveled_rand:uniform(100000)),
    H2 = erlang:phash2(leveled_rand:uniform(100000)),
    {_HK1, TTH1} = leveled_tictac:tictac_hash(K1, {is_hash, H1}),
    {_HK2, TTH2} = leveled_tictac:tictac_hash(K2, {is_hash, H2}),

    cache_alter(AAECache0, K1, H1, 0),

    Leaf1 = get_leaf(AAECache0, BranchID, LeafID),
    ?assertMatch(Leaf1, Leaf0 bxor TTH1),

    GUID0 = leveled_util:generate_uuid(),
    NOTGUID = "NOT GUID",
    
    cache_markdirtysegments(AAECache0, [S0], GUID0),
    % Replace with wrong GUID ignored
    cache_replacedirtysegments(AAECache0, [{S0, Leaf0}], NOTGUID),
    ?assertMatch(Leaf1, get_leaf(AAECache0, BranchID, LeafID)),

    % Replace with right GUID succeeds
    cache_replacedirtysegments(AAECache0, [{S0, Leaf0}], GUID0),
    ?assertMatch(Leaf0, get_leaf(AAECache0, BranchID, LeafID)),

    GUID1 = leveled_util:generate_uuid(),
    cache_markdirtysegments(AAECache0, [S0], GUID1),
    cache_alter(AAECache0, K2, H2, 0),
    Leaf2 = get_leaf(AAECache0, BranchID, LeafID),
    ?assertMatch(Leaf2, Leaf0 bxor TTH2),
    cache_replacedirtysegments(AAECache0, [{S0, Leaf0}], GUID1),
    % Replace has been ignored due to update - so still Leaf2
    ?assertMatch(Leaf2, get_leaf(AAECache0, BranchID, LeafID)),

    GUID2 = leveled_util:generate_uuid(),
    cache_markdirtysegments(AAECache0, [S0], GUID2),
    cache_startload(AAECache0),
    cache_replacedirtysegments(AAECache0, [{S0, Leaf0}], GUID2),
    % Replace has been ignored due to load - so still Leaf2
    ?assertMatch(Leaf2, get_leaf(AAECache0, BranchID, LeafID)),


    ok = cache_destroy(AAECache0).



get_leaf(AAECache0, BranchID, LeafID) ->
    [{BranchID, LeafBin}] = cache_leaves(AAECache0, [BranchID]),
    LeafStartPos = LeafID * 4,
    <<_Pre:LeafStartPos/binary, Leaf:32/integer, _Rest/binary>> = LeafBin,
    Leaf.



coverage_cheat_test() ->
    {ok, _State1} = code_change(null, #state{}, null),
    {stop, normal, _State2} = handle_info({'EXIT', self(), "Test"}, #state{}).


test_setup_funs(InitialKeys) ->
    AddFun = 
        fun(CachePid) ->
            fun({K, H}) ->
                cache_alter(CachePid, K, H, 0)
            end
        end,
    AlterFun =
        fun(CachePid) -> 
            fun({K, H}) ->
                {K, OH} = lists:keyfind(K, 1, InitialKeys),
                cache_alter(CachePid, K, H, OH)
            end
        end,
    RemoveFun = 
        fun(CachePid) ->
            fun({K, _H}) ->
                {K, OH} = lists:keyfind(K, 1, InitialKeys),
                cache_alter(CachePid, K, 0, OH)
            end
        end,
    {AddFun, AlterFun, RemoveFun}.


-endif.