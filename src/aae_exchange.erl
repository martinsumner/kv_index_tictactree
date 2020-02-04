%% -------- Overview ---------
%%
%% There are two primary types of exchange sorted
%% - a full exchange aimed at implementations with cached trees, where the
%% cached trees represent all the data in the location, and the comparion is
%% between two complete data sets
%% - a partial exchange where it is expected that trees will be dynamically
%% created covering a subset of data within the location
%%
%% The full exchange assumes access to cached trees, with a low cost of
%% repeated access, and a relatively high proportion fo the overall cost in
%% network bandwitdh.  These exchanges go through the following process:
%%
%% - Root Compare (x n)
%% - Branch Compare (x n)
%% - Clock Compare
%% - Repair
%%
%% The partial, dynamic tree exchange is based on dynamically produced trees,
%% where a relatively high proportion of the cost is in the production of the
%% trees. In a tree exchange, whole trees are compared (potentially reduced by
%% use of a segment filter), until the delta stops decreasing at a significant
%% rate and a Clock Compare is run.  So these exchanges for through the
%% following process:
%%
%% - Tree Compare (x n)
%% - Clock Compare
%% - Repair
%%
%% Each exchange has a 'blue' list and a 'pink' list.  Each list (blue and 
%% pink) is a set of partitions pertinent to this exchange, with the state
%% to be compared being the merging of all the trees referenced by the list.
%%
%% The lists can be a single item each (for a pairwise exchange), or a 
%% ring-size number of partitions for a coverage query exchange.  
%%
%% -------- Root Compare ---------
%%
%% This allows the comparison between the roots of trees.  Each root (with a
%% tree size of large and 4-byte hashes), will be 4KB in size.  The outcome of
%% the comparison should be a set of BranchIDs where the (merged) roots are 
%% showing differences.
%%
%% The Exchange can terminate if the set of differences is empty.  A timeout 
%% should trigger the commencement of the next stage (to provide a pause 
%% between vnode requests).
%%
%% -------- Root Confirm ---------
%%
%% In the next stage the roots are again requested, received and compared.
%% Again a set of branchIDs which differ is created - and the set of 
%% confirmed deltas is the intersection of the sets generated from both root
%% exchanges.
%%
%% The purpose of the confirm stage is to rule out false negative results 
%% related to timing differences in the result of PUTs.
%%
%% The Exchange can terminate if the set of differences is empty.  A timeout 
%% should trigger the commencement of the next stage (to provide a pause 
%% between vnode requests).
%%
%% -------- Branch Compare / Confirm ---------
%%
%% The set of branch ID differences should now be fetched (Compare), and then
%% re-fetched following a timeout (Confirm) to produce a set of SegmentIDs (or
%% tree leaves) that represent differences ebwteen blue and pink, eliminating
%% false negatives related to timing as with the Root Compare and Confirm.
%%
%% Each Branch is 1KB in size.  So if there are more than 16 branches which 
%% have differrences, only 16 should be chosen for the Compare and Confirm to
%% control the volume of network traffic prompted by the exchange.
%%
%% The Exchange can terminate if the set of differences is empty.  A timeout 
%% should trigger the commencement of the next stage (to provide a pause 
%% between vnode requests).
%%
%% -------- Clock Compare ---------
%%
%% The final stage is clock compare.  The clock compare can be done on up to
%% 128 segments across a maximum of 8 BranchIDs.  This is to control the 
%% potential overhead of the comparison and subsequent repairs.  This may mean
%% for empty vnodes o(1000) exchanges may be required to fully recover the 
%% store.  However, in these cases it is likely that handoff and read repair 
%% is already recovering the data so overly-aggressive read repair is 
%% unnecessary.
%%

-module(aae_exchange).

-behaviour(gen_fsm).

-ifdef(fsm_deprecated).
-compile({nowarn_deprecated_function, 
            [{gen_fsm, start, 3},
                {gen_fsm, send_event, 2}]}).
-endif.

-include("include/aae.hrl").

-define(TRANSITION_PAUSE_MS, 500).
    % A pause between phases - allow queue lengths to change, and avoid
    % generating an excess workload for AAE
-define(CACHE_TIMEOUT_MS, 60000). 
    % 60 seconds (used in fetch root/branches)
-define(SCAN_TIMEOUT_MS, 600000). 
    % 10 minutes (used in fetch clocks)
-define(UNFILTERED_SCAN_TIMEOUT_MS, 3600000).
    % 60 minutes (used in fetch trees with no filters)
-define(MAX_RESULTS, 128). 
    % Maximum number of results to request in one round of
-define(WORTHWHILE_REDUCTION, 0.3).
    % If the last comparison of trees has reduced the size of the dirty leaves
    % by 30%, probably worth comparing again before a clock fetch is run. 
    % Number a suck-teeth estimate, not even a fag-packet calculation involved.
-define(WORTHWHILE_FILTER, 256).
    % If the number of segment IDs to pass into a filter is too large, the
    % filter is probably not worthwhile - more effort checking the filter, than
    % time saved in the accumulator.  Another suck-teeth estimate here as to
    % what this value is, at this level with a small tree it will save opening
    % all but one block in most slots (with the sst file).  I suspect the
    % optimal number is more likely to be higher than lower.

-export([init/1,
            handle_sync_event/4,
            handle_event/3,
            handle_info/3,
            terminate/3,
            code_change/4]).

-export([waiting_all_results/2,
            prepare_full_exchange/2,
            prepare_partial_exchange/2,
            root_compare/2,
            branch_compare/2,
            clock_compare/2,
            tree_compare/2,
            merge_root/2,
            merge_branches/2]).

-export([compare_roots/2,
            compare_branches/2,
            compare_clocks/2,
            compare_trees/2]).

-export([start/4,
            start/7,
            reply/3]).

-include_lib("eunit/include/eunit.hrl").

-record(state, {root_compare_deltas = [] :: list(),
                branch_compare_deltas = [] :: list(),
                tree_compare_deltas = [] :: list(),
                key_deltas = [] :: list(),
                repair_fun,
                reply_fun,
                blue_list = [] :: input_list(),
                pink_list = [] :: input_list(),
                exchange_id = "not_set" :: list(),
                blue_returns = {0, 0} :: {integer(), integer()},
                pink_returns = {0, 0} :: {integer(), integer()},
                pink_acc,
                blue_acc,
                merge_fun,
                start_time = os:timestamp() :: erlang:timestamp(),
                pending_state :: atom(),
                reply_timeout = 0 :: integer(),
                exchange_type :: exchange_type(),
                exchange_filters = none :: filters(),
                last_tree_compare = none :: list(non_neg_integer())|none,
                last_root_compare = none :: list(non_neg_integer())|none,
                last_branch_compare = none :: list(non_neg_integer())|none,
                tree_compares = 0 :: integer(),
                root_compares = 0 :: integer(),
                branch_compares = 0 :: integer(),
                transition_pause_ms = ?TRANSITION_PAUSE_MS :: pos_integer(),
                log_levels :: aae_util:log_levels()|undefined
                }).

-type branch_results() :: list({integer(), binary()}).
    % Results to branch queries are a list mapping Branch ID to the binary for
    % that branch
-type exchange_state() :: #state{}.
-type exchange_type() :: full|partial.

-type compare_state() ::
    root_compare|tree_compare|branch_compare|clock_compare.
-type closing_state() ::
    compare_state()|timeout|error|not_supported.
-type bucket() :: 
    {binary(), binary()}|binary().
-type key_range() :: 
    {binary(), binary()}|all.
-type modified_range() :: 
    {non_neg_integer(), non_neg_integer()}|all.
-type segment_filter() :: 
    {segments, list(non_neg_integer()), leveled_tictac:tree_size()}|all.
-type hash_method() ::
    pre_hash|{rehash, non_neg_integer()}.
-type filters() :: 
    {filter,
        bucket(), key_range(),
        leveled_tictac:tree_size(),
        segment_filter(), modified_range(),
        hash_method()}|none.
    % filter to be used in partial exchanges
-type option_item() :: {transition_pause_ms, pos_integer()}.
-type options() :: list(option_item()).
-type send_message() ::
        fetch_root |
        {fetch_branches, list(non_neg_integer())} |
        {fetch_clocks, list(non_neg_integer())} |
        {merge_tree_range, filters()} |
        {fetch_clocks_range, filters()}.
-type send_fun() :: fun((send_message(), list(tuple()), blue|pink) -> ok).
-type input_list() :: [{send_fun(), list(tuple())|all}].
    % The Blue List and the Pink List are made up of:
    % - a SendFun, which should  be a 3-arity function, taking a preflist, 
    % a message and a colour to be used to flag the reply;
    % - a list of preflists, to be used in the SendFun to be filtered by the
    % target.  The Preflist might be {Index, Node} for remote requests or 
    % {Index, Pid} for local requests
    % For partial exchanges only, the preflist can and must be set to 'all'

-define(FILTERIDX_SEG, 5).
-define(FILTERIDX_TRS, 4).

%%%============================================================================
%%% API
%%%============================================================================


start(BlueList, PinkList, RepairFun, ReplyFun) ->
    % API for backwards compatability
    start(full, BlueList, PinkList, RepairFun, ReplyFun, none, []).


-spec start(exchange_type(),
            input_list(), input_list(),
            fun((list({any(), any(), any()})) -> ok),
            fun(({closing_state(), non_neg_integer()}) -> ok),
            filters(),
            options()) -> {ok, pid(), list()}.
%% @doc
%% Start an FSM to manage an exchange and compare the preflsist in the 
%% BlueList with those in the PinkList, using the RepairFun to repair any
%% keys discovered to have inconsistent clocks.  ReplyFun used to reply back
%% to calling client the StateName at termination.
%%
%% The ReplyFun should be a 1 arity function that expects a tuple with the
%% closing state and the cout of deltas.  
start(full, BlueList, PinkList, RepairFun, ReplyFun, none, Opts) ->
    ExchangeID = leveled_util:generate_uuid(),
    {ok, ExPID} = gen_fsm:start(?MODULE, 
                                [{full, none}, 
                                    BlueList, PinkList, RepairFun, ReplyFun,
                                    ExchangeID,
                                    Opts], 
                                []),
    {ok, ExPID, ExchangeID};
start(partial, BlueList, PinkList, RepairFun, ReplyFun, Filters, Opts) ->
    ExchangeID = leveled_util:generate_uuid(),
    {ok, ExPID} = gen_fsm:start(?MODULE, 
                                [{partial, Filters}, 
                                    BlueList, PinkList, RepairFun, ReplyFun,
                                    ExchangeID,
                                    Opts], 
                                []),
    {ok, ExPID, ExchangeID}.


-spec reply(pid(), any(), pink|blue) -> ok.
%% @doc
%% Support events to be sent back to the FSM
reply(Exchange, {error, Error}, _Colour) ->
    gen_fsm:send_event(Exchange, {error, Error});
reply(Exchange, Result, Colour) ->
    gen_fsm:send_event(Exchange, {reply, Result, Colour}).

%%%============================================================================
%%% gen_fsm callbacks
%%%============================================================================

init([{full, none},
        BlueList, PinkList, RepairFun, ReplyFun, ExChID, Opts]) ->
    leveled_rand:seed(),
    PinkTarget = length(PinkList),
    BlueTarget = length(BlueList),
    State = #state{blue_list = BlueList, 
                    pink_list = PinkList,
                    repair_fun = RepairFun,
                    reply_fun = ReplyFun,
                    exchange_id = ExChID,
                    pink_returns = {PinkTarget, PinkTarget},
                    blue_returns = {BlueTarget, BlueTarget},
                    exchange_type = full},
    State0 = process_options(Opts, State),
    aae_util:log("EX001",
                    [ExChID, PinkTarget + BlueTarget],
                    logs(),
                    State0#state.log_levels),
    {ok, prepare_full_exchange, State0, 0};
init([{partial, Filters},
        BlueList, PinkList, RepairFun, ReplyFun, ExChID, Opts]) ->
    leveled_rand:seed(),
    PinkTarget = length(PinkList),
    BlueTarget = length(BlueList),
    State = #state{blue_list = BlueList, 
                    pink_list = PinkList,
                    repair_fun = RepairFun,
                    reply_fun = ReplyFun,
                    exchange_id = ExChID,
                    pink_returns = {PinkTarget, PinkTarget},
                    blue_returns = {BlueTarget, BlueTarget},
                    exchange_type = partial,
                    exchange_filters = Filters},
    State0 = process_options(Opts, State),
    aae_util:log("EX001",
                    [ExChID, PinkTarget + BlueTarget],
                    logs(),
                    State0#state.log_levels),
    {ok, prepare_partial_exchange, State0, 0}.


prepare_full_exchange(timeout, State) ->
    aae_util:log("EX006",
                    [prepare_tree_exchange, State#state.exchange_id],
                    logs(),
                    State#state.log_levels),
    trigger_next(fetch_root, 
                    root_compare, 
                    fun merge_root/2, 
                    <<>>, 
                    false, 
                    ?CACHE_TIMEOUT_MS, 
                    State).

prepare_partial_exchange(timeout, State) ->
    aae_util:log("EX006",
                    [prepare_partial_exchange, State#state.exchange_id],
                    logs(),
                    State#state.log_levels),
    Filters = State#state.exchange_filters,
    ScanTimeout = filtered_timeout(Filters),
    TreeSize = element(?FILTERIDX_TRS, Filters),
    trigger_next({merge_tree_range, Filters},
                    tree_compare,
                    fun merge_tree/2,
                    leveled_tictac:new_tree(empty_tree, TreeSize),
                    false,
                    ScanTimeout,
                    State).

tree_compare(timeout, State) ->
    aae_util:log("EX006",
                    [root_compare, State#state.exchange_id],
                    logs(),
                    State#state.log_levels),
    DirtyLeaves = compare_trees(State#state.blue_acc, State#state.pink_acc),
    TreeCompares = State#state.tree_compares + 1,
    {StillDirtyLeaves, Reduction} = 
        case State#state.last_tree_compare of
            none ->
                {DirtyLeaves, 1.0};
            PreviouslyDirtyLeaves ->
                SDL = intersect_ids(PreviouslyDirtyLeaves, DirtyLeaves),
                {SDL, 1.0 - length(SDL) / length(PreviouslyDirtyLeaves)}
        end,
    % We want to keep comparing trees until the number of deltas stops reducing
    % significantly.  Then there should be a clock comparison.
    % It is expected there will be natural deltas with tree compare because of
    % timing differences.  Ideally the natural deltas will be small enough so
    % that there should be no more than 2 tree compares before a segment filter
    % can be applied to accelerate the process.
    Filters = State#state.exchange_filters,
    TreeSize = element(?FILTERIDX_TRS, Filters),
    case ((length(StillDirtyLeaves) > 0)
            and (Reduction > ?WORTHWHILE_REDUCTION)) of
        true ->
            % Keep comparing trees, this is reducing the segments we will
            % eventually need to compare
            Filters0 =
                case length(StillDirtyLeaves) < ?WORTHWHILE_FILTER of
                    true ->
                        Segments =
                            {segments, StillDirtyLeaves, TreeSize},
                        setelement(?FILTERIDX_SEG, Filters, Segments);
                    false ->
                        Filters
                end,
            ScanTimeout = filtered_timeout(Filters0),
            trigger_next({merge_tree_range, Filters0},
                            tree_compare,
                            fun merge_tree/2,
                            leveled_tictac:new_tree(empty_tree, TreeSize),
                            false,
                            ScanTimeout,
                            State#state{last_tree_compare = StillDirtyLeaves,
                                        tree_compares = TreeCompares});
        false ->
            % Compare clocks.  Note if there are no Mismatched segment IDs the
            % stop condition in trigger_next will be met
            SegmentIDs = select_ids(StillDirtyLeaves, 
                                    ?MAX_RESULTS,
                                    tree_compare, 
                                    State#state.exchange_id,
                                    State#state.log_levels),
            % TODO - select_ids doesn't account for TreeSize
            Filters0 =
                setelement(?FILTERIDX_SEG,
                            Filters,
                            {segments, SegmentIDs, TreeSize}),
            trigger_next({fetch_clocks_range, Filters0}, 
                            clock_compare, 
                            fun merge_clocks/2, 
                            [],
                            length(SegmentIDs) == 0, 
                            ?SCAN_TIMEOUT_MS, 
                            State#state{tree_compare_deltas = StillDirtyLeaves,
                                        tree_compares = TreeCompares})
    end.


root_compare(timeout, State) ->
    aae_util:log("EX006",
                    [root_compare, State#state.exchange_id],
                    logs(),
                    State#state.log_levels),
    DirtyBranches = compare_roots(State#state.blue_acc, State#state.pink_acc),
    RootCompares = State#state.root_compares + 1,
    {BranchIDs, Reduction} = 
        case State#state.last_root_compare of
            none ->
                {DirtyBranches, 1.0};
            PreviouslyDirtyBranches ->
                BDL = intersect_ids(PreviouslyDirtyBranches, DirtyBranches),
                {BDL, 1.0 - length(BDL) / length(PreviouslyDirtyBranches)}
        end,
    % Should we loop again on root_compare?  As longs as root_compare is
    % reducing the result set sufficiently, keep doing it until we switch to
    % branch_compare
    case ((length(BranchIDs) > 0)
            and (Reduction > ?WORTHWHILE_REDUCTION)) of
        true ->
            trigger_next(fetch_root, 
                            root_compare, 
                            fun merge_root/2, 
                            <<>>, 
                            false, 
                            ?CACHE_TIMEOUT_MS, 
                            State#state{last_root_compare = BranchIDs,
                                        root_compares = RootCompares});
        false ->
            BranchesToFetch = select_ids(BranchIDs, 
                                            ?MAX_RESULTS, 
                                            root_confirm, 
                                            State#state.exchange_id,
                                            State#state.log_levels),
            trigger_next({fetch_branches, BranchesToFetch}, 
                            branch_compare, 
                            fun merge_branches/2, 
                            [], 
                            length(BranchIDs) == 0, 
                            ?CACHE_TIMEOUT_MS, 
                            State#state{root_compare_deltas = BranchesToFetch,
                                        root_compares = RootCompares})
    end.


branch_compare(timeout, State) ->
    aae_util:log("EX006",
                    [branch_compare, State#state.exchange_id],
                    logs(),
                    State#state.log_levels),
    DirtySegments = compare_branches(State#state.blue_acc, State#state.pink_acc),
    BranchCompares = State#state.branch_compares + 1,
    {SegmentIDs, Reduction} = 
        case State#state.last_branch_compare of
            none ->
                {DirtySegments, 1.0};
            PreviouslyDirtySegments ->
                SDL = intersect_ids(PreviouslyDirtySegments, DirtySegments),
                {SDL, 1.0 - length(SDL) / length(PreviouslyDirtySegments)}
        end,
    % Should we loop again on root_compare?  As longs as root_compare is
    % reducing the result set sufficiently, keep doing it until we switch to
    % branch_compare
    case ((length(SegmentIDs) > 0)
            and (Reduction > ?WORTHWHILE_REDUCTION)) of
        true ->
            trigger_next({fetch_branches, State#state.root_compare_deltas}, 
                            branch_compare, 
                            fun merge_branches/2, 
                            [],
                            false, 
                            ?CACHE_TIMEOUT_MS, 
                            State#state{last_branch_compare = SegmentIDs,
                                        branch_compares = BranchCompares});
        false ->
            SegstoFetch = select_ids(SegmentIDs, 
                                        ?MAX_RESULTS,
                                        branch_confirm, 
                                        State#state.exchange_id,
                                        State#state.log_levels),
            trigger_next({fetch_clocks, SegstoFetch}, 
                            clock_compare, 
                            fun merge_clocks/2, 
                            [],
                            length(SegmentIDs) == 0, 
                            ?SCAN_TIMEOUT_MS, 
                            State#state{branch_compare_deltas = SegstoFetch,
                                        branch_compares = BranchCompares})
    end.

clock_compare(timeout, State) ->
    aae_util:log("EX006",
                    [clock_compare, State#state.exchange_id],
                    logs(),
                    State#state.log_levels),
    aae_util:log("EX008",
                    [State#state.blue_acc, State#state.pink_acc],
                    logs(),
                    State#state.log_levels),
    RepairKeys = compare_clocks(State#state.blue_acc, State#state.pink_acc),
    RepairFun = State#state.repair_fun,
    aae_util:log("EX004", 
                    [State#state.exchange_id, length(RepairKeys)], 
                    logs(),
                    State#state.log_levels),
    RepairFun(RepairKeys),
    {stop, 
        normal, 
        State#state{key_deltas = RepairKeys}}.


waiting_all_results({reply, not_supported, Colour}, State) ->
    aae_util:log("EX010",
                    [Colour, State#state.exchange_id],
                    logs(),
                    State#state.log_levels),
    {stop, normal, State#state{pending_state = not_supported}};
waiting_all_results({reply, Result, Colour}, State) ->
    aae_util:log("EX007",
                    [Colour, State#state.exchange_id],
                    logs(),
                    State#state.log_levels),
    {PC, PT} = State#state.pink_returns,
    {BC, BT} = State#state.blue_returns,
    MergeFun = State#state.merge_fun,
    {State0, AllPink, AllBlue} =
        case Colour of  
            pink ->
                PinkAcc = MergeFun(Result, State#state.pink_acc),
                {State#state{pink_returns = {PC + 1, PT}, pink_acc = PinkAcc},
                    PC + 1 == PT, BC == BT};
            blue ->
                BlueAcc = MergeFun(Result, State#state.blue_acc),
                {State#state{blue_returns = {BC + 1, BT}, blue_acc = BlueAcc},
                    PC == PT, BC + 1 == BT}
        end,
    case AllBlue and AllPink of 
        true ->
            {next_state, 
                State0#state.pending_state, 
                State0, 
                jitter_pause(State#state.transition_pause_ms)};
        false ->
            {next_state, 
                waiting_all_results, 
                State0, 
                set_timeout(State0#state.start_time, 
                            State0#state.reply_timeout)}
    end;
waiting_all_results(UnexpectedResponse, State) ->
    % timeout expected here, but also may get errors from vnode - such as
    % {error, mailbox_overload} when vnode has entered overload state.  Not
    % possible to complete exchange so stop
    {PC, PT} = State#state.pink_returns,
    {BC, BT} = State#state.blue_returns,
    MissingCount = PT + BT - (PC + BC),
    aae_util:log("EX002", 
                    [UnexpectedResponse,
                        State#state.pending_state, 
                        MissingCount, 
                        State#state.exchange_id], 
                        logs(),
                        State#state.log_levels),
    ReplyState =
        case UnexpectedResponse of
            timeout ->
                timeout;
            _ ->
                error
        end,
    {stop, normal, State#state{pending_state = ReplyState}}.


handle_sync_event(_msg, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_event(_Msg, StateName, State) ->
    {next_state, StateName, State}.

handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

terminate(normal, StateName, State) ->
    case State#state.exchange_type of
        full ->
            aae_util:log("EX003", 
                            [StateName,
                                State#state.exchange_id,
                                length(State#state.root_compare_deltas),
                                State#state.root_compares,
                                length(State#state.branch_compare_deltas),
                                State#state.branch_compares,
                                length(State#state.key_deltas)], 
                            logs(),
                            State#state.log_levels);
        partial ->
            aae_util:log("EX009", 
                            [StateName,
                                State#state.exchange_id,
                                length(State#state.tree_compare_deltas),
                                State#state.tree_compares,
                                length(State#state.key_deltas)], 
                            logs(),
                            State#state.log_levels)
    end,
    ReplyFun = State#state.reply_fun,
    ReplyFun({State#state.pending_state, length(State#state.key_deltas)}).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.



%%%============================================================================
%%% External Functions
%%%============================================================================


-spec merge_binary(binary(), binary()) -> binary().
%% @doc
%% Merge two binaries - where one might be empty (as nothing has been seen for
%% that preflist, or the accumulator is the initial one)
merge_binary(<<>>, AccBin) ->
    AccBin;
merge_binary(ResultBin, <<>>) ->
    ResultBin;
merge_binary(ResultBin, AccBin) ->
    leveled_tictac:merge_binaries(ResultBin, AccBin).

-spec merge_branches(branch_results(), branch_results()) -> branch_results().
%% @doc
%% Branches should be returned as a list of {BranchID, BranchBin} pairs.  For 
%% each branch in a result, merge into the accumulator.
merge_branches([], BranchAccL) ->
    BranchAccL;
merge_branches([{BranchID, BranchBin}|Rest], BranchAccL) ->
    case lists:keyfind(BranchID, 1, BranchAccL) of
        false ->
            % First response has an empty accumulator
            merge_branches(Rest, [{BranchID, BranchBin}|BranchAccL]);
        {BranchID, BinAcc} ->
            BinAcc0 = merge_binary(BranchBin, BinAcc),
            merge_branches(Rest, 
                            lists:keyreplace(BranchID, 
                                                1, 
                                                BranchAccL, 
                                                {BranchID, BinAcc0}))
    end.

-spec merge_root(binary(), binary()) -> binary().
%% @doc
%% Merge an individual result for a set of preflists into the accumulated 
%% binary for the tree root
merge_root(Root, RootAcc) ->
    merge_binary(Root, RootAcc).

-spec merge_tree(leveled_tictac:tictactree(), leveled_tictac:tictactree())
                                                -> leveled_tictac:tictactree().
%% @doc
%% Merge two trees into an XOR'd tree representing the total result set
merge_tree(Tree0, Tree1) ->
    leveled_tictac:merge_trees(Tree0, Tree1).

%%%============================================================================
%%% Internal Functions
%%%============================================================================

-spec process_options(options(), exchange_state()) -> exchange_state().
%% @doc
%% Alter state reflecting any passed in options
process_options([], State) ->
    State;
process_options([{transition_pause_ms, PauseMS}|Tail], State) ->
    process_options(Tail, State#state{transition_pause_ms = PauseMS});
process_options([{log_levels, LogLevels}|Tail], State) ->
    process_options(Tail, State#state{log_levels = LogLevels}).

-spec trigger_next(any(), atom(), fun(), any(), boolean(), 
                                        integer(), exchange_state()) -> any().
%% @doc
%% Trigger the next request 
trigger_next(NextRequest, PendingStateName, MergeFun, InitAcc, StopTest, 
                                                        Timeout, LoopState) ->
    case StopTest of 
        true ->
            {stop, normal, LoopState};
        false ->
            ok = send_requests(NextRequest, 
                                LoopState#state.blue_list, 
                                LoopState#state.pink_list, 
                                always_blue),
            {next_state,
                waiting_all_results,
                LoopState#state{start_time = os:timestamp(),
                                pending_state = PendingStateName,
                                pink_acc = InitAcc,
                                blue_acc = InitAcc,
                                merge_fun = MergeFun,
                                pink_returns = 
                                    reset(LoopState#state.pink_returns),
                                blue_returns = 
                                    reset(LoopState#state.blue_returns),
                                reply_timeout = Timeout},
                Timeout}
    end.


-spec set_timeout(erlang:timestamp(), pos_integer()) -> integer().
%% @doc
%% Set the timeout in a given state based on the time the state was commenced
set_timeout(StartTime, Timeout) ->
    max(0, Timeout - timer:now_diff(os:timestamp(), StartTime) div 1000).

-spec send_requests(any(), list(tuple()), list(tuple()), 
                                            always_blue|always_pink) -> ok.
%% @doc
%% Alternate between sending requests to items on the blue and pink list
send_requests({merge_tree_range, {filter, B, KR, TS, SF, MR, HM}},
                BlueList, PinkList, Always) ->
    % unpack the filter into a single tuple msg or merge_tree_range
    send_requests({merge_tree_range, B, KR, TS, SF, MR, HM},
                    BlueList, PinkList, Always);
send_requests({fetch_clocks_range, {filter, B, KR, _TS, SF, MR, _HM}},
                    BlueList, PinkList, Always) ->
    % unpack the filter into a single tuple msg or merge_tree_range
    send_requests({fetch_clocks_range, B, KR, SF, MR},
                    BlueList, PinkList, Always);
send_requests(_Msg, [], [], _Always) ->
    ok;
send_requests(Msg, [{SendFun, Preflists}|Rest], PinkList, always_blue) ->
    SendFun(Msg, Preflists, blue),
    case length(PinkList) > 0 of
        true ->
            send_requests(Msg, Rest, PinkList, always_pink);
        false ->
            send_requests(Msg, Rest, PinkList, always_blue)
    end;
send_requests(Msg, BlueList, [{SendFun, Preflists}|Rest], always_pink) ->
    SendFun(Msg, Preflists, pink),
    case length(BlueList) > 0 of
        true ->
            send_requests(Msg, BlueList, Rest, always_blue);
        false ->
            send_requests(Msg, BlueList, Rest, always_pink)
    end.

-spec merge_clocks(list(tuple()), list(tuple())) -> list(tuple()).
%% @doc
%% Accumulate keys and clocks returned in the segment query, outputting a 
%% sorted list of keys and clocks.
merge_clocks(KeyClockL, KeyClockLAcc) ->
    lists:merge(lists:usort(KeyClockL), KeyClockLAcc).


-spec compare_roots(binary(), binary()) -> list(integer()).
%% @doc
%% Compare the roots of two trees (i.e. the Pink and Blue root), and return a 
%% list of branch IDs which are mismatched.
compare_roots(BlueRoot, PinkRoot) ->
    leveled_tictac:find_dirtysegments(BlueRoot, PinkRoot).

-spec compare_branches(branch_results(), branch_results()) -> list(integer()).
%% @doc
%% Compare two sets of branches , and return a list of segment IDs which are 
%% mismatched
compare_branches(BlueBranches, PinkBranches) ->
    FoldFun =
        fun(Idx, Acc) ->
            {BranchID, BlueBranch} = lists:nth(Idx, BlueBranches),
            {BranchID, PinkBranch} = lists:keyfind(BranchID, 1, PinkBranches),
            DirtySegs =
                leveled_tictac:find_dirtysegments(BlueBranch, PinkBranch),
            lists:map(fun(S) -> 
                            leveled_tictac:join_segment(BranchID, S)
                        end,
                        DirtySegs) ++ Acc
        end,
    lists:foldl(FoldFun, [], lists:seq(1, length(BlueBranches))).

-spec compare_clocks(list(tuple()), list(tuple())) -> list(tuple()).
%% @doc
%% Find the differences between the lists - and return a list of
%% {B, K, blue-side VC, pink-side VC}
%% If theblue-side or pink-seide does not contain the key, then none is used
%% in place of the clock
compare_clocks(BlueList, PinkList) ->
    % Two lists of {B, K, VC} want to remove everything where {B, K, VC} is
    % the same in both lists

    BlueSet = ordsets:from_list(BlueList),
    PinkSet = ordsets:from_list(PinkList),

    BlueDelta = ordsets:subtract(BlueSet, PinkSet),
    PinkDelta = ordsets:subtract(PinkSet, BlueSet),
        % Want to subtract out from the Pink and Blue Sets any example where 
        % both pink and blue are the same
        %
        % This should speed up the folding and key finding to provide the 
        % joined list

    BlueDeltaList = 
        lists:reverse(
            ordsets:fold(fun({B, K, VCB}, Acc) -> 
                                % Assume for now that element may be only
                                % blue
                                [{{B, K}, {VCB, none}}|Acc] 
                            end, 
                            [], 
                            BlueDelta)),
        % BlueDeltaList is the output of compare clocks, assuming the item
        % is only on the Blue side (so it compares the blue vector clock with 
        % none)
    
    PinkEnrichFun =
        fun({B, K, VCP}, Acc) ->
            case lists:keyfind({B, K}, 1, Acc) of
                {{B, K}, {VCB, none}} ->
                    ElementWithClockDiff = 
                        {{B, K}, {VCB, VCP}},
                    lists:keyreplace({B, K}, 1, Acc, ElementWithClockDiff);
                false ->
                    ElementOnlyPink = 
                        {{B, K}, {none, VCP}},
                    lists:keysort(1, [ElementOnlyPink|Acc])
            end
        end,
        % The Foldfun to be used on the PinkDelta, will now fill in the Pink 
        % vector clock if the element also exists in Pink
    
    AllDeltaList = 
        ordsets:fold(PinkEnrichFun, BlueDeltaList, PinkDelta),
        % The accumulator starts with the Blue side only perspective, and 
        % either adds to it or enriches it by folding over the Pink side 
        % view 
    
    AllDeltaList.


-spec compare_trees(leveled_tictac:tictactree(),
                    leveled_tictac:tictactree()) -> list(non_neg_integer()).
%% @doc
%% Compare the trees - get list of dirty leaves (Segment IDs)
compare_trees(Tree0, Tree1) ->
    leveled_tictac:find_dirtyleaves(Tree0, Tree1).

-spec intersect_ids(list(integer()), list(integer())) -> list(integer()).
%% @doc
%% Provide the intersection of two lists of integer IDs
intersect_ids(IDs0, IDs1) ->
    lists:filter(fun(ID) -> lists:member(ID, IDs1) end, IDs0).


-spec select_ids(list(integer()), pos_integer(), atom(), list(),
                    aae_util:log_levels()|undefined) -> list(integer()).
%% @doc
%% Select a cluster of IDs if the list of IDs is smaller than the maximum 
%% output size.  The lookup based on these IDs will be segment based, so it 
%% is expected that the tightest clustering will yield the most efficient 
%% results. 
select_ids(IDList, MaxOutput, StateName, ExchangeID, LogLevels) ->
    IDList0 = lists:usort(IDList),
    FoldFun =
        fun(Idx, {BestIdx, MinOutput}) ->
            Space = lists:nth(MaxOutput + Idx - 1, IDList0) 
                        - lists:nth(Idx, IDList0),
            case Space < MinOutput of 
                true ->
                    {Idx, Space};
                false ->
                    {BestIdx, MinOutput}
            end
        end,
    case length(IDList0) > MaxOutput of 
        true ->
            aae_util:log("EX005", 
                            [ExchangeID, length(IDList0), StateName],
                            logs(),
                            LogLevels),
            {BestSliceStart, _Score} = 
                lists:foldl(FoldFun, 
                            {0, infinity}, 
                            lists:seq(1, 1 + length(IDList0) - MaxOutput)),
            lists:sublist(IDList0, BestSliceStart, MaxOutput);
        false ->
            IDList0
    end.
    
-spec jitter_pause(pos_integer()) -> pos_integer().
%% @doc
%% Jitter a pause, so if multiple FSMs started at once, they don't all use
%% the network at the same time
jitter_pause(Timeout) ->
    leveled_rand:uniform(Timeout) + Timeout div 2.


-spec reset({pos_integer(), pos_integer()}) 
                                        -> {non_neg_integer(), pos_integer()}.
%% @doc
%% Rest the count back to 0
reset({Target, Target}) -> {0, Target}. 

-spec filtered_timeout(filters()) -> pos_integer().
%% @doc
%% Has a filter been applied to the scan (true), or are we scanning the whole
%% bucket (false)
filtered_timeout({filter, _B, KeyRange, _TS, SegFilter, ModRange, _HM}) ->
    case ((KeyRange == all) and (SegFilter == all) and (ModRange == all)) of
        true ->
            ?UNFILTERED_SCAN_TIMEOUT_MS;
        false ->
            ?SCAN_TIMEOUT_MS
    end.

%%%============================================================================
%%% log definitions
%%%============================================================================

-spec logs() -> list(tuple()).
%% @doc
%% Define log lines for this module
logs() ->
    [{"EX001", 
            {info, "Exchange id=~s with target_count=~w expected"}},
        {"EX002",
            {error, "~w with pending_state=~w and missing_count=~w" 
                        ++ " for exchange id=~s"}},
        {"EX003",
            {info, "Normal exit for full exchange at"
                        ++ " pending_state=~w for exchange_id=~s"
                        ++ " root_compare_deltas=~w root_compares=~w"
                        ++ " branch_compare_deltas=~w branch_compares=~w"
                        ++ " keys_passed_for_compare=~w"}},
        {"EX004",
            {info, "Exchange id=~s led to prompting of repair_count=~w"}},
        {"EX005",
            {info, "Exchange id=~s throttled count=~w at state=~w"}},
        {"EX006",
            {debug, "State change to ~w for exchange id=~s"}},
        {"EX007", 
            {debug, "Reply received for colour=~w in exchange id=~s"}},
        {"EX008", 
            {debug, "Comparison between BlueList ~w and PinkList ~w"}},
        {"EX009",
            {info, "Normal exit for partial (dynamic) exchange at"
                        ++ " pending_state=~w for exchange_id=~s"
                        ++ " tree_compare_deltas=~w after tree_compares=~w"
                        ++ " keys_passed_for_compare=~w"}},
        {"EX010", 
            {warn, "Exchange not_supported for colour=~w in exchange id=~s"}}
        ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

select_id_test() ->
    L0 = [1, 2, 3],
    ?assertMatch(L0, select_ids(L0, 3, root_confirm, "t0", undefined)),
    L1 = [1, 2, 3, 5],
    ?assertMatch(L0, select_ids(L1, 3, root_confirm, "t1", undefined)),
    L2 = [1, 2, 3, 5, 6, 7, 8],
    ?assertMatch(L0, select_ids(L2, 3, root_confirm, "t2", undefined)),
    ?assertMatch([5, 6, 7, 8],
                    select_ids(L2, 4, root_confirm, "t3", undefined)),
    ?assertMatch(L0,
                    select_ids(intersect_ids(L1, L2),
                                3, root_confirm, "t4", undefined)),
    L3 = [8, 7, 1, 3, 2, 5, 6],
    ?assertMatch(L0, select_ids(L3, 3, root_confirm, "t5", undefined)),
    ?assertMatch([5, 6, 7, 8],
                    select_ids(L3, 4, root_confirm, "t6", undefined)),
    ?assertMatch(L0,
                    select_ids(intersect_ids(L1, L3),
                                3, root_confirm, "t7", undefined)).

compare_clocks_test() ->
    KV1 = {<<"B1">>, <<"K1">>, [{a, 1}]},
    KV2 = {<<"B1">>, <<"K2">>, [{b, 1}]},
    KV3 = {<<"B1">>, <<"K3">>, [{a, 2}]},
    KV4 = {<<"B1">>, <<"K1">>, [{a, 1}, {b, 2}]},
    KV5 = {<<"B1">>, <<"K2">>, [{b, 1}, {c, 1}]},

    BL1 = [KV1, KV2, KV3],
    PL1 = [KV1, KV2, KV3],
    ?assertMatch([], compare_clocks(BL1, PL1)),
    BL2 = [KV2, KV3, KV4],
    ?assertMatch([{{<<"B1">>, <<"K1">>}, {[{a, 1}, {b, 2}], [{a, 1}]}}], 
                        compare_clocks(BL2, PL1)),
    ?assertMatch([{{<<"B1">>, <<"K1">>}, {[{a, 1}], [{a, 1}, {b, 2}]}}],
                        compare_clocks(PL1, BL2)),
    PL2 = [KV4, KV5],
    ?assertMatch([{{<<"B1">>, <<"K1">>}, 
                            {[{a, 1}], [{a, 1}, {b, 2}]}},
                        {{<<"B1">>, <<"K2">>}, 
                            {[{b, 1}], [{b, 1}, {c, 1}]}},
                        {{<<"B1">>, <<"K3">>}, 
                            {[{a, 2}], none}}], 
                    compare_clocks(BL1, PL2)).

clean_exit_ontimeout_test() ->
    State0 = #state{pink_returns={4, 5}, blue_returns={8, 8},
                    exchange_type = full},
    State1 = State0#state{pending_state = timeout},
    {stop, normal, State1} = waiting_all_results(timeout, State0).


connect_error_test() ->
    SendFun =
        fun(_Msg, _PLs, Colour) ->
            Exchange = self(),
            reply(Exchange, {error, disconnected}, Colour)
        end,
    BlueList = [{SendFun, [{0, 1}]}],
    PinkList = [{SendFun, [{0, 1}]}],
    RepairFun = fun(_RL) -> ok end,
    ReceiveReply =
        spawn(fun() ->
                    receive
                        {error, 0} ->
                            ok
                    end
                end),
    
    ReplyFun = fun(R) -> ReceiveReply ! R end,
    {ok, Test, _ExID} = start(BlueList, PinkList, RepairFun, ReplyFun),
    ?assertMatch(true,
                    lists:foldl(fun(X, Acc) ->
                                    case Acc of
                                        true ->
                                            true;
                                        false ->
                                            timer:sleep(X),
                                            not is_process_alive(ReceiveReply)
                                    end
                                end,
                                false,
                                [1000, 1000, 1000])),
    ?assertMatch(false, is_process_alive(Test)).
    

coverage_cheat_test() ->
    {next_state, prepare, _State0} =
        handle_event(null, prepare, #state{exchange_type = full}),
    {reply, ok, prepare, _State1} =
        handle_sync_event(null, nobody, prepare, #state{exchange_type = full}),
    {next_state, prepare, _State2} =
        handle_info(null, prepare, #state{exchange_type = full}),
    {ok, prepare, _State3} =
        code_change(null, prepare, #state{exchange_type = full}, null).


-endif.