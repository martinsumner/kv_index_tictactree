%% -------- Overview ---------
%%
%% Centralised definition of log functions.  To make switching to Lager in 
%% the future a bit easier, and avoid repeated codes across modules

-module(aae_util).

-include("include/aae.hrl").

-export([log/2,
            log/3,
            log_timer/4,
            get_opt/2,
            get_opt/3,
            make_binarykey/2,
            safe_open/1]).

-export([clean_subdir/1,
            test_key_generator/1,
            flip_byte/3]).         

-ifdef(TEST).
-export([get_segmentid/2]).
-endif.

-define(DEFAULT_LOG_LEVELS, [warning, error, critical]).

-type log_levels() :: list(leveled_log:log_level())|undefined.

-export_type([log_levels/0]).

-define(LOGBASE,
    #{
        g0001 => 
            {info, <<"Generic log point">>},
        g0002 =>
            {info, <<"Generic log point with term ~w">>},
        d0001 =>
            {debug, <<"Generic debug log">>},
        aae01 => 
            {warning,
                <<
                    "AAE Key Store rebuild required on startup due to mismatch between vnode store state ~w "
                    "and AAE key store state of ~w maybe restart with node excluded from coverage "
                    "queries to improve AAE operation until rebuild is complete"
                >>
            },
        aae02 =>
            {info, <<"Native KeyStore type ~w startup request">>},
        aae03 =>
            {debug,
                <<"Unexpected Bucket ~w Key ~w passed with IndexN ~w that does not match any of ~w">>
            },
        aae04 =>
            {warning, <<"Misrouted request for IndexN ~w">>},
        aae06 =>
            {info, <<"Received rebuild trees request for IndexNs ~w">>},
        aae07 =>
            {info, <<"Dispatching test fold">>},
        aae08 =>
            {info, <<"Spawned worker receiving test fold">>},
        aae09 =>
            {info, <<"Change in IndexNs detected at rebuild - new IndexN ~w">>},
        aae10 =>
            {info, <<"AAE controller started with IndexNs ~w and StoreType ~w">>},
        aae11 =>
            {info, <<"Next rebuild scheduled for ~w">>},
        aae12 =>
            {info, <<"Received rebuild store for parallel store ~w">>},
        aae13 =>
            {info, <<"Completed tree rebuild">>},
        aae14 =>
            {debug, <<"Mismatch finding unexpected IndexN in fold of ~w">>},
        aae15 =>
            {info, <<"Ping showed time difference of ~w ms">>},
        aae16 =>
            {info, <<"Keystore ~w when tree rebuild requested">>},
        aae17 =>
            {warning, <<"Corrupted object with B=~p K=~p for ~w ~w">>},
        ex001 => 
            {info, <<"Exchange id=~s with target_count=~w expected purpose=~w">>},
        ex002 =>
            {error, <<"~w with pending_state=~w and missing_count=~w for exchange id=~s purpose=~w">>},
        ex003 =>
            {info,
                <<
                    "Normal exit for full exchange purpose=~w in_sync=~w pending_state=~w for exchange id=~s "
                    "scope of mismatched_segments=~w root_compare_loops=~w branch_compare_loops=~w keys_passed_for_repair=~w"
                >>
            },
        ex004 =>
            {info, <<"Exchange id=~s purpose=~w led to prompting  of repair_count=~w">>},
        ex005 =>
            {info, <<"Exchange id=~s throttled count=~w at state=~w">>},
        ex006 =>
            {debug, <<"State change to ~w for exchange id=~s">>},
        ex007 => 
            {debug, <<"Reply received for colour=~w in exchange id=~s">>},
        ex008 => 
            {debug, <<"Comparison between BlueList ~w and PinkList ~w">>},
        ex009 =>
            {info, 
                <<
                    "Normal exit for full exchange purpose=~w in_sync=~w pending_state=~w for exchange id=~s "
                    "scope of mismatched_segments=~w tree_compare_loops=~w  keys_passed_for_repair=~w"
                >>
            },
        ex010 =>
            {warning, <<"Exchange not_supported in exchange id=~s for colour=~w purpose=~w">>},
        ks001 => 
            {info, <<"Key Store loading with id=~w has reached deferred count=~w">>},
        ks002 =>
            {warning, <<"No valid manifest found for AAE keystore at ~s reason ~s">>},
        ks003 =>
            {info, <<"Storing manifest with current GUID ~s">>},
        ks004 =>
            {info, <<"Key Store building with id=~w has reached loaded_count=~w">>},
        ks005 =>
            {info, <<"Clean opening of manifest with current GUID ~s">>},
        ks006 =>
            {warning, <<"Pending store is garbage and should be deleted at ~s">>},
        ks007 =>
            {info, <<"Rebuild prompt ~w with GUID ~s">>},
        ks008 =>
            {info, <<"Rebuild queue load backlog_items=~w loaded_count=~w">>},
        r0001 =>
            {info, <<"AAE fetch clock runner has seen results=~w query_time=~w for a query_count=~w queries">>},
        r0002 =>
            {info, <<"Query backlog resulted in dummy fold">>},
        r0003 =>
            {debug, <<"Query complete in time ~w">>},
        r0004 =>
            {debug, <<"Prompting controller">>},
        r0005 =>
            {warning, <<"Query lead to error ~w pattern ~w">>},
        c0001 =>
            {info, <<"Pending filename ~s found and will delete">>},
        c0002 =>
            {warning, <<"File ~w opened with error=~w so will be ignored">>},
        c0003 =>
            {info, <<"Saving tree cache to path ~s and filename ~s">>},
        c0004 =>
            {info, <<"Destroying tree cache for partition ~w">>},
        c0005 =>
            {info, <<"Starting cache with is_restored=~w and IndexN of ~w">>},
        c0006 =>
            {debug, <<"Altering segment for PartitionID=~w ID=~w Hash=~w">>},
        c0007 =>
            {warning, <<"Treecache exiting after trapping exit from Pid=~w">>},
        c0008 =>
            {info, <<"Complete load of tree with length of change_queue=~w">>},
        c0009 =>
            {info, <<"During cache rebuild reached length of change_queue=~w">>}

    }).


%%%============================================================================
%%% External functions
%%%============================================================================

-spec log(atom(), list()) -> term().
%% @doc
%% Pick the log out of the logbase based on the reference 
log(LogReference, Subs) ->
    log(LogReference, Subs, undefined).

-spec log(atom(), list(), aae_util:log_levels()) -> term().
log(LogReference, Subs, undefined) ->
    log(LogReference, Subs, ?DEFAULT_LOG_LEVELS);
log(LogReference, Subs, LogLevels) ->
    leveled_log:log(LogReference, Subs, LogLevels, ?LOGBASE, tictacaae).

-spec log_timer(
    atom(), list(), erlang:timestamp(), aae_util:log_levels()) -> term().
log_timer(LogReference, Subs, StartTime, undefined) ->
    log_timer(LogReference, Subs, StartTime, ?DEFAULT_LOG_LEVELS);
log_timer(LogReference, Subs, StartTime, LogLevels) ->
    leveled_log:log_timer(
        LogReference, Subs, StartTime, LogLevels, ?LOGBASE, tictacaae
    ).

-spec get_opt(atom(), list()) -> any().
%% @doc 
%% Return an option from a KV list
get_opt(Key, Opts) ->
    get_opt(Key, Opts, undefined).

-spec get_opt(atom(), list(), any()) -> any().
%% @doc 
%% Return an option from a KV list, or a default if not present
get_opt(Key, Opts, Default) ->
    case proplists:get_value(Key, Opts) of
        undefined ->
            Default;
        Value ->
            Value
    end.


-spec make_binarykey(aae_keystore:bucket(), aae_keystore:key()) -> binary().
%% @doc
%% Convert Bucket and Key into a single binary 
make_binarykey({Type, Bucket}, Key)
                    when is_binary(Type), is_binary(Bucket), is_binary(Key) ->
    <<Type/binary, Bucket/binary, Key/binary>>;
make_binarykey(Bucket, Key) when is_binary(Bucket), is_binary(Key) ->
    <<Bucket/binary, Key/binary>>.

%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec safe_open(string()) -> {ok, binary()}|{error, atom()}.
safe_open(FileName) ->
    case filelib:is_file(FileName) of 
        true ->
            case file:read_file(FileName) of
                {ok, <<CRC32:32/integer, BinContent/binary>>} ->
                    case erlang:crc32(BinContent) of 
                        CRC32 ->
                            {ok, BinContent};
                        _ ->
                            {error, crc_wonky}
                    end;
                _ ->
                    {error, no_crc}
            end;        
        false ->
            {error, not_present}
    end.


%%%============================================================================
%%% Test
%%%============================================================================

flip_byte(Binary, Offset, Length) ->
    Byte1 = rand:uniform(Length) + Offset - 1,
    <<PreB1:Byte1/binary, A:8/integer, PostByte1/binary>> = Binary,
    case A of 
        0 ->
            <<PreB1:Byte1/binary, 255:8/integer, PostByte1/binary>>;
        _ ->
            <<PreB1:Byte1/binary, 0:8/integer, PostByte1/binary>>
    end.

test_key_generator(hash) -> 
    ValueFun = 
        fun() -> 
            V = rand:uniform(1000),
            <<Hash:32/integer, _Rest/binary>> 
                = crypto:hash(md5, <<V:32/integer>>),
            Hash
        end,
    internal_generator(ValueFun);
test_key_generator(v1) ->
    ValueFun = 
        fun() -> 
            Clock = [{rand:uniform(1000), rand:uniform(1000)}],
            BClock = term_to_binary(Clock),
            Size = rand:uniform(100000),
            SibCount = rand:uniform(3),
            <<Hash:32/integer, _Rest/binary>> = crypto:hash(md5, BClock),
            {Clock, Hash, Size, SibCount}
        end,
    internal_generator(ValueFun).

internal_generator(ValueFun) ->
    fun(I) ->
        Key = <<"Key", I:32/integer>>,
        Value = ValueFun(),
        {Key, Value}
    end.

clean_subdir(DirPath) ->
    case filelib:is_dir(DirPath) of
        true ->
            {ok, Files} = file:list_dir(DirPath),
            lists:foreach(fun(FN) ->
                                File = filename:join(DirPath, FN),
                                io:format("Attempting deletion ~s~n", [File]),
                                ok = 
                                    case filelib:is_dir(File) of 
                                        true -> 
                                            clean_subdir(File),
                                            file:del_dir(File);
                                        false -> 
                                            file:delete(File) 
                                    end,
                                io:format("Success deleting ~s~n", [File])
                                end,
                            Files);
        false ->
            ok
    end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

get_segmentid(B, K) ->
    Seg32 = leveled_tictac:keyto_segment32(make_binarykey(B, K)),
    leveled_tictac:get_segment(Seg32, ?TREE_SIZE).

flipbyte_test() ->
    Bin0 = <<0:256/integer>>,
    BinFB0 = flip_byte(Bin0, 0, 32),
    ?assertMatch(false, BinFB0 == Bin0),
    Bin1 = <<4294967295:32/integer>>,
    BinFB1 = flip_byte(Bin1, 1, 1),
    ?assertMatch(false, BinFB1 == Bin1).

clen_empty_subdir_test() ->
    FakePath = "test/foobar99",
    ok = clean_subdir(FakePath).

-endif.
