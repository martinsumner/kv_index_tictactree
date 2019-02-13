%% -------- Overview ---------
%%
%% Centralised definition of log functions.  To make switching to Lager in 
%% the future a bit easier, and avoid repeated codes across modules

-module(aae_util).

-include("include/aae.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([log/3,
            log/4,
            log_timer/4,
            log_timer/5,
            get_opt/2,
            get_opt/3,
            make_binarykey/2,
            min_loglevel/1]).

-export([clean_subdir/1,
            test_key_generator/1,
            flip_byte/3,
            get_segmentid/2]).         

-define(DEFAULT_LOGLEVEL, [info, warn, error, critical]).
-define(DEFAULT_LOGBASE, [

    {"G0001",
        {info, "Generic log point"}},
    {"G0002",
        {info, "Generic log point with term ~w"}},
    {"D0001",
        {debug, "Generic debug log"}}
    ]).

-define(UNDEFINED_LOG, {"G0003", {info, "Undefined log reference"}}).

-type log_level() :: debug|info|warn|error|critical.
-type log_levels() :: list(log_level()).

-export_type([log_levels/0]).

%%%============================================================================
%%% External functions
%%%============================================================================

-spec log(list(), list(), list()) -> ok.
%% @doc
%% Pick the log out of the logbase based on the reference 
log(LogReference, Subs, LogBase) ->
    log(LogReference, Subs, LogBase, ?DEFAULT_LOGLEVEL).

log(LogRef, Subs, LogBase, undefined) ->
    log(LogRef, Subs, LogBase, ?DEFAULT_LOGLEVEL);
log(LogRef, Subs, LogBase, SupportedLogLevels) ->
    {LogRef0, {LogLevel, LogText}} = get_logreference(LogRef, LogBase),
    case lists:member(LogLevel, SupportedLogLevels) of
        true ->
            io:format(format_time() ++ " "
                        ++ atom_to_list(LogLevel) ++ " "
                        ++ LogRef0 ++ " "
                        ++ "~w "
                        ++ LogText ++ "~n",
                        [self()|Subs]);
        false ->
            ok
    end.


-spec log_timer(list(), list(), tuple(), list()) -> ok.
%% @doc
%% Pick the log out of the logbase based on the reference, and also log
%% the time between the strat time and making the log
log_timer(LogReference, Subs, StartTime, LogBase) ->
    log_timer(LogReference, Subs, StartTime, LogBase, ?DEFAULT_LOGLEVEL).

log_timer(LogRef, Subs, StartTime, LogBase, undefined) ->
    log_timer(LogRef, Subs, StartTime, LogBase, ?DEFAULT_LOGLEVEL);
log_timer(LogRef, Subs, StartTime, LogBase, SupportedLogLevels) ->
    {LogRef0, {LogLevel, LogText}} = get_logreference(LogRef, LogBase),
    case lists:member(LogLevel, SupportedLogLevels) of
        true ->
            MicroS = timer:now_diff(os:timestamp(), StartTime),
            {Unit, Time} = case MicroS of
                                MicroS when MicroS < 1000 ->
                                    {"microsec", MicroS};
                                MicroS ->
                                    {"ms", MicroS div 1000}
                            end,
            io:format(format_time()
                            ++ " " ++ LogRef0 ++ " ~w "
                            ++ LogText
                            ++ " with time taken ~w " ++ Unit ++ "~n",
                        [self()|Subs] ++ [Time]);
        false ->
            ok
    end.


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

-spec min_loglevel(log_levels()|undefined) -> log_level().
%% @doc
%% Return the lowest log level to be used in leveled startup
min_loglevel(undefined) ->
    info;
min_loglevel([MinLevel|_Rest]) ->
    MinLevel.

%%%============================================================================
%%% Internal functions
%%%============================================================================

get_logreference(LogRef, LogBase) ->
    case lists:keyfind(LogRef, 1, LogBase) of
        false ->
            case lists:keyfind(LogRef, 1, ?DEFAULT_LOGBASE) of
                false ->
                    ?UNDEFINED_LOG;
                Log ->
                    Log
            end;
        Log ->
            Log 
    end.

format_time() ->
    format_time(localtime_ms()).

localtime_ms() ->
    {_, _, Micro} = Now = os:timestamp(),
    {Date, {Hours, Minutes, Seconds}} = calendar:now_to_local_time(Now),
    {Date, {Hours, Minutes, Seconds, Micro div 1000 rem 1000}}.

format_time({{Y, M, D}, {H, Mi, S, Ms}}) ->
    io_lib:format("~b-~2..0b-~2..0b", [Y, M, D]) ++ "T" ++
        io_lib:format("~2..0b:~2..0b:~2..0b.~3..0b", [H, Mi, S, Ms]).


%%%============================================================================
%%% Test
%%%============================================================================

flip_byte(Binary, Offset, Length) ->
    Byte1 = leveled_rand:uniform(Length) + Offset - 1,
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
            V = leveled_rand:uniform(1000),
            <<Hash:32/integer, _Rest/binary>> 
                = crypto:hash(md5, <<V:32/integer>>),
            Hash
        end,
    internal_generator(ValueFun);
test_key_generator(v1) ->
    ValueFun = 
        fun() -> 
            Clock = [{leveled_rand:uniform(1000), leveled_rand:uniform(1000)}],
            BClock = term_to_binary(Clock),
            Size = leveled_rand:uniform(100000),
            SibCount = leveled_rand:uniform(3),
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

get_segmentid(B, K) ->
    Seg32 = leveled_tictac:keyto_segment32(make_binarykey(B, K)),
    leveled_tictac:get_segment(Seg32, ?TREE_SIZE).

log_test() ->
    log("D0001", [], []),
    log_timer("G0001", [], os:timestamp(), []).

log_warn_test() ->
    ok = log("G0001", [], [], [warn, error]),
    ok = log("G8888", [], [], [info, warn, error]),
    SW = os:timestamp(),
    ok = log_timer("G0001", [], SW, [], [warn, error]),
    ok = log_timer("G8888", [], SW, [], [info, warn, error]),
    timer:sleep(2),
    ok = log_timer("G0001", [], SW, [], [info, warn, error]).

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
