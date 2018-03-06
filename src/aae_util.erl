%% -------- Overview ---------
%%
%% Centralised definition of log functions.  To make switching to Lager in 
%% the future a bit easier, and avoid repeated codes across modules

-module(aae_util).


-include_lib("eunit/include/eunit.hrl").

-export([log/3,
            log_timer/4,
            get_opt/2,
            get_opt/3]).

-export([clean_subdir/1,
            test_key_generator/0]).         

-define(LOG_LEVEL, [info, warn, error, critical]).
-define(DEFAULT_LOGBASE, [

    {"G0001",
        {info, "Generic log point"}},
    {"G0002",
        {info, "Generic log point with term ~w"}},
    {"D0001",
        {debug, "Generic debug log"}}
    ]).

-define(UNDEFINED_LOG, {"G0003", {info, "Undefined log reference"}}).

%%%============================================================================
%%% External functions
%%%============================================================================

-spec log(list(), list(), list()) -> ok.
%% @doc
%% Pick the log out of the logbase based on the reference 
log(LogReference, Subs, LogBase) ->
    log(LogReference, Subs, LogBase, ?LOG_LEVEL).

log(LogRef, Subs, LogBase, SupportedLogLevels) ->
    {LogRef0, {LogLevel, LogText}} = get_logreference(LogRef, LogBase),
    case lists:member(LogLevel, SupportedLogLevels) of
        true ->
            io:format(format_time()
                        ++ " " ++ LogRef0 ++ " ~w "
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
    log_timer(LogReference, Subs, StartTime, LogBase, ?LOG_LEVEL).

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

%%%============================================================================
%%% Internal functions
%%%============================================================================

get_logreference(LogRef, LogBase) ->
    case lists:keyfind(LogRef, 1, LogBase) of
        false ->
            case lists:keyfind(LogRef, 1, LogBase) of
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



test_key_generator() -> 
    fun(I) ->
        Key = <<"Key", I:32/integer>>,
        Value = random:uniform(100000),
        <<Hash:32/integer, _Rest/binary>> =
            crypto:hash(md5, <<Value:32/integer>>),
        {Key, Hash}
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

log_test() ->
    log("D0001", [], []),
    log_timer("D0001", [], os:timestamp(), []).

log_warn_test() ->
    ok = log("G0001", [], [], [warn, error]),
    ok = log("G8888", [], [], [info, warn, error]),
    ok = log_timer("G0001", [], os:timestamp(), [], [warn, error]),
    ok = log_timer("G8888", [], os:timestamp(), [], [info, warn, error]).



-endif.
