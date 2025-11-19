%%%============================================================================
%%% Non-configurable defaults
%%%============================================================================

-define(TREE_SIZE, large).
-define(MAGIC, 53).

%%%============================================================================
%%% Tags
%%%============================================================================
-define(HEAD_TAG, h).
-define(RIAK_TAG, o_rkv).

-if(?OTP_RELEASE < 26).
-type dynamic() :: any().
-endif.

%%%============================================================================
%%% Helper Functions
%%%============================================================================

-define(LOG_LOCATION, #{
    mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY},
    line => ?LINE,
    file => ?FILE
}).

-define(STD_LOG(LogRef, Subs),
    ?STD_LOG_INT(
        element(1, aae_util:get_log(LogRef)),
        LogRef,
        Subs,
        leveled_log:get_opts()
    )
).

-define(STD_LOG_INT(LogLevel, LogRef, Subs, LogOpts),
    case
        logger:allow(LogLevel, ?MODULE) andalso
            leveled_log:should_i_log(LogLevel, LogRef, LogOpts)
    of
        true ->
            erlang:apply(
                logger,
                macro_log,
                [
                    ?LOG_LOCATION
                    | aae_util:log(LogLevel, LogRef, LogOpts, Subs)
                ]
            );
        false ->
            ok
    end
).
