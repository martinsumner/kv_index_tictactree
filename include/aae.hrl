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
    erlang:apply(
        logger,
        macro_log,
        [?LOG_LOCATION | aae_util:log(LogRef, Subs)]
    )
).

-define(TMR_LOG(LogRef, Subs, StartTime),
    erlang:apply(
        logger,
        macro_log,
        [?LOG_LOCATION | aae_util:log_timer(LogRef, Subs, StartTime)]
    )
).
