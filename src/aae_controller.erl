%% -------- Overview ---------
%%
%% An AAE controller, that should receive updates from a KV store along with
%% requests for:
%% - Tictac trees, and parts thereof
%% - Keys and clocks by portion of the tree
%% - Snapshots of the KeyStore to support async object folds
%% - Periodic requests to rebuild
%% - Requests to startup and shutdown
%%
%% 


-module(aae_controller).

-behaviour(gen_server).
-include("include/aae.hrl").

-export([init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3]).

-export([aae_start/5]).
            
-include_lib("eunit/include/eunit.hrl").

-define(STORE_PATH, "_keystore/").
-define(TREE_PATH, "_aaetree/").
-define(MEGA, 1000000).



-record(state, {key_store :: pid(),
                tree_caches :: tree_caches(),
                initiate_node_worker_fun,
                reliable = false :: boolean(),
                next_rebuild :: erlang:timestamp()}).

-record(options, {keystore_type :: keystore_type(),
                    startup_storestate :: startup_storestate(),
                    rebuild_schedule :: rebuild_schedule(),
                    index_ns :: list(responsible_preflist()),
                    root_path :: list()}).

-type responsible_preflist() :: {binary(), integer()}. 
        % The responsible preflist is a reference to the partition associated 
        % with an AAE requirement.  The preflist is a reference to the id of 
        % the head partition in preflist, and and n-val.
-type tree_caches() 
        :: list({responsible_preflist(), pid()}).
        % A map between the responsible_preflist reference and the tree_cache 
        % for that preflist
-type keystore_type() 
        :: {parallel, aae_keystore:supported_stores()}|{native, pid()}.
        % Key Store can be native (no separate AAE store required) or
        % parallel when a seperate Key Store is needed for AAE.  The Type
        % for parallel stores must be a supported KV store by the aae_keystore
        % module 
-type startup_storestate() 
        :: {boolean(), list()|none}.
        % On startup the current state of the store is passed for the aae 
        % controller to determine if the startup of the aae_controller has 
        % placed the system in a 'reliable' and consistent state between the
        % vnode store and the parallel key store.  The first element is the 
        % is_empty status of the vnode backend, and the second element is a 
        % GUID which was stored in the backend prior to the last controlled 
        % close.
-type rebuild_schedule() 
        :: {integer(), integer()}.
        % A rebuild schedule, the first integer being the minimum number of 
        % hours to wait between rebuilds.  The second integer is a number of 
        % seconds by which to jitter the rebuild.  The actual rebuild will be 
        % scheduled by adding a random integer number of seconds (between 0 
        % and the jitter value) to the minimum time

%%%============================================================================
%%% API
%%%============================================================================

-spec aae_start(keystore_type(), 
                startup_storestate(), 
                rebuild_schedule(),
                list(responsible_preflist()), 
                list()) -> {ok, pid()}.
%% @doc
%% Start an AAE controller 
aae_start(KeyStoreT, StartupState, RebuildSch, Preflists, RootPath) ->
    AAEopts =
        #options{keystore_type = KeyStoreT,
                    startup_storestate = StartupState,
                    rebuild_schedule = RebuildSch,
                    index_ns = Preflists,
                    root_path = RootPath},
    gen_server:start(?MODULE, [AAEopts], []).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    RootPath = Opts#options.root_path,
    {ok, State0} = 
        case Opts#options.keystore_type of 
            {parallel, StoreType} ->
                StoreRP = 
                    filename:join([RootPath, StoreType, ?STORE_PATH]),
                {ok, {LastRebuild, ShutdownGUID, IsEmpty}, Pid} =
                    aae_keystore:store_parallelstart(StoreRP, StoreType),
                case Opts#options.startup_storestate of 
                    {IsEmpty, ShutdownGUID} ->
                        RebuildTS = 
                            schedule_rebuild(LastRebuild, 
                                                Opts#options.rebuild_schedule),
                        {ok, #state{key_store = Pid, 
                                        next_rebuild = RebuildTS, 
                                        reliable = true}};
                    StoreState ->
                        aae_util:log("AAE01", 
                                        [StoreState, {IsEmpty, ShutdownGUID}],
                                        logs()),
                        {ok, #state{key_store = Pid, 
                                        next_rebuild = os:timestamp(), 
                                        reliable = false}}
                end;
            KeyStoreType ->
                aae_util:log("AAE02", [KeyStoreType], logs())
        end,
    {ok, State0}.

handle_call(_Msg, _From, State) ->
    {reply, not_implemented, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================

schedule_rebuild({MegaSecs, Secs, MicroSecs}, {MinHours, JitterSeconds}) ->
    NewSecs = 
        MegaSecs * ?MEGA 
            + Secs 
            + MinHours * 3600 + random:uniform(JitterSeconds),
    {NewSecs div ?MEGA, NewSecs rem ?MEGA, MicroSecs}.



%%%============================================================================
%%% log definitions
%%%============================================================================

-spec logs() -> list(tuple()).
%% @doc
%% Define log lines for this module
logs() ->
    [{"AAE01", 
            {warn, "AAE Key Store rebuild required on startup due to " 
                    ++ "mismatch between vnode store state ~w "
                    ++ "and AAE key store state of ~w "
                    ++ "maybe restart with node excluded from coverage "
                    ++ "queries to improve AAE operation until rebuild "
                    ++ "is complete"}},
        {"AAE02",
            {error, "Unexpected KeyStore type information passed ~w"}}
    
    ].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-endif.


