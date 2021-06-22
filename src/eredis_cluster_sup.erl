%%%-------------------------------------------------------------------
%%% @author tangyuntao
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. 6月 2021 3:39 下午
%%%-------------------------------------------------------------------
-module(eredis_cluster_sup).
-author("tangyuntao").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-export([start_cluster_monitor_child/2, stop_cluster_monitor_child/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Starts the supervisor
-spec(start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @private
%% @doc Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]}}
    | ignore | {error, Reason :: term()}).
init([]) ->

    SupFlags = #{strategy => one_for_one,
        intensity => 1,
        period => 5},

    ChildSpec = #{id => eredis_cluster_pool,
        start => {eredis_cluster_pool, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [dynamic]},

    {ok, {SupFlags, [ChildSpec]}}.


start_cluster_monitor_child(InstanceName, Params) ->
    ChildSpec = #{id => name(InstanceName),
        start => {eredis_cluster_monitor, start_link, [InstanceName, Params]},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [eredis_cluster_monitor]},
    supervisor:start_child(?MODULE, ChildSpec).

stop_cluster_monitor_child(InstanceName) ->
    case supervisor:terminate_child(?MODULE, name(InstanceName)) of
        ok -> supervisor:delete_child(?MODULE, name(InstanceName));
        Error -> Error
    end.


name(Name) when is_list(Name) ->
    Name1 = Name ++ "_eredis_cluster_monitor",
    case catch(erlang:list_to_existing_atom(Name1)) of
        {'EXIT', _} -> erlang:list_to_atom(Name1);
        Atom when is_atom(Atom) -> Atom
    end;
name(Name) when is_atom(Name) ->
    name(atom_to_list(Name)).
%%%===================================================================
%%% Internal functions
%%%===================================================================
