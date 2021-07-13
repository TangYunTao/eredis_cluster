-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
%%-export([create/2]).
-export([create/7, create/8]).
-export([stop/1]).
-export([transaction/2]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("eredis_cluster.hrl").

create(Host, Port, DataBase, Password, Size, MaxOverflow, ReconnectInterVal) ->
    create(Host, Port, DataBase, Password, Size, MaxOverflow, ReconnectInterVal, []).
create(Host, Port, DataBase, Password, Size, MaxOverflow, ReconnectInterVal, Options) ->
    PoolName = get_name(Host, Port),
    case whereis(PoolName) of
        undefined ->
            WorkerArgs = [{host, Host},
                {port, Port},
                {database, DataBase},
                {password, Password}],
            PoolArgs = [{name, {local, PoolName}},
                {worker_module, eredis_cluster_pool_worker},
                {size, Size},
                {max_overflow, MaxOverflow},
                {reconnect_interval, ReconnectInterVal}],
            ChildSpec = poolboy:child_spec(PoolName, PoolArgs,
                case Options of
                    [] -> WorkerArgs;
                    _ -> WorkerArgs ++ [{options, Options}]
                end),
            {Result, _} = supervisor:start_child(?MODULE,ChildSpec),
            {Result, PoolName};
        _ ->
            {ok, PoolName}
    end.

-spec transaction(PoolName::atom(), fun((Worker::pid()) -> redis_result())) ->
    redis_result().
transaction(PoolName, Transaction) ->
    try
        poolboy:transaction(PoolName, Transaction)
    catch
        exit:_ ->
            {error, no_connection}
    end.

-spec stop(PoolName::atom()) -> ok.
stop(PoolName) ->
    supervisor:terminate_child(?MODULE,PoolName),
    supervisor:delete_child(?MODULE,PoolName),
    ok.

-spec get_name(Host::string(), Port::integer()) -> PoolName::atom().

get_name(Host, Port) when is_list(Host) andalso is_list(Port)  ->
    Name1 = Host ++ "#" ++ Port,
    case catch(erlang:list_to_existing_atom(Name1)) of
        {'EXIT', _} -> erlang:list_to_atom(Name1);
        Atom when is_atom(Atom) -> Atom
    end;
get_name(Host, Port) when is_list(Host) andalso is_integer(Port) ->
    get_name(Host, erlang:integer_to_list(Port));
get_name(Host, Port) when is_list(Port)  ->
    get_name(erlang:atom_to_list(Host), Port).
%%get_name(Host, Port)  ->
%%    get_name(erlang:atom_to_list(Host), erlang:integer_to_list(Port)).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
	-> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    ets:new(?INSTANCES, [public, set, named_table, {read_concurrency, true}]),
    ets:new(?SLOTS, [public, set, named_table, {read_concurrency, true}]),
	{ok, {{one_for_one, 1, 5}, []}}.
