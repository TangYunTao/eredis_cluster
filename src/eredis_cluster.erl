-module(eredis_cluster).
-behaviour(application).

-export([start_instance/2, stop_instance/1]).

%% Application
-export([start/2]).
-export([stop/1]).

% API.
-export([start/0]). % Application Management.
-export([stop/0]).

% API.
-export([connect/2]). % Application Management.

% Generic redis call
-export([q/2, qp/2, qw/2, qk/3, qa/2, qmn/2, transaction/2, transaction/3]).

% Specific redis command implementation
-export([flushdb/1]).

 % Helper functions
-export([update_key/3]).
-export([update_hash_field/4]).
-export([optimistic_locking_transaction/4]).
-export([eval/5]).

-include("eredis_cluster.hrl").

%% start | stop pool
-spec start() -> ok | {error, Reason::term()}.
start() ->
    application:start(?MODULE).

-spec stop() -> ok | {error, Reason::term()}.
stop() ->
    application:stop(?MODULE).

start(_Type, _Args) ->
    eredis_cluster_sup:start_link().

stop(_State) ->
    application:stop(?MODULE).

%% TODO
%%-spec start_instance(StartType::application:start_type(), StartArgs::term()) ->
%%    {ok, pid()}.

%%  start | stop instance server
-spec start_instance(InstanceName::atom(), StartArgs::term()) ->
    {ok, pid()}.
start_instance(InstanceName, Opts) ->
    eredis_cluster_sup:start_cluster_monitor_child(InstanceName, Opts).

-spec stop_instance(InstanceName::atom()) -> ok.
stop_instance(InstanceName) ->
    eredis_cluster_sup:stop_cluster_monitor_child(InstanceName).


%% =============================================================================
%% @doc Connect to a set of init node, useful if the cluster configuration is
%% not known at startup
%% @end
%% =============================================================================
-spec connect(instance_name(), InitServers::term()) -> Result::term().
connect(InstanceName, InitServers) ->
    eredis_cluster_monitor:connect(InstanceName, InitServers).

%% =============================================================================
%% @doc Wrapper function to execute a pipeline command as a transaction Command
%% (it will add MULTI and EXEC command)
%% @end
%% =============================================================================
-spec transaction(instance_name(), redis_pipeline_command()) -> redis_transaction_result().
transaction(InstanceName, Commands) when is_atom(InstanceName) ->
    Result = q(InstanceName, [["multi"]| Commands] ++ [["exec"]]),
    lists:last(Result).

%% =============================================================================
%% @doc Execute a function on a pool worker. This function should be use when
%% transaction method such as WATCH or DISCARD must be used. The pool used to
%% execute the transaction is specified by giving a key that this pool is
%% containing.
%% @end
%% =============================================================================
-spec transaction(instance_name(),fun((Worker::pid()) -> redis_result()), anystring()) -> any().
transaction(InstanceName, Transaction, PoolKey) ->
    Slot = get_key_slot(PoolKey),
    transaction(InstanceName, Transaction, Slot, undefined, 0).

transaction(InstanceName, Transaction, Slot, undefined, _) ->
    query(InstanceName, Transaction, Slot, 0);
transaction(InstanceName, Transaction, Slot, ExpectedValue, Counter) ->
    case query(InstanceName, Transaction, Slot, 0) of
        ExpectedValue ->
            transaction(InstanceName, Transaction, Slot, ExpectedValue, Counter - 1);
        {ExpectedValue, _} ->
            transaction(InstanceName, Transaction, Slot, ExpectedValue, Counter - 1);
        Payload ->
            Payload
    end.

%% =============================================================================
%% @doc Multi node query
%% @end
%% =============================================================================
-spec qmn(instance_name(), redis_pipeline_command()) -> redis_pipeline_result().
qmn(InstanceName, Commands) -> qmn(InstanceName, Commands, 0).

qmn(_, _, ?REDIS_CLUSTER_REQUEST_TTL) ->
    {error, no_connection};
qmn(InstanceName, Commands, Counter) ->
    %% Throttle retries
    throttle_retries(Counter),

    {CommandsByPools, MappingInfo, Version} = split_by_pools(InstanceName, Commands),
    case qmn2(InstanceName, CommandsByPools, MappingInfo, [], Version) of
        retry -> qmn(InstanceName, Commands, Counter + 1);
        Res -> Res
    end.

qmn2(InstanceName, [{Pool, PoolCommands} | T1], [{Pool, Mapping} | T2], Acc, Version) ->
    Transaction = fun(Worker) -> qw(Worker, PoolCommands) end,
    Result = eredis_cluster_pool:transaction(Pool, Transaction),
    case handle_transaction_result(InstanceName, Result, Version, check_pipeline_result) of
        retry -> retry;
        Res ->
            MappedRes = lists:zip(Mapping,Res),
            qmn2(InstanceName, T1, T2, MappedRes ++ Acc, Version)
    end;
qmn2(_, [], [], Acc, _) ->
    SortedAcc =
        lists:sort(
            fun({Index1, _},{Index2, _}) ->
                Index1 < Index2
            end, Acc),
    [Res || {_,Res} <- SortedAcc].

split_by_pools(InstanceName, Commands) ->
    State = eredis_cluster_monitor:get_state(InstanceName),
    split_by_pools(Commands, 1, [], [], State).

split_by_pools([Command | T], Index, CmdAcc, MapAcc, State) ->
    Key = get_key_from_command(Command),
    Slot = get_key_slot(Key),
    {Pool, _Version} = eredis_cluster_monitor:get_pool_by_slot(Slot, State),
    {NewAcc1, NewAcc2} =
        case lists:keyfind(Pool, 1, CmdAcc) of
            false ->
                {[{Pool, [Command]} | CmdAcc], [{Pool, [Index]} | MapAcc]};
            {Pool, CmdList} ->
                CmdList2 = [Command | CmdList],
                CmdAcc2  = lists:keydelete(Pool, 1, CmdAcc),
                {Pool, MapList} = lists:keyfind(Pool, 1, MapAcc),
                MapList2 = [Index | MapList],
                MapAcc2  = lists:keydelete(Pool, 1, MapAcc),
                {[{Pool, CmdList2} | CmdAcc2], [{Pool, MapList2} | MapAcc2]}
        end,
    split_by_pools(T, Index+1, NewAcc1, NewAcc2, State);
split_by_pools([], _Index, CmdAcc, MapAcc, State) ->
    CmdAcc2 = [{Pool, lists:reverse(Commands)} || {Pool, Commands} <- CmdAcc],
    MapAcc2 = [{Pool, lists:reverse(Mapping)} || {Pool, Mapping} <- MapAcc],
    {CmdAcc2, MapAcc2, eredis_cluster_monitor:get_state_version(State)}.

%% =============================================================================
%% @doc Wrapper function for command using pipelined commands
%% @end
%% =============================================================================
-spec qp(instance_name(), redis_pipeline_command()) -> redis_pipeline_result().
qp(InstanceName, Commands) -> q(InstanceName, Commands).

%% =============================================================================
%% @doc This function execute simple or pipelined command on a single redis node
%% the node will be automatically found according to the key used in the command
%% @end
%% =============================================================================
-spec q(instance_name(), redis_command()) -> redis_result().
q(InstanceName, Command) ->
    query(InstanceName, Command).

-spec qk(instance_name(), redis_command(), bitstring()) -> redis_result().
qk(InstanceName, Command, PoolKey) ->
    query(InstanceName, Command, PoolKey).

query(InstanceName, Command) ->
    PoolKey = get_key_from_command(Command),
    query(InstanceName, Command, PoolKey).

query(_, _, undefined) ->
    {error, invalid_cluster_command};
query(InstanceName, Command, PoolKey) ->
    Slot = get_key_slot(PoolKey),
    Transaction = fun(Worker) -> qw(Worker, Command) end,
    query(InstanceName, Transaction, Slot, 0).

query(_, _, _, ?REDIS_CLUSTER_REQUEST_TTL) ->
    {error, no_connection};
query(InstanceName, Transaction, Slot, Counter) ->
    %% Throttle retries
    throttle_retries(Counter),

    {Pool, Version} = eredis_cluster_monitor:get_pool_by_slot(InstanceName, Slot),

    Result = eredis_cluster_pool:transaction(Pool, Transaction),
    case handle_transaction_result(InstanceName, Result, Version) of
        retry -> query(InstanceName, Transaction, Slot, Counter + 1);
        Result -> Result
    end.

handle_transaction_result(InstanceName, Result, Version) ->
    case Result of
       % If we detect a node went down, we should probably refresh the slot
        % mapping.
        {error, no_connection} ->
            eredis_cluster_monitor:refresh_mapping(InstanceName, Version),
            retry;

        % If the tcp connection is closed (connection timeout), the redis worker
        % will try to reconnect, thus the connection should be recovered for
        % the next request. We don't need to refresh the slot mapping in this
        % case
        {error, tcp_closed} ->
            retry;

        % Redis explicitly say our slot mapping is incorrect, we need to refresh
        % it
        {error, <<"MOVED ", _/binary>>} ->
            eredis_cluster_monitor:refresh_mapping(InstanceName, Version),
            retry;

        Payload ->
            Payload
    end.
handle_transaction_result(InstanceName, Result, Version, check_pipeline_result) ->
    case handle_transaction_result(InstanceName, Result, Version) of
       retry -> retry;
       Payload when is_list(Payload) ->
           Pred = fun({error, <<"MOVED ", _/binary>>}) -> true;
                    (_) -> false
                 end,
           case lists:any(Pred, Payload) of
               false -> Payload;
               true ->
                   eredis_cluster_monitor:refresh_mapping(InstanceName, Version),
                   retry
           end;
       Payload -> Payload
    end.

-spec throttle_retries(integer()) -> ok.
throttle_retries(0) -> ok;
throttle_retries(_) -> timer:sleep(?REDIS_RETRY_DELAY).

%% =============================================================================
%% @doc Update the value of a key by applying the function passed in the
%% argument. The operation is done atomically
%% @end
%% =============================================================================
-spec update_key(instance_name(), Key::anystring(), UpdateFunction::fun((any()) -> any())) ->
    redis_transaction_result().
update_key(InstanceName, Key, UpdateFunction) ->
    UpdateFunction2 = fun(GetResult) ->
        {ok, Var} = GetResult,
        UpdatedVar = UpdateFunction(Var),
        {[["SET", Key, UpdatedVar]], UpdatedVar}
    end,
    case optimistic_locking_transaction(InstanceName, Key, ["GET", Key], UpdateFunction2) of
        {ok, {_, NewValue}} ->
            {ok, NewValue};
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Update the value of a field stored in a hash by applying the function
%% passed in the argument. The operation is done atomically
%% @end
%% =============================================================================
-spec update_hash_field(instance_name(), Key::anystring(), Field::anystring(),
    UpdateFunction::fun((any()) -> any())) -> redis_transaction_result().
update_hash_field(InstanceName, Key, Field, UpdateFunction) ->
    UpdateFunction2 = fun(GetResult) ->
        {ok, Var} = GetResult,
        UpdatedVar = UpdateFunction(Var),
        {[["HSET", Key, Field, UpdatedVar]], UpdatedVar}
    end,
    case optimistic_locking_transaction(InstanceName, Key, ["HGET", Key, Field], UpdateFunction2) of
        {ok, {[FieldPresent], NewValue}} ->
            {ok, {FieldPresent, NewValue}};
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Optimistic locking transaction helper, based on Redis documentation :
%% http://redis.io/topics/transactions
%% @end
%% =============================================================================
-spec optimistic_locking_transaction(instance_name(), Key::anystring(), redis_command(),
    UpdateFunction::fun((redis_result()) -> redis_pipeline_command())) ->
        {redis_transaction_result(), any()}.
optimistic_locking_transaction(InstanceName, WatchedKey, GetCommand, UpdateFunction) ->
    Slot = get_key_slot(WatchedKey),
    Transaction = fun(Worker) ->
        %% Watch given key
        qw(Worker,["WATCH", WatchedKey]),
        %% Get necessary information for the modifier function
        GetResult = qw(Worker, GetCommand),
        %% Execute the pipelined command as a redis transaction
        {UpdateCommand, Result} = case UpdateFunction(GetResult) of
            {Command, Var} ->
                {Command, Var};
            Command ->
                {Command, undefined}
        end,
        RedisResult = qw(Worker, [["MULTI"]] ++ UpdateCommand ++ [["EXEC"]]),
        {lists:last(RedisResult), Result}
    end,
	case transaction(InstanceName, Transaction, Slot, {ok, undefined}, ?OL_TRANSACTION_TTL) of
        {{ok, undefined}, _} ->
            {error, resource_busy};
        {{ok, TransactionResult}, UpdateResult} ->
            {ok, {TransactionResult, UpdateResult}};
        {Error, _} ->
            Error
    end.

%% =============================================================================
%% @doc Eval command helper, to optimize the query, it will try to execute the
%% script using its hashed value. If no script is found, it will load it and
%% try again.
%% @end
%% =============================================================================
-spec eval(instance_name(), bitstring(), bitstring(), [bitstring()], [bitstring()]) ->
    redis_result().
eval(InstanceName, Script, ScriptHash, Keys, Args) ->
    KeyNb = length(Keys),
    EvalShaCommand = ["EVALSHA", ScriptHash, KeyNb] ++ Keys ++ Args,
    Key = if
        KeyNb == 0 -> "A"; %Random key
        true -> hd(Keys)
    end,

    case qk(InstanceName, EvalShaCommand, Key) of
        {error, <<"NOSCRIPT", _/binary>>} ->
            LoadCommand = ["SCRIPT", "LOAD", Script],
            [_, Result] = qk(InstanceName, [LoadCommand, EvalShaCommand], Key),
            Result;
        Result ->
            Result
    end.

%% =============================================================================
%% @doc Perform a given query on all node of a redis cluster
%% @end
%% =============================================================================
-spec qa(instance_name(), redis_command()) -> ok | {error, Reason::bitstring()}.
qa(InstanceName, Command) ->
    Pools = eredis_cluster_monitor:get_all_pools(InstanceName),
    Transaction = fun(Worker) -> qw(Worker, Command) end,
    [eredis_cluster_pool:transaction(Pool, Transaction) || Pool <- Pools].

%% =============================================================================
%% @doc Wrapper function to be used for direct call to a pool worker in the
%% function passed to the transaction/2 method
%% @end
%% =============================================================================
%%-spec qw(Worker::pid(), redis_command()) -> redis_result().
%%qw(Worker, Command) ->
%%    eredis_cluster_pool_worker:query(Worker, Command).

-spec qw(instance_name(), redis_command()) -> redis_result().
qw(InstanceName, Command) ->
    eredis_cluster_pool_worker:query(InstanceName, Command).

%% =============================================================================
%% @doc Perform flushdb command on each node of the redis cluster
%% @end
%% =============================================================================
-spec flushdb(instance_name()) -> ok | {error, Reason::bitstring()}.
flushdb(InstanceName) ->
    Result = qa(InstanceName, ["FLUSHDB"]),
    case proplists:lookup(error,Result) of
        none ->
            ok;
        Error ->
            Error
    end.

%% =============================================================================
%% @doc Return the hash slot from the key
%% @end
%% =============================================================================
-spec get_key_slot(Key::anystring()) -> Slot::integer().
get_key_slot(Key) when is_bitstring(Key) ->
    get_key_slot(bitstring_to_list(Key));
get_key_slot(Key) ->
    KeyToBeHased = case string:chr(Key,${) of
        0 ->
            Key;
        Start ->
            case string:chr(string:substr(Key,Start+1),$}) of
                0 ->
                    Key;
                Length ->
                    if
                        Length =:= 1 ->
                            Key;
                        true ->
                            string:substr(Key,Start+1,Length-1)
                    end
            end
    end,
    eredis_cluster_hash:hash(KeyToBeHased).

%% =============================================================================
%% @doc Return the first key in the command arguments.
%% In a normal query, the second term will be returned
%%
%% If it is a pipeline query we will use the second term of the first term, we
%% will assume that all keys are in the same server and the query can be
%% performed
%%
%% If the pipeline query starts with multi (transaction), we will look at the
%% second term of the second command
%%
%% For eval and evalsha command we will look at the fourth term.
%%
%% For commands that don't make sense in the context of cluster
%% return value will be undefined.
%% @end
%% =============================================================================
-spec get_key_from_command(redis_command()) -> string() | undefined.
get_key_from_command([[X|Y]|Z]) when is_bitstring(X) ->
    get_key_from_command([[bitstring_to_list(X)|Y]|Z]);
get_key_from_command([[X|Y]|Z]) when is_list(X) ->
    case string:to_lower(X) of
        "multi" ->
            get_key_from_command(Z);
        _ ->
            get_key_from_command([X|Y])
    end;
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term1) ->
    get_key_from_command([bitstring_to_list(Term1),Term2|Rest]);
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term2) ->
    get_key_from_command([Term1,bitstring_to_list(Term2)|Rest]);
get_key_from_command([Term1,Term2|Rest]) ->
    case string:to_lower(Term1) of
        "info" ->
            undefined;
        "config" ->
            undefined;
        "shutdown" ->
            undefined;
        "slaveof" ->
            undefined;
        "eval" ->
            get_key_from_rest(Rest);
        "evalsha" ->
            get_key_from_rest(Rest);
        _ ->
            Term2
    end;
get_key_from_command(_) ->
    undefined.

%% =============================================================================
%% @doc Get key for command where the key is in th 4th position (eval and
%% evalsha commands)
%% @end
%% =============================================================================
-spec get_key_from_rest([anystring()]) -> string() | undefined.
get_key_from_rest([_,KeyName|_]) when is_bitstring(KeyName) ->
    bitstring_to_list(KeyName);
get_key_from_rest([_,KeyName|_]) when is_list(KeyName) ->
    KeyName;
get_key_from_rest(_) ->
    undefined.
