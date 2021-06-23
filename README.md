# eredis_cluster
[![Travis](https://img.shields.io/travis/adrienmo/eredis_cluster.svg?branch=master&style=flat-square)](https://travis-ci.org/adrienmo/eredis_cluster)
[![Hex.pm](https://img.shields.io/hexpm/v/eredis_cluster.svg?style=flat-square)](https://hex.pm/packages/eredis_cluster)

## Description

eredis_cluster is a wrapper for eredis to support cluster mode of redis 3.0.0+

## TODO

- Improve test suite to demonstrate the case where redis cluster is crashing,
resharding, recovering...

## Compilation && Test

The directory contains a Makefile and rebar3

	make
	make test

## Configuration

To configure the redis cluster, you can use an application variable (probably in
your app.config):


    {eredis_cluster,
        [
            {InstanceName1,
                [
                    {init_nodes,[
                        {"127.0.0.1", 30001},
                        {"127.0.0.1", 30002}
                    ]
                    },
                    {pool_size, 2},
                    {database, 0},
                    {pool_max_overflow, 2},
                    {password, "123456"}
                ]
            },
            {InstanceName2,
                [
                    {init_nodes,[
                        {"127.0.0.2", 30001},
                        {"127.0.0.2", 30002}
                    ]
                    },
                    {pool_size, 2},
                    {database, 0},
                    {pool_max_overflow, 2},
                    {password, "123456"}
                ]
            }
        ]

    }

You don't need to specify all nodes of your configuration as eredis_cluster will
retrieve them through the command `CLUSTER SLOTS` at runtime.

## Usage

```erlang
%% Start the application
eredis_cluster:start().

%% Simple command
eredis_cluster:q(InstanceName, ["GET","abc"]).

%% Pipeline
eredis_cluster:qp(InstanceName, [["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]).

%% Pipeline in multiple node (keys are sorted by node, a pipeline request is
%% made on each node, then the result is aggregated and returned. The response
%% keep the command order
eredis_cluster:qmn(InstanceName, [["GET", "a"], ["GET", "b"], ["GET", "c"]]).

%% Transaction
eredis_cluster:transaction(InstanceName, [["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]).

%% Transaction Function
Function = fun(Worker) ->
    eredis_cluster:qw(Worker, ["WATCH", "abc"]),
    {ok, Var} = eredis_cluster:qw(Worker, ["GET", "abc"]),

    %% Do something with Var %%
    Var2 = binary_to_integer(Var) + 1,

    {ok, Result} = eredis_cluster:qw(Worker,[["MULTI"], ["SET", "abc", Var2], ["EXEC"]]),
    lists:last(Result)
end,
eredis_cluster:transaction(InstanceName, Function, "abc").

%% Optimistic Locking Transaction
Function = fun(GetResult) ->
    {ok, Var} = GetResult,
    Var2 = binary_to_integer(Var) + 1,
    {[["SET", Key, Var2]], Var2}
end,
Result = optimistic_locking_transaction(InstanceName, Key, ["GET", Key], Function),
{ok, {TransactionResult, CustomVar}} = Result

%% Atomic Key update
Fun = fun(Var) -> binary_to_integer(Var) + 1 end,
eredis_cluster:update_key("abc", InstanceName, Fun).

%% Atomic Field update
Fun = fun(Var) -> binary_to_integer(Var) + 1 end,
eredis_cluster:update_hash_field("abc", InstanceName, "efg", Fun).

%% Eval script, both script and hash are necessary to execute the command,
%% the script hash should be precomputed at compile time otherwise, it will
%% execute it at each request. Could be solved by using a macro though.  
Script = "return redis.call('set', KEYS[1], ARGV[1]);",
ScriptHash = "4bf5e0d8612687699341ea7db19218e83f77b7cf",
eredis_cluster:eval(InstanceName, Script, ScriptHash, ["abc"], ["123"]).

%% Flush DB
%% eredis_cluster:flushdb().

%% Query on all cluster server
eredis_cluster:qa(InstanceName, ["FLUSHDB"]).

%% Execute a query on the server containing the key "TEST"
eredis_cluster:qk(InstanceName, ["FLUSHDB"], "TEST").
```
