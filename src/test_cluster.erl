%%%-------------------------------------------------------------------
%%% @author tangyuntao
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 6月 2021 10:37 上午
%%%-------------------------------------------------------------------
-module(test_cluster).
-author("tangyuntao").

%% API
-export([start_instances/0]).

start_instances() ->
    Instances =
        [
            {app_config,
                % init_nodes => servers
                % support cluster
                [
                    {servers, [
                        {"redis-mqtt-config", 6379}
                    ]
                    },
                    {pool_size, 2},
                    {database, 0},
                    {pool_max_overflow, 2},
                    {password, "123456"}
                    % , {socket_options, [{send_timeout, 6000}]}
                    % , {tls, [{cacertfile, "ca.crt"}]}
                ]
            },
            {app_session,
                % init_nodes => servers
                % support cluster
                [
                    {servers, [
                        {"redis-mqtt-session", 6379}
                    ]
                    },
                    {pool_size, 2},
                    {database, 0},
                    {pool_max_overflow, 2},
                    {password, "123456"}
                    % , {socket_options, [{send_timeout, 6000}]}
                ]
            }
        ],
    [start(InstanceName, Options) || {InstanceName, Options} <- Instances].

start(InstanceName, Options) ->
    {ok, _}= eredis_cluster:start_instance(InstanceName, Options).
