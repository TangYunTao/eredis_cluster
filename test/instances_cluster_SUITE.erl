%%%-------------------------------------------------------------------
%%% @author tangyuntao
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 6月 2021 10:37 上午
%%%-------------------------------------------------------------------
-module(instances_cluster_SUITE).
-author("tangyuntao").

-include_lib("eunit/include/eunit.hrl").

-define(InstanceName1, app_config).
-define(InstanceName2, app_session).

-export([all/0]).

-export([test_get_set/0, test_binary/0, test_delete/0, test_pip_line/0,
    test_multi_node_get/0, test_multi_node/0, test_transaction/0, test_eval_key/0,
    test_eval_sha/0, test_bitstring_support/0, test_atomic_get_set/0, test_atomic_hget_set/0, test_eval/0]).


all() ->
    start_instances(),
    test_get_set(),
    test_binary(),
    test_delete(),
    test_pip_line(),

    test_multi_node_get(),
    test_multi_node(),
    test_transaction(),

    test_eval_key(),
    test_eval_sha(),
    test_bitstring_support(),
    test_atomic_get_set(),
    test_atomic_hget_set(),
    test_eval(),
    stop().

start_instances() ->
    eredis_cluster:start(),
    Instances =
        [
            {?InstanceName1,
                [
                    {init_nodes, [
                        {"127.0.0.1", 30001},
                        {"127.0.0.1", 30002}
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
            {?InstanceName2,
                [
                    {init_nodes, [
                        {"127.0.0.2", 30001},
                        {"127.0.0.2", 30002}
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

stop() ->
    eredis_cluster_sup:stop_cluster_monitor_child(app_config),
    eredis_cluster_sup:stop_cluster_monitor_child(app_session).

test_get_set() ->
    ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?InstanceName1, ["SET", "key", "value"])),
    ?assertEqual({ok, <<"value">>}, eredis_cluster:q(?InstanceName1, ["GET", "key"])),
    ?assertEqual({ok, undefined}, eredis_cluster:q(?InstanceName1, ["GET", "nonexists"])),
%%                test instance 2
    ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?InstanceName2, ["SET", "key", "value"])),
    ?assertEqual({ok, <<"value">>}, eredis_cluster:q(?InstanceName2, ["GET", "key"])),
    ?assertEqual({ok, undefined}, eredis_cluster:q(?InstanceName2, ["GET", "nonexists"])).

test_binary() ->
%%                test instance 1
    ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?InstanceName1, [<<"SET">>, <<"key_binary">>, <<"value_binary">>])),
    ?assertEqual({ok, <<"value_binary">>}, eredis_cluster:q(?InstanceName1, [<<"GET">>,<<"key_binary">>])),
    ?assertEqual([{ok, <<"value_binary">>},{ok, <<"value_binary">>}], eredis_cluster:qp(?InstanceName1, [[<<"GET">>,<<"key_binary">>],[<<"GET">>,<<"key_binary">>]])),

%%                test instance 2
    ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?InstanceName2, [<<"SET">>, <<"key_binary">>, <<"value_binary">>])),
    ?assertEqual({ok, <<"value_binary">>}, eredis_cluster:q(?InstanceName2, [<<"GET">>,<<"key_binary">>])),
    ?assertEqual([{ok, <<"value_binary">>},{ok, <<"value_binary">>}], eredis_cluster:qp(?InstanceName2, [[<<"GET">>,<<"key_binary">>],[<<"GET">>,<<"key_binary">>]])).

test_delete() ->
%%                test instance 1
    ?assertMatch({ok, _}, eredis_cluster:q(?InstanceName1, ["DEL", "a"])),
    ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?InstanceName1, ["SET", "b", "a"])),
    ?assertEqual({ok, <<"1">>}, eredis_cluster:q(?InstanceName1, ["DEL", "b"])),
    ?assertEqual({ok, undefined}, eredis_cluster:q(?InstanceName1, ["GET", "b"])),

%%                test instance 2
    ?assertMatch({ok, _}, eredis_cluster:q(?InstanceName2, ["DEL", "a"])),
    ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?InstanceName2, ["SET", "b", "a"])),
    ?assertEqual({ok, <<"1">>}, eredis_cluster:q(?InstanceName2, ["DEL", "b"])),
    ?assertEqual({ok, undefined}, eredis_cluster:q(?InstanceName2, ["GET", "b"])).

test_pip_line() ->
    %%                test instance 1
    ?assertMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp(?InstanceName1, [["SET", "a1", "aaa"], ["SET", "a2", "aaa"], ["SET", "a3", "aaa"]])),
    ?assertMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp(?InstanceName1, [["LPUSH", "a", "aaa"], ["LPUSH", "a", "bbb"], ["LPUSH", "a", "ccc"]])),

%%                test instance 2
    ?assertMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp(?InstanceName2, [["SET", "a1", "aaa"], ["SET", "a2", "aaa"], ["SET", "a3", "aaa"]])),
    ?assertMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp(?InstanceName2, [["LPUSH", "a", "aaa"], ["LPUSH", "a", "bbb"], ["LPUSH", "a", "ccc"]])).

test_multi_node_get() ->

    N=1000,
    Keys = [integer_to_list(I) || I <- lists:seq(1,N)],

    %%                test instance 1
    [eredis_cluster:q(?InstanceName1, ["SETEX", Key, "50", Key]) || Key <- Keys],
    Guard1 = [{ok, integer_to_binary(list_to_integer(Key))} || Key <- Keys],
    ?assertMatch(Guard1, eredis_cluster:qmn(?InstanceName1, [["GET", Key] || Key <- Keys])),
    eredis_cluster:q(?InstanceName1, ["SETEX", "a", "50", "0"]),
    Guard2 = [{ok, integer_to_binary(0)} || _Key <- lists:seq(1,5)],
    ?assertMatch(Guard2, eredis_cluster:qmn(?InstanceName1, [["GET", "a"] || _I <- lists:seq(1,5)])),

    %%                test instance 2
    [eredis_cluster:q(?InstanceName2, ["SETEX", Key, "50", Key]) || Key <- Keys],
    Guard1 = [{ok, integer_to_binary(list_to_integer(Key))} || Key <- Keys],
    ?assertMatch(Guard1, eredis_cluster:qmn(?InstanceName2, [["GET", Key] || Key <- Keys])),
    eredis_cluster:q(?InstanceName2, ["SETEX", "a", "50", "0"]),
    Guard2 = [{ok, integer_to_binary(0)} || _Key <- lists:seq(1,5)],
    ?assertMatch(Guard2, eredis_cluster:qmn(?InstanceName2, [["GET", "a"] || _I <- lists:seq(1,5)])).

test_multi_node() ->
    N=1000,
    Keys = [integer_to_list(I) || I <- lists:seq(1,N)],

    %%                test instance 1
    [eredis_cluster:q(?InstanceName1, ["SETEX", Key, "50", Key]) || Key <- Keys],
    Guard1 = [{ok, integer_to_binary(list_to_integer(Key)+1)} || Key <- Keys],
    ?assertMatch(Guard1, eredis_cluster:qmn(?InstanceName1, [["INCR", Key] || Key <- Keys])),
    eredis_cluster:q(?InstanceName1, ["SETEX", "a", "50", "0"]),
    Guard2 = [{ok, integer_to_binary(Key)} || Key <- lists:seq(1,5)],
    ?assertMatch(Guard2, eredis_cluster:qmn(?InstanceName1, [["INCR", "a"] || _I <- lists:seq(1,5)])),

    %%                test instance 2
    [eredis_cluster:q(?InstanceName2, ["SETEX", Key, "50", Key]) || Key <- Keys],
    Guard1 = [{ok, integer_to_binary(list_to_integer(Key)+1)} || Key <- Keys],
    ?assertMatch(Guard1, eredis_cluster:qmn(?InstanceName2, [["INCR", Key] || Key <- Keys])),
    eredis_cluster:q(?InstanceName2, ["SETEX", "a", "50", "0"]),
    Guard2 = [{ok, integer_to_binary(Key)} || Key <- lists:seq(1,5)],
    ?assertMatch(Guard2, eredis_cluster:qmn(?InstanceName2, [["INCR", "a"] || _I <- lists:seq(1,5)])).

test_transaction() ->

    %%                test instance 1
    ?assertMatch({ok,[_,_,_]}, eredis_cluster:transaction(?InstanceName1, [["get","abc"],["get","abc"],["get","abc"]])),
    ?assertMatch({error,_}, eredis_cluster:transaction(?InstanceName1, [["get","abc"],["get","abc"],["other_command","abc"]])),

    %%                test instance 2
    ?assertMatch({ok,[_,_,_]}, eredis_cluster:transaction(?InstanceName2, [["get","abc"],["get","abc"],["get","abc"]])),
    ?assertMatch({error,_}, eredis_cluster:transaction(?InstanceName2, [["get","abc"],["get","abc"],["other_command","abc"]])).

test_eval_key() ->
    %%                test instance 1
    eredis_cluster:q(?InstanceName1, ["del", "foo"]),
    eredis_cluster:q(?InstanceName1, ["eval","return redis.call('set',KEYS[1],'bar')", "1", "foo"]),
    ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(?InstanceName1, ["GET", "foo"])),

    %%                test instance 2
    eredis_cluster:q(?InstanceName2, ["del", "foo"]),
    eredis_cluster:q(?InstanceName2, ["eval","return redis.call('set',KEYS[1],'bar')", "1", "foo"]),
    ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(?InstanceName2, ["GET", "foo"])).

test_eval_sha() ->
    %%                test instance 1
    eredis_cluster:q(?InstanceName1, ["del", "load"]),
    {ok, Hash} = eredis_cluster:q(?InstanceName1, ["script","load","return redis.call('set',KEYS[1],'bar')"]),
    eredis_cluster:q(?InstanceName1, ["evalsha", Hash, 1, "load"]),
    ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(?InstanceName1, ["GET", "load"])),

    %%                test instance 2
    eredis_cluster:q(?InstanceName2, ["del", "load"]),
    {ok, Hash} = eredis_cluster:q(?InstanceName2, ["script","load","return redis.call('set',KEYS[1],'bar')"]),
    eredis_cluster:q(?InstanceName2, ["evalsha", Hash, 1, "load"]),
    ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(?InstanceName2, ["GET", "load"])).

test_bitstring_support() ->
    %%                test instance 1
    eredis_cluster:q(?InstanceName1, [<<"set">>, <<"bitstring">>,<<"support">>]),
    ?assertEqual({ok, <<"support">>}, eredis_cluster:q(?InstanceName1, [<<"GET">>, <<"bitstring">>])),

    eredis_cluster:q(?InstanceName2, [<<"set">>, <<"bitstring">>,<<"support">>]),
    ?assertEqual({ok, <<"support">>}, eredis_cluster:q(?InstanceName2, [<<"GET">>, <<"bitstring">>])).

test_atomic_get_set() ->
    %%                test instance 1
    eredis_cluster:q(?InstanceName1, ["set", "hij", 2]),
    Incr = fun(Var) -> binary_to_integer(Var) + 1 end,
    Result = rpc:pmap({eredis_cluster, update_key}, [?InstanceName1, Incr], lists:duplicate(5, "hij")),
    IntermediateValues = proplists:get_all_values(ok, Result),
    ?assertEqual([3,4,5,6,7], lists:sort(IntermediateValues)),
    ?assertEqual({ok, <<"7">>}, eredis_cluster:q(?InstanceName1, ["get", "hij"])),

    %%                test instance 2
    eredis_cluster:q(?InstanceName2, ["set", "hij", 2]),
    Incr2 = fun(Var) -> binary_to_integer(Var) + 1 end,
    Result2 = rpc:pmap({eredis_cluster, update_key}, [?InstanceName2, Incr2], lists:duplicate(5, "hij")),
    IntermediateValues2 = proplists:get_all_values(ok, Result2),
    ?assertEqual([3,4,5,6,7], lists:sort(IntermediateValues2)),
    ?assertEqual({ok, <<"7">>}, eredis_cluster:q(?InstanceName2, ["get", "hij"])).

test_atomic_hget_set() ->
    %%                test instance 1
    eredis_cluster:q(?InstanceName1, ["hset", "klm", "nop", 2]),
    Incr = fun(Var) -> binary_to_integer(Var) + 1 end,
    Result = rpc:pmap({eredis_cluster, update_hash_field}, [?InstanceName1, "nop", Incr], lists:duplicate(5, "klm")),
    IntermediateValues = proplists:get_all_values(ok, Result),
    ?assertEqual([{<<"0">>,3},{<<"0">>,4},{<<"0">>,5},{<<"0">>,6},{<<"0">>,7}], lists:sort(IntermediateValues)),
    ?assertEqual({ok, <<"7">>}, eredis_cluster:q(?InstanceName1, ["hget", "klm", "nop"])),

    %%                test instance 2
    eredis_cluster:q(?InstanceName2, ["hset", "klm", "nop", 2]),
    Incr2 = fun(Var) -> binary_to_integer(Var) + 1 end,
    Result2 = rpc:pmap({eredis_cluster, update_hash_field}, [?InstanceName2, "nop", Incr2], lists:duplicate(5, "klm")),
    IntermediateValues2 = proplists:get_all_values(ok, Result2),
    ?assertEqual([{<<"0">>,3},{<<"0">>,4},{<<"0">>,5},{<<"0">>,6},{<<"0">>,7}], lists:sort(IntermediateValues2)),
    ?assertEqual({ok, <<"7">>}, eredis_cluster:q(?InstanceName2, ["hget", "klm", "nop"])).

test_eval() ->
    %%                test instance 1
    Script = <<"return redis.call('set', KEYS[1], ARGV[1]);">>,
    ScriptHash = << << if N >= 10 -> N -10 + $a; true -> N + $0 end >> || <<N:4>> <= crypto:hash(sha, Script) >>,
    eredis_cluster:eval(?InstanceName1, Script, ScriptHash, ["qrs"], ["evaltest"]),
    ?assertEqual({ok, <<"evaltest">>}, eredis_cluster:q(?InstanceName1, ["get", "qrs"])),

    %%                test instance 2
    Script = <<"return redis.call('set', KEYS[1], ARGV[1]);">>,
    ScriptHash = << << if N >= 10 -> N -10 + $a; true -> N + $0 end >> || <<N:4>> <= crypto:hash(sha, Script) >>,
    eredis_cluster:eval(?InstanceName2, Script, ScriptHash, ["qrs"], ["evaltest"]),
    ?assertEqual({ok, <<"evaltest">>}, eredis_cluster:q(?InstanceName2, ["get", "qrs"])).
