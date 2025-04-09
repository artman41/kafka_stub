-module(shell_default).

-compile(export_all).

start_brod() ->
    brod:start_client([{"localhost", 9092}], my_client, []),
    ks_ets:create(<<"my_topic">>),
    brod:start_producer(my_client, <<"my_topic">>, []).

produce(Key, Value) when is_binary(Key) andalso is_binary(Value) ->
    brod:produce_sync(my_client, <<"my_topic">>, random, Key, Value).