-module(ks_config).
-export([
    get_port/0
]).

get_port() ->
    get_value(port, 9092).

%% Internal Functions

get_value(Key) ->
    application:get_env(kafka_stub, Key).

get_value(Key, Default) ->
    case get_value(Key) of
        {ok, Value} ->
            Value;
        undefined ->
            Default
    end.