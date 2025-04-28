-module(ks_config).
-export([
    get_host/0,
    get_port/0
]).

get_host() ->
    get_value(host, <<"localhost">>).

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