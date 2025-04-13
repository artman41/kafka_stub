-module(kafka_stub_app).

-behaviour(application).

%%% application callbacks
-export([start/2]).
-export([stop/1]).

%%% application callbacks
start(_StartType, _StartArgs) ->
    kafka_stub_sup:start_link().

-spec stop(State :: term()) -> term().
stop(_) ->
    ok.

