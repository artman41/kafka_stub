-module(kafka_stub_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Procs = [
        ks_ets:child_spec(),
        ks_tcp_sup:child_spec(),
        ks_tcp_acceptor:child_spec()
    ],
    {ok, {{one_for_one, 1, 5}, Procs}}.
