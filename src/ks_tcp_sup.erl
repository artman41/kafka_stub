-module(ks_tcp_sup).
-behaviour(supervisor).

-export([child_spec/0]).
-export([start_child/1]).
-export([start_link/0]).
-export([init/1]).

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => supervisor
    }.

start_child(Socket) ->
    supervisor:start_child(?MODULE, [Socket]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Procs = [
        ks_tcp_worker:child_spec()
    ],
    {ok, {{simple_one_for_one, 1, 5}, Procs}}.
