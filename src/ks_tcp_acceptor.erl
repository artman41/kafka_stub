-module(ks_tcp_acceptor).
-behaviour(gen_server).

%% API.
-export([child_spec/0]).
-export([start_link/0]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
    socket :: gen_tcp:socket()
}).

%% API.

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker
    }.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server.

init([]) ->
    {ok, Socket} = gen_tcp:listen(ks_config:get_port(), [binary, {active, once}, {reuseaddr, true}]),
    self() ! accept,
    {ok, #state{
        socket = Socket
    }}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(accept, State) ->
    self() ! accept,
    case gen_tcp:accept(State#state.socket) of
        {ok, Socket} ->
            lager:info("Accepted connection from ~p", [inet:peername(Socket)]),
            {ok, Worker} = ks_tcp_sup:start_child(Socket),
            inet:tcp_controlling_process(Socket, Worker);
        {error, _} ->
            ok
    end,
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
