-module(ks_ets).
-behaviour(gen_server).

-include("ks_entry.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API.
-export([child_spec/0]).
-export([start_link/0]).
-export([
    create/1,
    insert/3,
    lookup/1,
    get_topics/0
]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
}).

-define(TAB, ?MODULE).

%% API.

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker
    }.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create(TopicName) ->
    gen_server:call(?MODULE, {create, TopicName}).

insert(TopicName, Key, Value) ->
    gen_server:call(?MODULE, {insert, TopicName, Key, Value}).

lookup(TopicName) ->
    case ets:lookup(?TAB, TopicName) of
        [] ->
            undefined;
        [Entry] ->
            {ok, Entry}
    end.

get_topics() ->
    ets:select(?TAB, ets:fun2ms(fun(#ks_entry{topic_name = TopicName}) -> TopicName end)).

%% gen_server.

init([]) ->
    ets:new(?TAB, [protected, named_table, {keypos, #ks_entry.topic_name}]),
    {ok, #state{}}.

handle_call({create, TopicName}, _From, State) ->
    ets:insert(?TAB, #ks_entry{
        topic_name = TopicName, 
        current_offset = 0,
        tab = ets:new(?MODULE, [protected, bag])
    }),
    {reply, ok, State};
handle_call({insert, TopicName, Key, Value}, _From, State) ->
    Ret =
        case lookup(TopicName) of
            {ok, #ks_entry{tab = Tab, current_offset = CurrentOffset}} ->
                ets:insert(Tab, {Key, Value}),
                ets:update_element(?TAB, TopicName, {#ks_entry.current_offset, CurrentOffset + 1}),
                {ok, CurrentOffset};
            undefined ->
                {error, not_found}
        end,
    {reply, Ret, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
