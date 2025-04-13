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
    get_latest_offset/1,
    get_topics/0,
    get_latest/2,
    get_from_offset/2, get_from_offset/3,
    get_all/0, get_all/1
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

create(TopicName) when is_binary(TopicName) ->
    gen_server:call(?MODULE, {create, TopicName}).

insert(TopicName, Key, Value) when is_binary(TopicName) andalso is_binary(Key) andalso is_binary(Value) ->
    gen_server:call(?MODULE, {insert, TopicName, Key, Value}).

lookup(TopicName) when is_binary(TopicName) ->
    case ets:lookup(?TAB, TopicName) of
        [] ->
            undefined;
        [Entry] ->
            {ok, Entry}
    end.

-spec get_latest_offset(TopicName :: binary()) -> undefined | non_neg_integer().
get_latest_offset(TopicName) when is_binary(TopicName) ->
    case lookup(TopicName) of
        undefined ->
            undefined;
        {ok, #ks_entry{current_offset = 0}} ->
            undefined;
        {ok, #ks_entry{current_offset = CurrentOffset}} ->
            CurrentOffset-1
    end.

get_topics() ->
    ets:select(?TAB, ets:fun2ms(fun(#ks_entry{topic_name = TopicName}) -> TopicName end)).

-spec get_latest(TopicName :: binary(), Key :: binary()) -> {ok, list(#ks_topic_entry{})} | undefined.
get_latest(TopicName, Key) when is_binary(TopicName) andalso is_binary(Key) ->
    exec_on_tab(TopicName, fun do_get_latest/4, [Key]).

-spec get_from_offset(binary(), non_neg_integer()) -> {ok, list(#ks_topic_entry{})} | {error, not_found}.
get_from_offset(TopicName, Offset) when is_binary(TopicName) andalso is_integer(Offset) andalso Offset > 0  ->
    exec_on_tab(TopicName, fun do_get_from_offset/4, [Offset]).

-spec get_from_offset(binary(), binary(), non_neg_integer()) -> {ok, list(#ks_topic_entry{})} | {error, not_found}.
get_from_offset(TopicName, Key, Offset) when is_binary(TopicName) andalso is_binary(Key) andalso is_integer(Offset) andalso Offset > 0  ->
    exec_on_tab(TopicName, fun do_get_from_offset/4, [Key, Offset]).

-spec get_all() -> list({binary(), list(#ks_topic_entry{})}).
get_all() ->
    [{TopicName, ets:tab2list(Tab)} || #ks_entry{tab = Tab, topic_name = TopicName} <- ets:tab2list(?TAB)].

-spec get_all(TopicName :: binary()) -> undefined | list(#ks_topic_entry{}).
get_all(TopicName) when is_binary(TopicName) ->
    case lookup(TopicName) of
        undefined ->
            undefined;
        {ok, #ks_entry{tab = Tab}} ->
            ets:tab2list(Tab)
    end.

%% gen_server.

init([]) ->
    ets:new(?TAB, [protected, named_table, {keypos, #ks_entry.topic_name}]),
    {ok, #state{}}.

handle_call({create, TopicName}, _From, State) ->
    ets:insert(?TAB, #ks_entry{
        topic_name = TopicName, 
        current_offset = 0,
        tab = ets:new(?MODULE, [protected, ordered_set, {keypos, #ks_topic_entry.offset}])
    }),
    {reply, ok, State};
handle_call({insert, TopicName, Key, Value}, _From, State) ->
    {reply, exec_on_tab(TopicName, fun do_insert/4, [Key, Value]), State};
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

%% Internal Functions

-spec exec_on_tab(TopicName, Fun, Args) -> {error, not_found} | any()
    when TopicName :: binary(),
         Fun :: fun((ets:tid(), TopicName, CurrentOffset :: non_neg_integer()) -> any()),
         Args :: list().
exec_on_tab(TopicName, Fun, Args) when is_function(Fun, 4) andalso is_list(Args) ->
    case lookup(TopicName) of
        {ok, #ks_entry{tab = Tab, current_offset = CurrentOffset}} ->
            Fun(Tab, TopicName, CurrentOffset, Args);
        undefined ->
            {error, not_found}
    end.

do_insert(Tab, TopicName, CurrentOffset, [Key, Value]) ->
    ets:insert(Tab, #ks_topic_entry{offset = CurrentOffset, key = Key, value = Value}),
    ets:update_element(?TAB, TopicName, {#ks_entry.current_offset, CurrentOffset + 1}),
    {ok, CurrentOffset}.

do_get_latest(Tab, _TopicName, CurrentOffset, [Key]) ->
    case do_get_latest_(Tab, CurrentOffset-1, Key) of
        undefined ->
            {error, not_found};
        Entry ->
            {ok, Entry}
    end.

do_get_from_offset(Tab, _TopicName, _CurrentOffset, [Offset]) ->
    MatchSpec = ets:fun2ms(fun(Entry = #ks_topic_entry{offset = O}) when O >= Offset -> Entry end),
    {ok, ets:select(Tab, MatchSpec)};
do_get_from_offset(Tab, _TopicName, _CurrentOffset, [Key, Offset]) ->
    MatchSpec = ets:fun2ms(fun(Entry = #ks_topic_entry{key = K, offset = O}) when K =:= Key andalso O >= Offset -> Entry end),
    {ok, ets:select(Tab, MatchSpec)}.

do_get_latest_(_, -1, _) ->
    undefined;
do_get_latest_(Tab, Offset, Key) ->
    case ets:lookup(Tab, Offset) of
        [Entry = #ks_topic_entry{key = Key}] ->
            Entry;
        _ ->
            do_get_latest_(Tab, Offset - 1, Key)
    end.
      