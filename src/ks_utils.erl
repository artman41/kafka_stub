-module(ks_utils).

-compile(export_all).

start_brod() ->
    brod:start_client([{"localhost", 9092}], my_client, []),
    ks_ets:create(<<"my_topic">>),
    brod:start_producer(my_client, <<"my_topic">>, []).

produce(Key, Value) when is_binary(Key) andalso is_binary(Value) ->
    brod:produce_sync(my_client, <<"my_topic">>, random, Key, Value).

consume() ->
    consume(0).

consume(FromOffset) when is_integer(FromOffset) andalso FromOffset >= 0 ->
    CbFun = 
        fun(Partition, Msg, State) -> 
            io:format("p: ~p, m: ~p, s: ~p~n", [Partition, Msg, State]), 
            {ok, State} 
        end,
    brod_topic_subscriber:start_link(my_client, <<"my_topic">>, _Partitions=all,
        _ConsumerConfig=[{begin_offset, FromOffset}],
        _CommittdOffsetss=[], message, CbFun,
        _CbState=self()).