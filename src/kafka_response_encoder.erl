-module(kafka_response_encoder).
-export([encode/3]).

%% API

encode(produce, CorrelationId, #{topics := Topics}) ->
    TopicCount = length(Topics),
    TopicSection = iolist_to_binary(lists:map(fun encode_topic/1, Topics)),
    <<
        CorrelationId:32,
        TopicCount:32,
        TopicSection/binary
    >>.

encode_topic(#{topic_name := TopicName, partitions := Partitions}) ->
    PartitionSection = iolist_to_binary([encode_partition(Partition, TopicName) || Partition <- Partitions]),
    <<
        (byte_size(TopicName)):16, TopicName/binary,
        (length(Partitions)):32,
        PartitionSection/binary
    >>.

encode_partition(#{partition_id := Id, message_set := MsgSet}, TopicName) ->
    %% In a real broker, you'd increment the offset here.
    {ErrorCode, LastOffset} =
        case action_request(MsgSet, TopicName, 0) of
            error ->
                {1, 0};
            Offset ->
                {0, Offset}
        end,
    <<
        Id:32,
        ErrorCode:16,
        LastOffset:64
    >>.

action_request([], _, Acc) -> 
    Acc;
action_request([#{key := Key, value := Value}|T], TopicName, _) ->
    case ks_ets:insert(TopicName, Key, Value) of
        {ok, Offset} ->
            action_request(T, TopicName, Offset);
        {error, _} ->
            error
    end.

    -ifdef(TEST).
    
    -include_lib("eunit/include/eunit.hrl").
    
encode_produce_test() ->
    meck:new(ks_ets),
    meck:expect(ks_ets, insert, 3, fun(_, _, _) -> {ok, 1} end),
    Data = 
        #{
            client_id => <<"my_client">>,
            required_acks => -1,
            timeout => 10000,
            topics => [
                #{
                    topic_name => <<"my_topic">>,
                    partitions => [
                        #{
                            partition_id => 0,
                            message_set => [
                                #{
                                    attributes => 0,
                                    offset => 0,
                                    timestamp => 0,
                                    key => <<"my_key">>,
                                    value => <<"my_value">>
                                }
                            ]
                        }
                    ]
                }
            ]
        },
    ?assertEqual(<<
        0,0,4,210, %correlation id
        0,0,0,1, %topic count
        0,8, %topic name length
        109,121,95,116,111,112,105,99, %topic name
        0,0,0,1, %partition count
        0,0,0,0, %partition id
        0,0, %error code
        0,0,0,0,0,0,0,1 %last offset
    >>, encode(produce, 1234, Data)).

-endif.