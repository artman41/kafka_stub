-module(kafka_request_decoder).
-export([
    decode_produce/1,
    decode_fetch/1
]).

%% API

decode_produce(Data0) ->
    <<RequiredAcks:16, Timeout:32, Data1/binary>> = Data0,
    #{
        required_acks => 
            case RequiredAcks =:= 16#ffff of
                true ->
                    -1;
                false ->
                    RequiredAcks
            end,
        timeout => Timeout,
        topics => decode_produce_topics(Data1)
    }.

decode_fetch(Data0) ->
    <<ReplicaId:32, MaxWaitTime:32, MinBytes:32, Data1/binary>> = Data0,
    #{
        replica_id => ReplicaId,
        max_wait_time => MaxWaitTime,
        min_bytes => MinBytes,
        topics => decode_fetch_topics(Data1)
    }.

%% Internal Functions

decode_produce_topics(<<TopicCount:32, Data/binary>>) ->
    lists:reverse(decode_produce_topics_(Data, TopicCount, [])).

decode_produce_topics_(<<>>, 0, Acc) ->
    Acc;
decode_produce_topics_(Data0, N, Acc0) ->
    <<TopicLen:16, Data1/binary>> = Data0,
    <<TopicName:TopicLen/binary, Data2/binary>> = Data1,
    {Partitions, Data3} = decode_produce_partitions(Data2),
    Acc1 =
        [#{
            topic_name => TopicName,
            partitions => Partitions
        } | Acc0],
    decode_produce_topics_(Data3, N-1, Acc1).

decode_produce_partitions(<<PartitionCount:32, Data0/binary>>) ->
    {Partitions, Data1} = decode_produce_partitions_(Data0, PartitionCount, []),
    {lists:reverse(Partitions), Data1}.

decode_produce_partitions_(Data, 0, Acc) ->
    {Acc, Data};
decode_produce_partitions_(Data0, N, Acc0) ->
    <<PartitionId:32, Data1/binary>> = Data0,
    <<MessageSetSize:32, Data2/binary>> = Data1,
    <<MessageSet:MessageSetSize/binary, Data3/binary>> = Data2,
    Acc1 = 
        [#{
            partition_id => PartitionId,
            message_set => decode_produce_message_set(MessageSet)
        } | Acc0],
    decode_produce_partitions_(Data3, N-1, Acc1).

decode_produce_message_set(Data) -> 
    lists:reverse(decode_produce_message_set_(Data, [])).

decode_produce_message_set_(<<>>, Acc) -> 
    Acc;
decode_produce_message_set_(<<Offset:64, MsgSize:32, Data0/binary>>, Acc0) ->
    <<Message:MsgSize/binary, Rest/binary>> = Data0,
    <<_CRC:32, Magic:8, Attrs:8, Data1/binary>> = Message,
    {Data2, Timestamp} =
        case Magic of
            0 -> 
                {Data1, 0};
            1 -> 
                <<Ts:64, Data/binary>> = Data1,
                {Data, Ts};
            _ -> 
                erlang:error({vsn_unsupported, Magic})
        end,
    <<KeyLen:32, Data3/binary>> = Data2,
    <<Key:KeyLen/binary, ValueLen:32, Data4/binary>> = Data3,
    <<Value:ValueLen/binary>> = Data4,
    Acc1 =
        [#{
            offset => Offset,
            attributes => Attrs,
            timestamp => Timestamp,
            key => Key,
            value => Value
        }|Acc0],
    decode_produce_message_set_(Rest, Acc1).

decode_fetch_topics(<<TopicCount:32, Data/binary>>) ->
    lists:reverse(decode_fetch_topics_(Data, TopicCount, [])).

decode_fetch_topics_(<<>>, 0, Acc) ->
    Acc;
decode_fetch_topics_(Data0, N, Acc0) ->
    <<TopicLen:16, Data1/binary>> = Data0,
    <<TopicName:TopicLen/binary, Data2/binary>> = Data1,
    {Partitions, Data3} = decode_fetch_partitions(Data2),
    Acc1 =
        [#{
            topic_name => TopicName,
            partitions => Partitions
        } | Acc0],
    decode_fetch_topics_(Data3, N-1, Acc1).

decode_fetch_partitions(<<PartitionCount:32, Data0/binary>>) ->
    {Partitions, Data1} = decode_fetch_partitions_(Data0, PartitionCount, []),
    {lists:reverse(Partitions), Data1}.

decode_fetch_partitions_(Data, 0, Acc) ->
    {Acc, Data};
decode_fetch_partitions_(Data0, N, Acc0) ->
    <<PartitionId:32, Offset:64, MaxBytes:32, Data1/binary>> = Data0,
    Acc1 = 
        [#{
            partition_id => PartitionId,
            fetch_offset => Offset,
            max_bytes => MaxBytes
        } | Acc0],
    decode_fetch_partitions_(Data1, N-1, Acc1).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

decode_produce_test() ->
    Data = <<255,255,0,0,39,16,0,0,0,1,0,8,109,121,95,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,40,0,0,0,0,0,0,0,0,0,0,0,28,111,82,192,204,0,0,0,0,0,6,109,121,95,107,101,121,0,0,0,8,109,121,95,118,97,108,117,101>>,
    ?assertEqual(#{
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
    }, decode_produce(Data)).

decode_fetch_test() ->
    Data = <<255,255,255,255,0,0,39,16,0,0,0,0,0,0,0,1,0,8,109,121,95,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,16,0,0>>,
    ?assertEqual(#{
        replica_id => 4294967295,
        max_wait_time => 10000,
        min_bytes => 0,
        topics => [
            #{
                topic_name => <<"my_topic">>,
                partitions => [
                    #{
                        fetch_offset => 0,
                        max_bytes => 1048576,
                        partition_id => 0
                    }
                ]
            }
        ]
    }, decode_fetch(Data)).

-endif.