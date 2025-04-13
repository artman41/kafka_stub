-module(kafka_response_encoder).

-include("ks_entry.hrl").

-export([
    encode_produce/2,
    encode_fetch/2
]).

%% API

encode_produce(CorrelationId, #{topics := Topics}) ->
    TopicCount = length(Topics),
    TopicSection = iolist_to_binary(lists:map(fun encode_produce_topic/1, Topics)),
    <<
        CorrelationId:32,
        TopicCount:32,
        TopicSection/binary
    >>.

encode_fetch(CorrelationId, #{topics := Topics}) ->
    TopicCount = length(Topics),
    TopicSection = iolist_to_binary(lists:map(fun encode_fetch_topic/1, Topics)),
    <<
        CorrelationId:32,
        TopicCount:32,
        TopicSection/binary
    >>.

%% Internal Functions

encode_produce_topic(#{topic_name := TopicName, partitions := Partitions}) ->
    PartitionSection = iolist_to_binary([encode_produce_partition(Partition, TopicName) || Partition <- Partitions]),
    <<
        (byte_size(TopicName)):16, TopicName/binary,
        (length(Partitions)):32,
        PartitionSection/binary
    >>.

encode_produce_partition(#{partition_id := Id, message_set := MsgSet}, TopicName) ->
    {ErrorCode, LastOffset} =
        case action_produce_request(MsgSet, TopicName, 0) of
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

encode_fetch_topic(#{topic_name := TopicName, partitions := Partitions}) ->
    PartitionBatches = [encode_fetch_partition(Partition, TopicName) || Partition <- Partitions],
    <<
        (byte_size(TopicName)):16, TopicName/binary,
        (length(PartitionBatches)):32,
        (iolist_to_binary(PartitionBatches))/binary
    >>.

encode_fetch_partition(#{partition_id := PartitionId, fetch_offset := FetchOffset, max_bytes := MaxBytes}, TopicName) ->
    case fetch_and_encode_messages(FetchOffset, 0, MaxBytes, TopicName, []) of
        {ok, {MessageSet, NewFetchOffset}} ->
            MessageSetBin = iolist_to_binary(lists:reverse(MessageSet)),
            <<
                PartitionId:32,
                0:16, % error code
                NewFetchOffset:64, % last offset
                (byte_size(MessageSetBin)):32,
                MessageSetBin/binary
            >>;
        {error, not_found} ->
            <<
                PartitionId:32,
                1:16, % error code
                FetchOffset:64, % last offset
                0:32 % message set size
            >>
    end.

fetch_and_encode_messages(FetchOffset, CurrentSize, MaxBytes, TopicName, Acc) ->
    case ks_ets:lookup(TopicName) of
        {ok, #ks_entry{tab = Tab}} ->
            fetch_and_encode_messages_(Tab, FetchOffset, CurrentSize, MaxBytes, TopicName, Acc);
        undefined ->
            {error, not_found}
    end.

fetch_and_encode_messages_(Tab, FetchOffset, CurrentSize, MaxBytes, TopicName, Acc) ->
    case ets:lookup(Tab, FetchOffset) of
        [#ks_topic_entry{offset = Offset, key = Key, value = Value}] ->
            Encoded = encode_message(Key, Value, Offset),
            %% Offset is 64, MessageLen is 32
            case byte_size(Encoded) + CurrentSize + 64 + 32 of
                MaxBytes ->
                    {ok, {[Encoded|Acc], Offset+1}};
                Size when Size < MaxBytes ->
                    fetch_and_encode_messages(Offset+1, byte_size(Encoded) + CurrentSize, MaxBytes, TopicName, [Encoded|Acc]);
                _ ->
                    {ok, {Acc, Offset}}
            end;
        [] ->
            {ok, {Acc, FetchOffset}}
    end.

encode_message(Key, Value, Offset) ->
    Payload =
        <<
            0:8, % Magic Byte
            0:8, % Attributes
            (byte_size(Key)):32, Key/binary,
            (byte_size(Value)):32, Value/binary
        >>,
    CRC = erlang:crc32(Payload),
    Message = <<CRC:32, Payload/binary>>,
    <<
        Offset:64, % Offset
        (byte_size(Message)):32, Message/binary
    >>.

action_produce_request([], _, Acc) -> 
    Acc;
action_produce_request([#{key := Key, value := Value}|T], TopicName, _) ->
    case ks_ets:insert(TopicName, Key, Value) of
        {ok, Offset} ->
            action_produce_request(T, TopicName, Offset);
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
    >>, encode_produce(1234, Data)),
    meck:unload(ks_ets).

encode_fetch_test() ->
    ks_ets:init([]),
    TopicTab = ets:new(?MODULE, [protected, ordered_set, {keypos, #ks_topic_entry.offset}]),
    ets:insert(ks_ets, #ks_entry{
        topic_name = <<"my_topic">>, 
        current_offset = 10,
        tab = TopicTab
    }),
    [ets:insert(TopicTab, #ks_topic_entry{offset = N, key = <<"Key", ($0 + N)>>, value = <<"Value", ($0 + N)>>}) || N <- lists:seq(0, 9)],
    Data = 
        #{
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
        },
    Expected = [
        <<
            0,0,4,210, %correlation id
            0,0,0,1, %topic count
            0,8, %topic name length
            "my_topic", %topic name
            0,0,0,1, %partition count
            0,0,0,0, %partition id
            0,0, %error code
            0,0,0,0,0,0,0,10, % last offset
            0,0,1,104 % message set len
        >>,
        <<
            0,0,0,0,0,0,0,0, % Offset
            0,0,0,24, % Message Length
            9,177,102,165, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key0", % key
            0,0,0,6, % value len
            "Value0" % value
        >>,
        <<
            0,0,0,0,0,0,0,1, % Offset
            0,0,0,24, % Message Length
            191,56,137,243, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key1", % key
            0,0,0,6, % value len
            "Value1" % value
        >>,
        <<
            0,0,0,0,0,0,0,2, % Offset
            0,0,0,24, % Message Length
            191,211,190,72, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key2", % key
            0,0,0,6, % value len
            "Value2" % value
        >>,
        <<
            0,0,0,0,0,0,0,3, % Offset
            0,0,0,24, % Message Length
            9,90,81,30, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key3", % key
            0,0,0,6, % value len
            "Value3" % value
        >>,
        <<
            0,0,0,0,0,0,0,4, % Offset
            0,0,0,24, % Message Length
            190,5,209,62, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key4", % key
            0,0,0,6, % value len
            "Value4" % value
        >>,
        <<
            0,0,0,0,0,0,0,5, % Offset
            0,0,0,24, % Message Length
            8,140,62,104, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key5", % key
            0,0,0,6, % value len
            "Value5" % value
        >>,
        <<
            0,0,0,0,0,0,0,6, % Offset
            0,0,0,24, % Message Length
            8,103,9,211, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key6", % key
            0,0,0,6, % value len
            "Value6" % value
        >>,
        <<
            0,0,0,0,0,0,0,7, % Offset
            0,0,0,24, % Message Length
            190,238,230,133, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key7", % key
            0,0,0,6, % value len
            "Value7" % value
        >>,
        <<
            0,0,0,0,0,0,0,8, % Offset
            0,0,0,24, % Message Length
            189,169,15,210, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key8", % key
            0,0,0,6, % value len
            "Value8" % value
        >>,
        <<
            0,0,0,0,0,0,0,9, % Offset
            0,0,0,24, % Message Length
            11,32,224,132, % CRC
            0, % magic byte
            0, % attributes
            0,0,0,4, % key len
            "Key9", % key
            0,0,0,6, % value len
            "Value9" % value
        >>
    ],
    ?assertEqual(iolist_to_binary(Expected), encode_fetch(1234, Data)).

-endif.