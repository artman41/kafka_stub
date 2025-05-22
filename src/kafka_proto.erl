-module(kafka_proto).
-export([
    decode/2,
    encode/2,
    schema/1
]).

-ifdef(OTP_RELEASE).
-define(CATCH(C, R, S), catch C:R:S ->).
-define(FORMAT_EX(C,R,S), erl_error:format_exception(C, R, S)).
-else.
-define(CATCH(C, R, S), catch C:R -> S = erlang:get_stacktrace(),).
-define(FORMAT_EX(C,R,S), io_lib:format("~p ~1000p", [{C,R},S])).
-endif.

decode(Schema, Bin) when is_binary(Bin) ->
    try
        put('$level', root),
        {Value, <<>>} = decode_(Schema, Bin, #{}),
        erase('$level'),
        Value
    ?CATCH(C,R,S)
        lager:error("Failed to decode at level [~p] with error ~ts~n", [get('$level'), ?FORMAT_EX(C,R,S)]),
        erlang:raise(C,R,S)
    end.

encode(Schema, Values) when is_map(Values) ->
    try
        put('$level', root),
        Value = encode_(Schema, Values),
        erase('$level'),
        Value
    ?CATCH(C,R,S)
        lager:error("Failed to encode at level [~p] with error ~ts~n", [get('$level'), ?FORMAT_EX(C,R,S)]),
        erlang:raise(C,R,S)
    end.

schema(produce_request) ->
    [
        {required_acks, int16},
        {timeout, int32},
        {topics, int32, [
            {topic_name, string},
            {partitions, int32, [
                {partition_id, int32},
                {message_set, bytes, [
                    {offset, int64},
                    {message, bytes, [
                        {crc, int32},
                        {magic, int8},
                        {attributes, int8},
                        {timestamp, int64, {magic, '==', 1}},
                        {key, bytes},
                        {value, bytes}
                    ]}
                ]}
            ]}
        ]}
    ];
schema(produce_response) ->
    [
        {topics, int32, [
            {topic_name, string},
            {partitions, int32, [
                {partition_id, int32},
                {error_code, int16},
                {last_offset, int64}
            ]}
        ]}
    ];
schema(fetch_request) ->
    [
        {replica_id, int32},
        {max_wait_time, int32},
        {min_bytes, int32},
        {topics, int32, [
            {topic_name, string},
            {partitions, int32, [
                {partition_id, int32},
                {offset, int64},
                {max_bytes, int32}
            ]}
        ]}
    ];
schema(fetch_response) ->
    [
        {topics, int32, [
            {topic_name, string},
            {partitions, int32, [
                {partition_id, int32},
                {error_code, int16},
                {last_offset, int64},
                {message_set, bytes, [
                    {offset, int64},
                    {'$action', {encode, bytes}, [
                        {'$action', {crc, int32}, [
                            {magic, int8},
                            {attributes, int8},
                            {timestamp, int64, {magic, '==', 1}},
                            {key, bytes},
                            {value, bytes}
                        ]}
                    ]}
                ]}
            ]}
        ]}
    ].
    
%% 

encode_([], _Values) ->
    <<>>;
encode_([{'$action', Action, Body}|Tail], Values) when is_list(Body) ->
    Encoded = encode_(Body, Values),
    <<(action(Action, Encoded))/binary, (encode_(Tail, Values))/binary>>;
encode_([{Name, Prim, Cond} | Tail], Values) when is_tuple(Cond) ->
    Value =
        case is_true(Cond, Values) of
            true ->
                enc(Prim, maps:get(Name, Values));
            false ->
                <<>>
        end,
    <<Value/binary, (encode_(Tail, Values))/binary>>;
encode_([{Name, SourcePrim, Body}|Tail], Values) when is_list(Body) ->
    SubValues = maps:get(Name, Values),
    Level = get('$level'),
    put('$level', Name),
    Encoded = [encode_(Body, Value) || Value <- SubValues],
    put('$level', Level),
    Bin =
        case SourcePrim of
            int32 ->
                <<(enc(int32, length(Encoded)))/binary, (iolist_to_binary(Encoded))/binary>>;
            bytes ->
                enc(bytes, iolist_to_binary(Encoded))
        end,
    <<Bin/binary, (encode_(Tail, Values))/binary>>;
encode_([{Name, Prim}|Tail], Values) ->
    Value = maps:get(Name, Values),
    <<(enc(Prim, Value))/binary, (encode_(Tail, Values))/binary>>.

decode_([], Bin, Acc) ->
    {Acc, Bin};
decode_([{Name, Prim, Cond} | Tail], Bin, Acc0) when is_tuple(Cond) ->
    {Acc1, Bin1} =
        case is_true(Cond, Acc0) of
            true ->
                {V, B} = dec(Prim, Bin),
                {Acc0#{Name => V}, B};
            false ->
                {Acc0, Bin}
        end,
    decode_(Tail, Bin1, Acc1);
decode_([{Name, SourcePrim, Body}|Tail], Bin0, Acc) when is_list(Body) ->
    {Source, Bin1} = dec(SourcePrim, Bin0),
    Level = get('$level'),
    put('$level', Name),
    {Value, Bin2} = 
        case SourcePrim of
            int32 ->
                iter_decode_int(Source, Body, Bin1, []);
            bytes ->
                V = iter_decode_bin(Source, Body, []),
                {V, Bin1}
        end,
    put('$level', Level),
    decode_(Tail, Bin2, Acc#{Name => Value});
decode_([{Name, Primitive}|Tail], Bin0, Acc) ->
    {Value, Bin1} = dec(Primitive, Bin0),
    decode_(Tail, Bin1, Acc#{Name => Value}).

iter_decode_int(0, _Schema, Bin, Acc) ->
    {lists:reverse(Acc), Bin};
iter_decode_int(N, Schema, Bin0, Acc) ->
    {Value, Bin1} = decode_(Schema, Bin0, #{}),
    iter_decode_int(N - 1, Schema, Bin1, [Value|Acc]).

iter_decode_bin(<<>>, _Schema, Acc) ->
    lists:reverse(Acc);
iter_decode_bin(Bin0, Schema, Acc) ->
    {Value, Bin1} = decode_(Schema, Bin0, #{}),
    iter_decode_bin(Bin1, Schema, [Value|Acc]).

dec(Primitive, Bin) -> 
    kpro_lib:decode(Primitive, Bin).
enc(Primitive, Val) -> 
    iolist_to_binary(kpro_lib:encode(Primitive, Val)).

action({encode, Prim}, Bin) when is_binary(Bin) ->
    enc(Prim, Bin);
action({crc, Prim}, Bin) when is_binary(Bin) ->
    CRC = erlang:crc32(Bin),
    <<(enc(Prim, CRC))/binary, Bin/binary>>.

is_true({Name, '==', Value}, Acc) ->
    case maps:find(Name, Acc) of
        {ok, Value} -> true;
        _ -> false
    end.

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
                                offset => 0,
                                message => [
                                    #{
                                        crc => 1867694284,
                                        magic => 0,
                                        attributes => 0,
                                        key => <<"my_key">>,
                                        value => <<"my_value">>
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }, decode(schema(produce_request), Data)).

encode_produce_test() ->
    Data =
        #{
            topics => [
                #{
                    topic_name => <<"my_topic">>,
                    partitions => [
                        #{
                            partition_id => 0,
                            error_code => 0,
                            last_offset => 1
                        }
                    ]
                }
            ]
        },
    ?assertEqual(<<
        0,0,0,1, %topic count
        0,8, %topic name length
        109,121,95,116,111,112,105,99, %topic name
        0,0,0,1, %partition count
        0,0,0,0, %partition id
        0,0, %error code
        0,0,0,0,0,0,0,1 %last offset
    >>, encode(schema(produce_response), Data)).

decode_fetch_test() ->
    Data = <<255,255,255,255,0,0,39,16,0,0,0,0,0,0,0,1,0,8,109,121,95,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,16,0,0>>,
    ?assertEqual(#{
        replica_id => -1,
        max_wait_time => 10000,
        min_bytes => 0,
        topics => [
            #{
                topic_name => <<"my_topic">>,
                partitions => [
                    #{
                        offset => 0,
                        max_bytes => 1048576,
                        partition_id => 0
                    }
                ]
            }
        ]
    }, decode(schema(fetch_request), Data)).

encode_fetch_test() ->
    Data = 
        #{
            topics => [
                #{
                    topic_name => <<"my_topic">>,
                    partitions => [
                        #{
                            partition_id => 0,
                            error_code => 0,
                            last_offset => 10,
                            message_set => [#{
                                offset => N,
                                magic => 0,
                                attributes => 0,
                                key => <<"Key", ($0 + N)>>,
                                value => <<"Value", ($0 + N)>>
                            } || N <- lists:seq(0, 9)]
                        }
                    ]
                }
            ]
        },
    Expected = [
        <<
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
    ?assertEqual(iolist_to_binary(Expected), encode(schema(fetch_response), Data)).

-endif.