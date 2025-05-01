% https://kafka.apache.org/protocol.html
-module(ks_tcp_worker).
-behaviour(gen_server).

%% API.
-export([child_spec/0]).
-export([start_link/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
    socket :: gen_tcp:socket(),
    binacc :: binary(),
    msg_n :: integer()
}).

-record(message, {
    api_key :: atom(),
    api_version :: integer(),
    correlation_id :: integer(),
    client_id :: binary(),
    request_data :: binary()
}).

%% API.

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker
    }.

start_link(Socket) ->
    gen_server:start_link(?MODULE, [Socket], []).

%% gen_server.

init([Socket]) ->
    lager:info("Starting worker for socket ~p", [Socket]),
    lager:md([{msg_n, 0}]),
    {ok, #state{
        socket = Socket,
        binacc = <<>>,
        msg_n = 0
    }}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, State0 = #state{socket = Socket, binacc = BinAcc0, msg_n = MsgN}) ->
    lager:md([{msg_n, MsgN}]),
    lager:info("Got data ~p~n", [Data]),
    inet:setopts(Socket, [{active, once}]),
    {KafkaMsgBin, BinAcc1} = read_kafka_message(<<BinAcc0/binary, Data/binary>>),
    State1 = State0#state{binacc = BinAcc1, msg_n = MsgN + 1},
    case parse_kafka_message(KafkaMsgBin) of
        undefined ->
            gen_tcp:send(Socket, <<0:32>>), %% Corrupt/invalid request
            {noreply, State1};
        {ok, Msg} ->
            lager:info("Got message ~p~n", [lager:pr(Msg, ?MODULE)]),
            handle_kafka_message(Msg, State1)
    end;
handle_info({tcp_closed, Socket}, State = #state{socket = Socket}) ->
    {stop, {shutdown, tcp_closed}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal Functions

read_kafka_message(<<Length:32/integer, Data/binary>>) ->
    case Data of
        <<Packet:Length/binary, Rest/binary>> ->
            {Packet, Rest};
        _ ->
            {<<>>, Data}
    end.

parse_kafka_message(<<ApiKey:16, _ApiVersion:16, CorrelationId:32, Data0/binary>>) ->
    <<ClientIdLen:16, Data1/binary>> = Data0,
    <<ClientId:ClientIdLen/binary, Data2/binary>> = Data1,
    {ok, #message{
        api_key = kpro_schema:api_key(ApiKey),
        api_version = 0,
        correlation_id = CorrelationId,
        client_id = ClientId,
        request_data = Data2
    }};
parse_kafka_message(_) ->
    undefined.

handle_kafka_message(#message{api_key = produce, correlation_id = CorrelationId, request_data = RequestData}, State = #state{socket = Socket}) ->
    DecodedRequest = kafka_request_decoder:decode_produce(RequestData),
    RespBody = kafka_response_encoder:encode_produce(CorrelationId, DecodedRequest),
    reply(Socket, RespBody),
    {noreply, State};
handle_kafka_message(#message{api_key = fetch, correlation_id = CorrelationId, request_data = RequestData}, State = #state{socket = Socket}) ->
    DecodedRequest = kafka_request_decoder:decode_fetch(RequestData),
    RespBody = kafka_response_encoder:encode_fetch(CorrelationId, DecodedRequest),
    lager:info("Sending fetch response: ~p", [RespBody]),
    reply(Socket, RespBody),
    {noreply, State};
handle_kafka_message(#message{api_key = metadata, correlation_id = CorrelationId}, State = #state{socket = Socket}) ->
    Hostname = ks_config:get_host(),
    Port = ks_config:get_port(),
    BrokersSection = <<
        1:32,                      % broker count
        %% Broker Meta
        0:32,                      % node id
        
        (byte_size(Hostname)):16, Hostname/binary, % host
        Port:32                    % port
    >>,

    Topics = ks_ets:get_topics(),

    TopicMetas =
        [
            <<
                %% Topic Meta
                0:16,  % error code
                (byte_size(TopicName)):16, TopicName/binary,
                1:32, % partition count
                %% Partition Meta
                0:16, % error code
                0:32, % partition id
                0:32, % leader broker id
                1:32, 0:32, % replicas count + 1 elem
                1:32, 0:32 % isr count + 1 elem
            >>
        || TopicName <- Topics],

    TopicsSection = <<
        (length(Topics)):32,
        (iolist_to_binary(TopicMetas))/binary
    >>,

    RespBody = <<
        CorrelationId:32,
        BrokersSection/binary,
        TopicsSection/binary
    >>,

    lager:info("Sending metadata response: ~p", [RespBody]),

    reply(Socket, RespBody),
    {noreply, State};
handle_kafka_message(#message{api_key = api_versions, correlation_id = CorrelationId}, State = #state{socket = Socket}) ->
    Versions = api_versions(),
    VersionsBin = iolist_to_binary(Versions),
    ApiCount = length(Versions),
    
    ErrorCode = 0,
    RespBody = <<
        CorrelationId:32,
        ErrorCode:16,                            % error_code = 0
        ApiCount:32,
        VersionsBin/binary
        %% Kafka v0.x doesn't include tagged fields — omit them
    >>,
    gen_tcp:send(Socket, <<(byte_size(RespBody)):32, RespBody/binary>>),
    {noreply, State};
handle_kafka_message(#message{api_key = _, correlation_id = CorrelationId}, State = #state{socket = Socket}) ->
    Data = <<CorrelationId:32>>,
    reply(Socket, Data),
    {noreply, State}.

reply(Socket, Data) when is_binary(Data) ->
    gen_tcp:send(Socket, <<(byte_size(Data)):32, Data/binary>>).

api_versions() ->
    [
        api_version(produce, 0, 0),
        api_version(fetch, 0, 0),
        api_version(metadata, 0, 0),
        api_version(api_versions, 0, 0)
    ].

api_version(Api, MinVsn, MaxVsn) when is_atom(Api) ->
    Int = kpro_schema:api_key(metadata),
    <<Int:16, MinVsn:16, MaxVsn:16>>.