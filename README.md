# Kafka Stub

## Testing in the shell

### Startup a brod producer using

Run `start_brod().` in the shell or call `shell_default:start_brod().`

### Sending a produce command

Run `produce(Key, Value).` in the shell or call `shell_default:produce(Key, Value).`

## Using in Suites

In an ideal world, the app will be started with `application:ensure_all_started(kafka_stub)` where a port is defined in config, defaulting to `9092` otherwise.

A client will then connect to this port on the host.

Currently, only producing works where data is stored in a ets table located in the `ks_ets` gen_server.

Eventually, consumers will be properly handled.