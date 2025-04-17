# Kafka Stub

## Testing in the shell

### Startup a brod producer using

Call `ks_utils:start_brod().`

### Sending a produce command

Call `ks_utils:produce(Key, Value).`

### Consuming data

Call `ks_utils:consume().` or `ks_utils:consume(FromOffset).`

## Using in Suites

In an ideal world, the app will be started with `application:ensure_all_started(kafka_stub)` where a port is defined in config, defaulting to `9092` otherwise.

A client will then connect to this port on the host.

Currently, only producing & consuming works where data is stored in a ets table located in the `ks_ets` gen_server.

### Ets storage Format

All ets records are defined in `ks_entry.hrl`.

The main, named, ets table `ks_ets` has entries of `#ks_entry{}`.

```
#ks_entry{
    topic_name :: binary(),
    current_offset :: non_neg_integer(),
    tab :: ets:tab()
}
```

Each entry represents a topic and is keyed on the Topic Name.

The topic entry itself contains an ets table (unnamed) with the property `ordered_set` which holds the data for that topic.

Topic tables have entries of `#ks_topic_entry{}`

```
#ks_topic_entry{
    offset :: non_neg_integer(),
    key :: binary(),
    value :: binary()
}
```

These entries are keyed on their offset.