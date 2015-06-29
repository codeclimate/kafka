# Code Climate Kafka

A generic Kafka client tuned for our own usage.

Features:

- Messages are expected to be hashes and are `BSON`-serialized
- Connections will be properly closed on exceptions
- Consumer will stop gracefully on `SIGTERM`
- *At most once* consumer semantics are used

## Usage

```rb
require "cc/kafka"
```

### Producer

```rb
producer = CC::Kafka::Producer.new("client-id", "kafka://host:1234/topic")
producer.send_message(foo: :bar, baz: :bat)
producer.close
```

### Consumer

```rb
consumer = CC::Kafka::Consumer.new("client-id", ["kafka://host:1234", "..."], "topic", 0)
consumer.on_message do |message|
  # message will be { "foo" => :bar, "baz" => :bat }
end

consumer.start
```

## Configuration

- `CC::Kafka.offset_model`

  Must respond to `transaction(&block)`.

  Must respond to `find_for_create!(attributes)` and return an object that
  responds to `set(attributes)`.

  The `attributes` used are `topic`, `partition`, and `current`. And the object
  returned from `find_or_create!` must expose methods for each of these.

  [`Minidoc`][minidoc] sub-classes work if a `find_or_create!` method is added.

  [minidoc]: https://github.com/brynary/minidoc

  ```rb
  class KafkaOffset < Minidoc
    attribute :topic, String
    attribute :partition, Integer
    attribute :current, Integer, default: 0

    def self.find_or_create!(attrs)
      find_one(attrs) || create!(attrs)
    end
  end

  CC::Kafka.offset_model = KafkaOffset
  ```

  *Note*: This is only necessary if using `Consumer`.

- `Kafka.logger`

  This is optional and defaults to `Logger.new(STDOUT)`. The configured object
  must have the same interface as the standard Ruby logger.

  Example:

  ```rb
  Kafka.logger = Rails.logger
  ```

- `Kafka.statsd`

  This is optional and defaults to a null object. The configured object should
  represent a [statsd][] client and respond to the usual methods, `increment`,
  `time`, etc.

  [statsd]: https://github.com/reinh/statsd

## Copyright

See [LICENSE](LICENSE)
