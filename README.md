# Code Climate Kafka

A generic Kafka client tuned for our own usage.

Features:

- Messages are expected to be hashes and are `BSON`-serialized
- Connections will be properly closed on exceptions
- Consumer will stop gracefully on `SIGTERM`
- *At most once* consumer semantics are used
- Production via an HTTP proxy (including SSL)

## Usage

```rb
require "cc/kafka"
```

### Producer

```rb
producer = CC::Kafka::Producer.new("kafka://host:1234/topic", "client-id")
producer.send_message(foo: :bar, baz: :bat)
producer.close
```

### Consumer

```rb
consumer = CC::Kafka::Consumer.new("client-id", ["kafka://host:1234", "..."], "topic", 0)
consumer.on_message do |message|
  # Given the producer above, message will be
  #
  #   {
  #     "foo" => :bar,
  #     "baz" => :bat,
  #     CC::Kafka::MESSAGE_OFFSET_KEY => "topic-0-1",
  #   }
  #
end

consumer.start
```

Note: the value for the `MESSAGE_OFFSET_KEY` identifies the message's offset
within the given topic and partition as `<topic>-<partition>-<offset>`. It can
be used by consumers to tie created data to the message that lead to it and
prevent duplicate processing.

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

- `Kafka.ssl_ca_file`

  Path to a custom SSL Certificate Authority file.

  Will result in:

  ```rb
  http.ca_file = Kafka.ca_file
  ```

- `Kafka.ssl_pem_file`

  Path to a custom SSL Certificate (and key) in concatenated, PEM format.

  Will result in:

  ```rb
  pem = File.read(Kafka.ssl_pem_file)

  http.cert = OpenSSL::X509::Certificate.new(pem)
  http.key = OpenSSL::PKey::RSA.new(pem)
  ```

## Copyright

See [LICENSE](LICENSE)
