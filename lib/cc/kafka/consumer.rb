require "poseidon"

module CC
  module Kafka
    class Consumer
      MESSAGE_OFFSET_KEY = "kafka_message_offset".freeze

      def initialize(client_id, seed_brokers, topic, partition)
        @offset = Kafka.offset_model.find_or_create!(
          topic: topic,
          partition: partition,
        )

        Kafka.logger.debug("offset: #{@offset.topic}/#{@offset.partition} #{current_offset(@offset)}")

        @consumer = Poseidon::PartitionConsumer.consumer_for_partition(
          client_id,
          seed_brokers,
          @offset.topic,
          @offset.partition,
          current_offset(@offset)
        )
      end

      def on_message(&block)
        @on_message = block
      end

      def start
        trap(:TERM) { stop }

        @running = true

        while @running do
          fetch_messages
        end

        Kafka.logger.info("shutting down due to TERM signal")
      ensure
        @consumer.close
      end

      def stop
        @running = false
      end

      private

      def current_offset(offset)
        offset.current || :earliest_offset
      end

      def fetch_messages
        @consumer.fetch.each do |message|
          Kafka.statsd.increment("messages.received")
          Kafka.statsd.time("messages.processing") do
            @offset.set(current: message.offset + 1)

            Kafka.offset_model.transaction do
              data = BSON.deserialize(message.value)
              data[MESSAGE_OFFSET_KEY] = [
                @offset.topic,
                @offset.partition,
                message.offset,
              ].join("-")

              @on_message.call(data)
            end
          end
          Kafka.statsd.increment("messages.processed")
        end
      rescue Poseidon::Errors::UnknownTopicOrPartition
        Kafka.logger.debug("topic #{@topic} not created yet")

        sleep 1
      end
    end
  end
end
