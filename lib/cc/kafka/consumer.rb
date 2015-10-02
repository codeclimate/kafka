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
        close
      end

      def stop
        @running = false
      end

      def fetch
        @consumer.fetch
      rescue Poseidon::Errors::UnknownTopicOrPartition
        Kafka.logger.debug("topic #{@offset.topic.inspect} not created yet")
        []
      end

      def set_offset(current_offset)
        @offset.set(current: current_offset)
      end

      def close
        @consumer.close
      end

      private

      def current_offset(offset)
        offset.current || :earliest_offset
      end

      def fetch_messages
        fetch.each do |message|
          Kafka.statsd.increment("messages.received")
          Kafka.statsd.time("messages.processing") do
            set_offset(message.offset + 1)

            Kafka.offset_model.transaction do
              data = BSON.deserialize(message.value)
              data[MESSAGE_OFFSET_KEY] = [
                @offset.topic,
                @offset.partition,
                message.offset,
              ].join("-")

              if @on_message.arity == 2
                @on_message.call(data, message)
              else
                @on_message.call(data)
              end
            end
          end
          Kafka.statsd.increment("messages.processed")
        end
      end
    end
  end
end
