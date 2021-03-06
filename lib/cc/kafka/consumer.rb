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
        @paused = false
      end

      def on_start(&block)
        @on_start = block
      end

      def on_stop(&block)
        @on_stop = block
      end

      def on_message(&block)
        @on_message = block
      end

      def start
        trap(:TERM) { stop }

        @running = true
        @on_start.call(@offset) if @on_start

        while @running do
          fetch_messages unless @paused
        end

        Kafka.logger.info("shutting down due to TERM signal")
      ensure
        @on_stop.call(@offset) if @on_stop

        close
      end

      def stop
        @running = false
      end

      def pause
        @paused = true
      end

      def unpause
        @paused = false
      end

      def paused?
        @paused
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

            data = BSON.deserialize(message.value)
            data[MESSAGE_OFFSET_KEY] = [
              @offset.topic,
              @offset.partition,
              message.offset,
            ].join("-")

            @on_message.call(data)
          end
          Kafka.statsd.increment("messages.processed")
        end
      end
    end
  end
end
