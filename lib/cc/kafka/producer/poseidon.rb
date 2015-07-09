require "poseidon"

module CC
  module Kafka
    class Producer
      class Poseidon
        def initialize(host, port, topic, client_id)
          @brokers = ["#{host}:#{port}"]
          @topic = topic
          @client_id = client_id
        end

        def send_message(message, key)
          Kafka.logger.debug("sending message direct via Poseidon")
          producer.send_messages([
            ::Poseidon::MessageToSend.new(@topic, message, key)
          ])
        end

        def close
          producer.close
        end

        private

        def producer
          @producer ||= ::Poseidon::Producer.new(
            @brokers,
            @client_id,
            compression_codec: :gzip
          )
        end
      end
    end
  end
end
