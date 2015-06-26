require "bson"
require "poseidon"

module CC
  module Kafka
    class Producer
      def initialize(client_id, url)
        @client_id = client_id
        @url = url
      end

      def send_message(data, key = nil)
        serialized = BSON.serialize(data).to_s
        message = Poseidon::MessageToSend.new(topic, serialized, key)

        producer.send_messages([message])
      rescue
        close

        raise
      end

      def close
        producer.close
      end

      private

      def producer
        @producer ||= Poseidon::Producer.new(
          broker,
          @client_id,
          compression_codec: :gzip
        )
      end

      def broker
        ["#{uri.host}:#{uri.port}"]
      end

      def topic
        uri.path.split("/")[1]
      end

      def uri
        @uri ||= URI.parse(@url)
      end
    end
  end
end
