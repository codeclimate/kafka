require "bson"
require "cc/kafka/producer/http"
require "cc/kafka/producer/poseidon"

module CC
  module Kafka
    class Producer
      InvalidScheme = Class.new(StandardError)

      def initialize(url, client_id = nil)
        @url = url
        @client_id = client_id

        Kafka.logger.debug("initialized client for #{@url} (id: #{@client_id.inspect})")
      end

      def send_message(data, key = nil)
        Kafka.logger.debug("data: #{data.inspect}, key: #{key.inspect}")

        producer.send_message(BSON.serialize(data).to_s, key)
      rescue
        producer.close
        raise
      end

      def close
        producer.close
      end

      private

      def producer
        @producer ||= choose_producer
      end

      def choose_producer
        case (scheme = uri.scheme)
        when "http" then HTTP.new(host, port, topic)
        when "https" then HTTP.new(host, port, topic, true)
        when "kafka" then Poseidon.new(host, port, topic, @client_id)
        else raise InvalidScheme, "invalid scheme #{scheme.inspect}"
        end
      end

      def scheme
        uri.scheme
      end

      def host
        uri.host
      end

      def port
        uri.port
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
