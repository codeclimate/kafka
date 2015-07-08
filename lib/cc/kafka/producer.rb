require "bson"
require "poseidon"

module CC
  module Kafka
    class Producer
      SCHEMES = [
        HTTP = "http",
        KAFKA = "kafka",
      ]

      HTTPError = Class.new(StandardError)
      HTTP_TIMEOUT= 60 # seconds

      def initialize(url, client_id = nil)
        @url = url
        @client_id = client_id

        Kafka.logger.debug("initialized client for #{@url} (id: #{@client_id.inspect})")
      end

      def send_message(data, key = nil)
        Kafka.logger.debug("data: #{data.inspect}, key: #{key.inspect}")

        serialized = BSON.serialize(data).to_s

        if http?
          Kafka.logger.debug("sending message over HTTP")
          send_http(serialized, key)
        else
          Kafka.logger.debug("sending message direct via Poseidon")
          send_poseidon(serialized, key)
        end
      end

      def close
        unless http?
          producer.close
        end
      end

      private

      def http?
        uri.scheme == HTTP
      end

      def send_http(serialized, key)
        data = {
          "topic" => topic,
          "message" => serialized,
        }
        data["key"] = key if key

        http = Net::HTTP.new(uri.host, uri.port)
        http.open_timeout = HTTP_TIMEOUT
        http.read_timeout = HTTP_TIMEOUT
        request = Net::HTTP::Post.new("/message")
        request.set_form_data(data)

        Kafka.logger.debug("POST #{uri.host}:#{uri.port}/message")
        Kafka.logger.debug("form data: #{data.inspect}")
        response = http.request(request)

        unless response.is_a?(Net::HTTPSuccess)
          raise HTTPError, "request not successful: (#{response.code}) #{response.body}"
        end
      end

      def send_poseidon(serialized, key)
        message = Poseidon::MessageToSend.new(topic, serialized, key)
        producer.send_messages([message])
      rescue
        producer.close
        raise
      end

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
