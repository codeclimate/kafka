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

      def initialize(client_id, url)
        @client_id = client_id
        @url = url
      end

      def send_message(data, key = nil)
        serialized = BSON.serialize(data).to_s

        if http?
          send_http(serialized, key)
        else
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
        request = Net::HTTP::Post.new("/message")
        request.set_form_data(data)

        response = http.request(request)

        unless response.is_a?(Net::HTTPSuccess)
          raise HTTPError, "request not successful: #{response.inspect}"
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
