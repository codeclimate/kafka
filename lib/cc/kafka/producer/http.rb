module CC
  module Kafka
    class Producer
      class HTTP
        HTTPError = Class.new(StandardError)
        HTTP_TIMEOUT= 60 # seconds

        def initialize(host, port, topic, ssl = false)
          @host = host
          @port = port
          @topic = topic
          @ssl = ssl
        end

        def send_message(message, key)
          Kafka.logger.debug("sending message over HTTP")
          http = Net::HTTP.new(@host, @port)
          http.open_timeout = HTTP_TIMEOUT
          http.read_timeout = HTTP_TIMEOUT

          request = Net::HTTP::Post.new("/")
          request["Topic"] = @topic
          request["Key"] = key if key
          request.body = message

          response = http.request(request)

          unless response.is_a?(Net::HTTPSuccess)
            raise HTTPError, "request not successful: (#{response.code}) #{response.body}"
          end
        end

        def close
          # no-op
        end
      end
    end
  end
end
