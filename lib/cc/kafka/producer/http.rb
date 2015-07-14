module CC
  module Kafka
    class Producer
      class HTTP
        HTTPError = Class.new(StandardError)
        HTTP_TIMEOUT = 60 # seconds

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

          if ssl?
            http.use_ssl = true
            http.verify_mode = OpenSSL::SSL::VERIFY_PEER
            add_ssl_certificates(http)
          end

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

        private

        def ssl?
          @ssl
        end

        def add_ssl_certificates(http)
          if Kafka.ssl_ca_file
            Kafka.logger.debug("CA certificate: #{Kafka.ssl_ca_file}"
            http.ca_file = Kafka.ssl_ca_file
          end

          if Kafka.ssl_pem_file
            Kafka.logger.debug("PEM certificate: #{Kafka.ssl_pem_file}"
            pem = File.read(Kafka.ssl_pem_file)
            http.cert = OpenSSL::X509::Certificate.new(pem)
            http.key = OpenSSL::PKey::RSA.new(pem)
          end
        end
      end
    end
  end
end
