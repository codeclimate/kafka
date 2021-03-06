require "logger"
require "cc/kafka/consumer"
require "cc/kafka/offset_storage/memory"
require "cc/kafka/offset_storage/minidoc"
require "cc/kafka/producer"

module CC
  module Kafka
    ConfigurationError = Class.new(StandardError)

    class DummyStatsd
      def method_missing(*)
        yield if block_given?
      end
    end

    class << self
      attr_accessor :ssl_ca_file, :ssl_pem_file

      attr_writer :offset_model, :logger, :statsd

      def logger
        @logger ||= Logger.new(STDOUT)
      end

      def offset_model
        if @offset_model.nil?
          raise ConfigurationError, "Kafka.offset_model not set"
        end

        @offset_model
      end

      def statsd
        @statsd ||= DummyStatsd.new
      end
    end
  end
end
