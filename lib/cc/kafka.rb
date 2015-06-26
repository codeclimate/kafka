require "logger"
require "cc/kafka/consumer"
require "cc/kafka/producer"

module CC
  module Kafka
    ConfigurationError = Class.new(StandardError)

    class << self
      attr_writer :offset_model, :logger

      def logger
        @logger ||= Logger.new(STDOUT)
      end

      def offset_model
        if @offset_model.nil?
          raise ConfigurationError, "Kafka.offset_model not set"
        end

        @offset_model
      end
    end
  end
end
