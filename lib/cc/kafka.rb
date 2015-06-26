require "cc/kafka/consumer"
require "cc/kafka/producer"

module CC
  module Kafka
    class << self
      attr_accessor :offset_model, :logger
    end
  end
end
