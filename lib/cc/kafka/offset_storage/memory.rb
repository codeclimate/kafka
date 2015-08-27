module CC
  module Kafka
    module OffsetStorage
      Memory = Struct.new(:topic, :partition, :current) do
        def self.transaction
          yield
        end

        def self.find_or_create!(attrs)
          @offset ||= new(attrs[:topic], attrs[:partition], nil)
        end

        def set(attrs)
          attrs.each { |k, v| self[k] = v }
        end
      end
    end
  end
end
