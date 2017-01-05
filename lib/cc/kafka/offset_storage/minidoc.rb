module CC
  module Kafka
    module OffsetStorage
      module Minidoc
        def self.included(base)
          base.extend ClassMethods
          base.attribute :topic, String
          base.attribute :partition, Integer
          base.attribute :current, Integer
        end

        module ClassMethods
          def find_or_create!(attributes)
            find_one(attributes) || create!(attributes)
          end
        end
      end
    end
  end
end
