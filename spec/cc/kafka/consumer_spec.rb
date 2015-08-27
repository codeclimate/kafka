require "spec_helper"

module CC::Kafka
  describe Consumer do
    before do
      CC::Kafka.offset_model = Offset
    end

    describe "#start" do
      it "consumes via Poseidon and tracks the current offset" do
        messages = [
          double("1", offset: 1, value: BSON.serialize({ x: 1 }).to_s),
          double("2", offset: 2, value: BSON.serialize({ x: 2 }).to_s),
          double("3", offset: 3, value: BSON.serialize({ x: 3 }).to_s),
        ]
        messages_seen = []
        poseidon_consumer = double("PartitionConsumer")
        expect(poseidon_consumer).to receive(:close)
        allow(poseidon_consumer).to receive(:fetch).and_return([])
        expect(poseidon_consumer).to receive(:fetch).and_return(messages)
        expect(Poseidon::PartitionConsumer).to receive(:consumer_for_partition).
          with("a-client-id", %w[seed brokers], "a-topic", "a-partition", :earliest_offset).
          and_return(poseidon_consumer)

        consumer = Consumer.new("a-client-id", %w[seed brokers], "a-topic", "a-partition")
        consumer.on_message { |message| messages_seen << message }
        run_consumer(consumer)

        offset = Offset.find_or_create!(topic: "a-topic", partition: "a-partition")
        expect(offset.current).to eq 4
        expect(messages_seen).to eq [
          {"x"=>1, "kafka_message_offset"=>"a-topic-a-partition-1"},
          {"x"=>2, "kafka_message_offset"=>"a-topic-a-partition-2"},
          {"x"=>3, "kafka_message_offset"=>"a-topic-a-partition-3"},
        ]
      end
    end

    # This is arguably a hack, but it does also test the graceful stop behavior
    def run_consumer(consumer)
      Thread.new { sleep 0.1 and consumer.stop }

      consumer.start
    end
  end

  Offset = Struct.new(:topic, :partition, :current) do
    def self.transaction
      yield
    end

    def self.find_or_create!(attrs)
      @offset ||= new(attrs[:topic], attrs[:partition], nil)
    end

    def set(attrs)
      attrs.each { |k,v| self[k] = v }
    end
  end
end
