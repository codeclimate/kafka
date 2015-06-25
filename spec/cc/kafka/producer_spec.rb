require "spec_helper"

module CC::Kafka
  describe Producer do
    describe "#send_message" do
      it "sends a message via Poseidon" do
        data = { some: :data }
        serialized = BSON.serialize(data).to_s

        poseidon_message = double("Message")
        expect(Poseidon::MessageToSend).to receive(:new).
          with("a-topic", serialized, "a-key").
          and_return(poseidon_message)

        poseidon_producer = double("Producer")
        expect(poseidon_producer).to receive(:send_messages).
          with([poseidon_message])

        expect(Poseidon::Producer).to receive(:new).
          with(["host:1234"], "a-client-id", compression_codec: :gzip).
          and_return(poseidon_producer)

        producer = Producer.new("a-client-id", "kafka://host:1234/a-topic")
        producer.send_message(data, "a-key")
      end
    end
  end
end
