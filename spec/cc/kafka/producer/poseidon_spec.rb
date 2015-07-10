require "spec_helper"

class CC::Kafka::Producer
  describe Poseidon do
    describe "#send_message" do
      it "sends a message via Poseidon" do
        poseidon_message = double("Message")
        expect(::Poseidon::MessageToSend).to receive(:new).
          with("a-topic", "some-data", "a-key").
          and_return(poseidon_message)

        poseidon_producer = double("Producer")
        expect(poseidon_producer).to receive(:send_messages).
          with([poseidon_message])

        expect(::Poseidon::Producer).to receive(:new).
          with(["host:1234"], "a-client-id", compression_codec: :gzip).
          and_return(poseidon_producer)

        producer = Poseidon.new("host", 1234, "a-topic", "a-client-id")
        producer.send_message("some-data", "a-key")
      end
    end

    describe "#close" do
      it "closes the poseidon producer" do
        poseidon_producer = double("Producer")
        allow(::Poseidon::Producer).to receive(:new).and_return(poseidon_producer)

        expect(poseidon_producer).to receive(:close)
        Poseidon.new("host", 1234, "a-topic", "").close
      end
    end

    def stub_producer
      double("Poseidon::Producer").tap do |producer|
        allow(::Poseidon::Producer).to receive(:new).and_return(producer)
      end
    end
  end
end
