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

      it "closes the producer on exceptions" do
        error = RuntimeError.new("boom")
        poseidon_producer = stub_producer

        allow(Poseidon::MessageToSend).to receive(:new).and_raise(error)

        producer = Producer.new("a-client-id", "kafka://host:1234/a-topic")

        expect(poseidon_producer).to receive(:close)
        expect { producer.send_message({}) }.to raise_error(error)
      end

      context "with an HTTP proxy" do
        it "POSTs to /message with serialized data" do
          data = { some: :data }
          serialized = BSON.serialize(data).to_s
          producer = Producer.new("", "http://host:8080/a-topic")

          request = stub_request(:post, "host:8080/message").
            with(body: {
              topic: "a-topic",
              message: serialized,
              key: "a-key"
            })

          producer.send_message(data, "a-key")

          expect(request).to have_been_made
        end

        it "doesn't include a nil key" do
          data = { some: :data }
          serialized = BSON.serialize(data).to_s
          producer = Producer.new("", "http://host:8080/a-topic")

          request = stub_request(:post, "host:8080/message").
            with(body: {
              topic: "a-topic",
              message: serialized,
            })

          producer.send_message(data)

          expect(request).to have_been_made
        end

        it "raises if the response is unsuccessful" do
          producer = Producer.new("", "http://host:8080/a-topic")

          stub_request(:post, "host:8080/message").to_return(status: 500)

          expect { producer.send_message({}) }.to raise_error(Producer::HTTPError)
        end
      end
    end

    describe "#close" do
      it "closes the Poseidon producer" do
        poseidon_producer = stub_producer

        expect(poseidon_producer).to receive(:close)
        Producer.new("a-client-id", "kafka://host:1234/a-topic").close
      end
    end

    def stub_producer
      double("Poseidon::Producer").tap do |producer|
        allow(Poseidon::Producer).to receive(:new).and_return(producer)
      end
    end
  end
end
