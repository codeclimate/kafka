require "spec_helper"

module CC::Kafka
  describe Producer do
    let(:producer) { double(send_message: nil, close: nil) }

    before do
      allow(Producer::HTTP).to receive(:new).and_return(producer)
      allow(Producer::Poseidon).to receive(:new).and_return(producer)
    end

    describe "#send_message" do
      it "chooses the right producer for http://" do
        expect(Producer::HTTP).to receive(:new).
          with("host", 8080, "a-topic").
          and_return(producer)

        Producer.new("http://host:8080/a-topic").send_message({})
      end

      it "chooses the right producer for kafka://" do
        expect(Producer::Poseidon).to receive(:new).
          with("host", 8080, "a-topic", "a-client-id").
          and_return(producer)

        Producer.new("kafka://host:8080/a-topic", "a-client-id").send_message({})
      end

      it "raises an exception on an invalid scheme" do
        expect { Producer.new("ftp://host").send_message({}) }.to raise_error(Producer::InvalidScheme)
      end

      it "sends BSON-serialized data" do
        data = { some: :data }

        expect(producer).to receive(:send_message).
          with(BSON.serialize(data).to_s, "a-key")

        Producer.new("http://host").send_message(data, "a-key")
      end

      it "closes the producer on exceptions" do
        ex = RuntimeError.new("boom")
        allow(BSON).to receive(:serialize).and_raise(ex)

        expect(producer).to receive(:close)
        expect { Producer.new("http://host").send_message({}, nil) }.to raise_error(ex)
      end
    end

    describe "#close" do
      it "closes the producer" do
        expect(producer).to receive(:close)

        Producer.new("http://host").close
      end
    end
  end
end
