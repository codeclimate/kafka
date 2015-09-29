require "spec_helper"

module CC::Kafka
  describe Producer do
    let(:inner_producer) { double(send_message: nil, close: nil) }

    before do
      allow(Producer::HTTP).to receive(:new).and_return(inner_producer)
      allow(Producer::Poseidon).to receive(:new).and_return(inner_producer)
    end

    describe "#send_message" do
      it "chooses the right producer for http://" do
        expect(Producer::HTTP).to receive(:new).
          with("host", 8080, "a-topic").
          and_return(inner_producer)

        Producer.new("http://host:8080/a-topic").send_message({})
      end

      it "chooses the right producer for https://" do
        expect(Producer::HTTP).to receive(:new).
          with("host", 8080, "a-topic", true).
          and_return(inner_producer)

        Producer.new("https://host:8080/a-topic").send_message({})
      end

      it "chooses the right producer for kafka://" do
        expect(Producer::Poseidon).to receive(:new).
          with("host", 8080, "a-topic", "a-client-id").
          and_return(inner_producer)

        Producer.new("kafka://host:8080/a-topic", "a-client-id").send_message({})
      end

      it "raises an exception on an invalid scheme" do
        expect { Producer.new("ftp://host").send_message({}) }.to raise_error(Producer::InvalidScheme)
      end

      it "sends BSON-serialized data" do
        data = { some: :data }

        expect(inner_producer).to receive(:send_message).
          with(BSON.serialize(data).to_s, "a-key")

        Producer.new("http://host").send_message(data, "a-key")
      end

      it "closes the producer on exceptions" do
        ex = RuntimeError.new("boom")
        allow(BSON).to receive(:serialize).and_raise(ex)

        expect(inner_producer).to receive(:close)
        expect { Producer.new("http://host").send_message({}, nil) }.to raise_error(ex)
      end
    end

    describe "#send_snapshot_document" do
      let(:producer) { Producer.new("http://host:8080/a-topic") }

      it "adds snapshot_id to document and forwards to #send_message" do
        snapshot_id = BSON::ObjectId.new
        expected_message = {
          type: "document",
          collection: "test.collection",
          document: {foo: "bar", snapshot_id: snapshot_id}
        }
        expect(inner_producer).to receive(:send_message).with(BSON.serialize(expected_message).to_s, snapshot_id.to_s)
        producer.send_snapshot_document(
          collection: "test.collection",
          document: {foo: "bar"},
          snapshot_id: snapshot_id
        )
      end

      it "handles a snapshot ID string" do
        snapshot_id_str = BSON::ObjectId.new.to_s
        expected_message = {
          type: "document",
          collection: "test.collection",
          document: {foo: "bar", snapshot_id: BSON::ObjectId(snapshot_id_str)}
        }
        expect(inner_producer).to receive(:send_message).with(BSON.serialize(expected_message).to_s, snapshot_id_str)
        producer.send_snapshot_document(
          collection: "test.collection",
          document: {foo: "bar"},
          snapshot_id: snapshot_id_str
        )
      end

      it "supports added envelope options" do
        enqueued_at = Time.now
        snapshot_id = BSON::ObjectId.new
        expected_message = {
          enqueued_at: enqueued_at,
          type: "document",
          collection: "test.collection",
          document: {foo: "bar", snapshot_id: snapshot_id},
        }
        expect(inner_producer).to receive(:send_message).with(BSON.serialize(expected_message).to_s, snapshot_id.to_s)
        producer.send_snapshot_document(
          collection: "test.collection",
          document: {foo: "bar"},
          snapshot_id: snapshot_id,
          enqueued_at: enqueued_at
        )
      end
    end

    describe "#close" do
      it "closes the producer" do
        expect(inner_producer).to receive(:close)

        Producer.new("http://host").close
      end
    end
  end
end
