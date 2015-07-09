require "spec_helper"

class CC::Kafka::Producer
  describe HTTP do
    describe "#send_message" do
      it "POSTs to / with serialized data" do
        producer = HTTP.new("host", 8080, "a-topic")
        request = stub_request(:post, "host:8080/").
          with(
            headers: {
              "Key" => "a-key",
              "Topic" => "a-topic"
            },
            body: "some-data",
          )

        producer.send_message("some-data", "a-key")

        expect(request).to have_been_made
      end

      it "doesn't include a nil key" do
        producer = HTTP.new("host", 8080, "a-topic")
        request = stub_request(:post, "host:8080/").
          with(
            headers: { "Topic" => "a-topic" },
            body: "some data",
          )

        producer.send_message("some data", nil)

        expect(request).to have_been_made
      end

      it "raises if the response is unsuccessful" do
        producer = HTTP.new("host", 8080, "a-topic")

        stub_request(:post, "host:8080/").to_return(status: 500)

        expect { producer.send_message({}, nil) }.to raise_error(HTTP::HTTPError)
      end
    end
  end
end
