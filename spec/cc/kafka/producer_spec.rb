require "spec_helper"

module CC::Kafka
  describe Producer do
    describe "#send_message" do
      it "chooses the right producer based on scheme"
      it "closes the producer on exceptions"
    end

    describe "#close" do
      it "closes the producer"
    end

    def stub_producer
      double("Poseidon::Producer").tap do |producer|
        allow(Poseidon::Producer).to receive(:new).and_return(producer)
      end
    end
  end
end
