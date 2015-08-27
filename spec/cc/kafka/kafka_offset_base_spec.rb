require "spec_helper"

module CC::Kafka
  class KafkaOffset < Minidoc
    include CC::Kafka::KafkaOffsetBase
  end

  describe KafkaOffset do
    context ".find_or_create!" do
      it "finds or creates the given record" do
        records = [
          KafkaOffset.find_or_create!(topic: "analysis_results", partition: 0),
          KafkaOffset.find_or_create!(topic: "analysis_results", partition: 0),
          KafkaOffset.find_or_create!(topic: "analysis_results", partition: 0),
        ]

        expect(KafkaOffset.count).to eq 1
        expect(records.uniq(&:id).count).to eq 1
      end

      it "sets current if provided" do
        offset = KafkaOffset.create!(
          topic: "analysis-results",
          partition: 0,
          current: 4
        )

        expect(offset.current).to eq 4
      end

      it "does not set current if not provided" do
        offset = KafkaOffset.create!(
          topic: "analysis-results",
          partition: 0
        )

        expect(offset.current).to be_nil
      end
    end
  end
end
