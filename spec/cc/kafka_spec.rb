require "spec_helper"

module CC
  describe Kafka do
    describe "#logger" do
      after { Kafka.logger = nil }

      it "defaults to a standard logger" do
        expect(Kafka.logger).to be_a(Logger)
      end

      it "can be overriden" do
        logger = Object.new

        Kafka.logger = logger

        expect(Kafka.logger).to eq logger
      end
    end

    describe "#offset_model" do
      after { Kafka.offset_model = nil }

      it "raises a useful exception if mis-configured" do
        Kafka.offset_model = nil

        expect { Kafka.offset_model }.to raise_error /offset_model not set/i
      end

      it "can be set" do
        offset_model = Object.new

        Kafka.offset_model = offset_model

        expect(Kafka.offset_model).to eq offset_model
      end
    end

    describe "#statsd" do
      it "defaults to a null object" do
        expect { Kafka.statsd.foo }.not_to raise_error
      end

      it "can be set" do
        statsd = Object.new

        Kafka.statsd = statsd

        expect(Kafka.statsd).to eq statsd
      end
    end
  end
end
