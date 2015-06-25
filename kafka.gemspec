$LOAD_PATH.unshift(File.join(__FILE__, "../lib"))
require "cc/kafka/version"

Gem::Specification.new do |s|
  s.name        = "kafka"
  s.version     = CC::Kafka::VERSION
  s.summary     = "Code Climate Kafka Client"
  s.license     = "MIT"
  s.authors     = "Code Climate"
  s.email       = "hello@codeclimate.com"
  s.homepage    = "https://github.com.com/codeclimate/kafka"
  s.description = "Code Climate Kafka Client"

  s.files         = Dir["lib/**/*.rb"]
  s.require_paths = ["lib"]

  s.add_dependency "bson"
  s.add_dependency "bson_ext"
  s.add_dependency "poseidon"
  s.add_development_dependency "rake"
end
