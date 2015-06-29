$LOAD_PATH.unshift(File.join(__FILE__, "../lib"))
require "cc/kafka/version"

Gem::Specification.new do |s|
  s.name        = "codeclimate-kafka"
  s.version     = CC::Kafka::VERSION
  s.summary     = "Code Climate Kafka Client"
  s.license     = "MIT"
  s.authors     = "Code Climate"
  s.email       = "hello@codeclimate.com"
  s.homepage    = "https://codeclimate.com"
  s.description = "Code Climate Kafka Client"

  s.files         = Dir["lib/**/*.rb"]
  s.require_paths = ["lib"]

  s.add_dependency "bson", "~> 1.12.2"
  s.add_dependency "bson_ext", "~> 1.12.2"
  s.add_dependency "poseidon", "~> 0.0.5"
  s.add_development_dependency "rake"
end
