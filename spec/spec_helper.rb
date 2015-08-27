require "dotenv"
require "cc/kafka"
require "webmock/rspec"
require "minidoc/test_helpers"

Dotenv.overload(".env", ".env.test", ".env.test.local")

CC::Kafka.logger.level = Logger::ERROR

RSpec.configure do |conf|
  mongo = Mongo::MongoClient.from_uri(ENV['KAFKA_MONGODB_URL'])

  Minidoc.connection = mongo
  Minidoc.database_name = mongo.db.name

  conf.before do
    Minidoc::TestHelpers.clear_database
  end
end
