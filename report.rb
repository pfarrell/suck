require 'splunk-sdk-ruby'
require 'byebug'

generic="\[(?<date>.*)\]\[(?<type>.*)\].* (?<listing_id>[0-9]*) (?<message>.*)"

def parse(str)
  regex="\[(?<date>.*)\]\[.*\]executionTime (?<listing_id>.*) (?<method>.*): (?<time>.*)"
  str.match(regex)
end

service = Splunk::connect(
  scheme: :https,
  host: ARGV[0],
  port: ARGV[1].to_i,
  username: ARGV[2], 
  password: ARGV[3]
)

service.login

stream = service.create_export("#{ARGV[4]}",
  earliest_time: "2015-06-03T18:00:00.000-07:00",
  latest_time: "now"
)

readers= Splunk::MultiResultsReader.new(stream)
readers.each do |reader|
  reader.each do |result|
    parse(result["_raw"])
  end
end

