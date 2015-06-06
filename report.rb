require 'splunk-sdk-ruby'
require 'json'
require 'httparty'
require 'byebug'

class GDS
  include HTTParty
  attr_accessor :host, :group, :type, :date, :data

  def initialize(host="http://prosper-gds.herokuapp.com")
#  def initialize(host="http://127.0.0.1:9292")
    @host = host
    @group = "gds"
  end

  def self.parse(str)
    gds = GDS.new
    generic=/\[(?<date>.*)\]\[(?<type>.*)\].* (?<listing_id>[0-9]*) (?<message>.*)/
    
    hsh = str.match(generic)
    gds.date = hsh["date"]
    gds.type = hsh["type"]
    gds.data = {}
    gds.data["listing_id"] = hsh["listing_id"]
    gds.data.merge!(gds.massage(hsh["type"], hsh["message"]))
    gds
  end

  def massage(type, msg)
    return {error: msg} if type == "ERROR"
    execution=/(?<method>.*): (?<time>.*)/
    hsh=msg.match(execution)
    {time: hsh["time"], method: hsh["method"]}
  end

  def to_json(opts={})
    {group: group, type: type, date: date, data: data}.to_json(opts)
  end

  def save
    self.class.post("#{host}/entries/#{group}", body: self.to_json)
  end
  
end

service = Splunk::connect(
  scheme: :https,
  host: ARGV[0],
  port: ARGV[1].to_i,
  username: ARGV[2], 
  password: ARGV[3]
)

service.login

stream = service.create_export("search #{ARGV[4]}",
  earliest_time: "2015-06-03T18:00:00.000-07:00",
  latest_time: "now"
)

readers= Splunk::MultiResultsReader.new(stream)
readers.each do |reader|
  reader.each do |result|
    print "."
    gds = GDS.parse(result["_raw"])
    gds.save
  end
end

