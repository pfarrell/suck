require 'splunk-sdk-ruby'
require 'json'
require 'httparty'
require 'em-http-request'
require 'digest/sha1'
require 'byebug'

class ElasticSearch
  include HTTParty
  attr_accessor :host

  def initialize(host="http://np32.c1.dev:9200", index="gds", type="timing")
    @host = host
    @index = index
    @type = type
  end

  def url(index, type, id)
    "#{@host}/#{index}/#{type}/#{id}"
  end

  def request(id)
    url(@index, @type, id)
  end
end

class GDS
  include HTTParty
  attr_accessor :id, :index, :host, :date, :listing_id, :type, :method

  def initialize(host="http://np32.c1.dev:9292")
    @host = host
    @data = {}
    @tags = []
  end

  def self.from_str(str)
    gds = GDS.new
    require 'byebug'
    byebug
    hsh = str.match(/\[(?<date>.*)\]\[(?<type>.*)\].* (?<listing_id>[0-9]*) (?<message>.*)/)
    msg = gds.massage(hsh["type"], hsh["message"])
    gds.index = "gds-#{hsh["date"].sub(/ .*/, '')}"
    gds.date = hsh["date"].sub(/ /, "T").sub(/ .*/, "")
    gds.listing_id = hsh["listing_id"]
    gds.type = msg[:type]
    gds.method = msg[:method]
    gds.id = Digest::SHA1.hexdigest "#{gds.date}|#{gds.listing_id}|#{gds.method}"
    gds
  end

  def massage(type, msg)
    return {error: msg} if type == "ERROR"
    execution=/(?<method>.*): PT(?<time>.*)S/
    hsh=msg.match(execution)
    {timing: hsh["time"], method: hsh["method"]}
  end

  def from_json(json)
    hsh = JSON.parse(json)
    gds = GDS.new
    gds.date = hsh["date"]
    gds.data = hsh["data"]
    gds.tags = hsh["tags"]
    gds.index = hsh["index"]
    gds
  end

  def to_json(opts={})
    {id: id, date: date, host: host, listing_id: listing_id, method: method, type: type, index: index}.to_json(opts)
  end

  def self.save(json,iter, callback)
    http = EventMachine::HttpRequest.new("http://np32.c1.dev:9292/entries/gds").post body: json
    http.callback { callback.call(http.response, iter)}
  end
end

def massage2(json_str)
  begin
    obj = JSON.parse(json_str)
    obj["data"].each do |k,v|
      obj[k] = v
    end
    ret = obj.delete_if do |k,v|
      ["data", "created_at", "updated_at"].select{|x| x == k}.size > 0
    end
    ret["index"] = "test-gds-#{ret["date"].sub(/ .*/, '')}"
    ret["date"].sub!(/ /, "T")
    ret["date"].sub!(/ .*/, "")
    ret["timing"] = ret["timing"].to_f
  rescue
  end
  ret
end

service = Splunk::connect(
  scheme: :https,
  host: ARGV[0],
  port: ARGV[1].to_i,
  username: ARGV[2], 
  password: ARGV[3]
)

service.login

start_date = ARGV[5] || (DateTime.strptime(GDS.new.latest.date, "%Y-%m-%d %H:%M:%S ")).iso8601
end_date = ARGV[6] || "now"

puts "getting splunk data between #{start_date} and #{end_date}"

stream = service.create_export("search #{ARGV[4]}",
  earliest_time: start_date,
  latest_time: end_date
)

readers= Splunk::MultiResultsReader.new(stream)

es = ElasticSearch.new

def request(id, index)
  "http://np32.c1.dev:9200/#{index}/timing/#{id}"
end

cnt = 0
EventMachine.run do
  puts "em loop"
  readers.each do |reader|
    puts 'reader'
    EM::Iterator.new(reader, 25).each do |result, iter|
      gds = GDS.from_str(result["_raw"])

      http = EventMachine::HttpRequest.new(request(gds.id, gds.index)).put body: gds.to_json()
      
      http.callback { 
        iter.next
      }
      http.errback { print http.error; EM.stop }

      cnt += 1
      if cnt % 100 == 0
        40.times {|i| print "\b" }
        print "#{gds.date}: #{cnt}"
      end
    end
  end
end
print "\n#{cnt}"
print "done"
EM.stop
print "\n"


