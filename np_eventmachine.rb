require 'splunk-sdk-ruby'
require 'json'
require 'httparty'
require 'em-http-request'

class ElasticSearch
  include HTTParty
  attr_accessor :host

  def initialize(host="http://10.121.184.107:9200", index="gds", type="timing")
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
  attr_accessor :host, :group, :date, :data, :tags

  def initialize(host="http://10.121.184.107:9292")
    @host = host
    @group = "gds"
    @data = {}
    @tags = []
  end

  def self.parse(str)
    gds = GDS.new
    generic=/\[(?<date>.*)\]\[(?<type>.*)\].* (?<listing_id>[0-9]*) (?<message>.*)/
    
    hsh = str.match(generic)
    gds.date = hsh["date"]
    gds.data = {}
    gds.data["listing_id"] = hsh["listing_id"]
    gds.data.merge!(gds.massage(hsh["type"], hsh["message"]))
    gds.tags << hsh["type"]
    gds.tags << hsh["listing_id"]
    unless gds.data[:method].nil?
      gds.data[:method].split(".").each do |str|
        gds.tags << str
      end
    end
    gds.tags << hsh["date"].gsub(/ .*/, "")
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
    gds
  end

  def to_json(opts={})
    {group: group, date: date, data: data, tags: tags}.to_json(opts)
  end

  def self.save(json,iter, callback)
    http = EventMachine::HttpRequest.new("http://np32.c1.dev:9292/entries/timing").post body: json
    http.callback { callback.call(http.response, iter)}
    #self.class.post("#{host}/entries/#{group}", body: self.to_json)
  end

  def latest
    json = self.class.get("#{host}/entries/#{group}/latest").body
    from_json(json)
  end
  
end

def massage(json_str)
  obj = JSON.parse(json_str)
  obj["data"].each do |k,v|
    obj[k] = v
  end
  ret = obj.delete_if do |k,v|
    ["data", "created_at", "updated_at"].select{|x| x == k}.size > 0
  end
  ret["date"].sub!(/ /, "T")
  ret["date"].sub!(/ .*/, "")
  ret["timing"] = ret["timing"].to_f
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

last_date = ARGV[5] || (DateTime.strptime(GDS.new.latest.date, "%Y-%m-%d %H:%M:%S ")).iso8601
end_date = ARGV[6] || "now"

puts "getting splunk data after: #{last_date}"

stream = service.create_export("search #{ARGV[4]}",
  earliest_time: last_date,
  latest_time: end_date
)

readers= Splunk::MultiResultsReader.new(stream)
es = ElasticSearch.new

def db_save_callback(json, iter)
  obj= massage(json)
  http = EventMachine::HttpRequest.new(request(obj["id"])).put body: obj.to_json()
  iter.next
  http.callback { print 'V' }
  http.errback { print http.error; EM.stop }
end

def request(id)
  "http://10.121.184.107:9200/gds/timing/#{id}"
end

EventMachine.run do
  puts "em loop"
  readers.each do |reader|
    puts "readers"
    EM::Iterator.new(reader, 20).each do |result, iter|
    #reader.each do |result|
        print "."
        gds = GDS.parse(result["_raw"])
        body = GDS.save(gds.to_json, iter, method(:db_save_callback))
    end
  end
end
EM.stop
print "\n"


