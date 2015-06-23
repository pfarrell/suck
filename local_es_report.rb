require 'splunk-sdk-ruby'
require 'json'
require 'httparty'

class ElasticSearch
  include HTTParty
  attr_accessor :host

  def initialize(host="http://localhost:9200", index="gds", type="timing")
    @host = host
    @index = index
    @type = type
  end

  def url(index, type, id)
    "#{@host}/#{index}/#{type}/#{id}"
  end

  def save(id, json)
    self.class.put(url(@index, @type, id), body: json)
  end
end

class GDS
  include HTTParty
  attr_accessor :host, :group, :date, :data, :tags

#  def initialize(host="http://prosper:Password23@prosper-gds.herokuapp.com")
  def initialize(host="http://pfarrell:password@127.0.0.1:9292")
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

  def save
    self.class.post("#{host}/entries/#{group}", body: self.to_json)
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

last_date = (DateTime.strptime(GDS.new.latest.date, "%Y-%m-%d %H:%M:%S ") + 7/24.0).iso8601
end_date = "now"
#last_date = "2015-06-03T12:00:00+7:00"
#end_date = "2015-06-09T13:00:00+7:00"

puts "getting splunk data after: #{last_date}"

stream = service.create_export("search #{ARGV[4]}",
  earliest_time: last_date,
  latest_time: end_date
)

readers= Splunk::MultiResultsReader.new(stream)
es = ElasticSearch.new

readers.each do |reader|
  reader.each do |result|
    print "."
    gds = GDS.parse(result["_raw"])
    json_str = gds.save.body
    obj= massage(json_str)
    es.save(obj["id"], obj.to_json())
  end
end
print "\n"

