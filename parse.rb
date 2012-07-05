#!/usr/bin/env ruby

require "time"
require "json"
require "fileutils"

FEATURE_FILE = "../data/features.csv"

def write_features(array)
  File.open(FEATURE_FILE, "a+") do |f|
    array.each do |hash|
      yield f, hash
    end
  end
end

File.delete("../data/features.csv")

files = Dir["../data/locations/query.*"]

# HEADERS
File.open(FEATURE_FILE, "a+") do |f|
  f.write("id,address,time,asset_name,asset_type,x,y\n")
end

files.each do |file|
  json = JSON.parse(open(file).read)
  feature_array = json["features"]

  # ROWS
  write_features(feature_array) do |file, hash|
    attributes = hash["attributes"]
    geometry   = hash["geometry"]

    object_id  = attributes["OBJECTID"]
    address    = "#{attributes["ADDRESS"]} CHICAGO, IL"
    time       = Time.parse(attributes["POSTING_TIME"]).to_s
    asset_name = attributes["ASSET_NAME"]
    asset_type = attributes["ASSET_TYPE"]
    x          = geometry["x"]
    y          = geometry["y"]

    line = <<-FEATURE
#{object_id},"#{address}","#{time}","#{asset_name}","#{asset_type}",#{x},#{y}
FEATURE
    file.write(line)
  end
end; nil

