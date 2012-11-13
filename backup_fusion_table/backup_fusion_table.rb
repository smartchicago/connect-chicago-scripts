#!/usr/bin/env ruby

require 'csv'
require 'fusion_tables'
require 'yaml'
require 'date'

SEP = ","
CSV_PATH = "backups/locations-#{Date.today.strftime("%m-%d-%Y")}.csv"

# helper functions
def header(hash)
  hash.keys.join SEP
end

def to_csv(hash)
  hash.values.map do |value|
    escape value unless value.nil?
  end.join SEP
end

# get login and fusion table settings
begin
  yaml = YAML.load_file("config.yml")
  fusion_table_id = yaml['fusion_table_id']
  google_account = yaml['google_account']
  google_password = yaml['google_password']
rescue Errno::ENOENT
  puts "config file not found"
end

# connect to fusion tables
unless google_account.nil? || google_account == ''
  puts 'connecting to fusion tables'
  FT = GData::Client::FusionTables.new
  FT.clientlogin(google_account, google_password)
end

if File.exist?(CSV_PATH) 
  puts 'CSV file exists - deleting'
  File.delete(CSV_PATH)
end

# fetch all rows
all_locations = FT.execute("SELECT * FROM #{fusion_table_id};")
#puts all_locations

# write to CSV
CSV.open(CSV_PATH, "wb") do |csv|
  csv << all_locations.first.keys
  all_locations.each do |location|
    csv << location.values
  end
end

# TODO FTP to ETL server

puts "done"
nil