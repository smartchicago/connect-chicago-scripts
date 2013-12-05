#!/usr/bin/env ruby

require 'csv'
require 'fusion_tables'
require 'yaml'
require 'date'
require 'net/ftp'
require 'fileutils'

SEP = ","
CSV_PATH = "backups/connect-chicago-locations-#{Date.today.strftime("%Y-%m-%d")}.csv"
CSV_ETL = "backups/connect-chicago-locations.csv"

# get login and fusion table settings
begin
  yaml = YAML.load_file("config.yml")
  fusion_table_id = yaml['fusion_table_id']
  google_account = yaml['google_account']
  google_password = yaml['google_password']
  google_api_key = yaml['google_api_key']
  ftp_url = yaml['ftp_url']
  ftp_user = yaml['ftp_user']
  ftp_pass = yaml['ftp_pass']
rescue Errno::ENOENT
  puts "config file not found"
end

# connect to fusion tables
unless google_account.nil? || google_account == ''
  puts 'connecting to fusion tables'
  FT = GData::Client::FusionTables.new
  FT.clientlogin(google_account, google_password)
  FT.set_api_key(google_api_key)
end

if File.exist?(CSV_PATH) 
  puts 'CSV file exists - deleting'
  File.delete(CSV_PATH)
end

# fetch all rows
all_locations = FT.execute("SELECT * FROM #{fusion_table_id};")
#puts all_locations

# write to CSV
puts "saving to #{CSV_PATH} for backup csv"
CSV.open(CSV_PATH, "wb") do |csv|
  csv << all_locations.first.keys
  all_locations.each do |location|
    csv << location.values
  end
end

puts "saving to #{CSV_ETL} for ETL"
CSV.open(CSV_ETL, "wb") do |csv|
  csv_headers = [:id,:url,:organization_name,:organization_type,:full_address,:address,:city,:state,:zip_code,:org_phone,:hours,:website,:appointment,:internet,:wifi,:training,:pc_use_restrictions,:hardware_public,:assistive_technology,:internet_upload,:internet_download,:volunteers_used,:volunteers_used_how,:volunteers_wanted_how,:public_wifi_detail,:nearest_parking,:nearest_parking_detail,:public_transportation_detail,:time_allowed_per_user,:time_allowed_per_user_detail,:room_list,:handicap_access_detail,:friendly_description,:agency_leadership_contact,:agency_staff_person_contact_email,:twitter_handle,:training_types,:flickr_tag,:training_headline,:training_description,:training_url,:location_leadership,:location_leadership_email,:pcc_staff_person,:pcc_staff_person_email,:latitude,:longitude,:location]
  # csv << csv_headers
  all_locations.each do |location|
    # remove unused columns
    location.delete(:checked)
    location.delete(:notes)
    location.delete(:tecservices_id)
    location.delete(:october_open_house)

    # append Connect Chicago url to slug
    location[:url] = "http://locations.weconnectchicago.org/location/#{location[:slug]}"
    location.delete(:slug)

    # append (lat,long) column
    location[:location] = "(#{location[:latitude]},#{location[:longitude]})"
    csv << csv_headers.map { |h| location[h] }
  end
end

# FTP to ETL server (with retries)
puts "FTPing to ETL server"
1.upto(4) do |i|
  begin
    Net::FTP.open(ftp_url, ftp_user, ftp_pass) do |ftp|
      ftp.passive = true
      ftp.debug_mode = true
      ftp.putbinaryfile(CSV_ETL)
    end
    break
  rescue
    puts "Failed FTP attempt ##{i}" 
    sleep 15
  end
end

puts "done"
nil