#!/usr/bin/env ruby

require 'sequel'
require 'csv'

#establish database connection
DB = Sequel.connect('postgres://derek@localhost:5432/early_childhood')

#clear out our table first
puts "clearing out existing data ..."
DB.run("truncate table location")
DB.run("truncate table program")

$total_inserted = 0
$total_duplicates = 0

def get_location_id(slug)
  location = DB[:location].filter(:slug => slug)
  if (location.count == 0)
    0
  else
    location.first[:id]
  end
end

def insert_location(line)

  #determine unique slug based on address
  slug = line[:address].downcase.strip.gsub(' ', '-').gsub(/[^\w-]/, '')
  
  if get_location_id(slug) == 0
    puts "inserting #{slug}"
  
    DB[:location].insert( :slug => slug, 
                        :name => line[:site_name],
                        :address => line[:address],
                        :phone_number => line[:phone])
                        
    $total_inserted += 1
  else
    $total_duplicates += 1
    puts "location exists: #{slug}"
  end
end

#start with the CPS Early Childhood Portal dataset - its the most complete
CSV.foreach("source/csv/CPS_Early_Childhood_Portal_scrape.csv", 
                                  :headers           => true,
                                  :header_converters => :symbol) do |line|
  insert_location line
  
end

puts "total inserted: #{$total_inserted}"
puts "duplicates: #{$total_duplicates}"

nil