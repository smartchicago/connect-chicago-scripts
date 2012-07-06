#!/usr/bin/env ruby

require 'sequel'
require 'csv'

#establish database connection
DB = Sequel.connect('postgres://derek@localhost:5432/early_childhood')

$locations_inserted = 0
$location_duplicates = 0
$programs_inserted = 0
$food_inspections_inserted = 0

def create_tables

  #clear out our table first
  puts "dropping existing tables ..."
  begin
    DB.run("DROP TABLE location;")
    DB.run("DROP TABLE program;")
    DB.run("DROP TABLE food_inspection;")
  rescue
    puts "tables don't exist"
  end
  
  location_sql = <<END_SQL
CREATE TABLE location
(
  id serial NOT NULL,
  slug character varying(100),
  name character varying(100),
  address character varying(250),
  phone_number character varying(50),
  environmental_characteristics text,
  transportation character varying(250),
  created_date timestamp without time zone DEFAULT ('now'::text)::date,
  last_edited_date timestamp without time zone DEFAULT ('now'::text)::date
)
END_SQL

  DB.run(location_sql)

  program_sql = <<END_SQL
CREATE TABLE program
(
  id serial NOT NULL,
  name character varying(100),
  location_id integer NOT NULL,
  eligibility_information text,
  selection_process text,
  site_affiliation character varying(50),
  ccap_acceptance integer,
  state_licensing integer,
  accreditation character varying(50),
  length_of_day character varying(100),
  full_or_part_time character varying(50),
  program_hours character varying(50),
  program_duration character varying(50),
  created_date timestamp without time zone DEFAULT ('now'::text)::date,
  last_edited_date timestamp without time zone DEFAULT ('now'::text)::date
)
END_SQL

  DB.run(program_sql)
  
  food_inspection_sql = <<END_SQL
CREATE TABLE food_inspection
(
  id serial NOT NULL,
  location_id integer,
  inspection_id character varying(50),
  dba_name character varying(100),
  aka_name character varying(100),
  license_num character varying(50),
  risk character varying(50),
  inspection_date timestamp without time zone,
  inspection_type character varying(50),
  results character varying(50),
  violations text,
  created_date timestamp without time zone DEFAULT ('now'::text)::date,
  last_edited_date timestamp without time zone DEFAULT ('now'::text)::date
)
END_SQL

  DB.run(food_inspection_sql)
  
end

def location_id_from_slug(slug)
  location = DB[:location].filter(:slug => slug)
  if (location.count == 0)
    0
  else
    location.first[:id]
  end
end

def location_id_from_address_and_name(name)
  search_sql = "select * from location where name ILIKE ?"
  location = DB[search_sql, name]
  
  if (location.count == 0)
    0
  else
    location.first[:id]
  end
end

def insert_location(line)

  #determine unique slug based on address
  slug = line[:address].downcase.strip.gsub(' ', '-').gsub(/[^\w-]/, '')
  
  loc_id = location_id_from_slug(slug)
  if loc_id == 0
    #puts "inserting #{slug}"
  
    loc = DB[:location].insert_select( :slug => slug, 
                        :name => line[:site_name],
                        :address => line[:address],
                        :phone_number => line[:phone])
    
    loc_id = loc[:id]
                        
    $locations_inserted += 1
  else
    $location_duplicates += 1
    #puts "location exists: #{slug}"
  end
  loc_id
end

def insert_program(line, loc_id)

  DB[:program].insert(  :location_id => loc_id, 
                        :name => line[:program_name],
                        :length_of_day => line[:length_of_day])
  $programs_inserted += 1
  #puts "inserting program: #{line[:program_name]}"
    
end

def insert_food_inspection(line, loc_id)

  DB[:food_inspection].insert(  
                        :location_id => loc_id, 
                        :inspection_id => line[:inspection_id],
                        :dba_name => line[:dba_name],
                        :aka_name => line[:aka_name],
                        :license_num => line[:license_num],
                        :risk => line[:risk],
                        :inspection_date => line[:inspection_date],
                        :inspection_type => line[:inspection_type],
                        :results => line[:results],
                        :violations => line[:violations])
  $food_inspections_inserted += 1
  #puts "inserting program: #{line[:program_name]}"
    
end


#start main thread
create_tables

#start with the CPS Early Childhood Portal dataset - its the most complete
puts "inserting locations and programs ..."
CSV.foreach("source/csv/CPS_Early_Childhood_Portal_scrape.csv", 
                                  :headers           => true,
                                  :header_converters => :symbol) do |line|
  loc_id = insert_location(line)
  insert_program(line, loc_id)
  
end

#food inspections
puts "inserting food inspections ..."
CSV.foreach("source/csv/Food Inspections_FacilityType_School.csv", 
                                  :headers           => true,
                                  :header_converters => :symbol) do |line|

  #puts line.inspect
  address = "#{line[:address]}"
  #puts address
  loc_id = location_id_from_address_and_name(line[:aka_name])
  if loc_id > 0
    #puts "found #{loc_id}"
    insert_food_inspection line, loc_id
  end  
  
end

puts "locations inserted: #{$locations_inserted}"
puts "location duplicates: #{$location_duplicates}"
puts "programs inserted: #{$programs_inserted}"
puts "food inspections inserted: #{$food_inspections_inserted}"
puts "done"

nil