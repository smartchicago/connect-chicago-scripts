#!/usr/bin/env ruby

require 'csv'

def to_slug(s)
  #strip the string
  ret = s.strip.downcase

  #blow away apostrophes
  ret.gsub! /['`.]/,""

  # @ --> at, and & --> and
  ret.gsub! /\s*@\s*/, " at "
  ret.gsub! /\s*&\s*/, " and "

  #replace all non alphanumeric, underscore or periods with underscore
   ret.gsub! /\s*[^A-Za-z0-9\.\-]\s*/, '-'  

   #convert double underscores to single
   ret.gsub! /_+/,"_"

   #strip off leading/trailing underscore
   ret.gsub! /\A[_\.]+|[_\.]+\z/,""

   ret
end

#CSV_FILE_PATH = File.join(File.dirname(__FILE__), "tech_locator_slugs.csv")

#File.exist?(CSV_FILE_PATH) ? File.delete(CSV_FILE_PATH) : File.delete(@CSV_FILE_PATH)

puts "creating slugs ..."
CSV.foreach("data/Tech Locator Master List Spreadsheet.csv", 
                                  :headers           => true,
                                  :header_converters => :symbol) do |line|         
                                  
 # CSV.open(CSV_FILE_PATH, "a") do |csv|
    #puts line.inspect
    slug = to_slug(line[:organizationname] + " " + line[:address])
    puts slug
#    line[:slug] = slug
#    csv << line
  #end

end

puts "done"

nil
