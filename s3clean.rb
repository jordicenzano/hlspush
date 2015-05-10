#!/usr/bin/env ruby

#
#Clean s3 directory
#

require 'optparse'

#Parse args
options = {:aws_cred_file => nil, :bucket =>nil, :path => nil}

optparse = OptionParser.new do |opts|
  opts.banner = "Deletes a S3 directoy (by Jordi Cenzano)\nUsage: ./s3clean -f \".s3cfg\" -b \"hls-origin\" -p \"path/to/del\""
  opts.on('-h', '--help', 'Displays help') do
    puts opts
    exit
  end
  #Mandatory options
  opts.on('-f', '--cred_file FILE', 'AWS credentials file') { |v| options[:aws_cred_file] = v }
  opts.on('-b', '--bucket NAME', 'Bucket name') { |v| options[:bucket] = v }
  opts.on('-p', '--path NAME', 'Path to del') { |v| options[:path] = v }
end

#Check parameters
begin
  optparse.parse!

  mandatory = [:aws_cred_file, :bucket, :path]

  missing = mandatory.select{ |param| options[param].nil? }
  unless missing.empty?
    puts "Missing options: #{missing.join(', ')}"
    puts optparse
    exit
  end

rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $!.to_s
  puts optparse
end

#Show readed options
#puts "Read parameters: #{options.inspect}"

#Execute del dir
cmd = "s3cmd --config=#{options[:aws_cred_file]} del -r s3://#{options[:bucket]}/#{options[:path]}"
puts "Executing: #{cmd}"
`#{cmd}`