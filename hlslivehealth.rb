#!/usr/bin/env ruby

#
#Check HLS live feed health from S3
#

require 'open-uri'
require 'uri'
require 'pathname'
require 'fog'
require 'optparse'

def log(type, str, verbose = true)
  if verbose == true || type != :debug
    time = Time.now
    puts "[#{time.strftime("%Y-%m-%d %H:%M:%S.%L")}] #{type.to_s.upcase} - #{str}"
  end
end

def download_data(url)

  f = open(url)
  downloaded_data = f.read

  downloaded_data
end

def get_renditions_from_manifest(playlist_manifest)
  rendition_manifests_url = Array.new

  playlist_manifest.lines.each do |strline|
    url = strline.scan(/^[^#].*.m3u8/)
    if !url.empty?
      rendition_manifests_url << url
    end
  end

  rendition_manifests_url
end

def get_segment_duration(chunklist_data)
  segment_duration = nil

  chunklist_data.lines.each do |strline|
    data = strline.scan(/#EXT-X-TARGETDURATION:([0-9.]+)/)
    if !data.empty?
      segment_duration = data[0][0].to_f
      break
    end
  end

  segment_duration
end

def create_abs_url(file_name, url_source)
  ret = file_name
  uri = URI.parse(file_name)
  if uri.scheme == nil
    ret = merge_url(url_source,file_name)
  end

  ret.to_s
end

def merge_url(playlist_url, renditions_manifest)
  uri = URI.parse(playlist_url)
  base_url = File.join( uri.scheme + "://" + uri.host, File.dirname(uri.path) )

  File.join(base_url, renditions_manifest)
end

def s3_get_path_from_url(url)
  uri_path = URI(url).path.split("/")
  uri_path[2..uri_path.length].join("/")
end

def s3_get_files_last_updated_time(file_url_list, options)
  file_list_last_updated_times = Array.new

  fog_options = {:provider => 'AWS', :aws_access_key_id => options[:key], :aws_secret_access_key => options[:secret], :region => options[:region]}

  connection = Fog::Storage.new(fog_options)

  bucket = connection.directories.get(options[:bucket])

  file_url_list.each do |file_url|
    file = bucket.files.get(s3_get_path_from_url(file_url))
    last_update = 0
    if !file.nil?
      last_update = Time.now.to_f - file.last_modified.to_f
    end
    file_list_last_updated_times << {:url => file_url, :secs_since_last_update => last_update }
  end

  file_list_last_updated_times
end

def s3_delete_files(files_url, options)

  fog_options = {:provider => 'AWS', :aws_access_key_id => options[:key], :aws_secret_access_key => options[:secret], :region => options[:region]}
  connection = Fog::Storage.new(fog_options)
  bucket = connection.directories.get(options[:bucket])

  files_url.each do |file_url|
    file_path = s3_get_path_from_url(file_url)
    file = bucket.files.get(file_path)
    if !file.nil?
      file.destroy
      log(:info, "Deleted #{options[:bucket]}/#{file_path}.")
    end
  end
end

def get_chunklist_to_delete(chunklist_times, update_treshold_secs)
  chunklist_to_delete = Array.new
  delete_element = nil

  chunklist_times.each do |chunk_list|
    if chunk_list[:secs_since_last_update] > update_treshold_secs
      delete_element = chunk_list
      log(:warning, "Detected error updating #{delete_element[:url]}, updated #{chunk_list[:secs_since_last_update]} secs ago")
      break
    end
  end

  #Delete all chunklists from the same source
  uri_delete = URI(delete_element[:url])
  chunklist_times.each do |chunk_list|
    url_chunklist = URI(chunk_list[:url])
    if uri_delete.scheme == url_chunklist.scheme && uri_delete.host == url_chunklist.host && File.dirname(uri_delete.path) == File.dirname(url_chunklist.path)
      chunklist_to_delete << url_chunklist.to_s
    end
  end

  chunklist_to_delete
end

# START SCRIPT ***********************

#Parse args
options = {:key => nil, :secret => nil, :region => nil, :bucket =>nil, :source_url => nil, :error_threshold => 1.5, :verbose => false}

optparse = OptionParser.new do |opts|
  opts.banner = "HLS live feed check health from S3 (by Jordi Cenzano)\nUsage: ./hlslivehealth -s \"https://s3-us-west-1.amazonaws.com/hls-origin/live/playlist_redundant.m3u8\" -k \"AGAGAGAGGAGAGU\" -s \"hajhjashjh&*kajskajs\" -r \"us-west-1\" -f 1.5"
  opts.on('-h', '--help', 'Displays help') do
    puts opts
    exit
  end
  #Mandatory options
  opts.on('-u', '--source_url URL', 'Source url of HLS manifest') { |v| options[:source_url] = v }

  opts.on('-k', '--key KEY', 'AWS key used to delete files') { |v| options[:key] = v }
  opts.on('-s', '--secret SECRET', 'AWS secret used to delete files') { |v| options[:secret] = v }
  opts.on('-r', '--region REGION', 'AWS S3 region used') { |v| options[:region] = v }
  opts.on('-b', '--bucket BUCKET', 'AWS bucket name') { |v| options[:bucket] = v }
  opts.on('-t', '--threshold MUL', "Update detection threshold in segments time (default = #{options[:error_threshold]})") { |v| options[:error_treshold] = v }

  #Optional
  opts.on('-v', '--verbose', 'Verbose log (Default = false)') { |v| options[:verbose] = true }
end

#Check parameters
begin
  optparse.parse!

  mandatory = [:key, :secret, :region, :source_url]

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
log(:info, "Read parameters: #{options.inspect}", options[:verbose])

#Compute the bucket name
path = URI(options[:source_url]).path.split("/")
options[:bucket] = path[1]

playlist_manifest_data = nil
segment_duration_secs = nil
loop_time_max_secs = 1

exit = false

while exit == false
  begin
    time_start = Time.now.to_f

    if playlist_manifest_data.nil?
      #Download playlist manifest
      playlist_manifest_data = download_data(options[:source_url])
      log(:debug, "Playlist manifest data (from #{options[:source_url]}): #{playlist_manifest_data}", options[:verbose])

      #Analise playlist manifest for rendition manifests, for every source
      chunklists = get_renditions_from_manifest(playlist_manifest_data)
      log(:debug, "rendition list: #{chunklists.join(", ")}", options[:verbose])
    end

    #Get segment duration
    if !chunklists.nil? && segment_duration_secs.nil?
      chunklists.each do |chunklist_file|
        url = create_abs_url(chunklist_file[0], options[:source_url])
        log(:debug, "Reading chunklist to get segment duration, path: #{chunklist_file[0]}, url: #{url}", options[:verbose])
        segment_duration_secs = get_segment_duration(download_data(url))
        if !segment_duration_secs.nil?
          log(:info, "Detected segment duration of: #{segment_duration_secs} secs", options[:verbose])
          break
        end
      end
    end

    #Check update times
    chunklist_abs_url = Array.new
    chunklists.each do |chunklist_file|
      chunklist_abs_url << create_abs_url(chunklist_file[0], options[:source_url])
    end
    chunklist_updated_times = s3_get_files_last_updated_time(chunklist_abs_url, options)
    log(:debug, "Chunklist updated times: #{chunklist_updated_times.inspect}", options[:verbose])

    #Process update times and decide if is needed to delete the chunklists from any of the sources
    chunklist_to_delete = get_chunklist_to_delete(chunklist_updated_times, segment_duration_secs * options[:error_threshold])
    log(:warning, "Chunklist to delete: #{chunklist_to_delete.join(", ")}", options[:verbose])

    #Delete chunklist from outdated source
    if !chunklist_to_delete.empty?
      s3_delete_files(chunklist_to_delete, options)
    end

    loop_time_secs = Time.now.to_f - time_start
    sleep_secs = [loop_time_max_secs - loop_time_secs, 0.01].max
    log(:debug, "Process loop time: #{loop_time_secs}s, next sleep #{sleep_secs}")

    sleep (sleep_secs)
  rescue SystemExit, Interrupt
    exit = true
    log(:info, "Captured SIGINT / SIGTERM), exiting...")
  rescue Exception => e
    log(:error, "#{e.message}, #{e.backtrace}", options[:verbose])
  end
end