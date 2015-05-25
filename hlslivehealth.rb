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
  uri_path = File.dirname(uri.path).to_s

  #Process ./ and ../
  while renditions_manifest[0..1] == "./"
    renditions_manifest = renditions_manifest[2..renditions_manifest.length]
  end

  eliminate_dirs = 0
  while renditions_manifest[0..2] == "../"
    renditions_manifest = renditions_manifest[3..renditions_manifest.length]
    eliminate_dirs = eliminate_dirs + 1
  end
  if eliminate_dirs > 0
    tmp = uri_path.split("/")
    uri_path = tmp[1..tmp.length - eliminate_dirs - 1].join("/")
  end

  base_url = File.join( uri.scheme + "://" + uri.host, uri_path )

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
    file_list_last_updated_times << {:url => file_url, :secs_since_last_update => last_update, :delete => false}
  end

  file_list_last_updated_times
end

def s3_delete_files(files_specs, options)

  fog_options = {:provider => 'AWS', :aws_access_key_id => options[:key], :aws_secret_access_key => options[:secret], :region => options[:region]}
  connection = Fog::Storage.new(fog_options)
  bucket = connection.directories.get(options[:bucket])

  files_specs.each do |file|
    file_path = s3_get_path_from_url(file[:url])
    file = bucket.files.get(file_path)
    if !file.nil?
      begin
        file.destroy
        log(:info, "Deleted #{options[:bucket]}/#{file_path}.")
      rescue Exception => e
        log(:warning, "Problem deleting #{options[:bucket]}/#{file_path}.")
      end
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
  if !delete_element.nil?
    uri_delete = URI(delete_element[:url])
    chunklist_times.each do |chunk_list|
      url_chunklist = URI(chunk_list[:url])
      if uri_delete.scheme == url_chunklist.scheme && uri_delete.host == url_chunklist.host && File.dirname(uri_delete.path) == File.dirname(url_chunklist.path)
        chunk_list[:delete] = true
        chunklist_to_delete << chunk_list
      else
        chunklist_to_delete << chunk_list
      end
    end
  end

  chunklist_to_delete
end

def s3_send_report(dest_path, chunklist, segment_duration_secs, delete_chunklist_treshold_secs, report_update_secs, healthy, options)
  if ((Time.now.to_i - @report_last_sent.to_i) > report_update_secs) || healthy == false
    @report_last_sent = Time.now
    report = {:healthy => healthy, :updated => @report_last_sent.to_i, :segment_duration_secs => segment_duration_secs, :delete_chunklist_treshold_secs => delete_chunklist_treshold_secs, :chunklist_data => chunklist}

    fog_options = {:provider => 'AWS', :aws_access_key_id => options[:key], :aws_secret_access_key => options[:secret], :region => options[:region]}
    connection = Fog::Storage.new(fog_options)
    bucket = connection.directories.get(options[:bucket])

    bucket.files.create(:key => File.join(dest_path, "health_report.json"), :body => report.to_json, :metadata => {}, :public => true )
  end
end

# START SCRIPT ***********************

#Parse args
options = {:key => nil, :secret => nil, :region => nil, :bucket =>nil, :source_url => nil, :error_threshold => 1.5, :verbose => false}

optparse = OptionParser.new do |opts|
  opts.banner = "HLS live feed check health from S3 (by Jordi Cenzano)\nUsage: ./hlslivehealth -u \"https://s3-us-west-1.amazonaws.com/hls-origin/live/playlist.m3u8\" -k \"AGAGAGAGGAGAGU\" -s \"hajhjashjh&*kajskajs\" -r \"us-west-1\" -t 1.5"
  opts.on('-h', '--help', 'Displays help') do
    puts opts
    exit
  end
  #Mandatory options
  opts.on('-u', '--source_url URL', 'Source url of HLS manifest') { |v| options[:source_url] = v }

  opts.on('-k', '--key KEY', 'AWS key used to delete files') { |v| options[:key] = v }
  opts.on('-s', '--secret SECRET', 'AWS secret used to delete files and upload report') { |v| options[:secret] = v }
  opts.on('-r', '--region REGION', 'AWS S3 region used to delete files and upload report') { |v| options[:region] = v }
  opts.on('-b', '--bucket BUCKET', 'AWS bucket name used to delete files and upload report') { |v| options[:bucket] = v }
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

#Local vars
playlist_manifest_data = nil
segment_duration_secs = nil
loop_time_max_secs = 1
delete_chunklist_treshold_secs = 3600
chunklist_abs_url = nil
report_update_secs = 10
report_dest_path = nil
exit = false

while exit == false
  time_start = Time.now.to_f

  begin
    if playlist_manifest_data.nil?
      #Download playlist manifest
      playlist_manifest_data = download_data(options[:source_url])
      log(:debug, "Playlist manifest data (from #{options[:source_url]}): #{playlist_manifest_data}", options[:verbose])

      tmp = File.dirname(URI.parse(options[:source_url]).path).split("/")
      report_dest_path = tmp[2..tmp.length].join("/")

      #Analise playlist manifest for rendition manifests, for every source
      chunklists = get_renditions_from_manifest(playlist_manifest_data)
      log(:debug, "rendition list: #{chunklists.join(", ")}", options[:verbose])

      chunklist_abs_url = Array.new
      chunklists.each do |chunklist_file|
        chunklist_abs_url << create_abs_url(chunklist_file[0], options[:source_url])
      end
    end

    #Get segment duration
    if !chunklists.nil? && segment_duration_secs.nil?
      chunklist_abs_url.each do |chunklist_url|
        log(:debug, "Reading chunklist to get segment duration from url: #{chunklist_url}", options[:verbose])
        segment_duration_secs = get_segment_duration(download_data(chunklist_url))
        if !segment_duration_secs.nil?
          delete_chunklist_treshold_secs = segment_duration_secs * options[:error_threshold]
          log(:info, "Detected segment duration of: #{segment_duration_secs} secs, deletion threshold: #{delete_chunklist_treshold_secs}", options[:verbose])
          break
        end
      end
    end

    #TODO: Create a thread for chunklist -> More efficient

    if !segment_duration_secs.nil?
      #Get update times
      chunklist_updated_times = s3_get_files_last_updated_time(chunklist_abs_url, options)

      #Process update times and decide if is needed to delete the chunklists from any of the sources
      chunklist_processed = get_chunklist_to_delete(chunklist_updated_times, delete_chunklist_treshold_secs)

      chunklist_to_delete = chunklist_processed.select{ |i| i[:delete] == true }
      if !chunklist_to_delete.empty?
        #Delete chunklist from outdated source
        log(:warning, "Chunklist to delete: #{chunklist_to_delete.join(", ")}", options[:verbose])
        s3_delete_files(chunklist_to_delete, options)

        sent = s3_send_report(report_dest_path, chunklist_to_delete, segment_duration_secs, delete_chunklist_treshold_secs, report_update_secs, false, options)
        if !sent.nil?
          log(:info, "Sent report to S3. Healthy: #{false}", options[:verbose])
        end
      else
        log(:debug, "Chunklist updated times: #{chunklist_processed.join(", ")}", options[:verbose])

        sent = s3_send_report(report_dest_path, chunklist_processed, segment_duration_secs, delete_chunklist_treshold_secs, report_update_secs, true, options)
        if !sent.nil?
          log(:info, "Sent report to S3. Healthy: #{true}", options[:verbose])
        end
      end
    end
  rescue SystemExit, Interrupt
    exit = true
    log(:info, "Captured SIGINT / SIGTERM, exiting...")
  rescue Exception => e
    log(:error, "#{e.message}, #{e.backtrace}", options[:verbose])
  end

  loop_time_secs = Time.now.to_f - time_start
  sleep_secs = [loop_time_max_secs - loop_time_secs, 0.01].max
  log(:debug, "Process loop time: #{loop_time_secs}s, next sleep #{sleep_secs}")

  sleep (sleep_secs)

end