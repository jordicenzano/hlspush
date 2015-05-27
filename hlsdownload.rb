#!/usr/bin/env ruby

#
#Donwload HLS and push the files (m3u8 and ts) to a local path or S3
#

require 'open-uri'
require 'fileutils'
require 'uri'
require 'pathname'
require 'fog'
require 'optparse'

def download_file(url, local_path, skip_if_file_exists_in_dest = false)
  downloaded = false

  #Create directory if it is needed
  FileUtils::mkdir_p(local_path) unless File.exists?(local_path)

  #Save all files into local dir
  uri = URI.parse(url)
  local_file_path = File.join(local_path, File.basename(uri.path).to_s)

  if (skip_if_file_exists_in_dest == false) || ((skip_if_file_exists_in_dest == true) && (!File.exist?(local_file_path)))
    #Download file
    download = open(url)

    #Overwite file
    File.delete(local_file_path) if File.exist?(local_file_path)
    IO.copy_stream(download, local_file_path)
    downloaded = true

    log(:debug, "Downloaded #{local_file_path} from #{url}")
  else
    log(:debug, "Skipped download #{local_file_path} from #{url}")
  end

  {:local_path => local_file_path, :url_source => url, :downloaded => downloaded}
end

def log(type, str)

  if (@verbose_level.to_i == 2) || (@verbose_level.to_i == 1 && (type == :info || type == :error))
    time = Time.now
    puts "[#{time.strftime("%Y-%m-%d %H:%M:%S.%L")}] #{type.to_s.upcase} - #{str}"
  end

end

def get_renditions_manifests_urls(parent_manifest_file)
  rendition_manifests_url = Array.new

  File.open(parent_manifest_file, 'r') do |f_manifest|
    while strline = f_manifest.gets
      url = strline.scan(/^[^#].*.m3u8/)
      if !url.empty?
        rendition_manifests_url << url
      end
    end
  end

  rendition_manifests_url
end

def get_absolute_urls(url_list, base)
  abs_urls = Array.new

  url_list.each do |url|
    abs_urls << URI.join(base, url.first.to_s).to_s
  end

  abs_urls
end

def get_path_from_url(url, prepend_path = nil)
  if prepend_path.nil?
    File.dirname(URI.parse(url).path)
  else
    File.join(prepend_path, File.dirname(URI.parse(url).path))
  end

end

def get_segments_from_manifests(rendition_manifest_file)
  rendition_segments_url = Array.new

  File.open(rendition_manifest_file, 'r') do |f_manifest|
    while strline = f_manifest.gets
      url = strline.scan(/^[^#].*.ts|^[^#].*.aac/)
      if !url.empty?
        rendition_segments_url << url
      end
    end
  end

  rendition_segments_url
end

def upload_file(file_specs, options, skip_if_file_exists_in_dest)
  if options[:dest_type] == "local"
    copy_file(file_specs, options, skip_if_file_exists_in_dest)
  end
  if options[:dest_type] == "s3"
    s3_upload(file_specs, options, skip_if_file_exists_in_dest)
  end
end

def copy_file(file_specs, options, skip_if_file_exists_in_dest)
  #Create directory if it is needed
  source = file_specs[:local_path]
  dest_path = get_path_from_url(file_specs[:url_source], options[:prepend_id])
  dest = File.join(options[:dest_options][:path], dest_path, File.basename(file_specs[:local_path]))

  if (skip_if_file_exists_in_dest == false) || ((skip_if_file_exists_in_dest == true) && (!File.exist?(dest)))
    FileUtils::mkdir_p(File.dirname(dest)) unless File.exists?(File.dirname(dest))
    FileUtils.cp(source, dest)
    log(:info, "Copied file #{source} to #{dest}.")
    "COPIED"
  else
    log(:debug, "Copy file SKIPPED from #{source} to #{dest}.")
    "SKIPPED"
  end

end

def fog_connection(options)
  fog_options = {:provider => 'AWS', :aws_access_key_id => options[:dest_options][:key], :aws_secret_access_key => options[:dest_options][:secret], :region => options[:dest_options][:region]}

  @app_fog_options[:fog_mutex].synchronize {
    # access shared resource
     if @app_fog_options[:fog_connection].nil? || @app_fog_options[:fog_bucket].nil?
       log(:info, "Establishing S3 connection")
       @app_fog_options[:fog_connection] = Fog::Storage.new(fog_options)
       @app_fog_options[:fog_bucket] = @app_fog_options[:fog_connection].directories.get(options[:dest_options][:bucket])
       log(:info, "Established S3 connection")
     end
  }

  @app_fog_options[:fog_bucket]
end

def fog_reset
    @app_fog_options[:fog_mutex].synchronize {
    @app_fog_options[:fog_connection] = nil
    @app_fog_options[:fog_bucket] = nil
  }
end

def s3_upload(file_specs, options, skip_if_file_exists_in_dest)
  #fog_options = {:provider => 'AWS', :aws_access_key_id => options[:dest_options][:key], :aws_secret_access_key => options[:dest_options][:secret], :region => options[:dest_options][:region]}
  #connection = Fog::Storage.new(fog_options)
  #bucket = connection.directories.get(options[:dest_options][:bucket])

  bucket = fog_connection(options)

  source = file_specs[:local_path]
  dest_path = File.join(get_path_from_url(file_specs[:url_source], options[:prepend_id]), File.basename(file_specs[:local_path]))
  if dest_path[0] == "/"
    dest_path = dest_path[1..(dest_path.length-1)]
  end
  cache_control_max_age = file_specs[:cache_max_age]

  do_upload = true
  if skip_if_file_exists_in_dest == true
    if bucket.files.head(dest_path) != nil
      #File exists in destination
      do_upload = false
      log(:debug,"Skipped upload of file #{source} to bucket #{options[:dest_options][:bucket]}/#{dest_path}.")
    end
  end

  if do_upload == true
    file = bucket.files.create(:key => dest_path, :body => File.open(source), :metadata => {"Cache-Control" => "max-age=#{cache_control_max_age}"}, :public => true )
    log(:info, "Uploaded file #{source} to bucket #{options[:dest_options][:bucket]}/#{dest_path}.")

    file
  end

end

def process_rendition(rendition_manifest_url, options, first_upload_after_activation, upload_on = true)
  #Download chunklist
  rendition_manifest = download_file(rendition_manifest_url, get_path_from_url(rendition_manifest_url, options[:local_tmp_path]))
  rendition_manifest[:cache_max_age] = options[:cache_max_age_chunklists]

  #Analise rendition manifest to find segments
  renditions_segments = get_absolute_urls(get_segments_from_manifests(rendition_manifest[:local_path]), rendition_manifest_url)

  #For every segment...
  upload_chunklist = false
  renditions_segments.each do |segment|
    #download segments
    local_segment = download_file(segment, get_path_from_url(segment, options[:local_tmp_path]), true)
    local_segment[:cache_max_age] = options[:cache_max_age_segments]

    if upload_on == true
      #If this client was activated, ensure to upload all segments of the rendition manifest
      if local_segment[:downloaded] == true || first_upload_after_activation == true
       #Upload new rendition
       upload_file(local_segment, options, true)
       upload_chunklist = true
      end
    end
  end

  if upload_on == true && (first_upload_after_activation == true || upload_chunklist == true)
    #Upload chunklist
    upload_file(rendition_manifest, options, false)
  end

end

def append_to_filename(filename, append_text)
  File.join(File.dirname(filename), File.basename(filename, ".*").to_s + append_text + File.extname(filename))
end

def create_master_manifest_redundant_streams_s3(playlist_manifest, options)
  manifest_dest_file = append_to_filename(playlist_manifest[:local_path], "_redundant_s3")

  local_prepend_id = options[:prepend_id]
  backup_prepend_id = options[:prepend_backup_id]

  File.open(manifest_dest_file, 'w') do |f_write|
    File.open(playlist_manifest[:local_path], 'r') do |f_read|
      while strline = f_read.gets
        datainf = strline.scan(/^#EXT-X-STREAM-INF:/)
        if !datainf.empty?
          while strlinem3u8 = f_read.gets
            datam3u8 = strlinem3u8.scan(/.*.m3u8$/)
            if !datam3u8.empty?
              f_write.puts strline

              #Main
              strlinem3u8_local = options[:dest_options][:schema] + "://s3-" + options[:dest_options][:region] + ".amazonaws.com/" + options[:dest_options][:bucket] + "/" + File.join(get_path_from_url(playlist_manifest[:url_source], options[:prepend_id]), strlinem3u8)
              f_write.puts strlinem3u8_local

              #Failover
              f_write.puts strline

              strlinem3u8_bck = options[:dest_options][:schema] + "://s3-" + options[:dest_options][:region] + ".amazonaws.com/" + options[:dest_options][:bucket_backup] + "/" + File.join(get_path_from_url(playlist_manifest[:url_source], options[:prepend_backup_id]), strlinem3u8)
              f_write.puts strlinem3u8_bck
              break
            end
          end
        else
          f_write.puts strline
        end
      end
    end
  end
  {:local_path => manifest_dest_file, :url_source => playlist_manifest[:url_source], :cache_max_age => options[:cache_max_age_playlist], :downloaded => false}
end

def create_master_manifest_redundant_streams_cf(playlist_manifest, options)
  manifest_dest_file = append_to_filename(playlist_manifest[:local_path], "_redundant_cf")

  local_prepend_id = options[:prepend_id]
  backup_prepend_id = options[:prepend_backup_id]

  File.open(manifest_dest_file, 'w') do |f_write|
    File.open(playlist_manifest[:local_path], 'r') do |f_read|
      while strline = f_read.gets
        datainf = strline.scan(/^#EXT-X-STREAM-INF:/)
        if !datainf.empty?
          while strlinem3u8 = f_read.gets
            datam3u8 = strlinem3u8.scan(/.*.m3u8$/)
            if !datam3u8.empty?
              f_write.puts strline

              #Main
              strlinem3u8_local = options[:dest_options][:schema] + "://" + options[:dest_options][:cf_dist] + "/" + File.join(get_path_from_url(playlist_manifest[:url_source], options[:prepend_id]), strlinem3u8)
              f_write.puts strlinem3u8_local

              #Failover
              f_write.puts strline

              strlinem3u8_bck = options[:dest_options][:schema] + "://" + options[:dest_options][:cf_dist] + "/" + File.join(get_path_from_url(playlist_manifest[:url_source], options[:prepend_backup_id]), strlinem3u8)
              f_write.puts strlinem3u8_bck
              break
            end
          end
        else
          f_write.puts strline
        end
      end
    end
  end
  {:local_path => manifest_dest_file, :url_source => playlist_manifest[:url_source], :cache_max_age => options[:cache_max_age_playlist], :downloaded => false}
end

# START SCRIPT ***********************

#Parse args
@verbose_level = 1

aws_options = {:key => nil, :secret => nil, :region => nil, :bucket =>nil, :bucket_backup => nil, :schema => 'http'}
local_options = {:path => nil}

options = {:source_url => nil, :local_tmp_path => nil, :dest_type => nil, :dest_options => nil, :prepend_id => nil, :prepend_backup_id => nil, :skip_upload_file => nil, :overwrite => false, :cache_max_age_playlist => 3600, :cache_max_age_chunklists => 1, :cache_max_age_segments => 3600}

optparse = OptionParser.new do |opts|
  opts.banner = "HLS download push (by Jordi Cenzano)\nUsage for push: ./hlsdownload -s \"http://localhost/vod/hello.m3u8\" -l ~/test -k \"AGAGAGAGGAGAGU\" -s \"hajhjashjh&*kajskajs\" -r \"us-west-1\" -b \"hls-origin\" -b \"hls-origin-bck\" -m 1 -t 3600 -j ~/skip_upload -p A -q B\nUsage for download: ./hlsdownload -s \"http://localhost/vod/hello.m3u8\" -l ~/test "
  opts.on('-h', '--help', 'Displays help') do
    puts opts
    exit
  end
  #Mandatory options
  opts.on('-u', '--source_url URL', 'Source url of HLS manifest') { |v| options[:source_url] = v }
  opts.on('-l', '--local_tmp_path PATH', 'temporal local path to store the downloaded files') { |v| options[:local_tmp_path] = v }

  #Mandatory if we want to use the push mode
  opts.on('-d', '--dest_type (S3/local)', 'Sets the destination of processed HLS, can be "S3" or "local"') do |v|
    if "s3".casecmp(v) != 0
      options[:dest_type] = 'local'
      options[:dest_options] = local_options
    else
      options[:dest_type] = 's3'
      options[:dest_options] = aws_options
    end
  end
  #Mandatory if dest = local
  opts.on('-z', '--path path', 'Local destination path for processed HLS. Used only if dest_type = "local"') { |v| options[:dest_options][:path] = v }

  #Mandatory if dest = S3
  opts.on('-k', '--key KEY', 'AWS key') { |v| options[:dest_options][:key] = v }
  opts.on('-s', '--secret SECRET', 'AWS secret') { |v| options[:dest_options][:secret] = v }
  opts.on('-r', '--region REGION', 'AWS S3 region to upload the files') { |v| options[:dest_options][:region] = v }
  opts.on('-b', '--bucket primary BUCKET', 'AWS destination primary bucket name') { |v| options[:dest_options][:bucket] = v }

  #General optional
  opts.on('-p', '--prepend_id pID', 'Upload directory prepend ID') { |v| options[:prepend_id] = v }
  opts.on('-j', '--skip_upload_file FILE', 'If this file exists the upload/copy is stopped') { |v| options[:skip_upload_file] = v }
  opts.on('-o', '--overwrite', 'Upload or copy even the segment file is already in the destination (Default = false') { |v| options[:overwrite] = true }

  opts.on('-m', '--cache_max_age_chunklists SECS', 'Cache control data for chunklist files (Default = 1)') { |v| options[:cache_max_age_chunklists] = v }
  opts.on('-t', '--cache_max_age_segments SECS', 'Cache control data for segments files (Default = 3600)') { |v| options[:cache_max_age_segments] = v }
  opts.on('-y', '--cache_max_age_playlist SECS', 'Cache control data for master playlist files (Default = 3600)') { |v| options[:cache_max_age_playlist] = v }

  #Options to check the "backup" upload (from its own point of view the "other" is always backup)
  opts.on('-q', '--prepend_backup_id pID', 'Prepend id of the backup upload') { |v| options[:prepend_backup_id] = v }
  opts.on('-x', '--bucket backup BUCKET', 'AWS destination backup bucket name') { |v| options[:dest_options][:bucket_backup] = v }
  opts.on('-c', '--schema SCHEMA', 'Schema to use to create the redundant manifest: http or https (Default = http)') { |v| options[:dest_options][:schema] = v }
  opts.on('-f', '--cfdist CFDISTNAME', 'Cloudfront distribuiton domain name used to create redundant streams manifest') { |v| options[:dest_options][:cf_dist] = v }

  opts.on('-v', '--verbose NUM', 'Verbose options (1 = errors & main info (default), 2-debug') { |v| @verbose_level = v }
end

#Check parameters
begin
  optparse.parse!

  mandatory = [:source_url, :local_tmp_path]
  missing = mandatory.select{ |param| options[param].nil? }
  unless missing.empty?
    puts "Missing options: #{missing.join(', ')}"
    puts optparse
    exit
  end

  dest_mandatory = nil
  if options[:dest_type] == "s3"
    dest_mandatory = [:key, :secret, :region, :bucket]
  end
  if options[:dest_type] == "local"
    dest_mandatory = [:path]
  end

  if !dest_mandatory.nil?
    missing = dest_mandatory.select{ |param| options[:dest_options][param].nil? }
    unless missing.empty?
      puts "Missing destination #{options[:dest_type]} options: #{missing.join(', ')}"
      puts optparse
      exit
    end
  end
rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $!.to_s
  puts optparse
end

#Show readed options
log(:info, "Read parameters: #{options.inspect}")

#prepend_id
#Prepend to the "prepend_id" to the upload path
#Using this parameter we can upload different jobs with the same uid to the same bucket
#It helps to create a organized environment (easier to read for humans than using only uid)

#overwrite
#If the segment file is already in S3 don't upload

#Control vars
exit = false
populated_playlist_manifest = false
uploaded_playlist_manifest = false
first_upload_after_activation = true
disable_upload = false
upload_enabled = true

loop_time_max_secs = 0.5

@app_fog_options = {:fog_mutex => Mutex.new, :fog_connection => nil, :fog_bucket => nil}

while exit == false
  time_start = Time.now.to_f

  begin
    #Check if s3 upload is disabled at every iteration
    if !options[:dest_options].nil?
      if !options[:skip_upload_file].nil?
        disable_upload = File.exist?(options[:skip_upload_file].to_s)
      end

      if upload_enabled == false
        if disable_upload == false
          log(:info, "Upload ON!!!!")
          first_upload_after_activation = true
          upload_enabled = true
        end
      else
        if disable_upload == true
          log(:info, "Upload OFF!!!!")
          upload_enabled = false
        end
      end
    else
      upload_enabled = false
    end

    #Download playlist manifest
    playlist_manifest = download_file(options[:source_url], options[:local_tmp_path])
    playlist_manifest[:cache_max_age] = options[:cache_max_age_playlist]

    #Analise playlist manifest for rendition manifests (chunklist)
    renditions_manifests_urls = get_absolute_urls(get_renditions_manifests_urls(playlist_manifest[:local_path]), options[:source_url])

    #Create a thread for each rendition
    rendition_threads = Array.new
    renditions_manifests_urls.each do |rendition_manifest_url|
      #Download files, upload them, and clean
      rendition_threads << Thread.new{process_rendition(rendition_manifest_url, options, first_upload_after_activation, upload_enabled )}
      populated_playlist_manifest = true
    end

    #Wait all segments and chunklists to uplaod
    rendition_threads.each do |thread|
      thread.join
    end

    #Upload master playlist and generate redundant
    if populated_playlist_manifest == true && uploaded_playlist_manifest == false && upload_enabled == true
      #Upload simple playlist manifest
      upload_file(playlist_manifest, options, false)

      if !options[:prepend_id].nil? && !options[:prepend_backup_id].nil? && !options[:dest_options][:bucket].nil? && !options[:dest_options][:bucket_backup].nil?
        #Create the main playlist with redundant streams for s3 & upload it
        playlist_manifest_redundant_s3 = create_master_manifest_redundant_streams_s3(playlist_manifest, options)
        playlist_manifest_redundant_cf = create_master_manifest_redundant_streams_cf(playlist_manifest, options)

        #Upload the main playlist with redundant streams
        upload_file(playlist_manifest_redundant_s3, options, false)
        upload_file(playlist_manifest_redundant_cf, options, false)

        if !options[:dest_options][:cf_dist].nil?
          #Create the main playlist with redundant streams for cf & upload it
          playlist_manifest_redundant_cf = create_master_manifest_redundant_streams_cf(playlist_manifest, options)

          #Upload the main playlist with redundant streams
          upload_file(playlist_manifest_redundant_cf, options, false)
        end
      end
      uploaded_playlist_manifest = true
    end

    first_upload_after_activation = false
  rescue SystemExit, Interrupt
    exit = true
    log(:info, "Captured SIGINT / SIGTERM, exiting...")
  rescue Exception => e
    #Reset connection parameters, this will force a reconnection
    fog_reset

    log(:error, "Error: #{e.message}, #{e.backtrace}")
  end

  loop_time_secs = Time.now.to_f - time_start
  sleep_secs = [loop_time_max_secs - loop_time_secs, 0.01].max
  log(:info, "Process loop time: #{loop_time_secs}s, next sleep #{sleep_secs}")

  sleep (sleep_secs)
end