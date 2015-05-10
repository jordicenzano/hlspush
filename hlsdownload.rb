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

def log(type, str)
  time = Time.now
  puts "[#{time.strftime("%Y-%m-%d %H:%M:%S.%L")}] #{type.to_s.upcase} - #{str}"
end

def transfer(dest_type, source, dest_path, dest_options, cache_control_max_age, overwrite)

  if dest_type.casecmp('s3') != 0
    dest = File.join(dest_options[:bucket].to_s + dest_path)

    #Create directory if it is needed
    FileUtils::mkdir_p(File.dirname(dest)) unless File.exists?(File.dirname(dest))

    FileUtils.cp(source, dest)
    log(:info, "Copied file #{source} to #{dest}.")

    "COPIED"
  else
    fog_options = {:provider => 'AWS', :aws_access_key_id => dest_options[:key], :aws_secret_access_key => dest_options[:secret], :region => dest_options[:region]}
    connection = Fog::Storage.new(fog_options)

    bucket = connection.directories.get(dest_options[:bucket])

    do_upload = true
    if overwrite == false
      if bucket.files.head(dest_path) != nil
        #File exists in destination
        do_upload = false
        log(:info,"Skipping upload of  file #{source} to bucket #{dest_options[:bucket]}/#{dest_path}.")
      end
    end

    if do_upload == true
      file = bucket.files.create(:key => dest_path, :body => File.open(source), :metadata => {"Cache-Control" => "max-age=#{cache_control_max_age}"},:public => true )
      log(:info, "Uploaded file #{source} to bucket #{dest_options[:bucket]}/#{dest_path}.")

      file
    end
  end
end

def remote_delete(dest_type, files_path, dest_options)

  if !files_path.empty?
    if dest_type.casecmp('s3') != 0
      files_path.each do |file_path|
        if File.exists?(file_path)
          File.delete(file_path)
          log(:info, "Deleted #{file_path}.")
        end
      end
    else
      fog_options = {:provider => 'AWS', :aws_access_key_id => dest_options[:key], :aws_secret_access_key => dest_options[:secret], :region => dest_options[:region]}
      connection = Fog::Storage.new(fog_options)
      bucket = connection.directories.get(dest_options[:bucket])

      files_path.each do |file_path|
        file = bucket.files.get(file_path)
        if !file.nil?
          file.destroy
          log(:info, "Deleted #{dest_options[:bucket]}/#{file_path}.")
        end
      end
    end
  end

end

def get_chunklist_last_updated_sec(dest_type, files_path, dest_options)
  sec = 0
  last_update = nil

  if !files_path.empty?
    if dest_type.casecmp('s3') != 0
      files_path.each do |file_path|
        if File.exists?(file_path)
          last_update = [File.mtime(file_path).to_f, last_update.to_f].max
        end
      end
    else
      fog_options = {:provider => 'AWS', :aws_access_key_id => dest_options[:key], :aws_secret_access_key => dest_options[:secret], :region => dest_options[:region]}
      connection = Fog::Storage.new(fog_options)
      bucket = connection.directories.get(dest_options[:bucket])

      files_path.each do |file_path|
        file = bucket.files.get(file_path)
        if !file.nil?
          last_update = [file.last_modified.to_f, last_update.to_f].max
        end
      end
    end
  end

  if !last_update.nil?
    sec =  Time.now.to_f - last_update
  end

  sec
end

def get_segments_from_manifests(rendition_manifest_file)
  rendition_segments_url = Array.new

  File.open(rendition_manifest_file, 'r') do |f_manifest|
    while strline = f_manifest.gets
      url = strline.scan(/^[^#].*.ts/)
      if !url.empty?
        rendition_segments_url << url
      end
    end
  end

  rendition_segments_url
end

def download(url, local_base_path, skip_download_if_file_exists = false)
  downloaded = false

  #Create directory if it is needed
  FileUtils::mkdir_p(local_base_path) unless File.exists?(local_base_path)

  uri = URI.parse(url)
  local_file_path = File.join(local_base_path, File.basename(uri.path))

  if (skip_download_if_file_exists == false) || ((skip_download_if_file_exists == true) && (!File.exist?(local_file_path)))
    #Download file
    download = open(url)

    #Overwite file
    File.delete(local_file_path) if File.exist?(local_file_path)
    IO.copy_stream(download, local_file_path)
    downloaded = true

    #puts "Downloaded #{local_file_path} from #{url}"
  else
    #puts "Skipped download #{local_file_path} from #{url}"
  end

  {:local_path => local_file_path, :downloaded => downloaded}
end

def get_renditions_manifests(parent_manifest_file)
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

def merge_url(playlist_url, renditions_manifest)
  uri = URI.parse(playlist_url)
  base_url = File.join( uri.scheme + "://" + uri.host, File.dirname(uri.path) )

  File.join(base_url, renditions_manifest)
end

def get_relative_path(project_path, absolute_path)
  absolute_path_ext = Pathname.new(File.expand_path(absolute_path))
  project_path_ext  = Pathname.new(File.expand_path(project_path))

  absolute_path_ext.relative_path_from(project_path_ext)
end

def change_media_id(src, new_media_id)
  new_name = ""

  begin
    data = src.to_s.scan(/(.*-).*(_\d*.ts)/)
    if !data.empty?
      tmp = data[0][0] + new_media_id.to_s + data[0][1]
      if File.dirname(tmp) != "."
        new_name = File.join(File.dirname(tmp), File.basename(tmp))
      else
        new_name = File.basename(tmp)
      end
    else
      raise
    end
  rescue Exception => e
    raise "Media segment name malformed (not wowza live http origin convention media-ucxikcdtm_2.ts). Current: #{src}"
  end

  new_name
end

def get_segment_duration(manifest_file)
  segment_duration = nil

  File.open(manifest_file, 'r') do |f_read|
    while strline = f_read.gets
      data = strline.scan(/#EXT-X-TARGETDURATION:([0-9.]+)/)
      if !data.empty?
        segment_duration = data[0][0].to_f
        log(:info, "Detected segment duration of: #{segment_duration} secs")
        break
      end
    end
  end

  segment_duration
end

def change_manifest_media_ids(manifest_file, new_media_id)

  manifest_file_modi = manifest_file + ".modi"

  File.open(manifest_file_modi, 'w') do |f_write|
    File.open(manifest_file, 'r') do |f_read|
      while strline = f_read.gets
        data = strline.scan(/([^#].*-).*(_\d*.ts)/)
        if !data.empty?
          tmp = data[0][0] + new_media_id.to_s + data[0][1]
          if File.dirname(tmp) != "."
            strline = File.join(File.dirname(tmp), File.basename(tmp))
          else
            strline = File.basename(tmp)
          end
        end
        f_write.puts strline
      end
    end
  end

  File.delete(manifest_file)
  File.rename(manifest_file_modi, manifest_file)
end

def create_master_manifest_redundant_streams(manifest_source_file, manifest_dest_file, local_prepend_id, backup_prepend_id)

  File.open(manifest_dest_file, 'w') do |f_write|
    File.open(manifest_source_file, 'r') do |f_read|
      while strline = f_read.gets
        datainf = strline.scan(/^#EXT-X-STREAM-INF:/)
        if !datainf.empty?
          while strlinem3u8 = f_read.gets
            datam3u8 = strlinem3u8.scan(/.*.m3u8$/)
            if !datam3u8.empty?
              f_write.puts strline

              strlinem3u8_local = strlinem3u8
              f_write.puts strlinem3u8_local

              #Bck
              f_write.puts strline

              strlinem3u8_bck = "../../" + backup_prepend_id.to_s + strlinem3u8
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

end

# START SCRIPT ***********************

#Parse args
aws_options = {:key => nil, :secret => nil, :region => nil, :bucket =>nil}
local_options = {:path => nil}

options = {:source_url => nil, :local_tmp_path => nil, :dest_type => 's3', :dest_options => aws_options, :uid =>nil, :prepend_id => nil, :prepend_backup_id => nil, :skip_upload_file => nil, :overwrite => false, :cache_max_age_manifest => 1, :cache_max_age_segments => 3600}

optparse = OptionParser.new do |opts|
  opts.banner = "HLS download push (by Jordi Cenzano)\nUsage: ./hlsdownload -s \"http://localhost/vod/hello.m3u8\" -l ~/test -k \"AGAGAGAGGAGAGU\" -s \"hajhjashjh&*kajskajs\" -r \"us-west-1\" -b \"hls-origin\" -m 1 -t 3600 -j ~/skip_upload -i abcdf -p a"
  opts.on('-h', '--help', 'Displays help') do
    puts opts
    exit
  end
  #Mandatory options
  opts.on('-u', '--source_url URL', 'Source url of HLS manifest') { |v| options[:source_url] = v }
  opts.on('-l', '--local_tmp_path PATH', 'temporal local path to store the downloaded files') { |v| options[:local_tmp_path] = v }
  opts.on('-d', '--dest_type (S3/local)', 'Sets the destination of processed HLS, can be "S3" or "local" (default = s3)') do |v|
    if "s3".casecmp(v) != 0
      options[:dest_type] = 'local'
      options[:dest_options] = local_options
    end
  end

  #Mandatory if dest = local
  opts.on('-f', '--path path', 'Local destination path for processed HLS. Used only if dest_type = "local"') { |v| options[:dest_options][:path] = v }

  #Mandatory if dest = S3
  opts.on('-k', '--key KEY', 'AWS key') { |v| options[:dest_options][:key] = v }
  opts.on('-s', '--secret SECRET', 'AWS secret') { |v| options[:dest_options][:secret] = v }
  opts.on('-r', '--region REGION', 'AWS S3 region to upload the files') { |v| options[:dest_options][:region] = v }
  opts.on('-b', '--bucket BUCKET', 'AWS destination bucket name') { |v| options[:dest_options][:bucket] = v }

  #General optional
  opts.on('-i', '--uid id ID', 'Unique ID to for segment files') { |v| options[:uid] = v }
  opts.on('-p', '--prepend_id pID', 'Upload directory prepend ID') { |v| options[:prepend_id] = v }
  opts.on('-j', '--skip_upload_file FILE', 'If this file exists the upload/copy is stopped') { |v| options[:skip_upload_file] = v }
  opts.on('-o', '--overwrite', 'Upload or copy even the segment file is already in the destination (Default = false') { |v| options[:overwrite] = true }

  opts.on('-m', '--cache_max_age_manifest SECS', 'Cache control data for manifest files (Default = 1)') { |v| options[:cache_max_age_manifest] = v }
  opts.on('-t', '--cache_max_age_segments SECS', 'Cache control data for segments files (Default = 3600)') { |v| options[:cache_max_age_segments] = v }

  #Options to check the "backup" upload (from its own point of view the "other" is always backup)
  opts.on('-q', '--prepend_backup_id pID', 'Prepend id of the backup upload') { |v| options[:prepend_backup_id] = v }
end

#Check parameters
begin
  optparse.parse!

  mandatory = [:source_url, :local_tmp_path, :dest_type]
  missing = mandatory.select{ |param| options[param].nil? }
  unless missing.empty?
    puts "Missing options: #{missing.join(', ')}"
    puts optparse
    exit
  end

  dest_mandatory = nil
  if options[:dest_type] == "s3"
    dest_mandatory = [:key, :secret, :region, :bucket]
  else
    dest_mandatory = [:path]
  end

  missing = dest_mandatory.select{ |param| options[:dest_options][param].nil? }
  unless missing.empty?
    puts "Missing destination #{options[:dest_type]} options: #{missing.join(', ')}"
    puts optparse
    exit
  end

rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $!.to_s
  puts optparse
end

#Show readed options
log("info", "Read parameters: #{options.inspect}")

#Compute the full local path
options[:local_tmp_path_stream] = File.join(options[:local_tmp_path], File.dirname(URI.parse(options[:source_url]).path))

#uid
#Rename segments in order to sync the data from different wowza https origins (Wowza origin segments example: media-ucxikcdtm_2.ts media-ucxikcdtm_212.ts)
#The idea is to overwrite the ID -> media-NEW_ID_2.ts
#Doing that we ensure the uniqueness of every job in different machines, and we simplify the implementation of redundant jobs

#prepend_id
#Prepend to the "prepend_id" to the upload path
#Using this parameter we can upload different jobs with the same uid to the same bucket
#It helps to create a organized environment (easier to read for humans than using only uid)

#overwrite
#If the segment file is already in S3 don't upload

#Control vars
exit = false
uploaded_playlist_manifest = false
disable_upload = false
upload_state = :enabled
first_upload_after_activation = true
master_playlist_remote_file_name = nil
segment_duration_secs = nil

loop_time_max_secs = 2.5

last_path_uploaded_chunklist = Array.new
last_path_uploaded_chunklist_bck = Array.new

#Set the correct local destination path in case that destination = local
dst_local_path = ""
if options[:dest_type] != "s3"
  dst_local_path = options[:dest_options][:path]
  if dst_local_path[dst_local_path.length - 1 ] != "/"
    dst_local_path = dst_local_path + "/"
  end
end

while exit == false
  begin
    time_start = Time.now.to_f

    #Check if s3 upload is disabled at every iteration
    if !options[:skip_upload_file].nil?
      disable_upload = File.exist?(options[:skip_upload_file].to_s)
    end

    if upload_state == :disabled
      if disable_upload == false
        log(:info, "Upload ON!!!!")
        first_upload_after_activation = true
        upload_state = :enabled
      end
    else
      if disable_upload == true
        log(:info, "Upload OFF!!!!")
        #Delete chunk list manifest
        remote_delete(options[:dest_type], last_path_uploaded_chunklist, options[:dest_options])
        upload_state = :disabled
      end
    end

    #Clear last chunklist paths
    last_path_uploaded_chunklist = Array.new
    last_path_uploaded_chunklist_bck = Array.new

    #Download playlist manifest
    playlist_manifest_file = download(options[:source_url], options[:local_tmp_path_stream])

    #Analise playlist manifest for rendition manifests
    renditions_manifests = get_renditions_manifests(playlist_manifest_file[:local_path])

    #For every rendition manifest
    renditions_manifests.each do |rendition_manifest|
      rendition_segment_uploaded = false
      renditions_manifest_url = merge_url(options[:source_url], rendition_manifest.first)

      #download rendition manifest
      rendition_manifest_path = File.join(options[:local_tmp_path_stream], File.dirname(rendition_manifest.first))
      rendition_manifest_file = download(renditions_manifest_url, rendition_manifest_path)

      #Analise rendition manifest to find segments
      renditions_segments = get_segments_from_manifests(rendition_manifest_file[:local_path])

      #For every segment...
      renditions_segments.each do |segment|
        segment_url = merge_url(renditions_manifest_url, segment.first)

        #download rendition manifest if not exists
        rendition_path = File.join(File.dirname(rendition_manifest_path), File.dirname(rendition_manifest.first))
        rendition_segment = download(segment_url, rendition_path, true)

        #If this client was activated, ensure to upload all segments of the rendition manifest
        if (rendition_segment[:downloaded] == true || first_upload_after_activation == true) && upload_state == :enabled
          #Upload new rendition to S3
          dst_file_name =  dst_local_path + options[:prepend_id].to_s + get_relative_path(options[:local_tmp_path], rendition_segment[:local_path]).to_s
          if !options[:uid].nil?
            dst_file_name = dst_local_path + options[:prepend_id].to_s + change_media_id(get_relative_path(options[:local_tmp_path], rendition_segment[:local_path]), options[:uid])
          end
          if (transfer(options[:dest_type], rendition_segment[:local_path], dst_file_name, options[:dest_options], options[:cache_max_age_segments], options[:overwrite]) != nil)
             rendition_segment_uploaded = true
          end
        end
      end

      if rendition_segment_uploaded == true && upload_state == :enabled
        if !options[:uid].nil?
          change_manifest_media_ids(rendition_manifest_file[:local_path], options[:uid])
        end
        #Upload rendition manifest
        tmp = get_relative_path(options[:local_tmp_path], rendition_manifest_file[:local_path])
        dst_file_name = dst_local_path + options[:prepend_id].to_s + File.join(File.dirname(tmp), File.basename(tmp)).to_s
        dst_file_name_bck = dst_local_path + options[:prepend_backup_id].to_s + File.join(File.dirname(tmp), File.basename(tmp)).to_s
        transfer(options[:dest_type], rendition_manifest_file[:local_path], dst_file_name, options[:dest_options], options[:cache_max_age_manifest], true)

        last_path_uploaded_chunklist << dst_file_name
        last_path_uploaded_chunklist_bck << dst_file_name_bck
        if segment_duration_secs.nil?
          segment_duration_secs = get_segment_duration(rendition_manifest_file[:local_path])
        end
      end
    end

    if uploaded_playlist_manifest == false && upload_state == :enabled
      #Upload simple playlist manifest
      tmp = get_relative_path(options[:local_tmp_path], playlist_manifest_file[:local_path])
      dst_file_name = dst_local_path + options[:prepend_id].to_s + File.join(File.dirname(tmp), File.basename(tmp)).to_s
      transfer(options[:dest_type], playlist_manifest_file[:local_path], dst_file_name, options[:dest_options], options[:cache_max_age_manifest], true)

      if !options[:prepend_id].nil? && !options[:prepend_backup_id].nil?
        #Create the main playlist with redundant streams & upload it
        master_playlist_local_file_name = File.join(options[:local_tmp_path], File.basename(playlist_manifest_file[:local_path]))

        tmp = get_relative_path(options[:local_tmp_path], playlist_manifest_file[:local_path])
        prepend_local = dst_local_path + options[:prepend_id].to_s + File.dirname(tmp).to_s + "/"
        prepend_bck = dst_local_path + options[:prepend_backup_id].to_s + File.dirname(tmp).to_s + "/"

        create_master_manifest_redundant_streams(playlist_manifest_file[:local_path], master_playlist_local_file_name, prepend_local, prepend_bck)

        #Upload the main playlist with redundant streams
        master_playlist_remote_file_name = dst_local_path + options[:prepend_id].to_s + File.join(File.dirname(tmp), "#{File.basename(tmp, ".*")}_redundant.m3u8").to_s
        transfer(options[:dest_type], master_playlist_local_file_name, master_playlist_remote_file_name, options[:dest_options], options[:cache_max_age_manifest], false)
      end

      uploaded_playlist_manifest = true
    end

    loop_time_secs = Time.now.to_f - time_start
    sleep_secs = [loop_time_max_secs - loop_time_secs, 0.01].max
    log(:debug, "Process loop time: #{loop_time_secs}s, next sleep #{sleep_secs}")

    sleep (sleep_secs)

    first_upload_after_activation = false

  rescue SystemExit, Interrupt
    exit = true
    log(:info, "Captured SIGINT / SIGTERM), exiting...")
  rescue Exception => e
    log(:error, "Error: #{e.message}, #{e.backtrace}")
  end
end