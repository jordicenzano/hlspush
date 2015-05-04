#!/usr/bin/env ruby

#
#Donwload HLS -> Pus the,
#

require 'open-uri'
require 'fileutils'
require 'uri'
require 'pathname'
require 'fog'
require 'optparse'


def aws_upload_S3(bucket_name, upload_path, file_name, max_age, credentials, not_upload_if_remote_file_exists = false)

  if bucket_name[0] == "."
    dest = File.join(bucket_name.to_s + upload_path)

    #Create directory if it is needed
    FileUtils::mkdir_p(File.dirname(dest)) unless File.exists?(File.dirname(dest))

    FileUtils.cp(file_name, dest)
    puts "Copied file #{file_name} to #{dest}."

    "COPIED"
  else
    connection = Fog::Storage.new(credentials)

    bucket = connection.directories.get(bucket_name)

    do_upload = true
    if not_upload_if_remote_file_exists == true
      if bucket.files.head(upload_path) != nil
        do_upload = false
        puts "Skipping upload of  file #{file_name} to bucket #{bucket_name}/#{upload_path}."
      end
    end

    if do_upload == true
      file = bucket.files.create(:key => upload_path, :body => File.open(file_name), :metadata => {"Cache-Control" => "max-age=#{max_age}"},:public => true )
      puts "Uploaded file #{file_name} to bucket #{bucket_name}/#{upload_path}."

      file
    end
  end
end

def aws_delete_S3(bucket_name, upload_path, credentials)

  if (upload_path != nil)

    if bucket_name[0] == "."
      dest = File.join(bucket_name.to_s + upload_path)

      if File.exists?(dest)
        File.delete(dest)
        puts "Deleted #{dest}."
      end
    else
      connection = Fog::Storage.new(credentials)

      bucket = connection.directories.get(bucket_name)
      file = bucket.files.get(upload_path)
      if !file.nil?
        file.destroy
        puts "Deleted #{bucket_name}/#{upload_path}."
      end
    end
  end
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

    #$stderr.puts "Downloaded #{local_file_path} from #{url}"
  else
    #$stderr.puts "Skipped download #{local_file_path} from #{url}"
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

def change_media_id(src, new_media_id, id)
  new_name = ""

  begin
    data = src.to_s.scan(/(.*-).*(_\d*.ts)/)
    if !data.empty?
      tmp = data[0][0] + new_media_id.to_s + data[0][1]
      if File.dirname(tmp) != "."
        new_name = File.join(File.dirname(tmp), id.to_s + File.basename(tmp))
      else
        new_name = id.to_s + File.basename(tmp)
      end
    else
      raise
    end
  rescue Exception => e
    raise "Media segment name malformed (not wowza live http origin convention media-ucxikcdtm_2.ts). Current: #{src}"
  end

  new_name
end

def change_manifest_reference_ids(playlist_manifest_file, id)
  manifest_file_modi = playlist_manifest_file + ".modi"

  File.open(manifest_file_modi, 'w') do |f_write|
    File.open(playlist_manifest_file, 'r') do |f_read|
      while strline = f_read.gets
        data = strline.scan(/([^#].*.m3u8)/)
        if !data.empty?
          if File.dirname(strline) != "."
            strline = File.join(File.dirname(strline), id.to_s + File.basename(strline))
          else
            strline = id.to_s + File.basename(strline)
          end
        end
        f_write.puts strline
      end
    end
  end

  File.delete(playlist_manifest_file)
  File.rename(manifest_file_modi, playlist_manifest_file)
end

def change_manifest_media_ids(manifest_file, new_media_id, id)

  manifest_file_modi = manifest_file + ".modi"

  File.open(manifest_file_modi, 'w') do |f_write|
    File.open(manifest_file, 'r') do |f_read|
      while strline = f_read.gets
        data = strline.scan(/([^#].*-).*(_\d*.ts)/)
        if !data.empty?
          tmp = data[0][0] + new_media_id.to_s + data[0][1]
          if File.dirname(tmp) != "."
            strline = File.join(File.dirname(tmp), id.to_s + File.basename(tmp))
          else
            strline = id.to_s + File.basename(tmp)
          end
        end
        f_write.puts strline
      end
    end
  end

  File.delete(manifest_file)
  File.rename(manifest_file_modi, manifest_file)
end

# START SCRIPT ***********************
$stderr.sync = true

#Parse args
aws_options = {:key => nil, :secret => nil, :region => nil, :bucket =>nil, :cache_max_age_manifest => 1, :cache_max_age_segments => 3600}
local_options = {:path => nil}

options = {:source_url => nil, :local_tmp_path => nil, :dest_type => 's3', :dest_options => aws_options, :uid =>nil, :prepend_id => nil, :skip_upload_file => nil}

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
    if "s3".strcmp(v) != true
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

  opts.on('-m', '--cache_max_age_manifest SECS', 'Cache control data for manifest files (Default = 1)') { |v| options[:dest_options][:cache_max_age_manifest] = v }
  opts.on('-t', '--cache_max_age_segments SECS', 'Cache control data for segments files (Default = 3600)') { |v| options[:dest_options][:cache_max_age_segments] = v }

  #General optional
  opts.on('-i', '--uid id ID', 'Unique ID to for segment files') { |v| options[:uid] = v }
  opts.on('-p', '--prepend_id', 'Upload directory prepend id ID') { |v| options[:prepend_id] = v }
  opts.on('-j', '--skip_upload_file', 'If this file exists the upload/copy is stopped') { |v| options[:skip_upload_file] = v }
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
$stderr.puts "Read parameters: #{options.inspect}"

#Compute the full local path
options[:local_tmp_path_stream] = File.join(options[:local_tmp_path], File.dirname(URI.parse(optionsp[:source_url]).path))


#uid
#Rename segments in order to sync the data from different wowza https origins (Wowza origin segments example: media-ucxikcdtm_2.ts media-ucxikcdtm_212.ts)
#The idea is to overwite the ID -> media-NEW_ID_2.ts
#Doing that we ensure the uniqueness of every job in different machines, and we simplify the implementation of redundant jobs

#local_id = nil
#if ARGV.length > 10
#  local_id = ARGV[10]
#end

#prepend_id
#Prepend to the "prepend_id" to the upload path
#Using this parameter we can upload different jobs with the same uid to the same bucket
#It helps to create a organaised environment (easier to read for humans than using only uid)

#TODO -------

local_path_id = nil
if ARGV.length > 10
  local_path_id = ARGV[10]
end

s3_credentials = {:provider => 'AWS', :aws_access_key_id => aws_key,:aws_secret_access_key => aws_secret, :region => aws_region}

exit = false

uploaded_playlist_manifest = false

upload_to_s3 = true
first_upload_after_activation = false

#If is in S3 means it will be in CDN, so don not waste time uploading
s3_check_file_enabled = true

last_path_uploaded_chunklist = nil

while exit == false
  begin
    #Check if s3 upload is disabled
    if File.exist?(skip_upload_file.to_s)
      if (upload_to_s3 == true)
        $stderr.puts "Upload OFF!!!!"
        #Delete chunk list manifest
        aws_delete_S3(aws_bucket_name, last_path_uploaded_chunklist, s3_credentials)
        upload_to_s3 = false
      end
    else
      if (upload_to_s3 == false)
        $stderr.puts "Upload ON!!!!"
        upload_to_s3 = true
        first_upload_after_activation = true
      end
    end

    #Download playlist manifest
    playlist_manifest_file = download(playlist_manifest_url, local_path_stream)

    #Analise playlist manifest for rendition manifests
    renditions_manifests = get_renditions_manifests(playlist_manifest_file[:local_path])

    #For every rendition manifest

    renditions_manifests.each do |rendition_manifest|
      rendition_segment_uploaded = false
      renditions_manifest_url = merge_url(playlist_manifest_url, rendition_manifest.first)

      #download rendition manifest
      rendition_manifest_path = File.join(local_path_stream, File.dirname(rendition_manifest.first))
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
        if (rendition_segment[:downloaded] == true || first_upload_after_activation == true) && upload_to_s3 == true
          #Upload new rendition to S3
          s3_filename = local_path_id.to_s + get_relative_path(local_path_base, rendition_segment[:local_path]).to_s
          if !rename_segments.nil?
            s3_filename = local_path_id.to_s + change_media_id(get_relative_path(local_path_base, rendition_segment[:local_path]), rename_segments, local_id)
          end
          if (aws_upload_S3(aws_bucket_name, s3_filename, rendition_segment[:local_path], max_age_segments, s3_credentials, s3_check_file_enabled) != nil)
            rendition_segment_uploaded = true
          end
        end
      end

      if rendition_segment_uploaded == true && upload_to_s3 == true
        if !rename_segments.nil?
          change_manifest_media_ids(rendition_manifest_file[:local_path], rename_segments, local_id)
        end
        #Upload rendition manifest
        tmp = get_relative_path(local_path_base, rendition_manifest_file[:local_path])
        s3_file_name = local_path_id.to_s + File.join(File.dirname(tmp), local_id.to_s + File.basename(tmp)).to_s
        aws_upload_S3(aws_bucket_name, s3_file_name,rendition_manifest_file[:local_path], max_age_manifest, s3_credentials)
        last_path_uploaded_chunklist = s3_file_name
      end
    end

    if uploaded_playlist_manifest == false && upload_to_s3 == true
      #Upload playlist manifest
      change_manifest_reference_ids(playlist_manifest_file[:local_path], local_id)
      tmp = get_relative_path(local_path_base, playlist_manifest_file[:local_path])
      s3_file_name = local_path_id.to_s + File.join(File.dirname(tmp), local_id.to_s + File.basename(tmp)).to_s
      aws_upload_S3(aws_bucket_name, s3_file_name, playlist_manifest_file[:local_path], max_age_manifest, s3_credentials, s3_check_file_enabled)
      uploaded_playlist_manifest = true
    end
  rescue Exception => e
    $stderr.puts "Error: #{e.message}, #{e.backtrace}"
  end

  sleep (1.0)

  first_upload_after_activation = false
end