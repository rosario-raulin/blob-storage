#!/usr/bin/ruby1.9.3

require 'base64'

$account_id = "rosariomd"
$account_key = Base64.decode64("")
$account_uri = "http://rosariomd.blob.core.windows.net/"
$default_container_name = "bazbaz"

$chunk_padding = "3"
$default_chunksize = 512 * 1024

require_relative "file"

def main
	if ARGV.length < 2
					puts "usage: blob.rb (upload|download|container|deleteAll) path [use-threads]"
	else
		use_threads = if (ARGV.length == 3) then true else false end
		case ARGV[0]
			when "upload" then upload_blob(ARGV[1], ARGV[1])
			when "download" then download_blob(ARGV[1], use_threads)
			when "container" then create_container(ARGV[1])
			when "deleteAll" then delete_container(ARGV[1])
		end
	end
end

main

