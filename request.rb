require 'net/http'
require 'time'
require 'openssl'
require 'base64'
require 'uri'

$account_id = "rosariomd"
$account_key = Base64.decode64("")
$account_uri = "http://rosariomd.blob.core.windows.net/"
#$account_uri ="http://localhost:8080/"
$default_container_name = "foobar"

$chunk_padding = "3"
$default_chunksize = 512 * 1024

def canonicalized_headers (req)
	headers = []
	req.each_key { |k|
		headers << k.downcase if k.match(/^x-ms-/)
	}
	headers.sort!.map{ |k| "%s:%s\n" % [k, req[k]]}.join
end

#TODO: improve ?comp= appending
def canonicalized_resource (uri)
	x = "/%s%s" % [$account_id, uri.path]
	comp = nil
	uri.query.split(/\&|=/).each_cons(2) { |x, y|
		comp = y if x == "comp"
	} if uri.query
	x + (if comp then "?comp=" + comp else "" end)
end

def gen_auth_header (req, uri)
	x = req.class.name.split("::").last.upcase + "\n\n" +
			if req.content_type then req.content_type else "" end + "\n" +
			req["Date"] + "\n" +
			canonicalized_headers(req) +
			canonicalized_resource(uri)
	Base64.strict_encode64(OpenSSL::HMAC.digest('sha256', $account_key, x))
end

def make_request (req, uri, body = nil, content_type = "text/plain")
	req["Date"] = Time.now.httpdate
	req["x-ms-version"] = "2009-09-19"
	req["Content-Length"] = if body then body.length else 0 end
	req.body = body if body
	req.content_type = content_type if body
	req["Authorization"] = "SharedKeyLite %s:%s" % [$account_id, gen_auth_header(req, uri)]

	Net::HTTP.start(uri.hostname, uri.port) { |c|
		c.request(req)
	}
end

def create_container (name = $default_container_name)
	uri = $account_uri + name + "?restype=container"
	make_request(Net::HTTP::Put.new(uri), URI(uri))
end

def delete_container (name)
	uri = $account_uri + name + "?restype=container"
	make_request(Net::HTTP::Delete.new(uri), URI(uri))
end

def put_block (name, id, block, container = $default_container_name)
	uri = $account_uri + container + "/" + name + "?comp=block&blockid=" + URI.escape(Base64.strict_encode64(id))
	puts uri 
	make_request(Net::HTTP::Put.new(uri), URI(uri), block)
end

def put_block_list (name, parts, container = $default_container_name)
	uri = "%s%s/%s?comp=blocklist" % [ $account_uri, container, name]
	body = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n"
	(1..parts).each { |i|
		id = Base64.strict_encode64("%0#{$chunk_padding}d" % [i])
		body += "\t<Latest>#{id}</Latest>\n"
	}
	body += "</BlockList>"
	puts body
	make_request(Net::HTTP::Put.new(uri), URI(uri), body, "text/xml")
end

def get_block_list (name, container = $default_container_name)
	uri = "%s%s/%s?comp=blocklist" % [ $account_uri, container, name]
	make_request(Net::HTTP::Get.new(uri), URI(uri))
end

def get_blob (name, range = nil, container = $default_container_name)
	uri = "%s%s/%s" % [ $account_uri, container, name]
	req = Net::HTTP::Get.new(uri)
	req["Range"] = "bytes=%d-%d" % [range.min, range.max] if range
	make_request(req, URI(uri))
end

