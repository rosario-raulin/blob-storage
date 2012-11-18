require_relative 'request'
require 'rexml/document'

def with_chunks_from_file (path, action, chunksize = $default_chunksize)
	i = 0
	File.open(path, "r") { |f|
		until f.eof?
			action.call(f.read(chunksize), i)
			i += 1
		end
	}
end

def join_file (in_path, out_path = in_path)
	i = 0
	File.open(out_path, "w") { |out|
		while File.exists?("%s.%0#{$chunk_padding}d" % [in_path, i]) do
			File.open("%s.%0#{$chunk_padding}d" % [in_path, i], "r") { |f|
				out.write(f.read)		
			}
			i += 1
		end
	}
end

def get_chunk (blob, from, size, part, container, use_threads)
	code = Proc.new {
		to = from + size - 1
		target = "%s.%0#{$chunk_padding}d" % [blob, part]
		File.open(target, "w") { |out|
			out.write(get_blob(blob, (from..to), container).body)
		}
	}
	if use_threads
		Thread.new { code.call }
	else
		code.call
	end
end

def download_blob (blob, use_threads = false, container = $default_container_name)
	i = 0
	written = 0
	threads = [] if use_threads
	REXML::Document.new(get_block_list(blob, container).body).elements.each('BlockList/CommittedBlocks/Block/Size') { |s|
		size = s.text.to_i
		x = get_chunk(blob, written, size, i, container, use_threads)
		i += 1
		written += size
		threads << x if use_threads
	}
	threads.each { |t|
		t.join
	} if use_threads
end

