require_relative 'request'
require 'rexml/document'

def with_chunks_from_file (path, action, chunksize)
	i = 1 
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
		i += 1
		size = s.text.to_i
		fname = "%s.%0#{$chunk_padding}d" % [blob, i]
		fsize = File.size?(fname)
		File.delete(fsize) if fsize && fsize != size
		if (!fsize || fsize != size) 
			x = get_chunk(blob, written, size, i, container, use_threads)
			threads << x if use_threads
		end
		written += size
	}
	threads.each { |t|
		t.join
	} if use_threads
	File.open(blob, "w") { |out|
		(1..i).each { |j|
			file = "%s.%0#{$chunk_padding}d" % [blob, j]
			File.open(file, "r") { |inp|
				out.write(inp.read)			
			}
			File.delete(file)
		}
	}
end

def upload_blob (file, blob, chunksize = $default_chunksize, container = $default_container_name)
	parts = 0
	with_chunks_from_file(file, lambda { |chunk, i|
		id = "%0#{$chunk_padding}d" % [i]
		put_block(blob, id, chunk, container)
		parts += 1
	}, chunksize)
	put_block_list(blob, parts, container)
end

