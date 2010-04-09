#--
# Copyright (c) 2006 Anthony Eden
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#++
#


THIS_NAME = "ActiveWarehouse-ETL"
THIS_VERSION = ETL::VERSION::STRING

require 'benchmark'

require 'optparse'
require 'ostruct'

def options_parse(args)
  options = OpenStruct.new
  # set default option values if any
  # options.backtrace = false
  # options.outfile   = "/dev/null/"
  # options.noemail   = true
  # options.quiet     = false
  # options.test      = false
  # options.update    = false
  # etc.

  # Create parser
  opts = OptionParser.new do |opts| 

    opts.banner = "Usage: #{File.basename($0)} [options] " + 
      "[control_file.ctl [control_file.ctl]] \n\n"

    opts.on("-c file", "--config file", "Provide a database configuration",
            "  file",
            "  database.yml (default) \n") do |dbyml|
      options.config = dbyml
    end

    opts.on("-l N", "--limit=N", "Limit the number of input rows read",
            "  no limit (default) \n") do |limit|
      options.limit  = limit.to_i
    end

    opts.on("-L", "--read-locally",
            "Use locally cached source file",
            "  produced by previous run",
            "  do not use cached source (default) \n") do
      options.read_locally = true
    end

    opts.on("-n", "--newlog",
            "Write out a new log file",
            "  or overwrite existing log file",
            "  append to existing (default) \n") do
      options.newlog = false
    end

    opts.on("-o N", "--offset=N", "Read input rows starting from offset",
            "  0 (default) \n") do |offset|
      options.limit  = offset.to_i
    end

    opts.on("-r path", "--rails-root=path",
            "Specify path to RoR root directory ",
            "  nil (default) \n") do |path|
      options.rails_root = path
    end

    opts.on("-S", "--skip-bulk-import",
            "Do not use bulk import", 
            "  even if DBMS supports it",
            "  Use bulk import if available (default) \n") do
      options.skip_bulk_import = true
    end
    
    opts.on("-v", "--version", "Show name and version, then exit \n") do
      puts
      puts "#{THIS_NAME}"
      puts "Running as: #{$0}"
      puts "Version: #{THIS_VERSION}"
      puts
      exit
    end

    # No argument, shows at tail.  This will print an options summary.
    # Try it and see!
    opts.on_tail("-h", "--help", "Show this message and exit \n") do
      puts
      puts "#{THIS_NAME}"
      puts "Running as: #{$0}"
      puts "Version: #{THIS_VERSION}"
      puts
      puts opts
      puts
      exit # print help and exit script
    end
  
  end # end opts

  opts.parse!(args)

  # What is left in args from the command line drops to here
  #

  return options # return the parsed options array

end # end parse

def execute

  @argv = ARGV
  options = options_parse(@argv).marshal_dump

  puts "skip bulk import enabled" unless options[:skip_bulk_import]
  puts "read locally enabled" if options[:read_locally]

  puts "Starting ETL process"

  ETL::Engine.init(options)
  ARGV.each do |f|
    ETL::Engine.realtime_activity = true
    ETL::Engine.process(f)
  end
  
  puts "ETL process complete\n\n"

end

execute
