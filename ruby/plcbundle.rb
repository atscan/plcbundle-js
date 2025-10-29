#!/usr/bin/env ruby
# frozen_string_literal: true

# plcbundle.rb - Ruby implementation of plcbundle V1 specification
# Creates compressed, cryptographically-chained bundles of DID PLC operations
#
# PLC Bundle v1 Specification:
#   https://tangled.org/atscan.net/plcbundle/blob/main/SPECIFICATION.md

require 'json'
require 'digest'
require 'net/http'
require 'uri'
require 'fileutils'
require 'time'
require 'set'
require 'zstd-ruby'

# Configuration constants
BUNDLE_SIZE = 10_000
INDEX_FILE = 'plc_bundles.json'
PLC_URL = 'https://plc.directory'

class PlcBundle
  def initialize(dir)
    @dir = dir
    @pool = []           # Mempool of operations waiting to be bundled
    @seen = Set.new      # CID deduplication set (pruned after each bundle)
    
    FileUtils.mkdir_p(@dir)
    @idx = load_idx
    puts "plcbundle v1 | Dir: #{@dir} | Last: #{@idx[:last_bundle]}\n"
    
    # Seed deduplication set with boundary CIDs from previous bundle
    seed_boundary if @idx[:bundles].any?
  end

  def run
    cursor = @idx[:bundles].last&.dig(:end_time)
    
    loop do
      puts "\nFetch: #{cursor || 'start'}"
      ops = fetch(cursor) or (puts('Done.') and break)
      
      add_ops(ops)                                  # Validate and add to mempool
      cursor = ops.last[:time]
      create_bundle while @pool.size >= BUNDLE_SIZE # Create bundles when ready
      
      sleep 0.2  # Rate limiting
    end
    
    save_idx
    puts "\nBundles: #{@idx[:bundles].size} | Pool: #{@pool.size} | Size: #{'%.1f' % (@idx[:total_size_bytes] / 1e6)}MB"
  rescue => e
    puts "\nError: #{e.message}" and save_idx
  end

  private

  # Fetch operations from PLC directory export endpoint
  def fetch(after)
    uri = URI("#{PLC_URL}/export?count=1000#{after ? "&after=#{after}" : ''}")
    res = Net::HTTP.get_response(uri)
    res.is_a?(Net::HTTPSuccess) or return nil
    
    # Parse each line and preserve raw JSON for reproducibility (Spec 4.2)
    res.body.strip.split("\n").map do |line|
      {**JSON.parse(line, symbolize_names: true), raw: line, time: JSON.parse(line)['createdAt']}
    end
  rescue
    nil
  end

  # Process and validate operations before adding to mempool
  def add_ops(ops)
    last_t = @pool.last&.dig(:time) || @idx[:bundles].last&.dig(:end_time) || ''
    added = 0
    
    ops.each do |op|
      next if @seen.include?(op[:cid])  # Skip duplicates (boundary + within-batch)
      
      # Spec 3: Validate chronological order
      raise "Order fail" if op[:time] < last_t
      
      @pool << op
      @seen << op[:cid]
      last_t = op[:time]
      added += 1
    end
    
    puts "  +#{added} ops"
  end

  # Create a bundle file and update index
  def create_bundle
    ops = @pool.shift(BUNDLE_SIZE)
    parent = @idx[:bundles].last&.dig(:hash) || ''
    
    # Spec 4.2: Serialize using raw JSON strings for reproducibility
    jsonl = ops.map { |o| o[:raw] + "\n" }.join
    
    # Spec 6.3: Calculate hashes
    ch = sha(jsonl)                                                      # Content hash
    h = sha(parent.empty? ? "plcbundle:genesis:#{ch}" : "#{parent}:#{ch}") # Chain hash
    zst = Zstd.compress(jsonl)                                          # Compress
    
    # Write bundle file
    num = @idx[:last_bundle] + 1
    file = format('%06d.jsonl.zst', num)
    File.binwrite("#{@dir}/#{file}", zst)

    # Create metadata entry
    @idx[:bundles] << {
      bundle_number: num,
      start_time: ops[0][:time],
      end_time: ops[-1][:time],
      operation_count: ops.size,
      did_count: ops.map { |o| o[:did] }.uniq.size,
      hash: h,
      content_hash: ch,
      parent: parent,
      compressed_hash: sha(zst),
      compressed_size: zst.bytesize,
      uncompressed_size: jsonl.bytesize,
      cursor: @idx[:bundles].last&.dig(:end_time) || '',
      created_at: Time.now.utc.iso8601
    }
    
    @idx[:last_bundle] = num
    @idx[:total_size_bytes] += zst.bytesize
    
    # Prune seen CIDs: only keep boundary + mempool (memory efficient)
    @seen = boundary_cids(ops) | @pool.map { |o| o[:cid] }.to_set
    
    save_idx
    puts "✓ #{file} | #{h[0..12]}... | seen:#{@seen.size}"
  end

  # Load index from disk or create new
  def load_idx
    JSON.parse(File.read("#{@dir}/#{INDEX_FILE}"), symbolize_names: true)
  rescue
    {version: '1.0', last_bundle: 0, updated_at: '', total_size_bytes: 0, bundles: []}
  end

  # Atomically save index using temp file + rename
  def save_idx
    @idx[:updated_at] = Time.now.utc.iso8601
    tmp = "#{@dir}/#{INDEX_FILE}.tmp"
    File.write(tmp, JSON.pretty_generate(@idx))
    File.rename(tmp, "#{@dir}/#{INDEX_FILE}")
  end

  # Seed deduplication set with CIDs from last bundle's boundary
  def seed_boundary
    last = @idx[:bundles].last
    file = format('%06d.jsonl.zst', last[:bundle_number])
    
    data = Zstd.decompress(File.binread("#{@dir}/#{file}"))
    ops = data.strip.split("\n").map do |line|
      {time: JSON.parse(line)['createdAt'], cid: JSON.parse(line)['cid']}
    end
    
    @seen = boundary_cids(ops)
    puts "Seeded: #{@seen.size} CIDs from bundle #{last[:bundle_number]}"
  rescue
    puts "Warning: couldn't seed boundary"
  end

  # Get CIDs from operations at the same timestamp as the last op (boundary)
  def boundary_cids(ops)
    return Set.new if ops.empty?
    
    t = ops[-1][:time]
    ops.reverse.take_while { |o| o[:time] == t }.map { |o| o[:cid] }.to_set
  end

  # SHA-256 hash helper
  def sha(data)
    Digest::SHA256.hexdigest(data)
  end
end

# Entry point
PlcBundle.new(ARGV[0] || './plc_bundles_rb').run if __FILE__ == $PROGRAM_NAME
