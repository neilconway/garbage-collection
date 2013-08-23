require 'rubygems'
require 'bud'

module ReadProtocol
  state do
    channel :req_chn, [:@addr, :id] => [:key, :deps]
    channel :resp_chn, [:@addr, :id, :key, :val]
  end
end

class CausalClient
  include Bud
  include ReadProtocol

  state do
    table :read_req, req_chn.schema
  end

  bloom do
    req_chn <~ read_req
    stdio <~ resp_chn {|r| ["CLIENT: #{r.key} => #{r.val} (#{r.id})"]}
  end
end

# Places where we would like to apply RCE:
#   (1) read_req => read_chn
#   (2) (node * log) => chn
#   (3) read_resp => resp_chn
#
# Places where we would like to apply RSE:
#   (1) read_req.notin(read_chn_approx) (post-RCE)
#   (2) (node * log).notin(chn_approx) (post-RCE)
#   (3) read_resp.notin(resp_chn_approx) (post-RCE)
#   (4) safe_log.notin(dominated)
#   (5) read_buf.notin(read_response)
#
# Places where we would like to apply notin compression:
#   (1) read_chn_approx (post-RCE)
#   (2) chn_approx (post-RCE)
#   (3) resp_chn_approx (post-RCE)
#   (4) read_response
#       => This is a little tricky: the intuition is that once a response has
#          been generated and sent to the client, we only need to keep around
#          enough information so that the negation against read_buf can be
#          performed. So this is like a combination of normal notin compression
#          and RSE.
class CausalDict
  include Bud
  include ReadProtocol

  state do
    sealed :node, [:addr]
    table :log, [:id] => [:key, :val, :deps]
    channel :chn, [:@addr, :id] => [:key, :val, :deps]

    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, flat_dep.schema
    table :safe_log, log.schema
    scratch :dominated, log.schema
    scratch :view, log.schema

    table :read_buf, [:id] => [:key, :deps, :src_addr]
    scratch :read_pending, read_buf.schema
    scratch :read_dep, [:id, :dep]
    scratch :missing_read_dep, read_dep.schema
    scratch :safe_read, read_buf.schema
    table :read_response, resp_chn.schema
  end

  bloom :replication do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads
  end

  bloom :check_deps do
    flat_dep <= log.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    missing_dep <= flat_dep.notin(safe_log, :dep => :id)
    safe_log <+ log.notin(missing_dep, :id => :id)
  end

  bloom :active_view do
    dominated <= (safe_log * safe_log).pairs(:key => :key) do |w1,w2|
      w2 if w1 != w2 and w1.deps.include? w2.id
    end
    view <= safe_log.notin(dominated)
  end

  bloom :read_path do
    # XXX: Intuitively there should be a cleaner way to write this. We'd like to
    # say that "read_pending is the delta between read_response and read_buf".
    read_buf <= req_chn {|r| [r.id, r.key, r.deps, r.source_addr]}
    read_pending <= read_buf.notin(read_response, :id => :id)
    read_dep <= read_pending.flat_map {|r| r.deps.map {|d| [r.id, d]}}
    missing_read_dep <= read_dep.notin(safe_log, :dep => :id)
    safe_read <= read_buf.notin(missing_read_dep, :id => :id)
    read_response <+ (safe_read * view).pairs(:key => :key) {|r,v| [r.src_addr, r.id, r.key, v.val]}
    resp_chn <~ read_response
  end

  def print_view
    puts "View @ #{port}:"
    puts view.map {|v| "\t#{v.key} => #{v.val}"}.join("\n")
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| CausalDict.new(:ip => "localhost", :port => p, :channel_stats => true)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

first = rlist.first
first.log <+ [[[first.port, 1], 'foo', 'bar', []]]

last = rlist.last
last.log <+ [[[last.port, 1], 'baz', 'qux', [[first.port, 1]]]]
last.log <+ [[[last.port, 2], 'baz', 'kkk', []],
             [[last.port, 3], 'baz', 'kkk2', [[first.port, 2]]]]

c = CausalClient.new(:channel_stats => true)
c.tick
c.read_req <+ [[last.ip_port, [c.port, 1], 'foo', [[first.port, 1]]]]
c.tick

4.times { rlist.each(&:tick); sleep 0.3; c.tick }

first.print_view
last.print_view

c.stop
rlist.each(&:stop)
