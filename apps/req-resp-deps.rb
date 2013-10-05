require 'rubygems'
require 'bud'

class RequestResponseDeps
  include Bud

  state do
    channel :req_chn, [:@addr, :client, :id] => [:key, :deps]
    channel :resp_chn, [:@addr, :id] => [:key, :val]

    table :req_log, [:client, :id] => [:key, :deps]
    table :resp_log, [:client, :id] => [:key, :val]
    table :did_resp, [:client, :id]
    table :state

    scratch :need_resp, req_log.schema
    scratch :flat_dep, [:client, :id, :dep]
    scratch :missing_dep, flat_dep.schema
    scratch :do_resp, req_log.schema

    # Client-side state
    table :read_req, req_chn.schema
    table :read_resp, resp_chn.schema
  end

  bloom do
    req_log <= req_chn.payloads
    resp_chn <~ resp_log

    need_resp <= req_log.notin(did_resp, :client => :client, :id => :id)
    flat_dep <= need_resp.flat_map {|r| r.deps.map {|d| [r.client, r.id, d]}}
    missing_dep <= flat_dep.notin(state, :dep => :key)
    do_resp <= need_resp.notin(missing_dep, :client => :client, :id => :id)
    resp_log <= (do_resp * state).pairs(:key => :key) do |r,s|
      [r.client, r.id, r.key, s.val]
    end
    did_resp <+ resp_log {|r| [r.client, r.id]}
  end

  bloom :client do
    req_chn <~ read_req
    read_resp <= resp_chn
  end

  def print_resp
    puts "Responses at client:"
    puts read_resp.map {|r| "\t#{r.key} => #{r.val}"}.sort.join("\n")
  end
end

def do_report(s, c)
  c.print_resp
  puts "(Server) Size of req_log: #{s.req_log.to_a.size}"
  puts "(Server) Size of resp_log: #{s.resp_log.to_a.size}"
  puts "(Server) Size of did_resp: #{s.did_resp.to_a.size}"
  puts "(Client) Size of read_req: #{c.read_req.to_a.size}"
end

opts = { :channel_stats => false, :print_rules => false,
         :disable_rce => false, :disable_rse => false }
nodes = Array.new(2) { RequestResponseDeps.new(opts) }
nodes.each(&:tick)

s, c = nodes
s.state <+ [["foo1", "bar"], ["foo2", "baz"]]
c.read_req <+ [[s.ip_port, c.ip_port, 1, "foo1", ["foo2"]],
               [s.ip_port, c.ip_port, 2, "foo2", ["foo99"]]]
10.times { nodes.each(&:tick); sleep(0.1) }
do_report(s, c)

s.state <+ [["foo99", "qux"]]
10.times { nodes.each(&:tick); sleep(0.1) }
do_report(s, c)

nodes.each(&:stop)
