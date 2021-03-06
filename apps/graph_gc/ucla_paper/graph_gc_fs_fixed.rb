require 'rubygems'
require 'bud'

# Graph garbage collection. Fixed set of nodes. 

class GraphGCFixed
  include Bud

  state do
    # State at each node
    sealed :node, [:addr]
    table :objects, [:id] => [:val]
    table :active_objects, objects.schema
    
    # Phase 1 communication
    table :p1_sbuf, [:id] => [:val]
    channel :p1_chn, [:@addr, :id] => [:val]
    table :p1_rbuf, p1_sbuf.schema
    channel :p1_ack, [:@sender] + p1_chn.key_cols
    table :p1_approx, p1_chn.key_cols
    table :missing_msgs_p1, [:addr, :id] => [:val] 
    table :complete_msgs_p1, p1_sbuf.schema

    # Phase 2 communication
    table :p2_sbuf, p1_sbuf.schema
    channel :p2_chn, [:@addr, :id] => [:val]
    table :p2_rbuf, p1_rbuf.schema
    channel :p2_ack, [:@sender] + p1_chn.key_cols
    table :p2_approx, p1_approx.schema
    table :missing_msgs_p2, [:addr, :id] => [:val] 
    table :complete_msgs_p2, p1_sbuf.schema

    
  end
  
  # Notify all nodes you want to delete an object.
  bloom :phase1 do
    p1_chn <~ ((p1_sbuf * node).pairs {|p, n| n + p}).notin(p1_approx, 0 => :addr, 1 => :id)
    p1_rbuf <= p1_chn.payloads
    p1_sbuf <= p1_rbuf

    # By acking, you are saying that you've marked all refs deleted. 
    p1_ack <~ p1_chn {|c| [c.source_addr, c.addr, c.id]}
    p1_approx <= p1_ack.payloads
    #stdio <~ p1_chn {|p| ["Got message on p1, I am #{self.port}: #{p.inspect}"]}
    missing_msgs_p1 <= p1_approx.notin(node, :addr => :addr)
    complete_msgs_p1 <= p1_sbuf.notin(missing_msgs_p1, :id => :id)
    # Proceed to phase 2 once all nodes have acked
    # This means they are all know we're trying to delete 
    p2_sbuf <= complete_msgs_p1 {|c| [c.id, c.val]}    
  end
    
  # Notify all nodes that all nodes are aware that you want to delete an object
  bloom :phase2 do
    p2_chn <~ ((p2_sbuf * node).pairs {|p,n| n + p}).notin(p2_approx, 0 => :addr, 1 => :id)
    p2_rbuf <= p2_chn.payloads
    p2_ack <~ p2_chn {|c| [c.source_addr, c.addr, c.id]}
    p2_approx <= p2_ack.payloads
    #stdio <~ p2_chn {|p| ["Got message on p2, I am #{self.port}: #{p.inspect}"]}
    missing_msgs_p2 <= p2_approx.notin(node, :addr => :addr)
    complete_msgs_p2 <= p2_sbuf.notin(missing_msgs_p2, :id => :id)
    active_objects <= objects.notin(complete_msgs_p2, :id => :id)
  end

  def print_obj
    puts "Objs"
    puts objects.map {|v| "\t#{v.id} => #{v.val}"}
  end

end

opts = { :channel_stats => true, :disable_rce => false, :disable_rse => false }

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| GraphGCFixed.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end
rlist.each(&:run_bg)

s = rlist.first
r = rlist[1]

r.sync_do {
  r.objects <+ [[1, 'Object1']]
}

s.sync_do {
  s.p1_sbuf <+ [[1, 'Object1'], [3, 'Object3']]
  s.objects <+ [[1, 'Object1'], [2, 'Object2'], [3, 'Object3']]
}

sleep 2

s.sync_do {
  puts s.print_obj
  puts "#{s.port}: log size = #{s.p1_sbuf.to_a.size}"
  puts "#{s.port}: p1_approx = #{s.p1_approx.to_a.inspect}"
  puts "#{s.port}: p2_approx = #{s.p2_approx.to_a.inspect}"
  puts "p1_rbuf = #{s.p1_rbuf.to_a.inspect}"
  puts "nodes = #{s.node.to_a}"
  puts "missing_msgs_p1 = #{s.missing_msgs_p1.to_a.inspect}"
  puts "complete_msgs = #{s.complete_msgs_p1.to_a.inspect}"
  puts "p2_sbuf: #{s.p2_sbuf.to_a.inspect}"
  puts "P2 complete #{s.complete_msgs_p2.to_a.inspect}"
  puts "active objects: #{s.active_objects.to_a.inspect}"
  puts s.print_obj
}

sleep 2

r.sync_do {
  puts "#{r.port}: log size = #{r.p1_sbuf.to_a.size}"
  puts "#{r.port}: p1_approx = #{r.p1_approx.to_a.inspect}"
  puts "#{r.port}: p2_approx = #{r.p2_approx.to_a.inspect}"
  puts "p1_rbuf = #{r.p1_rbuf.to_a.inspect}"
  puts "nodes = #{r.node.to_a}"
  puts "missing_msgs_p1 = #{r.missing_msgs_p1.to_a.inspect}"
  puts "complete_msgs = #{r.complete_msgs_p1.to_a.inspect}"
  puts "p2_sbuf: #{r.p2_sbuf.to_a.inspect}"
  puts "P2 complete #{r.complete_msgs_p2.to_a.inspect}"
  puts "active objects: #{r.active_objects.to_a.inspect}"
  puts r.print_obj
}

s.stop
rlist.each(&:stop)