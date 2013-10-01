require 'rubygems'
require 'bud'

VOTE_ABORT = "abort"
VOTE_COMMIT = "commit"

# Assumptions:
#   * fixed set of voters
class TwoPhaseCommit
  include Bud

  state do
    sealed :voter, [:addr]
    channel :vote_req, [:@addr, :xid, :coord]
    channel :vote_resp, [:@addr, :vote_addr, :xid] => [:vote]
    table :vote_log, [:addr, :xid] => [:vote]
    table :xact, [:xid]
    table :abort_xact, [:xid]
    table :commit_xact, [:xid]
    scratch :commit_log, vote_log.schema
    scratch :voter_xid, [:addr, :xid]
    scratch :missing_commit, [:xid]

    # Voter state
    sealed :xact_status, [:xid] => [:status]
  end

  bloom do
    vote_req <~ (voter * xact).pairs {|v,x| [v.addr, x.xid, ip_port]}
    vote_resp <~ (vote_req * xact_status).pairs(:xid => :xid) do |r,s|
      [r.coord, r.addr, s.xid, s.status]
    end
    vote_log <= vote_resp.payloads

    # A transaction is aborted if we see any abort votes
    abort_xact <= vote_log {|v| [v.xid] if v.vote == VOTE_ABORT}

    # A transaction is committed if we see a commit vote from every participant
    commit_log <= vote_log {|v| v if v.vote == VOTE_COMMIT}
    voter_xid <= (voter * xact).pairs {|v,x| v + x}
    missing_commit <= voter_xid.notin(commit_log, :addr => :addr, :xid => :xid).pro {|v| [v.xid]}
    commit_xact <= xact.notin(missing_commit, :xid => :xid)
  end
end

ports = (1..3).map {|i| i + 10001}
rlist = ports.map {|p| TwoPhaseCommit.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.xact_status <+ [[1, VOTE_COMMIT]]
  vote_abort = (r == rlist.first)
  r.xact_status <+ [[2, vote_abort ? VOTE_ABORT : VOTE_COMMIT]]
  r.tick
end

coord = TwoPhaseCommit.new(:channel_stats => true)
coord.voter <+ ports.map {|p| ["localhost:#{p}"]}
coord.xact <+ [[1], [2]]

15.times { coord.tick; rlist.each(&:tick); sleep(0.1) }

puts "COMMITTED XACTS:"
puts coord.commit_xact.to_a.sort.inspect
puts "ABORTED XACTS:"
puts coord.abort_xact.to_a.sort.inspect
puts "VOTE_LOG:"
puts coord.vote_log.to_a.sort.inspect

rlist.each(&:stop)
coord.stop
