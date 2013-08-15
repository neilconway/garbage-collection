require 'rubygems'
require 'bud'

class RseSimpleTest
  include Bud

  state do
    table :sbuf, [:id] => [:val]
    table :sbuf2, [:id] => [:val]
    scratch :chn, sbuf.schema
    table :chn_approx, chn.schema
  end

  bloom do
    chn <= sbuf.notin(chn_approx)
    chn <= sbuf2.notin(chn_approx, :val => :val)

    # Inferred deletion rules
    sbuf <- chn_approx
    sbuf2 <- (sbuf * chn_approx).lefts(:val => :val)
  end
end

r = RseSimpleTest.new(:disable_rse => true)
r.tick

puts r.t_depends.inspected
puts "========="
puts r.t_rules.inspected
