require 'rubygems'
require 'bud'

# Simple variant of MVCC: a single node system which consists of versioned
# objects and transactions; the latter have a "snapshot" that defines which
# objects they can read. Snapshots advance forward monotonically over time.
class SimpleMvcc
  include Bud

  state do
    table :obj, [:id, :create_t, :del_t]
  end
end
