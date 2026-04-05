// iter
// iter_with
// snapshot


// TODO Later:
// compact_range
// F: FnOnce(bool) -> Result<Filter, E>    -> Result<(), E>
// or c_r_a_d_force, F: FnOnce(bool) -> filter     -> bool
//    c_r_a_d, filter      -> Result<(), E>
// compact_range_and_delete // delete any keys meeting a certain bound, IF there are not
// outstanding snapshots.
// compact_full // seems like I probably *won't* do this, in favor of a way to more easily
//   read a database with one set of settings+options and write it with a different set
//   of settings+options, **but the same comparator**. Yes, this comes with pretty poor
//   space efficiency (temporarily double the memory usage in size).
// has_outstanding_snapshots
// has_outstanding_iters
