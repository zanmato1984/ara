// TODO: Case about operator chaining in a pipeline.
// NOTE: How to implement hash join probe operator in a parallel-agnostic and future-agnostic fashion.  That is:
// 1. How to invoke and chain the `Stream()` methods of all the operators in this pipeline.
// 2. How to invoke the `Finish()` methods of all the operators in this pipeline.
// This is critical for cases like a pipeline having two hash right outer join probe operators,
// the `Finish()` method of each will scan the hash table and produce non-joined rows.
// 3. How to turn the sync `Stream()` and `Finish()` methods to future.

// TODO: Case about operator ping-pong between RUNNING and SPILLING states.