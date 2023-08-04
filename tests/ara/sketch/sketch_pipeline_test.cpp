// TODO: The following case demonstrates the ultimate challenges to a pipeline model:
//         UnionAll
//            |
//      -------------
//      |           |
//     RJ3         RJ6
//      |           |
//   -------     -------
//   |     |     |     |
//  RJ1   RJ2   RJ4   RJ5
//   |     |     |     |
//  ----  ----  ----  ----
//  |  |  |  |  |  |  |  |
//  S1 S2 S3 S4 S5 S6 S7 S8
//
// The pipelines would be:
// P1: S2 -> Build1
// P2: S4 -> Build2
// P3: S6 -> Build4
// P4: S8 -> Build5
// P5: S3 -> Probe2 -> Build3
// P6: S7 -> Probe5 -> Build6
// P7: S1 -> Probe1 -> Probe3 -|
//                             |-> UnionAll
//     S5 -> Probe4 -> Probe6 -|
// 
// The pipeline dependency dag would be:
//     P7
//     |
// ----------
// |  |  |  |
// P1 P5 P3 P6
//    |     |
//    P2    P4
// 
//
// Challenges:
// 1. P1 ~ P4 are independent pipelines, so a staged plan execution model would need to consider if to parallel them.
// 2. P5 and P6 have both join probe and build, this could imply some unknown challenges to a pipeline execution model.
// 3. P7 has two source paths, so a pipeline execution model would need to consider if to parallel them.
// 3. Each RJ (RightJoin) probe needs a scan pass of the hash table to produce unmatched batches when the source is fully drained.
// 4. Each source path contains multiple chained RJ probes whose scan passes need to be executed in sequence.