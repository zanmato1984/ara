// TODO: The following case demonstrates the ultimate challenge to a pipeline model:
//        UnionAll
//           |
//      -----------
//      |         |
//     RJ2       RJ4
//      |         |
//    ------    ------
//    |    |    |    |
//   RJ1   S3  RJ3   S6
//    |         |
//  ------    ------
//  |    |    |    |
//  S1   S2   S4   S5
//
// The pipelines would be:
// P1:
// S2 -> Build1
// P2:
// S3 -> Build2
// P3:
// S5 -> Build3
// P4:
// S6 -> Build4
// P5: 
// S1 -> Probe1 -> Probe2 -|
//                         |- UnionAll
// S4 -> Probe3 -> Probe4 _|
// 
// The pipeline dependency dag would be:
//     P5
//     |
// ----------
// |  |  |  |
// P1 P2 P3 P4
//
// Challenges:
// 1. P1 ~ P4 are independent pipelines, so a staged plan execution model would need to consider if to parallel them.
// 2. P5 has two source paths, so a pipeline execution model would need to consider if to parallel them.
// 3. Each RJ (RightJoin) probe needs a scan pass of the hash table to produce unmatched batches when the source is fully drained.
// 4. Each source path contains multiple chained RJ probes whose scan passes need to be executed in sequence.