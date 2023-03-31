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
// Explain:
// 1. The top UnionAll operators introduces a pipeline with two parallel sources.
// 2. RJs (RightJoins) introduce 