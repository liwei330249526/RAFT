liwei@liwei-virtual-machine:~/books/raft-course/src/github.com/raft-course/src/raft$ go test -run PartA --race
Test (PartA): initial election ...
... Passed --   3.1  3   84   23518    0
Test (PartA): election after network failure ...
... Passed --   4.5  3  174   35164    0
Test (PartA): multiple elections ...
... Passed --   5.6  7  816  159918    0
PASS
ok      course/raft     13.233s
liwei@liwei-virtual-machine:~/books/raft-course/src/github.com/raft-course/src/raft$ go test -run PartB --race
Test (PartB): basic agreement ...
... Passed --   0.5  3   20    5228    3
Test (PartB): RPC byte count ...
... Passed --   1.8  3   52  115100   11
Test (PartB): test progressive failure of followers ...
... Passed --   4.4  3  164   34152    3
Test (PartB): test failure of leaders ...
... Passed --   4.8  3  242   53789    3
Test (PartB): agreement after follower reconnects ...
... Passed --   5.6  3  160   43184    8
Test (PartB): no agreement if too many followers disconnect ...
... Passed --   3.5  5  304   60450    4
Test (PartB): concurrent Start()s ...
... Passed --   0.7  3   24    6396    6
Test (PartB): rejoin of partitioned leader ...
... Passed --   4.0  3  178   42994    4
Test (PartB): leader backs up quickly over incorrect follower logs ...
... Passed --  22.7  5 2496 2047465  102
Test (PartB): RPC counts aren't too high ...
... Passed --   2.2  3   60   17216   12
PASS
ok      course/raft     50.133s
liwei@liwei-virtual-machine:~/books/raft-course/src/github.com/raft-course/src/raft$ go test -run PartC --race
Test (PartC): basic persistence ...
... Passed --   4.8  3  150   40157    7
Test (PartC): more persistence ...
... Passed --  16.9  5 1318  287467   17
Test (PartC): partitioned leader and one follower crash, leader restarts ...
... Passed --   1.2  3   41   10643    4
Test (PartC): Figure 8 ...
... Passed --  29.8  5 1556  306785   33
Test (PartC): unreliable agreement ...
... Passed --   4.3  5  232   85095  246
Test (PartC): Figure 8 (unreliable) ...
... Passed --  36.2  5 4316 8104662  376
Test (PartC): churn ...
... Passed --  16.2  5 1032  555799  371
Test (PartC): unreliable churn ...
... Passed --  16.5  5  959  596220  324
PASS
ok      course/raft     125.902s
liwei@liwei-virtual-machine:~/books/raft-course/src/github.com/raft-course/src/raft$ ../tools/dstest PartA -p 10 -n 10
Verbosity level set to 0
┏━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test  ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ PartA │      0 │    10 │ 14.31 ± 0.12 │
└───────┴────────┴───────┴──────────────┘
liwei@liwei-virtual-machine:~/books/raft-course/src/github.com/raft-course/src/raft$ ../tools/dstest PartB -p 10 -n 10
Verbosity level set to 0
┏━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test  ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ PartB │      0 │    10 │ 51.78 ± 1.48 │
└───────┴────────┴───────┴──────────────┘
liwei@liwei-virtual-machine:~/books/raft-course/src/github.com/raft-course/src/raft$ ../tools/dstest PartC -p 10 -n 10
Verbosity level set to 0
┏━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Test  ┃ Failed ┃ Total ┃          Time ┃
┡━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━┩
│ PartC │      0 │    10 │ 125.05 ± 4.00