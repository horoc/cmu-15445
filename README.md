# CMU-15445 Database Systems (2022 Fall)

Course Home Page : https://15445.courses.cs.cmu.edu/fall2022/

# Homework

Some SQL practies, SKIP

# Project

https://15445.courses.cs.cmu.edu/fall2022/assignments.html

## Roadmaps

- [x] C++ Primer
- [x] Buffer Pool Manager
- [x] B+Tree Index
  - [x] Query
  - [x] Insert
  - [x] Iterator
  - [x] Delete
  - [x] Concurrency
- [ ] Query Execution
- [ ] Concurrency Control

## Testing Report

### C+ Primer

starter_trie_test

```
[==========] Running 5 tests from 2 test suites.
[----------] Global test environment set-up.
[----------] 3 tests from StarterTest
[ RUN      ] StarterTest.TrieNodeInsertTest
[       OK ] StarterTest.TrieNodeInsertTest (0 ms)
[ RUN      ] StarterTest.TrieNodeRemoveTest
[       OK ] StarterTest.TrieNodeRemoveTest (0 ms)
[ RUN      ] StarterTest.TrieInsertTest
[       OK ] StarterTest.TrieInsertTest (0 ms)
[----------] 3 tests from StarterTest (0 ms total)

[----------] 2 tests from StarterTrieTest
[ RUN      ] StarterTrieTest.RemoveTest
[       OK ] StarterTrieTest.RemoveTest (0 ms)
[ RUN      ] StarterTrieTest.ConcurrentTest1
[       OK ] StarterTrieTest.ConcurrentTest1 (164 ms)
[----------] 2 tests from StarterTrieTest (164 ms total)

[----------] Global test environment tear-down
[==========] 5 tests from 2 test suites ran. (164 ms total)
[  PASSED  ] 5 tests.

```

### Buffer Pool Manager

extendible_hash_table_test

```
[==========] Running 2 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 2 tests from ExtendibleHashTableTest
[ RUN      ] ExtendibleHashTableTest.SampleTest
[       OK ] ExtendibleHashTableTest.SampleTest (0 ms)
[ RUN      ] ExtendibleHashTableTest.ConcurrentInsertTest
[       OK ] ExtendibleHashTableTest.ConcurrentInsertTest (5 ms)
[----------] 2 tests from ExtendibleHashTableTest (5 ms total)

[----------] Global test environment tear-down
[==========] 2 tests from 1 test suite ran. (5 ms total)
[  PASSED  ] 2 tests.

```

lru_k_replacer_test

```
[==========] Running 1 test from 1 test suite.
[----------] Global test environment set-up.
[----------] 1 test from LRUKReplacerTest
[ RUN      ] LRUKReplacerTest.SampleTest
[       OK ] LRUKReplacerTest.SampleTest (0 ms)
[----------] 1 test from LRUKReplacerTest (0 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (0 ms total)
[  PASSED  ] 1 test.
```

buffer_pool_manager_instance_test

```
[==========] Running 2 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 2 tests from BufferPoolManagerInstanceTest
[ RUN      ] BufferPoolManagerInstanceTest.BinaryDataTest
[       OK ] BufferPoolManagerInstanceTest.BinaryDataTest (1 ms)
[ RUN      ] BufferPoolManagerInstanceTest.SampleTest
[       OK ] BufferPoolManagerInstanceTest.SampleTest (0 ms)
[----------] 2 tests from BufferPoolManagerInstanceTest (2 ms total)

[----------] Global test environment tear-down
[==========] 2 tests from 1 test suite ran. (2 ms total)
[  PASSED  ] 2 tests.
```

### B+Tree Index

b_plus_tree_insert_test 

```
[==========] Running 3 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 3 tests from BPlusTreeTests
[ RUN      ] BPlusTreeTests.InsertTest1
[       OK ] BPlusTreeTests.InsertTest1 (1 ms)
[ RUN      ] BPlusTreeTests.InsertTest2
[       OK ] BPlusTreeTests.InsertTest2 (0 ms)
[ RUN      ] BPlusTreeTests.InsertTest3
[       OK ] BPlusTreeTests.InsertTest3 (0 ms)
[----------] 3 tests from BPlusTreeTests (3 ms total)

[----------] Global test environment tear-down
[==========] 3 tests from 1 test suite ran. (3 ms total)
[  PASSED  ] 3 tests.
```

b_plus_tree_delete_test

```
[==========] Running 2 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 2 tests from BPlusTreeTests
[ RUN      ] BPlusTreeTests.DeleteTest1
[       OK ] BPlusTreeTests.DeleteTest1 (1 ms)
[ RUN      ] BPlusTreeTests.DeleteTest2
[       OK ] BPlusTreeTests.DeleteTest2 (0 ms)
[----------] 2 tests from BPlusTreeTests (2 ms total)

[----------] Global test environment tear-down
[==========] 2 tests from 1 test suite ran. (2 ms total)
[  PASSED  ] 2 tests.
```

b_plus_tree_concurrent_test

```
[==========] Running 5 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 5 tests from BPlusTreeConcurrentTest
[ RUN      ] BPlusTreeConcurrentTest.InsertTest1
[       OK ] BPlusTreeConcurrentTest.InsertTest1 (4 ms)
[ RUN      ] BPlusTreeConcurrentTest.InsertTest2
[       OK ] BPlusTreeConcurrentTest.InsertTest2 (2 ms)
[ RUN      ] BPlusTreeConcurrentTest.DeleteTest1
[       OK ] BPlusTreeConcurrentTest.DeleteTest1 (1 ms)
[ RUN      ] BPlusTreeConcurrentTest.DeleteTest2
[       OK ] BPlusTreeConcurrentTest.DeleteTest2 (1 ms)
[ RUN      ] BPlusTreeConcurrentTest.MixTest
[       OK ] BPlusTreeConcurrentTest.MixTest (0 ms)
[----------] 5 tests from BPlusTreeConcurrentTest (9 ms total)

[----------] Global test environment tear-down
[==========] 5 tests from 1 test suite ran. (10 ms total)
[  PASSED  ] 5 tests.
```
