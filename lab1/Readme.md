# Lab 1
## Introduction

In this lab you'll build a MapReduce system. 
You'll implement a worker process that calls application Map and Reduce functions and handles reading and writing files, and a coordinator process that
hands out tasks to workers and copes with failed workers. You'll be building something similar to the MapReduce paper. (Note: the lab uses "coordinator" instead of the paper's "master".)

## Getting started

You need to setup Go to the labs and run the test-mr.sh in src/main

    $ bash test-mr.sh 

When you've finished, the test script output should look like this:

    $ bash test-mr.sh
    *** Starting wc test.
    --- wc test: PASS
    *** Starting indexer test.
    --- indexer test: PASS
    *** Starting map parallelism test.
    --- map parallelism test: PASS
    *** Starting reduce parallelism test.
    --- reduce parallelism test: PASS
    *** Starting crash test.
    --- crash test: PASS
    *** PASSED ALL TESTS
