#!/usr/bin/env bash

#
# basic map-reduce test
#

#RACE=

# comment this to run the tests without the Go race detector.
RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
#(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output （通过串形方式计算稳定结果最为对照组）
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

# 启动协调者，存储任务信息（pg*txt）
timeout -k 2s 180s ../mrcoordinator ../pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# 启动三个Worker座位Mapper和Reducer的资源。
# start multiple workers.
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &

# wait for the coordinator to exit.
wait $pid

# 结果校验
# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

##########################################################
## now indexer
#rm -f mr-*
#
## generate the correct output
#../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
#sort mr-out-0 > mr-correct-indexer.txt
#rm -f mr-out*
#
#echo '***' Starting indexer test.
#
#timeout -k 2s 180s ../mrcoordinator ../pg*txt &
#sleep 1
#
## start multiple workers
#timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so &
#timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so
#
#sort mr-out* | grep . > mr-indexer-all
#if cmp mr-indexer-all mr-correct-indexer.txt
#then
#  echo '---' indexer test: PASS
#else
#  echo '---' indexer output is not the same as mr-correct-indexer.txt
#  echo '---' indexer test: FAIL
#  failed_any=1
#fi
#
#wait

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
