#!/bin/bash

./run_multiple_redis.sh &
RUN_MULTI_PID=$?
sleep 1

./redis-shatter --config-file=redis-shatter.conf --parallel=1 &
SHATTER_PID=$?
sleep 1

for TEST in *Test
do
    ./$TEST
    if [ "$?" != "0" ]
    then
        FAILURES_FOUND=1
    fi
done

kill -TERM $SHATTER_PID
# TODO: we should kill run_multiple_redis.sh directly too, and make it forward
# the signal to the redis procs
ps aux | grep redis-server | grep -v grep | grep -v xargs | awk '{print $2;}' | xargs kill -TERM

echo -e "\n\n\n"
if [ "$FAILURES_FOUND" == "1" ]
then
    echo "Some tests failed!"
    exit 1
else
    echo "All tests passed"
    exit 0
fi
