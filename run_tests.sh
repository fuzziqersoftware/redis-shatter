#!/bin/bash

./run_multiple_redis.sh &
sleep 1
./redis-shatter --config-file=redis-shatter.conf --parallel=1 &
sleep 1

for TEST in *_test
do
    ./$TEST
    if [ "$?" != "0" ]
    then
        FAILURES_FOUND=1
    fi
done

ps aux | grep redis-shatter | grep -v grep | awk '{print $2;}' | xargs kill -TERM
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
