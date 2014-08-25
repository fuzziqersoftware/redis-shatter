#!/bin/bash

seq 1 8 | xargs -I {} -P 8 bash -c "cat redis.conf | sed s/@@REDIS_NUM@@/{}/g | redis-server -"
