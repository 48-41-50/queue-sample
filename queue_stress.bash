#! /usr/bin/env bash

docker-compose up -d server

sleep 1

export PUBLISHER_URL="http://localhost:8888"

# topic1
python3 ./http/queue_pub_stress.py topic1 create &
sleep 0.25
python3 ./http/queue_sub_stress.py topic1 & 
python3 ./http/queue_pub_stress.py topic1 &
sleep 0.25
python3 ./http/queue_sub_stress.py topic1 & 
python3 ./http/queue_sub_stress.py topic1 & 

# topic2
python3 ./http/queue_pub_stress.py topic2 create &
sleep 0.25
python3 ./http/queue_sub_stress.py topic2 & 
python3 ./http/queue_sub_stress.py topic2 & 
python3 ./http/queue_sub_stress.py topic2 & 
python3 ./http/queue_sub_stress.py topic2 & 
python3 ./http/queue_sub_stress.py topic2 & 

# topic3
python3 ./http/queue_pub_stress.py topic3 create &
sleep 0.25
python3 ./http/queue_pub_stress.py topic3 & 
sleep 0.25
python3 ./http/queue_pub_stress.py topic3 &
sleep 0.25
python3 ./http/queue_pub_stress.py topic3 & 
sleep 0.25
python3 ./http/queue_sub_stress.py topic3 & 
python3 ./http/queue_sub_stress.py topic3 & 

echo "Waiting on all jobs to complete"
wait
