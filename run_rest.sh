# /bin/bash

for i in 13 22 23 38 34 63; do 
    ~/spark-2.4.5-bin-hadoop2.7/bin/spark-submit run_queries.py $i &
done
wait