# bin/bash

for i in `seq 1 1 4`; do
    host=node$i
    scp $USER@$host:~/alluxio/logs/blocktraces.log ./log/blocktrace$i.log
done