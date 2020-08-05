import os
import signal
import sys
import time
import subprocess
import glob
from os.path import expanduser
from util_serilize import *
from termcolor import colored
import datetime
from multiprocessing.dummy import Pool as ThreadPool
import threading

home = expanduser("~")

# non-blocking or blocking actually depends on whether cmd is bg or fg
def blocking_run(cmd):
    ret = subprocess.check_output(['/bin/bash', '-c', cmd], universal_newlines=True)
    # ret = subprocess.check_output(['/bin/bash', '-c', cmd], stderr=subprocess.STDOUT, universal_newlines=True)
    return str(ret)

# always non-blocking, as it is running in a subprocess.
def non_blocking_run(cmd):
    subprocess.Popen(['/bin/bash', '-c', cmd])

# def exe_gem5_sim(cmd_line):
#     try:
#         print(f'{threading.currentThread().getName()} running: {cmd_line}', flush=True)
#         os.popen(cmd_line).read()
#         print(f'{threading.currentThread().getName()} okay: {cmd_line}', flush=True)
#         return f'okay: {cmd_line}'
#     except Exception:
#         print(f'{threading.currentThread().getName()} fails: {cmd_line}', flush=True)
#         return f'fails: {cmd_line}'

# def run_gem5_sim(commands):
#     # 1 thread is left.
#     pool = ThreadPool(64)
#     results = pool.map(exe_gem5_sim, commands)
#     pool.close()
#     pool.join()
#     for res in results:
#         print(res)

if __name__ == "__main__":
    database = 'tpcds10'
    toStorage = 'alluxio://node0:19998'
    # 'hdfs://node0:9000'

    if len(sys.argv) == 3:
        database = sys.argv[1]
        if sys.argv[2] == 'alluxio':
            toStorage = 'alluxio://node0:19998'
        elif sys.argv[2] == 'hdfs':
            toStorage = 'hdfs://node0:9000'
        else:
            print('Usage: python hdfs2alluxio.py tpcds_bin_partitioned_orc_2 alluxio/hive')
            sys.exit(0)

    print(database, toStorage)

    cmd = f'~/spark-2.4.5-bin-hadoop2.7/bin/spark-sql --database {database} -e "show tables;"'
    tables = blocking_run(cmd).strip().split()[1::3]
    print(tables)
    sql = open('./log/transfer.sql', 'w')
    for table in tables:
        cmd = f'ALTER TABLE {table} SET LOCATION "{toStorage}/user/hive/warehouse/{database}.db/{table}";'
        sql.write(cmd + '\n')
        # cmd = f'~/spark-2.4.5-bin-hadoop2.7/bin/spark-sql --database {database} -e "show partitions {table};"'
        # try:
        #     partitions = blocking_run(cmd).strip().split()
        # except Exception:
        #     continue
        # partitions = list(filter(lambda x: '=' in x, partitions))
        # print(partitions)
        # for partition in partitions:
        #     partition_str = partition.replace('__HIVE_DEFAULT_PARTITION__', '"__HIVE_DEFAULT_PARTITION__"')
        #     cmd = f'ALTER TABLE {table} partition ({partition_str}) SET LOCATION "{toStorage}/user/hive/warehouse/{database}.db/{table}/{partition}";'
        #     sql.write(cmd + '\n')
    sql.close()
    cmd = f'~/spark-2.4.5-bin-hadoop2.7/bin/spark-sql --database {database} -f ./log/transfer.sql'    
    blocking_run(cmd)

