import os
import signal
import sys
import time
import subprocess
import glob
from os.path import expanduser
from termcolor import colored
import datetime
from multiprocessing.dummy import Pool as ThreadPool
import threading

home = expanduser("~")
cmd_run = {
    'run_query': home + '/spark-2.4.5-bin-hadoop2.7/bin/spark-submit run_queries.py {idx}',
}

# non-blocking or blocking actually depends on whether cmd is bg or fg
def blocking_run(cmd):
    print(cmd)
    ret = subprocess.check_output(['/bin/bash', '-c', cmd], stderr=subprocess.STDOUT, universal_newlines=True)
    return str(ret)

# always non-blocking, as it is running in a subprocess.
def non_blocking_run(cmd):
    print(cmd)
    subprocess.Popen(['/bin/bash', '-c', cmd], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

def run_query_batch(cmd):
    try:
        print(colored(f'Running ({threading.currentThread().getName()}): {cmd}', 'blue'))
        blocking_run(cmd)
        print(colored(f'Success: {cmd}', 'green'))
        return f'okay: {cmd}'
    except Exception:
        print(colored(f'Failure: {cmd} N/A', 'red'))
        return f'fails: {cmd}'
        
def run_query_batch_wrapper(cmds):
    pool = ThreadPool(4)
    results = pool.map(run_query_batch, cmds)
    pool.close()
    pool.join()

if __name__ == "__main__":
    cmds = []
    # for i in range(1):
    for i in range(99):
    # for i in range(99):
    # for i in [13, 22, 23, 38, 34, 63]:
        if i == 76:
            continue
        cmd = cmd_run['run_query'].format(idx=i)
        cmds.append(cmd)
    run_query_batch_wrapper(cmds)