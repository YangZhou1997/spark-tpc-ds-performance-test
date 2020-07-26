from util_serilize import *
from collections import OrderedDict

blockId2file = {}
# time -> event (readfromUfs, writetoUfs, evictBlock)
trimmedLogs = OrderedDict()

blockIdStack = []
filepathStack = []

def parsing_asyncCache(lines):
    line = lines[0]
    time = line[:line.find(' INFO')]
    blockId = lines[0].split()[-1]
    for i, line in enumerate(lines):
        if 'ufs_path:' in line:
            filepath = line.strip().split()[-1].strip('"')
            blockId2file[blockId] = filepath
            logMsg = f'readfromUfs {filepath}'
            trimmedLogs[time] = logMsg
            print(logMsg)
        if len(line) == 0:
            return i
    return i

def parsing_remove(lines):
    line = lines[0]
    time = line[:line.find(' INFO')]
    blockId = lines[0].split()[-1]
    try: 
        filepath = blockId2file[blockId]
    except Exception:
        filepath = 'unknown'
        print(f'parsing_remove: blockId {blockId} not found filepath')
        # trying evicting a block with not filepath mapping, eviction fails
        # assert(False)
        return 0
    logMsg = f'evictBlock {filepath}'
    trimmedLogs[time] = logMsg
    print(logMsg)
    return 0

def get_real_filepath(filepath):
    s = filepath.find('.parquet/') + len('.parquet/')
    e = filepath.find('part-')
    return filepath[:s] + filepath[e:]

def parsing_ShortCircuitBlockWriteHandler(lines):
    line = lines[0]
    time = line[:line.find(' INFO')]
    blockId = lines[0].split()[-1]
    if len(filepathStack):
        filepath = filepathStack.pop()
        blockId2file[blockId] = filepath
        logMsg = f'writetoUfs {filepath}'
        trimmedLogs[time] = logMsg
        print(logMsg)
    else:
        blockIdStack.append(blockId)

    for i, line in enumerate(lines):
        if len(line) == 0:
            return i
    return i

def parsing_DelegationWriteHandler(lines):
    line = lines[0]
    time = line[:line.find(' INFO')]
    for i, line in enumerate(lines):
        if 'ufs_path:' in line:
            filepath = line.strip().split()[-1].strip('"')
            if '_temporary' in filepath:
                filepath = get_real_filepath(filepath)
                if len(blockIdStack):
                    blockId = blockIdStack.pop()
                    blockId2file[blockId] = filepath
                    logMsg = f'writetoUfs {filepath}'
                    trimmedLogs[time] = logMsg
                    print(logMsg)
                else:
                    filepathStack.append(filepath)
        if len(line) == 0:
            return i
    return i

if __name__ == "__main__":
    for i in range(4):
        blockIdStack.clear()
        filepathStack.clear()

        filepath = f'log/blocktrace{i+1}.log'
        lines = open(filepath, 'r').read().strip().split('\n')
        # for i, line in enumerate(lines):
        for i in range(0, len(lines)):
            line = lines[i]
            if 'blocktracesLogger' in line:
                if 'asyncCache' in line:
                    i += parsing_asyncCache(lines[i:])
                if 'remove' in line:
                    i += parsing_remove(lines[i:])
                if 'ShortCircuitBlockWriteHandler' in line:
                    i += parsing_ShortCircuitBlockWriteHandler(lines[i:])
                if 'DelegationWriteHandler' in line:
                    i += parsing_DelegationWriteHandler(lines[i:])


    write_to_file(trimmedLogs, 'log/trimed.log')



