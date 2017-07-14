"""
Usage:
  bloom build (-w|-b) <source> <name>
  bloom search <source>

Options:
   --help               Show this screen.
   --version            Show the version.
   -w, --white-list     Add to white list.
   -b, --black-list     Add to black list.
"""
VERSION="0.1"

from docopt import docopt
import hashlib
from multiprocessing import Process, active_children, JoinableQueue, cpu_count, Pool
import os
from pybloom import BloomFilter
import time
from collections import defaultdict
import pickle
import datetime

def save(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

def hash_func(job):
    sha1 = hashlib.sha1()
    sha1.update(job[0])
    return [sha1.hexdigest(),job[1]]

def read_thread(source, data_queue):
    f = open(source, 'rb')
    block_size = 1048576 #1MB
    byte_position = 0
    data = " "
    while len(data) > 0:
        data = None
        try:
            data = f.read(block_size)
            if len(data) != 0:
                data_queue.put([data,byte_position])
            byte_position += block_size
        except IOError:
            print "IO ERROR"
            pass
    data_queue.put(False)

def hash_thread(data_queue, hash_queue):
    work = True
    while work:
        job = data_queue.get()
        if job:
            hashes = []
            for start in xrange(0, len(job[0]), 4096):
                sha1 = hashlib.sha1()
                sha1.update(job[0][start:start+4096])
                hashes.append([sha1.hexdigest(),job[1]+start])
            hash_queue.put(hashes)
            data_queue.task_done()
        else:
            data_queue.task_done()
            data_queue.put(False)
            hash_queue.put(False)
            work = False
    print "hash done"

def list_thread(item_name, hash_queue, active_list, white_list):
    work = True
    while work:
        job = hash_queue.get()
        if job:
            for hash_value in job:
                active_list[hash_value[0]].add(item_name)
            hash_queue.task_done()
        else:
            hash_queue.task_done()
            if white_list:
                save(active_list, 'white-list.b')
            else:
                save(active_list, 'black-list.b')
            work = False

def bloom_thread(hash_queue, results_queue, white_list, black_list):
    white_list_bloom = BloomFilter(capacity=len(white_list), error_rate=0.0001)
    black_list_bloom = BloomFilter(capacity=len(black_list), error_rate=0.0001)
    for item in white_list:
        white_list_bloom.add(item)
    for item in black_list:
        black_list_bloom.add(item)
    work = True
    while work:
        job = hash_queue.get()
        hash_queue.task_done()
        if job:
            for hash_value in job:
                #check if in bloom
                if hash_value[0] in white_list_bloom:
                    #print hash_value[1],
                    #print "white"
                    pass
                if hash_value[0] in black_list_bloom:
                    #print hash_value[0],
                    #print hash_value[1],
                    #print "black"
                    pass
        else:
            print "bloom done"
            hash_queue.put(False)
            work = False

def build(arguments, white_list, black_list):
    data_queue = JoinableQueue(maxsize=4096)
    hash_queue = JoinableQueue()

    Process(target = read_thread, args = (arguments['<source>'],data_queue)).start()
    for x in range(cpu_count()):
        Process(target = hash_thread, args = (data_queue, hash_queue)).start()

    if arguments['--white-list']:
        Process(target = list_thread,
                args = (arguments['<name>'], hash_queue, white_list, True)).start()
    elif arguments['--black-list']:
        Process(target = list_thread,
                args = (arguments['<name>'], hash_queue, black_list, False)).start()

    while len(active_children()) != 0:
        time.sleep(0.1) #wait for threads to end

def search(arguments, white_list, black_list):
    data_queue = JoinableQueue(maxsize=1024)
    hash_queue = JoinableQueue()
    results_queue = JoinableQueue()

    Process(target = read_thread, args = (arguments['<source>'],data_queue)).start()
    for x in range(2):
        Process(target = hash_thread, args = (data_queue, hash_queue)).start()
    for x in range(1):
        Process(target = bloom_thread, args = (hash_queue, results_queue, white_list, black_list)).start()

    while len(active_children()) != 0:
        print active_children()
        time.sleep(0.1) #wait for threads to end

if __name__ == '__main__':
    start = datetime.datetime.now()
    arguments = docopt(__doc__, version=VERSION)
    if os.path.isfile('white-list.b'):
        white_list = load('white-list.b')
    else:
        white_list = defaultdict(set)
    if os.path.isfile('black-list.b'):
        black_list = load('black-list.b')
    else:
        black_list = defaultdict(set)
    if arguments['build']:
        build(arguments, white_list, black_list)
    elif arguments['search']:
        search(arguments, white_list, black_list)
    end = datetime.datetime.now()
    print end - start
