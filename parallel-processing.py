import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Pool
import sys
from time import time

"""
~~ This is a framework for implementing parallel processing in Python ~~

To Use:
1) Update all TODO items respective to your task
2) Select the best multiprocessing method for the task
3) Benchmark in the command line by using the system argument "benchmark"
    python parallel_processing.py benchmark
4) Run in the command line by initalizing the number of bots (example: 8)
    python parallel_processing.py 8
"""

# TODO remove datasetSize
datasetSize = 100000000

class BotNet:
    def __init__(self, bots):
        t0 = time()
        # TODO update task
        task = range(0, datasetSize) 
        workloadInterval = round(len(task) / bots)
        location = 0
        futures = []
        # TODO select the multiprocessing method
        with Pool() as executor: # async - fastest on large datasets
        # with ProcessPoolExecutor(max_workers=8) as executor: # async - good for large datasets and best if needing futures
        # with ThreadPoolExecutor(max_workers=8) as executor: # sequential - fastest on small datasets
            workloads = []
            for idx in range(bots):
                if idx < bots - 1:
                    workload = task[location:location + workloadInterval]
                    location += workloadInterval
                else:
                    workload = task[location:]
                workloads.append(workload)
                # print('Bot {i} (workload: {wl}) ready'.format(i=idx + 1, wl=str(len(workload))))
            futures.append(executor.map(Bot, workloads))
        print('Total elapsed time: ' + str(time() - t0))

class Bot:
    def __init__(self, workload):
        self.workload = workload
        # print('Bot pid: ' + str(os.getpid()) + ' spawned')
        self.work()

    # TODO update work
    def work(self):
        done = []
        for workitem in self.workload:
            done.append(workitem)
            # print(workitem)

def benchmark():
    t0 = time()
    task = range(0, datasetSize)
    Bot(task)
    print('Benchmark time: ' + str(time() - t0))


if __name__=='__main__':
    if sys.argv[1] == 'benchmark':
        benchmark()
    else:
        bots = int(sys.argv[1])
        BotNet(bots)
