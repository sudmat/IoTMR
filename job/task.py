from storage.file_io import write, append, files
from collections import defaultdict
from communication.channel import OutChannel
import json
import time

class MapTask:

    name = 'map'
    max_buffer_size=1000

    def __init__(self, job_id, task_id, upstream_task_id,
                 map_func, n_partition=None, data_proc=None, partition_func=None):
        self.job_id = job_id
        self.task_id = task_id
        self.upstream_task_id = upstream_task_id
        self.map_func = map_func
        self.data_proc = data_proc or self.partition_func
        self.n_partition = n_partition or 1
        self.partition_func = partition_func or self.partition_func
        self.buffer_size = 0
        self.data = defaultdict(list)
        self._overwrite = True
        self.stats = {}

    def info(self):
        return 'task_name=%s, job_id=%s, task_id=%s, upstream_task_id=%s, ' \
               'n_partition=%s' % (self.name, self.job_id, self.task_id, self.upstream_task_id, self.n_partition)

    def execute(self):
        # time.sleep(10)
        print('start executing map task. task_info: %s'%self.info())
        for item in self.data_proc():
            for k, v in self.map_func(item):
                self.buffer_size += 1
                par = self.partition_func(k)
                self.data[par].append('%s,%s'%(k, v))
            if self.buffer_size > self.max_buffer_size:
                self.flush()
        self.flush()
        print('finish executing map task. task_info: %s' % self.info())

    def flush(self):
        for p, paris in self.data.items():
            if p in self.stats:
                self.stats[p] += len(paris)
            else:
                self.stats[p] = len(paris)
            update_func = write if self._overwrite else append
            update_func('%s/%s/partition_%s'%(self.job_id, self.task_id, p), '\n'.join(paris))
        self.buffer_size = 0
        self._overwrite = False
        self.data = defaultdict(list)

    def partition_func(self, k):
        return hash(k) % self.n_partition

    def data_proc(self):
        for file in files('%s/%s'%(self.job_id, self.upstream_task_id)):
            with open(file) as f:
                for line in f.readlines():
                    yield line



class ReduceTask:

    name = 'reduce'

    def __init__(self, job_id, task_id, upstream_task_id, reduce_func, mappers, partition_id):
        self.job_id = job_id
        self.task_id = task_id
        self.upstream_task_id = upstream_task_id
        self.reduce_func = reduce_func
        self.mappers = mappers
        self.partition_id = partition_id
        self.data = []
        self.result = []
        self.stats = {}

    def info(self):
        return 'task_name=%s, job_id=%s, task_id=%s, upstream_task_id=%s, partition_id=%s, mappers=%s'%(
            self.name, self.job_id, self.task_id, self.upstream_task_id, self.partition_id, self.mappers)

    def execute(self):

        print('start executing reduce task. task_info=%s' % self.info())
        self.collect_map_result()
        self.sort()
        cur_k = None
        cur_values = []
        for item in self.data:
            k, v = item[0], float(item[1])
            if k != cur_k:
                if cur_k is not None:
                    for reduced_k, reduced_v in self.reduce_func(cur_k, cur_values):
                        self.result.append('%s, %s'%(reduced_k, reduced_v))
                    cur_values = []
                cur_k = k
                cur_values.append(v)
            else:
                cur_k = k
                cur_values.append(v)
        if cur_k is not None:
            for reduced_k, reduced_v in self.reduce_func(cur_k, cur_values):
                self.result.append('%s, %s' % (reduced_k, reduced_v))
        self.flush()
        print('start executing reduce task. task_info=%s' % self.info())

    def collect_map_result(self):

        for target in self.mappers:

            print('start reading data from mapper=%s'%target)

            msg = {'type': 'read_partition', 'task_id': self.upstream_task_id,
                   'partition_id': 'partition_%s'%self.partition_id, 'job_id': self.job_id}
            outcha = OutChannel(target)
            data = outcha.send_msg(json.dumps(msg))
            data = [item.split(',') for item in data['data'].split('\n')]
            self.data += data

            print('finish reading data from mapper=%s' % target)


    def sort(self):
        self.data = sorted(self.data, key=lambda x: x[0])

    def flush(self):
        write('%s/%s/result'%(self.job_id, self.task_id), '\n'.join(self.result))


