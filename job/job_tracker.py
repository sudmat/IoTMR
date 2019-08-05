from kazoo.recipe.watchers import DataWatch, ChildrenWatch
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
import json
from .task import MapTask, ReduceTask
import dill
import codecs
from communication.channel import OutChannel


class JobTracker:

    def __init__(self, job, zk_addr):
        self.job = job
        self.job_id = None
        self.zk = KazooClient(zk_addr)
        self.status = 0
        self.cur_task = 0
        self.cur_task_id = None
        self.cur_wait = []
        self.upstream_task_id = -1

    def start_job(self):

        self.zk.start()
        job_id = self.zk.create('MR/job/job_', sequence=True, makepath=True)
        job_id = job_id.split('/')[-1]
        self.job_id = job_id

        print('job starting. job_id=%s'%job_id)

        worker_watcher = ChildrenWatch(self.zk, 'MR/worker', self.remove_from_wait)
        self.next_task()

    def next_task(self):

        task_id = 'task_%02d'%self.cur_task

        print('executing task: %s'%task_id)

        self.zk.create('MR/job/%s/%s'%(self.job_id, task_id))
        self.cur_task_id = task_id
        self.zk.create('MR/job/%s/%s/wait' % (self.job_id, task_id))
        self.zk.create('MR/job/%s/%s/finish' % (self.job_id, task_id))

        task_func = self.job['tasks'][self.cur_task][1]
        if self.cur_task == 0:
            workers = self.zk.get_children('MR/data/%s'%self.job['target_data'])
            task = MapTask(job_id=self.job_id, task_id=task_id, upstream_task_id=-1,
                           map_func=task_func, data_proc=self.job['data_proc'], n_partition=self.job['n_partition'])
            for worker in workers:
                self.dispatch_task(worker=worker, task=task)
        else:
            if self.job['tasks'][self.cur_task][0] == 'map':
                workers = self.zk.get_children('MR/job/%s/%s/finish' % (self.job_id, self.upstream_task_id))
                task = MapTask(job_id=self.job_id, task_id=task_id, upstream_task_id=self.upstream_task_id,
                               map_func=task_func, data_proc=self.job['data_proc'], n_partition=self.job['n_partition'])
                for worker in workers:
                    self.dispatch_task(worker=worker, task=task)
            else:
                worker_partiton = self.select_reduce_worker()
                mappers = self.zk.get_children('MR/job/%s/%s/finish' % (self.job_id, self.upstream_task_id))
                mappers_addr = []
                for mapper in mappers:
                    info = self.zk.get('MR/worker/%s' % mapper)[0].decode()
                    info = json.loads(info)
                    mappers_addr.append('%s:%s' % (info['ip'], info['port']))
                for p, worker in worker_partiton.items():
                    task = ReduceTask(job_id=self.job_id, task_id=task_id, upstream_task_id=self.upstream_task_id,
                                   reduce_func=task_func, mappers=mappers_addr, partition_id=0)
                    self.dispatch_task(worker=worker, task=task)

        finish_watcher = ChildrenWatch(self.zk, 'MR/job/%s/%s/finish' % (self.job_id, task_id), self.check_task)

    def dispatch_task(self, worker, task):

        print('dispatching task to %s. task_info: %s'%(worker, task.info()))

        pickled = codecs.encode(dill.dumps(task), "base64").decode()
        info = self.zk.get('MR/worker/%s' % worker)[0].decode()
        info = json.loads(info)
        outcha = OutChannel('%s:%s' % (info['ip'], info['port']))
        msg = {'type': 'execute_task', 'task': pickled}
        outcha.send_msg(json.dumps(msg))
        self.cur_wait.append(worker)
        self.zk.create('MR/job/%s/%s/wait/%s' % (self.job_id, self.cur_task_id, worker))

    def select_reduce_worker(self):
        workers = self.zk.get_children('MR/worker')
        worker_partition = {}
        for i in range(self.job['n_partition']):
            worker_partition[i] = workers[i%len(workers)]
        return worker_partition

    def check_task(self, finish):
        wait = self.zk.get_children('MR/job/%s/%s/wait'%(self.job_id, self.cur_task_id))
        if set(wait) == set(finish):
            self.on_task_finish()

    def remove_from_wait(self, workers):
        for worker in set(self.cur_wait)-set(workers):
            try:
                self.zk.delete('MR/job/%s/%s/wait/%s'%(self.job_id, self.cur_task_id, worker))
            except NoNodeError:
                pass

    def on_task_finish(self):

        print('task finish. task_id=%s'%self.cur_task_id)

        if self.cur_task == len(self.job['tasks'])-1:
            self.on_job_finish()
        else:
            self.cur_task += 1
            self.upstream_task_id = self.cur_task_id
            self.cur_task_id = None
            self.cur_wait = []
            self.next_task()

    def on_job_finish(self):
        print('job finish. job_id=%s'%self.job_id)

    def collect_result(self):
        pass