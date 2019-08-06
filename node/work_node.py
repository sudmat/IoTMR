from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from communication.channel import InChannel
import json
import dill
import codecs
from threading import Thread, Lock
import psutil
from storage.file_io import read
import time

mutex = Lock()

class WorkNode:

    def __init__(self, config):
        self.config = config
        self.zk = KazooClient(hosts=config['zookeeper'])
        self.incha = InChannel(config['port'])
        self.available_thread = config.get('threads', 5)

    def start(self):
        self.zk.start()
        self.register_self()

        print('work node started. port=%s' % self.config['port'])

        while 1:
            msg = self.incha.recv_msg()
            rep = self.handle_msg(msg)
            self.incha.respond(rep)

    def handle_msg(self, msg):
        req = json.loads(msg)
        rep = ''
        if req['type'] == 'execute_task':
            task = dill.loads(codecs.decode(req['task'].encode(), "base64"))
            rep = self.execute_task(task=task)
        if req['type'] == 'read_partition':
            data = self.read_partition(req['job_id'], req['task_id'], req['partition_id'])
            rep = {'data': data}
        if req['type'] == 'read_result':
            data = self.read_result(req['job_id'], req['task_id'])
            rep = {'data': data}
        if req['type'] == 'stats':
            rep = self.get_stats()
        return rep

    def execute_task(self, task):
        while self.available_thread <= 0:
            continue
        self.available_thread -= 1
        t = Thread(target=self._execute_task, args=(task, ))
        t.start()
        return 1

    def _execute_task(self, task):
        task.execute()
        self.zk.create('MR/job/%s/%s/finish/%s' % (task.job_id, task.task_id, self.config['name']), ephemeral=True)
        self.zk.set('MR/job/%s/%s/finish/%s' % (task.job_id, task.task_id, self.config['name']),
                    json.dumps(task.stats).encode())
        mutex.acquire()
        self.available_thread += 1
        mutex.release()

    def register_self(self):
        path1 = '/MR/worker/%s' % self.config['name']
        path2 = '/MR/data/%s/%s' % (self.config.get('data', 'default'), self.config['name'])
        for path in [path1, path2]:
            try:
                self.zk.create(path, makepath=True, ephemeral=True)
            except NodeExistsError:
                time.sleep(3)
                self.zk.create(path, makepath=True, ephemeral=True)
        value = {'ip': self.config['ip'], 'port': self.config['port']}
        self.zk.set(path1, json.dumps(value).encode())

    @staticmethod
    def read_partition(job_id, task_id, partition_id):
        data = read('%s/%s/%s'%(job_id, task_id, partition_id))
        return data

    @staticmethod
    def read_result(job_id, task_id):
        data = read('%s/%s/result' % (job_id, task_id))
        return data

    @staticmethod
    def get_stats():
        cpu = psutil.cpu_percent(1)
        phymem = psutil.virtual_memory()
        mem_used = int(phymem.used / 1024 / 1024)
        mem_total = int(phymem.used / 1024 / 1024)
        mem_left = mem_total-mem_used
        return {'cpu': cpu, 'mem_left': mem_left}

    def handle_task(self):
        pass