from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from communication.channel import InChannel
import json
from job.job_tracker import JobTracker
import dill
import codecs
from storage.file_io import read

class MasterNode:

    def __init__(self, config):
        self.config = config
        self.zk = KazooClient(hosts=config['zookeeper'])
        self.incha = InChannel(config['port'])
        self.job_trackers = {}

    def start(self):
        self.zk.start()
        self.create_path()

        print('master node started. port=%s'%self.config['port'])

        while 1:
            msg = self.incha.recv_msg()
            rep = self.handle_msg(msg)
            self.incha.respond(rep)

    def create_path(self):
        for item in ['master', 'worker', 'job', 'data']:
            try:
                self.zk.create('/MR/%s'%item, makepath=True)
            except NodeExistsError:
                pass

    def handle_msg(self, msg):
        req = json.loads(msg)
        rep = ''
        if req['type'] == 'new_job':
            job_id = self.register_job(job_req=req)
            rep = job_id
        if req['type'] == 'collect':
            if self.job_trackers[req['job_id']] != 1:
                rep = ''
            else:
                rep = self.collect(req['job_id'])
        return rep

    def register_job(self, job_req):
        unpickled = dill.loads(codecs.decode(job_req['job'].encode(), "base64"))
        tracker = JobTracker(job=unpickled,
                             zk_addr=self.config['zookeeper'])
        job_id = tracker.start_job()
        self.job_trackers[job_id] = tracker
        return job_id

    def collect(self, job_id):
        return read('%s/result'%job_id)

