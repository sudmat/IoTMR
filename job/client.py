from communication.channel import OutChannel
import pickle
import json
import codecs
import dill

def map1(v):
    yield 'x', 1

def reduce(k, v):
    yield 'x', 1

class MRJob:

    n_partition = 1

    def __init__(self, target_data='', master='127.0.0.1:5000', tasks=None, data_proc=None):
        self.target_data = target_data
        self.outcha = OutChannel(target=master)
        self.tasks = tasks or []
        self.data_proc = data_proc

    def lineage(self):
        return []

    def process_data(self):
        return []

    def store_map_result(self):
        pass

    def map1(self, v):
        pass

    def reduce(self, k, vs):
        pass

    def submit(self):
        jobstr = self.serialize()
        msg = {'type': 'new_job', 'job': jobstr}
        rep = self.outcha.send_msg(json.dumps(msg))
        return rep

    def serialize(self):

        job = {
            'target_data': self.target_data,
            'data_proc': self.data_proc,
            'tasks': self.tasks,
            'n_partition': self.n_partition
        }

        pickled = codecs.encode(dill.dumps(job), "base64").decode()
        # unpickled = dill.loads(codecs.decode(pickled.encode(), "base64"))
        return pickled
