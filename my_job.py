from job.client import MRJob


def map1(v):
    for w in v.split(' '):
        yield (w, 1)

def reduce(k, vs):
    yield k, sum(vs)

def data_processor():
    with open('test_data/words') as f:
        lines = f.readlines()
        for l in lines:
            l = l.replace(',', '')
            yield l


if __name__ == '__main__':

    job = MRJob(tasks=[('map', map1), ('reduce', reduce)], target_data='default', data_proc=data_processor)
    job_id = job.submit()