import os

def _make_path(file):
    path = '/'.join(file.split('/')[: -1])
    if not os.path.exists(path):
        os.makedirs(path)

def files(path):
    return os.listdir(path)

def read(file):
    file = 'tmp/'+file
    _make_path(file)
    with open(file) as f:
        result = f.read()
    return result

def write(file, content):
    file = 'tmp/' + file
    _make_path(file)
    with open(file, 'w') as f:
        f.write(content)

def append(file, content):
    file = 'tmp/' + file
    _make_path(file)
    with open(file, 'a') as f:
        f.write('\n'+content)

