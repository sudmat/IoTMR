import zmq


class OutChannel:

    def __init__(self, target, timeout=10000):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.RCVTIMEO = timeout
        socket.connect("tcp://%s" % target)
        self.socket = socket

    def send_msg(self, msg):
        self.socket.send_json(msg)
        rep = self.socket.recv_json()
        return rep

class InChannel:

    def __init__(self, port):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % port)
        self.socket = socket

    def recv_msg(self, block=True):
        if block:
            msg = self.socket.recv_json()
        else:
            try:
                msg = self.socket.recv_json(flags=zmq.NOBLOCK)
            except zmq.Again:
                return None
        return msg

    def respond(self, rep):
        self.socket.send_json(rep)

if __name__ == '__main__':

    import time
    c = InChannel(5000)
    while 1:
        m = c.recv_msg(block=True)
        print(m)
        time.sleep(0.5)