from node.master_node import MasterNode
from configobj import ConfigObj

config = ConfigObj('config/test.ini')
config = config['master']

mn = MasterNode(config)
mn.start()