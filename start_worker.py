from node.work_node import WorkNode
from configobj import ConfigObj


name = 'worker1'
config = ConfigObj('config/test.ini')
config = config[name]

mn = WorkNode(config)
mn.start()