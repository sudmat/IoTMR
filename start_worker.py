from node.work_node import WorkNode
from configobj import ConfigObj
import sys

name = 'worker0'
if len(sys.argv) >= 2:
    name = sys.argv[1]
config = ConfigObj('config/test.ini')
config = config[name]

mn = WorkNode(config)
mn.start()