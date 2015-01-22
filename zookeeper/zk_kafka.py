__author__ = 'aouyang1'

import zc.zk

zk = zc.zk.ZooKeeper("localhost:2181")

try:
    zk.delete_recursive("brokers/topics/94306")
except:
    pass

try:
    zk.delete_recursive("brokers/topics/94303")
except:
    pass

try:
    zk.delete_recursive("brokers/topics/94301")
except:
    pass

try:
    zk.delete_recursive("consumers/puppy-love")
except:
    pass

try:
    zk.delete_recursive("consumers/puppy_group")
except:
    pass

try:
    zk.delete_recursive("config/topics/94306")
except:
    pass

try:
    zk.delete_recursive("config/topics/94303")
except:
    pass

try:
    zk.delete_recursive("config/topics/94301")
except:
    pass


zk.print_tree()