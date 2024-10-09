
import time
from functools import partial
import sys


from raft_pysync import raft_pysync_obj
from raft_pysync.decorators import replicated

class TestObj(raft_pysync_obj.RaftPysyncObject):

    def __init__(self, self_node_addr, other_node_addrs):
        print("TestObj init")
        print("self_node_addr:", self_node_addr)
        print("other_node_addrs:", other_node_addrs)
        super(TestObj, self).__init__(self_node_addr, other_node_addrs)
        self.__counter = 0

    @replicated
    def incCounter(self):
        self.__counter += 1
        return self.__counter


    @replicated
    def addValue(self, value, cn):
        self.__counter += value
        return self.__counter, cn


    def getCounter(self):
        return self.__counter


def onAdd(res, err, cnt):
    print("onAdd %d:" % cnt, res, err)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: %s self_port partner1_port partner2_port ..." % sys.argv[0])
        sys.exit(-1)

    port = int(sys.argv[1])
    partners = ["localhost:%d" % int(p) for p in sys.argv[2:]]
    o = TestObj("localhost:%d" % port, partners)
    n = 0
    old_value = -1
    while True:
        # time.sleep(0.005)
        time.sleep(0.5)
        if o.getCounter() != old_value:
            old_value = o.getCounter()
        if o.get_leader() is None:
            continue
        # if n < 2000:
        if n < 20:
            o.addValue(10, n, callback=partial(onAdd, cnt=n))
        n += 1
