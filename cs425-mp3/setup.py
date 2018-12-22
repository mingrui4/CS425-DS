import logging
import os
import sys
from master import Master
from node import Node
from client import Client
from server import *

if __name__ == '__main__':
    logging.basicConfig(filename='mp3.log', level=logging.INFO, filemode='w',format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level=logging.DEBUG)
    logging.getLogger("").addHandler(stream_handler)

    os.system("rm sdfs/*")

    master = Master()
    node = Node(logging, master)
    node.run()
    tcp_client = Client(node, logging)
    tcp_client.run()
    tcp_server = Server(master, node, logging)
    tcpserver.register_instance(tcp_server)
    tcpserver.serve_forever()



