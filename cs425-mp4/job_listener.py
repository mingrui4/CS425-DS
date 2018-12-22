import pickle
import logging
import threading
import socket
from server import node
from global_vars import *


class Job_Listener():
    def __init__(self, socket):
        self.client_socket = socket

    def run_job(self):
        try:
            while True:
                msg, address = self.client_socket.recvfrom(32768)
                message = pickle.loads(msg)
                if message:
                    if message['type'] == 'result':
                        logging.info('job completed')
                        logging.info(message)
                        break
            #master will send reset message to other nodes
            self.reset_role()
            node.job_completed = True
        except:
            logging.info("ERROR")


    def reset_role(self):
        message = {'type': 'role_reset',
                   'send_ip': socket.gethostbyname(socket.gethostname())}
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for member in node.membership_list:
            if member[0] != MASTER:
                member[4] = False
                member[3] = None
                ip_port = (member[0], 9000)
                sk.sendto(pickle.dumps(message), ip_port)


    def run(self):
        job_listen_thread = threading.Thread(target=self.run_job)
        job_listen_thread.start()