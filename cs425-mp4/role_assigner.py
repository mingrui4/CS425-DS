import random
import threading
import socket
import pickle
import logging
import server
import global_vars
from job_listener import Job_Listener

class Role_Assigner():
    def __init__(self, ip, app_id):
        self.ip = ip
        self.app_id = app_id

    def assign_role(self):
        member_hosts = [member[0] for member in global_vars.membership_list]
        if self.ip == global_vars.MASTER:
            global_vars.membership_list[0][4] = global_vars.SPOUT
            global_vars.membership_list[0][-1] = self.app_id
            global_vars.membership_list[0][3] = True
            global_vars.stream_read_stop = False
            global_vars.fault_stop = False
            server_counts = len(global_vars.membership_list)
            bolts = []
            if server_counts >= 5:
                for member in global_vars.membership_list:
                    #if the type of vm is None, assign the role of bolt
                    if member[4] == None:
                        bolts.append(member[0])
                #listen to the aggregator bolt to detect whether the job has been done
                global_vars.job_completed = False
                if not global_vars.job_completed:
                    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    job_listen = Job_Listener(sk)
                    job_listen.run()

                # random select a join bolt
                join_id = random.randint(0,len(bolts)-1)#index
                for i in range(len(bolts)):
                    id = member_hosts.index(bolts[i])
                    #send role message to bolts
                    self.run(id, bolts[i], bolts[join_id])
                    #update the local role
                    if i == join_id:
                        global_vars.membership_list[id][4] = global_vars.BOLT_AGG
                    else:
                        global_vars.membership_list[id][4] = global_vars.BOLT_FILTER
                    global_vars.membership_list[id][-1] = self.app_id
                    global_vars.membership_list[id][3] = True
                logging.info(global_vars.membership_list)

    def run_assign(self, id, ip, join_id):
        message = {'cmd': 'assign',
                   'send_ip': socket.gethostbyname(socket.gethostname()),
                   'join_id': join_id,
                   'app_id': self.app_id}
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ip_port = (ip, 9000)
        sk.sendto(pickle.dumps(message), ip_port)

    def run(self, id, ip, join_id):
        send_thread = threading.Thread(target=self.run_assign(id, ip, join_id))
        send_thread.start()


