import time
import getpass
import traceback
import pickle
import logging
import threading
import xmlrpc.client
import socket
import os
from operator import itemgetter
from threading import Lock
from slave import Slave
from queue import Queue

MACHINE_NUM = 5
INTRODUCER = 'fa18-cs425-g43-01.cs.illinois.edu'
SDFS = 'sdfs/'
MP_DIR = '/home/sitiz2/cs425-mp3/'
TIME = 2.5

class Node():

    def __init__(self, logger, master):
        self.membership_list = []
        self.neighbors_list = []
        self.removed = []
        self.numbers = []
        self.IsVote = False
        self.vote = {}
        self.alive = False

        self.socket = socket
        self.sdfs = Slave()
        self.master = master
        self.introduce = INTRODUCER
        self.InProgress = {}
        self.lock = Lock()

    def get_tcp_client(self, ip):
        return xmlrpc.client.ServerProxy('http://' + ip + ':8080')

    def is_alive(self):
        Flag = len(self.membership_list) >= MACHINE_NUM
        return self.alive and Flag

    def quorum(self, num):
        if num % 2 == 1:
            return (num+1)//2
        else:
            return num//2 + 1

    def start_join(self):
        self.alive = True
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ip_port = (socket.gethostbyname(INTRODUCER), 9000)
        message = {
            'cmd': 'join',
            'ip': socket.gethostbyname(socket.gethostname()),
            'port': 9000,
            'time': time.time()
        }
        sk.sendto(pickle.dumps(message), ip_port)
        logging.info("Join Command")

    def leave(self):
        new_membership_list = []
        message = {
            'cmd': 'leave',
            'ip': socket.gethostbyname(socket.gethostname()),
            'port': 9000,
        }
        leave_member = socket.gethostbyname(socket.gethostname())
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for member in self.membership_list:
            if member[0] == leave_member:
                continue
            new_membership_list.append(member)
            ip_port = (member[0], 9000)
            sk.sendto(pickle.dumps(message), ip_port)
        logging.info("Leave Command")
        # reset membership_list for rejoin
        self.membership_list = new_membership_list

    def update_neighbors(self):
        member_hosts = [member[0] for member in self.membership_list]
        ip = socket.gethostbyname(socket.gethostname())
        id = member_hosts.index(ip)
        length = len(member_hosts)
        if length < 5:
            self.neighbors_list=[]
        else:
            next_one = (id+1)%length
            next_two = (id+2)%length
            next_three = (id+3)%length
            self.neighbors_list = [member_hosts[next_one], member_hosts[next_two], member_hosts[next_three]]

    def udp_thread(self,out_q):
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sk.bind(('0.0.0.0', 9000))
        while True:
            msg, address = sk.recvfrom(32768)
            message = pickle.loads(msg)
            if message['cmd'] == "join":
                self.join_request(message)
                logging.info("New Node join. Membership List Update." )
                continue
            if message['cmd'] == 'leave':
                self.leave_request(message)
                self.add_replica()
                logging.info("One Node leave. Membership List Update.")
                continue
            if message['cmd'] == 'crash':
                self.membership_list = message['members']
                self.update_neighbors()
                self.master.update_memeber(self.membership_list)
                self.add_replica()
                logging.info("One Node crash. Membership List Update")
                continue
            if message['cmd'] == 'send':
                self.recv_ping(sk, address[0])
            else:
                # if message == ack, use queue to send message to another thread
                data = [msg,address]
                out_q.put(data)

    def join_request(self,message):
        if socket.gethostbyname(socket.gethostname()) == socket.gethostbyname(INTRODUCER):
            # there is no need to join the node that was already in the group
            member_hosts = [member[0] for member in self.membership_list]
            if message['ip'] in member_hosts:
                logging.info("This node already join the group.")
                return
            if message['ip'] == socket.gethostbyname(INTRODUCER):
                new_node = (message['ip'], 9000, time.time())
                self.membership_list.append(new_node)
                self.membership_list = sorted(self.membership_list, key=itemgetter(0))
                logging.info("Time : {}, {} is joining the group.".format(time.asctime(time.localtime(time.time())), message['ip']))
                self.update_neighbors()
                new_message = {
                    'cmd': 'join',
                    'ip': socket.gethostbyname(socket.gethostname()),
                    'port': 9000,
                    'time': time.time(),
                    'members': self.membership_list,
                }
                sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                # send updated member list to all machines
                for member in self.membership_list:
                    ip_port = (member[0], 9000)
                    sk.sendto(pickle.dumps(new_message), ip_port)
            else:
                if not self.membership_list:
                    return
                elif self.membership_list[0][0] == socket.gethostbyname(INTRODUCER):
                    new_node = (message['ip'], 9000, time.time())
                    self.membership_list.append(new_node)
                    self.membership_list = sorted(self.membership_list, key=itemgetter(0))
                    logging.info("Time : {}, {} is joining the group.".format(time.time(), message['ip']))
                    self.update_neighbors()
                    new_message = {
                        'cmd': 'join',
                        'ip': socket.gethostbyname(socket.gethostname()),
                        'port': 9000,
                        'time': time.time(),
                        'members': self.membership_list,
                    }
                    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    # send updated member list to all machines
                    for member in self.membership_list:
                        ip_port = (member[0], 9000)
                        sk.sendto(pickle.dumps(new_message), ip_port)
        else:
            self.membership_list = message['members']
            self.membership_list = sorted(self.membership_list, key=itemgetter(0))
            logging.info("Time : {}, {} is joining the group.".format(time.time(), message['ip']))
            self.update_neighbors()

    # voluntary leave from the group
    def leave_request(self,message):
        leave_host = message['ip']
        logging.info("Time : {}, No.{} volunterally left, current membership list: {}".format(time.time(), leave_host, self.membership_list))
        member_hosts = [member[0] for member in self.membership_list]
        id = member_hosts.index(leave_host)
        self.membership_list.pop(id)
        self.update_neighbors()

    # crash from the group
    def quit_request(self,message):
        member_hosts = [member[0] for member in self.membership_list]
        if message in member_hosts:
            leave_host = message
            id = member_hosts.index(leave_host)
            self.membership_list.pop(id)
            self.update_neighbors()
            message = {
                'cmd': 'crash',
                'ip': socket.gethostbyname(socket.gethostname()),
                'port': 9000,
                'members':self.membership_list,
            }
            new_member_hosts = [member[0] for member in self.membership_list]
            sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for member in new_member_hosts:
                ip_port = (member, 9000)
                sk.sendto(pickle.dumps(message), ip_port)
            logging.info("Crash Command")

    # send_ping to neighbors
    def send_ping(self,in_q):
        while True:
            member_hosts = [member[0] for member in self.membership_list]
            if socket.gethostbyname(socket.gethostname()) not in member_hosts or len(self.membership_list) < 5:
                continue
            for ip in self.neighbors_list:
                try:
                    FLAG = False
                    time.sleep(0.2)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.settimeout(2)
                    ping_data = 'sendPing/'+ ip
                    pack = pickle.dumps({
                        'cmd': 'send',
                        'data': ping_data,
                        'ip': socket.gethostbyname(socket.gethostname()),
                        'sender_port': 9000,
                        'sender_timestamp': time.time()
                    })
                    sock.sendto(pack, (ip,9000))
                    # try to receive data from queue
                    try:
                        EMPTY = in_q.empty()
                        tips = time.time()
                        if EMPTY is True:
                            FLAG = False
                            time.sleep(0.1)
                            now = time.time()
                            while now - tips <= TIME:
                                FLAG = False
                                time.sleep(0.1)
                                now = time.time()
                                EMPTY = in_q.empty()
                                if EMPTY is not True:
                                    FLAG = True
                                    break
                            if FLAG == False:
                                self.quit_request(ip)
                                logging.info(
                                "Time[{}]: {} has gone offline, current member_list: {}".format(time.time(),ip,
                                                                                                self.membership_list))
                        if EMPTY is False or FLAG is True:
                            data = in_q.get()
                            msg = data[0]
                            address = data[1]
                            message = pickle.loads(msg)
                            continue
                    except socket.timeout:
                        logging.info('socket timeout')
                        self.quit_request(ip)
                except (socket.error,socket.gaierror) as err_msg:
                    logging.error("Socket Error")
                    logging.exception(err_msg)
                finally:
                    sock.close()

    # test if the vm is crashed
    def test_ping(self,message, address):
        cur_time = time.time()
        logging.info('test')
        timing = message['sender_timestamp']
        logging.info(cur_time-timing)
        if timing <= cur_time - TIME:
            self.quit_request(address)
            logging.info(
            "Time[{}]: {} has gone offline, current member_list: {}".format(time.time(), address, self.membership_list))
            self.update_neighbors()

    # send ack ping to sender
    def recv_ping(self, sock, sender):
        ack_data = 'recvPing/'+ sender
        pack = pickle.dumps({
            'cmd': 'ack',
            'data': ack_data,
            'ip': socket.gethostbyname(socket.gethostname()),
            'sender_port': 9000,
            'sender_timestamp': time.time()
        })
        sock.sendto(pack, (sender,9000))

    # initial a list for a new sdfs file
    def initial(self,sdfsfilename):
        self.lock.acquire()
        self.InProgress[sdfsfilename] = []
        self.lock.release()

    # put command
    def put(self, localfilename, sdfsfilename):
        start = time.time()

        # if there is no related file, break
        if not os.path.exists(localfilename):
            logging.info("Cannot find " + localfilename)
            return False

        master = self.get_tcp_client(self.introduce)
        putInfo = master.file_info(sdfsfilename)

        # wrong input, break
        if not putInfo:
            logging.info('Cannot Put')
            return False

        self.initial(sdfsfilename)
        replicas = putInfo['replicas']
        version = putInfo['version']
        for replica in replicas:
            thread_rep = threading.Thread(target = self.put_replica, args = (replica,localfilename,sdfsfilename,version))
            thread_rep.start()

        while True:
            time.sleep(1)
            if len(self.InProgress[sdfsfilename]) >= self.quorum(len(replicas)):
                logging.debug('quorum count: {}'.format(len(self.InProgress[sdfsfilename])))
                logging.info('Put %s-%d [%fs]' % (
                    sdfsfilename,
                    version,
                    time.time() - start
                ))
                self.lock.acquire()
                del self.InProgress[sdfsfilename]
                self.lock.release()
                return True

    # use scp to put
    def put_replica(self, ip, localfilename, sdfsfilename, version):
        replica = self.get_tcp_client(ip)
        save_name = sdfsfilename + '--' + str(version)
        try:
            cmd = 'scp {} {}@{}:{}'.format(
                localfilename,
                getpass.getuser(),
                ip,
                MP_DIR + SDFS + save_name
            )
            os.system(cmd)
            replica.update_version(sdfsfilename, version)
            logging.debug("put {} to {}".format(sdfsfilename,ip))

        except:
            traceback.print_exc()
            logging.info("local file "+localfilename+" inexistence.")

        if not localfilename.startswith(SDFS):
            self.finish(sdfsfilename, 1)

    # input data into sdfs file list
    def finish(self, sdfsfilename, data):
        self.lock.acquire()
        if sdfsfilename in self.InProgress:
            self.InProgress[sdfsfilename].append(data)
        self.lock.release()

    # get command
    def get(self, sdfsfilename, localfilename):
        start = time.time()
        logging.info('metadata from ' + sdfsfilename)
        master = self.get_tcp_client(self.introduce)
        result = master.get_info(sdfsfilename)
        if len(result) == 0:
            logging.info('Cannot find ' + sdfsfilename)
            return False

        replicas = result['replicas']
        version = result['version']
        self.initial(sdfsfilename)

        for replica in replicas:
            thread_rep = threading.Thread(target=self.get_replica, args=(replica, sdfsfilename, version))
            thread_rep.start()

        while True:
            time.sleep(1)
            if len(self.InProgress[sdfsfilename]) >= self.quorum(len(replicas)):
                logging.debug('quorum count: {}'.format(len(self.InProgress[sdfsfilename])))
                break

        self.lock.acquire()
        for file in self.InProgress[sdfsfilename]:
            localVersion = file['version']
            save_name = sdfsfilename + '--' + str(version)
            if localVersion == version or len(self.InProgress[sdfsfilename]) == 1:
                cmd = 'scp {}@{}:{} {}'.format(
                    getpass.getuser(),
                    file['replicas'],
                    MP_DIR + SDFS + save_name,
                    localfilename
                )
                os.system(cmd)
                break
        del self.InProgress[sdfsfilename]
        self.lock.release()

        if localfilename.startswith(SDFS):
            self.sdfs.update_version(sdfsfilename, version)
            logging.debug("repair file"+ sdfsfilename)
        else:
            logging.info("write to local file "+ localfilename + " in "+ str(time.time() - start))

    # get from replica
    def get_replica(self, ip, sdfsfilename, version):
        replica = self.get_tcp_client(ip)
        data = replica.get_file(sdfsfilename, version)
        self.finish(sdfsfilename, data)
        return data

    def get_data(self, sdfsfilename, version):
        result = {}
        localVersion = self.sdfs.get_versions(sdfsfilename)
        if localVersion[-1] < version:
            thread_version = threading.Thread(target=self.get, args=(sdfsfilename, SDFS + sdfsfilename))
            thread_version.start()
        result['version'] = localVersion[-1]
        result['replicas'] = socket.getfqdn()
        return result

    # get-versions command, use cat to get and merge
    def get_versions(self, sdfsfilename, num, localfilename):
        start = time.time()
        logging.info('metadata from ' + sdfsfilename)
        master = self.get_tcp_client(self.introduce)
        result = master.get_info(sdfsfilename)
        if len(result) == 0:
            logging.info('Cannot find ' + sdfsfilename)
            return False

        replicas = result['replicas']
        version = result['version']
        self.initial(sdfsfilename)

        for replica in replicas:
            thread_rep = threading.Thread(target=self.get_versions_replica, args=(replica, sdfsfilename, num, version))
            thread_rep.start()

        while True:
            time.sleep(1)
            if len(self.InProgress[sdfsfilename]) >= self.quorum(len(replicas)):
                logging.debug('quorum count: {}'.format(len(self.InProgress[sdfsfilename])))
                break

        self.lock.acquire()
        for file in self.InProgress[sdfsfilename]:
            localVersion = file['version']
            i = version
            save = ''
            while i > version - int(num):
                save_name = sdfsfilename + '--' + str(i)
                save = save + ' ' +  MP_DIR + SDFS + save_name
                i -= 1

            if localVersion == version or len(self.InProgress[sdfsfilename]) == 1:
                cmd = 'ssh {}@{} "cat {}">> {}'.format(
                    getpass.getuser(),
                    file['replicas'],
                    save,
                    localfilename
                )
                os.system(cmd)
                break
        del self.InProgress[sdfsfilename]
        self.lock.release()

        if localfilename.startswith(SDFS):
            self.sdfs.update_version(sdfsfilename, version)
            logging.debug("repair file" + sdfsfilename)
        else:
            logging.info("write to local file" + localfilename + "in" + str(time.time() - start))

    def get_versions_replica(self, ip, sdfsfilename, num, version):
        replica = self.get_tcp_client(ip)
        data = replica.get_versions_file(sdfsfilename, num, version)
        self.finish(sdfsfilename, data)
        return data

    def get_versions_data(self, sdfsfilename, num, version):
        result = {}
        localVersion = self.sdfs.get_versions(sdfsfilename)
        if localVersion[-1] < version:
            thread_version = threading.Thread(target=self.get_versions, args=(sdfsfilename, num, SDFS + sdfsfilename))
            thread_version.start()
        result['version'] = localVersion[-1]
        result['replicas'] = socket.getfqdn()
        return result

    # delete file from all replicas
    def delete(self, sdfsfilename):
        master = self.get_tcp_client(self.introduce)
        nodes = master.delete_sdfsfile_info(sdfsfilename)
        for node in nodes:
            if node == socket.getfqdn():
                self.sdfs.delete_file(sdfsfilename)
                continue
            handleNode = self.get_tcp_client(node)
            handleNode.delete_node_file(sdfsfilename)
        logging.info("delete" + sdfsfilename)

    def delete_node_file(self, sdfsfilename):
        self.sdfs.delete_file(sdfsfilename)
        logging.info("File is deleted: {}".format(sdfsfilename))

    def list(self, sdfsfilename):
        master = self.get_tcp_client(self.introduce)
        replicas = master.get_info(sdfsfilename)

        if(len(replicas)) == 0:
            logging.info("Cannot find.")
            return

        replica = replicas['replicas']
        version = replicas['version']
        logging.info('File: {} Ver:{}'.format(sdfsfilename, version))
        for i, ip in enumerate(replica):
            logging.info('Replica {}: {}'.format(i,ip))

    # store command
    def store(self):
        files = self.sdfs.ls_file()
        for file, version in files.items():
            logging.info('File: {} Ver: {}'.format(file, version))

    # when introduce crash, revote a new master
    def revote(self):
        if not self.IsVote:
            self.numbers = 0
            self.vote = {}
            self.IsVote = True
        if socket.getfqdn() == self.membership_list[0][0]:
            self.numbers += 1
            return
        votes = self.get_tcp_client(self.membership_list[0][0])
        votes.vote(socket.getfqdn())

    def get_vote(self, vote):
        if not self.IsVote:
            self.numbers = 0
            self.vote = {}
            self.IsVote = True
        if vote not in self.vote:
            self.vote[vote] = 1
            self.numbers += 1
        if self.introduce != socket.getfqdn() and self.numbers > len(self.membership_list)/2:
            self.introduce = socket.getfqdn()
            logging.info("This is the new master")
            master = threading.Thread(target=self.new_master)
            master.start()

    def new_master(self):
        time.sleep(1)
        tmp = {}
        for n in self.membership_list:
            files = {}
            if n[0] == socket.getfqdn():
                files = self.sdfs.ls_file()
            else:
                member_handle = self.get_tcp_client(n[0])
                files = member_handle.reelect_master(socket.getfqdn())
            for filename, ver in files.items():
                if filename not in tmp:
                    tmp[filename] = []
                tmp[filename].append([n[0], ver])
            for filename, file_list in tmp.items():
                sorted(file_list, key=lambda x: x[1])
                value = [[],[],[]]
                value[0].append([x[0] for x in file_list][0:min(len(file_list), 4)])
                value[1].append(file_list[0][1])
                value[2].append(time.time())
                self.master.put_lookup(filename, value)
            logging.info("SDFS file metadata has been rebuilt")
            self.IsVote = False
            self.vote = {}

            # vote_thread = threading.Thread(target=self.add_replica)
            # vote_thread.start()

    def reelect_master(self,new_master):
        self.introduce = new_master
        self.IsVote = False
        logging.info("new matser: {}".format(new_master))
        return dict(self.sdfs.ls_file())

    # when replica crash, add new replicas
    def add_replica(self):
        self.master.update_memeber(self.membership_list)
        start_time = time.time()
        update_meta = self.master.update_lookup(self.membership_list)
        if len(update_meta) == 0:
            return
        for filename, meta in update_meta.items():
            node, ver, new_nodes = meta
            node_handle = self.get_tcp_client(node)
            for ip in new_nodes:
                logging.info('Add {}@{}'.format(filename, ip))
                for i in ver:
                    save_name = filename + '--' + str(i)
                    if node == socket.getfqdn():
                        self.put_replica(ip, SDFS + save_name, filename, i)
                    else:
                        node_handle.re_replicate(ip, SDFS + save_name, filename, i)
        logging.info("Add at [{}s]".format(time.time() - start_time))

    def main_func(self):
        while True:
            time.sleep(1)
            if not self.is_alive():
                continue
            self.master.update_memeber(self.membership_list)
            introduce_ip = socket.gethostbyname(self.introduce)
            if introduce_ip not in [x[0] for x in self.membership_list]:
                self.revote()

    def update_version(self, filename, version):
        self.sdfs.update_version(filename, version)

    def run(self):
        q = Queue()

        UDP_thread = threading.Thread(target=self.udp_thread, args=(q,))
        ping_thread = threading.Thread(target=self.send_ping, args=(q,), daemon=True)
        main_thread = threading.Thread(target=self.main_func)

        UDP_thread.start()
        ping_thread.start()
        main_thread.start()


