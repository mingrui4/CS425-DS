from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import subprocess
import base64
import time
import socket
import logging
import os
import sys
import pickle
import threading
from queue import Queue
from operator import itemgetter
import traceback
import global_vars


introducer = 'fa18-cs425-g43-01.cs.illinois.edu'
# membership_list = [(ip,port,time,listening,type,app),()]

# set TIME for fail detect
TIME = 2
EMPTY = False

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

def tcp_thread():
    # Create server
    server = SimpleXMLRPCServer(('0.0.0.0', 8080), requestHandler=RequestHandler)
    server.register_introspection_functions()

    # Query distributed log files
    def query(file, parttern):
        cmd = 'cat ' + file + ' | grep ' + parttern
        res = subprocess.run(cmd, stdout = subprocess.PIPE, shell = True, encoding = 'utf-8')
        # using base64 for trasferring data in xmlRPC
        result = base64.b64encode(res.stdout.encode('utf-8'))
        return result
    server.register_function(query,'query')

    # Run the server's main loop
    server.serve_forever()


class node():
    neighbors_list = []

    def user_command(self):
        global start_time
        cur = socket.gethostbyname(socket.gethostname())
        while True:
            cmd = input('Please enter your command: ')
            if cmd == 'join':
                self.start_join()
            elif cmd == 'leave':
                self.leave()
            elif cmd == 'lm':
                for m in global_vars.membership_list:
                    logging.info(m)
                logging.info("Time : {} - {}".format(time.asctime(time.localtime(time.time())), global_vars.membership_list))
            elif cmd == 'vm':
                logging.info('Time : {} - {}'.format(time.asctime(time.localtime(time.time())), socket.getfqdn()))
            elif cmd == 'exit':
                os._exit(0)
            elif cmd.startswith('assign'):
                var = cmd.split(' ')
                from role_assigner import Role_Assigner
                role_assign = Role_Assigner(cur, var[1])
                role_assign.assign_role()
            elif cmd == 'start':
                import spout
                spout.Spout().start()
                start_time = time.time()
            else:
                print("The command is invaild, please re-enter.")
                traceback.print_exc()

    def start_join(self):
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ip_port = (socket.gethostbyname(introducer), 9000)
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
        for member in global_vars.membership_list:
            if member[0] == leave_member:
                continue
            new_membership_list.append(member)
            ip_port = (member[0], 9000)
            sk.sendto(pickle.dumps(message), ip_port)
        logging.info("Leave Command")
        # reset membership_list for rejoin
        global_vars.membership_list = new_membership_list


    def update_neighbors(self):
        member_hosts = [member[0] for member in global_vars.membership_list]
        ip = socket.gethostbyname(socket.gethostname())
        id = member_hosts.index(ip)
        length = len(member_hosts)
        if length < 5:
            node.neighbors_list=[]
        else:
            next_one = (id+1)%length
            next_two = (id+2)%length
            next_three = (id+3)%length
            node.neighbors_list = [member_hosts[next_one], member_hosts[next_two], member_hosts[next_three]]


    def udp_thread(self,out_q):
        global start_time
        cur = socket.gethostbyname(socket.gethostname())
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sk.bind(('0.0.0.0', 9000))
        while True:
            msg, address = sk.recvfrom(32768)
            message = pickle.loads(msg)
            if message['cmd'] == "join":
                self.join_request(message)
                logging.info("New Node join. Membership List Update.")
                continue
            elif message['cmd'] == 'leave':
                self.leave_request(message)
                logging.info("One Node leave. Membership List Update.")
                continue
            elif message['cmd'] == 'crash':
                if message['crash_node'] == global_vars.MASTER:
                    if cur == global_vars.membership_list[0][0]:
                        global_vars.MASTER = cur
                logging.info("receive crash message")
                global_vars.fault_stop = True
                global_vars.membership_list = message['members']
                app_id = global_vars.membership_list[-1][-1]
                self.update_neighbors()
                import spout
                logging.info("restart spout")
                global_vars.fault_stop = False
                spout.Spout().terminate()
                from role_assigner import Role_Assigner
                logging.info("restart role assign")
                role_assign = Role_Assigner(cur, app_id)
                role_assign.assign_role()
                spout.Spout().start()
                logging.info("One Node crash. Membership List Update. Restart job.")
                continue
            elif message['cmd'] == 'send':
                self.recv_ping(sk, address[0])
            elif message['cmd'] == 'assign':
                self.assign_request(message)
            elif message['cmd'] == 'result':
                result = message['data']
                end_time = time.time()
                logging.info("this is result:")
                logging.info(result)
                logging.info("use time(s): "+str(end_time-start_time))
                global_vars.result = True
            else:
                # if message == ack, use queue to send message to another thread
                data = [msg, address]
                out_q.put(data)

    def assign_request(self, message):
        global_vars.fault_stop = False
        join_id = message['join_id']
        app_id = message['app_id']
        # update membership list in current VM
        for member in global_vars.membership_list:
            if member[0] == join_id:
                member[4] = global_vars.BOLT_AGG
            elif member[0] != global_vars.MASTER:
                member[4] = global_vars.BOLT_FILTER
            else:
                member[4] = global_vars.SPOUT
            member[3] = True
            member[-1] = app_id
        # logging.info('start bolt listener')
        # from bolt import Bolt
        # Bolt().bolt_work()

        logging.info('start filter/agg bolt worker')
        cur = socket.gethostbyname(socket.gethostname())
        if cur == join_id:
            from bolt_agg import bolt_agg
            bolt_agg().bolt_agg_work()
        else:
            from bolt_filter import bolt_filter
            bolt_filter().bolt_filter_work()
        logging.info('assign success')
        logging.info(global_vars.membership_list)


    def join_request(self, message):
        if socket.gethostbyname(socket.gethostname()) == socket.gethostbyname(introducer):
            # there is no need to join the node that was already in the group
            member_hosts = [member[0] for member in global_vars.membership_list]
            if message['ip'] in member_hosts:
                logging.info("This node already join the group.")
                return
            if message['ip'] == socket.gethostbyname(introducer):
                new_node = [message['ip'], 9000, time.time(), False, None, 0]
                global_vars.membership_list.append(new_node)
                global_vars.membership_list = sorted(global_vars.membership_list, key=itemgetter(0))
                logging.info("Time : {}, {} is joining the group.".format(time.asctime(time.localtime(time.time())), message['ip']))
                self.update_neighbors()
                new_message = {
                    'cmd': 'join',
                    'ip': socket.gethostbyname(socket.gethostname()),
                    'port': 9000,
                    'time': time.time(),
                    'members': global_vars.membership_list,
                }
                sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                # send updated member list to all machines
                for member in global_vars.membership_list:
                    ip_port = (member[0], 9000)
                    sk.sendto(pickle.dumps(new_message), ip_port)
            else:
                if not global_vars.membership_list:
                    return
                elif global_vars.membership_list[0][0] == socket.gethostbyname(introducer):
                    new_node = [message['ip'], 9000, time.time(), False, None, 0]
                    global_vars.membership_list.append(new_node)
                    global_vars.membership_list = sorted(global_vars.membership_list, key=itemgetter(0))
                    logging.info("Time : {}, {} is joining the group.".format(time.time(), message['ip']))
                    self.update_neighbors()
                    new_message = {
                        'cmd': 'join',
                        'ip': socket.gethostbyname(socket.gethostname()),
                        'port': 9000,
                        'time': time.time(),
                        'members': global_vars.membership_list,
                    }
                    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    # send updated member list to all machines
                    for member in global_vars.membership_list:
                        ip_port = (member[0], 9000)
                        sk.sendto(pickle.dumps(new_message), ip_port)
        else:
            global_vars.membership_list = message['members']
            global_vars.membership_list = sorted(global_vars.membership_list, key=itemgetter(0))
            logging.info("Time : {}, {} is joining the group.".format(time.time(), message['ip']))
            self.update_neighbors()


    # voluntary leave from the group
    def leave_request(self, message):
        leave_host = message['ip']
        logging.info("Time : {}, No.{} volunterally left, current membership list: {}".format(time.time(), leave_host, global_vars.membership_list))
        member_hosts = [member[0] for member in global_vars.membership_list]
        id = member_hosts.index(leave_host)
        global_vars.membership_list.pop(id)
        self.update_neighbors()


    # crash from the group
    def quit_request(self, message):
        member_hosts = [member[0] for member in global_vars.membership_list]
        if message in member_hosts:
            leave_host = message
            #logging.info("LEFT: time : {}, No.{}, current membership list: {}".format(time.time(), leave_host, membership_list))
            #member_hosts = [member[0] for member in membership_list]
            id = member_hosts.index(leave_host)
            global_vars.membership_list.pop(id)
            #when crash happens, reset role
            for member in global_vars.membership_list:
                member[4] = None
                member[3] = False
            self.update_neighbors()
            message = {
                'cmd': 'crash',
                'ip': socket.gethostbyname(socket.gethostname()),
                'port': 9000,
                'members': global_vars.membership_list,
                'crash_node': leave_host
            }
            new_member_hosts = [member[0] for member in global_vars.membership_list]
            sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for member in new_member_hosts:
                ip_port = (member, 9000)
                sk.sendto(pickle.dumps(message), ip_port)
            logging.info("Crash Command")

    # send_ping to neighbors
    def send_ping(self, in_q):
        while True:
            member_hosts = [member[0] for member in global_vars.membership_list]
            if socket.gethostbyname(socket.gethostname()) not in member_hosts or len(global_vars.membership_list) < 5:
                continue
            for ip in node.neighbors_list:
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
                                                                                                global_vars.membership_list))
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
    def test_ping(self, message, address):
        cur_time = time.time()
        logging.info('test')
        timing = message['sender_timestamp']
        logging.info(cur_time-timing)
        if timing <= cur_time - TIME:
            self.quit_request(address)
            logging.info(
            "Time[{}]: {} has gone offline, current member_list: {}".format(time.time(), address, global_vars.membership_list))
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


if __name__ == '__main__':
    logging.basicConfig(filename='mp2.log', level=logging.INFO, filemode='w',format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level=logging.DEBUG)
    logging.getLogger("").addHandler(stream_handler)
    q = Queue()
    node = node()
    UDP_thread = threading.Thread(target=node.udp_thread,args=(q,))
    client_thread = threading.Thread(target=node.user_command)
    ping_thread = threading.Thread(target=node.send_ping,args=(q,),daemon=True)

    UDP_thread.start()
    client_thread.start()
    ping_thread.start()

    UDP_thread.join()
    client_thread.join()
    ping_thread.join()