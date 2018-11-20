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

introducer = 'fa18-cs425-g43-01.cs.illinois.edu'
membership_list = []  # membership_list = [(ip,port,time),()]
neighbors_list = []  # ip2,ip3

# set TIME for fail detect
TIME = 2

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


def user_command():
    global membership_list
    global neighbors_list
    while True:
        cmd = input('Please enter your command: ')
        if cmd == 'join':
            start_join()
        elif cmd == 'leave':
            leave()
        elif cmd == 'lm':
            for m in membership_list:
                logging.info(m)
            logging.info("Time : {} - {}".format(time.asctime(time.localtime(time.time())), membership_list))
        elif cmd == 'vm':
            logging.info('Time : {} - {}'.format(time.asctime(time.localtime(time.time())), socket.getfqdn()))
        elif cmd == 'exit':
            os._exit(0)
        else:
            print("The command is invaild, please re-enter.")


def start_join():
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


def leave():
    global membership_list
    global neighbors_list
    new_membership_list = []
    message = {
        'cmd': 'leave',
        'ip': socket.gethostbyname(socket.gethostname()),
        'port': 9000,
    }
    leave_member = socket.gethostbyname(socket.gethostname())
    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for member in membership_list:
        if member[0] == leave_member:
            continue
        new_membership_list.append(member)
        ip_port = (member[0], 9000)
        sk.sendto(pickle.dumps(message), ip_port)
    logging.info("Leave Command")
    # reset membership_list for rejoin
    membership_list = new_membership_list


def update_neighbors():
    global neighbors_list
    member_hosts = [member[0] for member in membership_list]
    ip = socket.gethostbyname(socket.gethostname())
    id = member_hosts.index(ip)
    length = len(member_hosts)
    if length < 5:
        neighbors_list=[]
    else:
        next_one = (id+1)%length
        next_two = (id+2)%length
        next_three = (id+3)%length
        neighbors_list = [member_hosts[next_one], member_hosts[next_two], member_hosts[next_three]]
    # logging.info(neighbors_list)


def udp_thread(out_q):
    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sk.bind(('0.0.0.0', 9000))
    while True:
        msg, address = sk.recvfrom(32768)
        message = pickle.loads(msg)
        if message['cmd'] == "join":
            join_request(message)
            logging.info("New Node join. Membership List Update." )
            continue
        if message['cmd'] == 'leave':
            leave_request(message)
            logging.info("One Node leave. Membership List Update.")
            continue
        if message['cmd'] == 'send':
            recv_ping(sk, address[0])
        else:
            # if message == ack, use queue to send message to another thread
            data = [msg,address]
            out_q.put(data)


def join_request(message):
    global membership_list
    if socket.gethostbyname(socket.gethostname()) == socket.gethostbyname(introducer):
        # there is no need to join the node that was already in the group
        member_hosts = [member[0] for member in membership_list]
        if message['ip'] in member_hosts:
            logging.info("This node already join the group.")
            return
        if message['ip'] == socket.gethostbyname(introducer):
            new_node = (message['ip'], 9000, time.time())
            membership_list.append(new_node)
            membership_list = sorted(membership_list, key=itemgetter(0))
            logging.info("Time : {}, {} is joining the group.".format(time.asctime(time.localtime(time.time())), message['ip']))
            update_neighbors()
            new_message = {
                'cmd': 'join',
                'ip': socket.gethostbyname(socket.gethostname()),
                'port': 9000,
                'time': time.time(),
                'members': membership_list,
            }
            sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # send updated member list to all machines
            for member in membership_list:
                ip_port = (member[0], 9000)
                sk.sendto(pickle.dumps(new_message), ip_port)
        else:
            if not membership_list:
                return
            elif membership_list[0][0] == socket.gethostbyname(introducer):
                new_node = (message['ip'], 9000, time.time())
                membership_list.append(new_node)
                membership_list = sorted(membership_list, key=itemgetter(0))
                logging.info("Time : {}, {} is joining the group.".format(time.time(), message['ip']))
                update_neighbors()
                new_message = {
                    'cmd': 'join',
                    'ip': socket.gethostbyname(socket.gethostname()),
                    'port': 9000,
                    'time': time.time(),
                    'members': membership_list,
                }
                sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                # send updated member list to all machines
                for member in membership_list:
                    ip_port = (member[0], 9000)
                    sk.sendto(pickle.dumps(new_message), ip_port)
    else:
        membership_list = message['members']
        membership_list = sorted(membership_list, key=itemgetter(0))
        logging.info("Time : {}, {} is joining the group.".format(time.time(), message['ip']))
        update_neighbors()


# voluntary leave from the group
def leave_request(message):
    global membership_list
    leave_host = message['ip']
    logging.info("Time : {}, No.{} volunterally left, current membership list: {}".format(time.time(), leave_host, membership_list))
    member_hosts = [member[0] for member in membership_list]
    id = member_hosts.index(leave_host)
    membership_list.pop(id)
    update_neighbors()


# crash from the group
def quit_request(message):
    global membership_list
    leave_host = message
    logging.info("LEFT: time : {}, No.{}, current membership list: {}".format(time.time(), leave_host, membership_list))
    member_hosts = [member[0] for member in membership_list]
    id = member_hosts.index(leave_host)
    membership_list.pop(id)
    update_neighbors()


# send_ping to neighbors
def send_ping(in_q):
    global neighbors_list
    global membership_list
    while True:
        member_hosts = [member[0] for member in membership_list]
        if socket.gethostbyname(socket.gethostname()) not in member_hosts or len(membership_list) < 5:
            continue
        for ip in neighbors_list:
            logging.info('try success :)')
            try:
                # time.sleep(1)
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
                    logging.info('test try')
                    data = in_q.get()
                    msg = data[0]
                    address = data[1]
                    message = pickle.loads(msg)
                    test_ping(message,address[0])
                except socket.timeout:
                    logging.info('socket timeout')
                    quit_request(ip)
                    update_neighbors()
            except (socket.error,socket.gaierror) as err_msg:
                logging.error("Socket Error")
                logging.exception(err_msg)
            finally:
                sock.close()


# test if the vm is crashed
def test_ping(message, address):
    global neighbors_list
    global membership_list
    cur_time = time.time()
    timing = message['sender_timestamp']
    if timing <= cur_time - TIME:
        quit_request(address)
        logging.info(
        "Time[{}]: {} has gone offline, current member_list: {}".format(time.time(), address, membership_list))
        update_neighbors()


# send ack ping to sender
def recv_ping(sock, sender):
    global membership_list
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

    UDP_thread = threading.Thread(target=udp_thread,args=(q,))
    TCP_thread = threading.Thread(target=tcp_thread)
    client_thread = threading.Thread(target=user_command)
    ping_thread = threading.Thread(target=send_ping,args=(q,),daemon=True)

    UDP_thread.start()
    TCP_thread.start()
    client_thread.start()
    ping_thread.start()

    UDP_thread.join()
    TCP_thread.join()
    client_thread.join()
    ping_thread.join()