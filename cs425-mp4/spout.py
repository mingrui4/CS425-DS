import logging
import global_vars
import time
import threading
import socket
import pickle
import server
import inspect
import ctypes
# membership_list = []  # membership_list = [(ip,port,time,type,listening),()]


class Spout():
    def __init__(self):
        self.ip = socket.gethostbyname(socket.gethostname())
        self.file_length = 0
        self.stop = global_vars.fault_stop
        self.is_stop = False
        self.line_total = []
        self.lines = 0
        self.sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)



    def start(self):
        self.stop = False
        if self.ip == global_vars.MASTER:
            if not self.is_ready():
                logging.info("There is no topology, please assign it.")

            else:
                self.read_stream()
                self.spout_work()

    def spout_work(self):
        global id
        spout_thread = threading.Thread(target=self.spout_thread)
        spout_thread.start()
        id = spout_thread.ident
        logging.info(spout_thread)


    def terminate(self):
        global id
        if self.ip == global_vars.MASTER:
            logging.info("start to stop")
            if not global_vars.result:
                self.stop_thread(id)
            self.sk.close()
            self.stop = True
            global_vars.stream_list.queue.clear()
            sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            logging.info("********terminate**********")
            message = {
                'type': 'stream_data',
                'send_ip': socket.gethostbyname(socket.gethostname()),
                'data': 'TERMINATE'
            }
            for member in global_vars.membership_list:
                ip_port = (member[0], 7000)
                sk.sendto(pickle.dumps(message), ip_port)
            sk.close()


    def is_ready(self):
        flag = True
        if len(global_vars.membership_list) < 2:
            flag = False
        return flag

    def read_stream(self):
        file_name = ""
        if global_vars.membership_list[0][5] == 0:
            logging.info("Please select application.")
        elif global_vars.membership_list[0][5] == "1":
            file_name = global_vars.file_name_one
        elif global_vars.membership_list[0][5] == "2":
            file_name = global_vars.file_name_two
        elif global_vars.membership_list[0][5] == "3":
            file_name = global_vars.file_name_three
        while file_name:
            f = open(file_name, "r")
            lines = f.readlines()
            if not lines:
                break
            for line in lines:
                global_vars.stream_list.put(line)
            self.file_length = len(lines)
            f.close()
            break

    def spout_thread(self):
        time.sleep(0.5)
        while not self.stop and not self.is_stop:
            target_ip =[]
            for member in global_vars.membership_list:
                if member[4] == global_vars.BOLT_FILTER:
                    target_ip.append(member[0])
            ip_length = len(target_ip)
            logging.info("spout out doing now")
            logging.info(target_ip)
            # only read one line
            num = self.file_length // ip_length
            logging.info(num)
            for ips in target_ip:
                ip_port = (ips, 7000)
                count = 0
                if ips != target_ip[-1]:
                    logging.info("SpoutWorker to the filter bolt: " + ips + " start")
                    while count < num:
                        logging.info('+++++++++++++++')
                        send_message = global_vars.stream_list.get()
                        message = {
                            'type': 'stream_data',
                            'send_ip': socket.gethostbyname(socket.gethostname()),
                            'target_ip':ips,
                            'data': send_message
                        }
                        logging.info(message)
                        self.sk.sendto(pickle.dumps(message), ip_port)
                        time.sleep(0.005)
                        count += 1
                    message = {
                        'type': 'stream_data',
                        'send_ip': socket.gethostbyname(socket.gethostname()),
                        'target_ip': ips,
                        'data': global_vars.Stream_Stop
                    }
                    self.sk.sendto(pickle.dumps(message), ip_port)
                    logging.info(message)

                else:
                    logging.info("SpoutWorker to the filter bolt: " + ips + " start")
                    remain = self.file_length - num * (ip_length - 1)
                    while count < remain:
                        logging.info('+++++++++++++++')
                        send_message = global_vars.stream_list.get()
                        message = {
                            'type': 'stream_data',
                            'send_ip': socket.gethostbyname(socket.gethostname()),
                            'target_ip': ips,
                            'data': send_message
                        }
                        logging.info(message)
                        self.sk.sendto(pickle.dumps(message), ip_port)
                        time.sleep(0.006)
                        count += 1
                    message = {
                        'type': 'stream_data',
                        'send_ip': socket.gethostbyname(socket.gethostname()),
                        'target_ip': ips,
                        'data': global_vars.Stream_Stop
                    }
                    self.sk.sendto(pickle.dumps(message), ip_port)
                    logging.info(message)

            break
        global_vars.stream_list.queue.clear()
        logging.info("Stream concurrent list is empty, all the message have been sent out.")
        self.sk.close()

    def listen_thread(self):
        sk2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sk2.bind(('0.0.0.0', 5050))
        target_ip = []
        for member in global_vars.membership_list:
            if member[4] == global_vars.BOLT_FILTER:
                target_ip.append(member[0])
        listen_ip = target_ip[-1]
        while True:
            msg, address = sk2.recvfrom(32768)
            receive_msg = pickle.loads(msg)
            if receive_msg['type'] == 'ack':
                send_i = receive_msg['data']
                if send_i in self.line_total:
                    self.line_total.remove(send_i)
            break
        ip_port = (listen_ip, 7000)
        if len(self.line_total) != 0:
            for k in self.line_total:
                message = {
                    'type': 'stream_data',
                    'send_ip': socket.gethostbyname(socket.gethostname()),
                    'target_ip': listen_ip,
                    'data': self.lines[k],
                    'lines': k
                }

                sk2.sendto(pickle.dumps(message), ip_port)
                logging.info(message)
        message = {
            'type': 'stream_data',
            'send_ip': socket.gethostbyname(socket.gethostname()),
            'target_ip': listen_ip,
            'data': global_vars.Stream_Stop,
            'lines': 0
        }
        sk2.sendto(pickle.dumps(message), ip_port)
        logging.info(message)

    def _async_raise(self,tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stop_thread(self,id):
        self._async_raise(id, SystemExit)
