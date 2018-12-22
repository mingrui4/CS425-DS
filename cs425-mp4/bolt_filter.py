import socket
import threading
import logging
import server
import pickle
import global_vars
from queue import Queue
import time
# from global_vars import *
# membership_list = []  # membership_list = [(ip,port,time,type,listening),()]


class bolt_filter():

    def __init__(self):
        self.ip = socket.gethostbyname(socket.gethostname())
        self.true_name = ""
        self.sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.q = Queue()
        self.stop = global_vars.fault_stop
        self.isStop = False

    def bolt_filter_thread(self):
        STOP = False
        self.word_count = {
            "Facebook": 0,
            "Google": 0,
            "Twitter": 0,
            "Amazon": 0,
            "Apple": 0,
            "Test": 0,
        }
        # wirte data
        file_name = global_vars.membership_list[0][5]
        if file_name == "1":
            self.true_name = global_vars.file_name_one
            f1 = open(self.true_name, "w")
            f1.truncate()
            f1.close()
        elif file_name == "2":
            self.true_name = global_vars.file_name_two
            f2 = open(self.true_name, "w")
            f2.truncate()
            f2.close()
        elif file_name == "3":
            self.true_name = global_vars.file_name_three
            true_name = global_vars.file_name_three
            f3 = open(self.true_name, "w")
            f3.truncate()
            f3.close()

        # self.sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sk.bind(('0.0.0.0', 7000))
        self.stop = False

        while not STOP and not self.stop:
            msg, address = self.sk.recvfrom(32768)
            receive_msg = pickle.loads(msg)
            self.isStop = False
            # logging.info(receive_msg)
            if receive_msg['type'] == 'stream_data':
                data = receive_msg['data']
                if data == 'TERMINATE':
                    # global_vars.stream_list.queue.clear()
                    self.q.queue.clear()
                    self.stop = True
                    self.isStop = True
                    self.sk.close()
                    break
                # global_vars.stream_list.put(data)
                self.q.put(data)
                if data == global_vars.Stream_Stop:
                    STOP = True
        # sk.close()

    def solve_thread(self):
        for member in global_vars.membership_list:
            if member[4] == global_vars.BOLT_AGG:
                target_ip = member[0]
        file_name = global_vars.membership_list[0][5]
        SIGN = True
        while SIGN:
            if self.q.empty() == True:
                continue
            else:
                while not self.isStop:
                    # streamline = global_vars.stream_list.get()
                    streamline = self.q.get()
                    # logging.info("this is streamline in bolt filter")
                    # logging.info(streamline)
                    res = ""
                    if streamline == global_vars.Stream_Stop:
                        # global_vars.stream_read_stop = True
                        SIGN = False
                        self.q.queue.clear()
                        break
                    elif streamline is not None:
                        res = self.filter_app(streamline)
                        while file_name:
                            f = open(self.true_name, 'a')
                            f.write(res + '\n')
                            f.close()
                            break
                        if "&" not in res:
                            continue
                        message = {
                            'type': 'stream_data',
                            'send_ip': socket.gethostbyname(socket.gethostname()),
                            'data': res
                        }
                        ip_port = (target_ip, 7000)
                        # logging.info(res)
                        self.sk.sendto(pickle.dumps(message), ip_port)
                        # logging.info("send stream to " + target_ip + message)

                message = {
                    'type': 'stream_data',
                    'send_ip': socket.gethostbyname(socket.gethostname()),
                    'data': global_vars.Stream_Stop
                }
                ip_port = (target_ip, 7000)
                time.sleep(0.006)
                self.sk.sendto(pickle.dumps(message), ip_port)
                logging.info("send stream to JOIN finish")
                # global_vars.stream_list.queue.clear()
                self.q.queue.clear()
        # self.sk.close()

    def filter_app(self,streamline):
        words = streamline.split(" ")
        # logging.info(words)
        if global_vars.membership_list[0][5] == "1":
            for word in self.word_count:
                self.word_count[word] = 0
            for item in words:
                item = item.strip('\n')
                if item in self.word_count.keys():
                    self.word_count[item] += 1
                    # 如果item为value, count+1
        elif global_vars.membership_list[0][5] == "2":
            self.word_count.clear()
            for item in words:
                item = item.strip('\n')
                if item in self.word_count.keys():
                    self.word_count[item] += 1
                else:
                    self.word_count[item] = 1
        elif global_vars.membership_list[0][5] == "3":
            self.word_count.clear()
            for item in words:
                item = item.strip('\n')
                if item in self.word_count.keys():
                    self.word_count[item] += 1
                else:
                    self.word_count[item] = 1
        buffer = []

        for word, count in self.word_count.items():
            string_buffer = word + '@' + str(count) + '&'
            buffer.append(string_buffer)

        return "".join(buffer)

    def ack_thread(self):
        sk2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sk2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        message = {
            'type': 'ack',
            'send_ip': socket.gethostbyname(socket.gethostname()),
            'data': self.ack.get()
        }
        ip_port = (global_vars.MASTER, 5050)
        sk2.sendto(pickle.dumps(message), ip_port)

    def bolt_filter_work(self):
        self.sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        bolt_thread = threading.Thread(target=self.bolt_filter_thread)
        solve_thread = threading.Thread(target=self.solve_thread)
        bolt_thread.start()
        solve_thread.start()
