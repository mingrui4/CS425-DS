import socket
import logging
import threading
import server
import global_vars
import pickle
import time
# membership_list = []  # membership_list = [(ip,port,time,type,listening),()]


class bolt_agg():

    def __init__(self):
        self.ip = socket.gethostbyname(socket.gethostname())
        self.stop = global_vars.fault_stop

    def bolt_agg_thread(self):
        global_vars.stream_list.queue.clear()
        self.stop = False

        num_filter = 0
        for members in global_vars.membership_list:
            if members[4] == global_vars.BOLT_FILTER:
                num_filter += 1
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sk.bind(('0.0.0.0', 7000))
        while not self.stop:
            msg, address = sk.recvfrom(32768)
            receive_msg = pickle.loads(msg)
            if receive_msg['type'] == 'stream_data':
                data = receive_msg['data']
                if data == 'TERMINATE':
                    global_vars.stream_list.queue.clear()
                    global_vars.result_map.clear()
                    sk.close()
                    self.stop = True
                    break

                global_vars.stream_list.put(data)
        # read data and write it
            streamline = global_vars.stream_list.get()
            if streamline == global_vars.Stream_Stop:
                logging.info("this is num filter:")
                logging.info(num_filter)
                if num_filter == 1:
                    break
                else:
                    num_filter -= 1
            elif streamline is not None:
                self.agg_app(streamline)


        if self.stop:
            logging.info("failure detect!")
        else:
            for value,item in global_vars.result_map.items():
                logging.info("||| Key word : "+ str(value) + " | Counts: " + str(item) +" |||")
            target_ip = global_vars.MASTER
            for member in global_vars.membership_list:
                if member[4] == global_vars.SPOUT:
                    target_ip = member[0]
            message = {
                'cmd': 'result',
                'send_ip': socket.gethostbyname(socket.gethostname()),
                'data': global_vars.result_map
            }
            ip_port = (target_ip, 9000)
            sk.sendto(pickle.dumps(message), ip_port)
            logging.info("sending answer to introducer.")
            global_vars.stream_list.queue.clear()
            global_vars.result_map.clear()
        sk.close()

    def agg_app(self,streamline):
        sentences = streamline.split("&")
        sentences = sentences[0:len(sentences)-1]
        for item in sentences:
            save = item.split("@")
            word = save[0]
            count = save[1]
            if word in global_vars.result_map.keys(): # what is result map
                num = int(count)
                global_vars.result_map[word] += num
            else:
                num = int(count)
                global_vars.result_map[word] = num
        logging.info(global_vars.result_map)

    def bolt_agg_work(self):
        bolt_thread = threading.Thread(target=self.bolt_agg_thread)
        bolt_thread.start()
