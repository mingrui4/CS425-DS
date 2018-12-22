import traceback
import time
import threading
import socket

class Client():
    def __init__(self, node, log):
        self.node = node
        self.log = log

    def run_client(self):
        while True:
            cmd = input('Please enter your command: ')
            try:
                if cmd == 'join':
                    self.node.start_join()

                elif cmd == 'leave':
                    self.node.leave()

                elif cmd == 'lm':
                    for m in self.node.membership_list:
                        self.log.info(m)
                    self.log.info("Time : {} - {}".format(time.asctime(time.localtime(time.time())), self.node.membership_list))

                elif cmd == 'vm':
                    self.log.info('Time : {} - {}'.format(time.asctime(time.localtime(time.time())), socket.getfqdn()))

                elif cmd.startswith('put'):
                    var = cmd.split(' ')
                    print(var)
                    self.node.put(var[1], var[2])

                elif cmd.startswith('get-versions'):
                    var = cmd.split(' ')
                    self.node.get_versions(var[1], var[2], var[3])

                elif cmd.startswith('get'):
                    var = cmd.split(' ')
                    print(var)
                    self.node.get(var[1], var[2])

                elif cmd.startswith('delete'):
                    var = cmd.split(' ')
                    print(var)
                    self.node.delete(var[1])

                elif cmd.startswith('ls'):
                    var = cmd.split(' ')
                    print(var)
                    if len(var) < 2:
                        self.node.store()
                    else:
                        self.node.list(var[1])

                elif cmd.startswith('store'):
                    self.node.store()

            except:
                print("The command is invaild, please re-enter.")
                traceback.print_exc()

    def run(self):
        c_thread = threading.Thread(target=self.run_client)
        c_thread.start()

