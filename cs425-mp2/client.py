import xmlrpc.client
import threading
import time
import sys
import base64


# class for thread
class serverThread(threading.Thread):

    def __init__(self, serverID, address, filepath, pattern, result):
        threading.Thread.__init__(self)
        self.serverID = serverID
        self.address = address
        self.filepath = filepath
        self.pattern = pattern
        self.result = result

    def run(self):
        vmID = self.serverID - 1
        server = xmlrpc.client.ServerProxy('http://' + self.address + ':8080')
        # if the server is running, then:
        try:
            query = server.query(self.filepath, self.pattern)
            query_decoded = base64.b64decode(query.data).decode("utf-8")
            self.result[vmID] = query_decoded
        # if the server is not work, output the exception:
        except Exception as exception:
            self.result[vmID] = str(self.serverID)+' is shut down'
            print(self.result[vmID])
            print(exception)


# for each vm, create a thread for it
def createThread(filepath, pattern):
    result = [None]*10
    serverAdd = []
    threads = []
    output = []
    for i in range(10):
        serverAdd.append('fa18-cs425-g43-' + str(i + 1).zfill(2) + '.cs.illinois.edu')
        # because the input is different with the path in each vm, transfer it into specified file
        output.append(filepath)
    for i, address in enumerate(serverAdd):
        threads.append(serverThread(i + 1, address, output[i], pattern, result))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return result


if __name__ == '__main__':
    start = time.time()
    total = 0
    lines = []
    if len(sys.argv) > 1:
        print("Log file:" + sys.argv[1] + "pattern:" + sys.argv[2])
        filepath = sys.argv[1]
        pattern = sys.argv[2]
        result = createThread(filepath, pattern)
    # if no input, use regular pattern
    else:
        print('Run as regular pattern')
        filepath = 'mp2.log'
        pattern = 'Join'
        result = createThread(filepath, pattern)
    end = time.time()

    # result from each server
    for i, s in enumerate(result):
        serverRe = result[i].splitlines()
        lines.append(len(serverRe))
        total = total + len(serverRe)
        # if there is no file or the server is not work
        if (len(serverRe) == 1 and ('No such file or directory' in serverRe[0] or 'is shut down' in serverRe[0])):
            total = total - 1
            lines[i] = 0

    # output for each vm
    for i, num in enumerate(lines):
        ID = i+1
        print("VM No.",ID,"matches",num,"lines.")
    print("Total found:",total,"lines.")
    print("Total time:",(end - start),"seconds.")
