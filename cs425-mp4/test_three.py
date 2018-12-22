# from queue import Queue
# def set():
#     global q
#     q = Queue()
#     for i in range(5):
#         q.put(str(i)+'\n')
#
# def sum():
#     global q
#     ip =['a','b']
#     for item in ip:
#         i = 0
#         while i < 2:
#             data = q.get()
#             print(item+str(i)+data)
#             i += 1
#
# if __name__ == '__main__':
#     set()
#     sum()
#     q.queue.clear()
#     print("end")
# file_length - num * (ip_length-1)
import pickle
import socket

TCP_IP = '172.22.158.142'
TCP_PORT = 5005
BUFFER_SIZE = 1024
MESSAGE = "Hello, World!"

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))
s.send(pickle.dumps(MESSAGE))
data = s.recv(BUFFER_SIZE)
s.close()

print("received data:", data)
