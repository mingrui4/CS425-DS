# f = open("qaq.txt", "a")
# i = 0
# test = ['Google','Twitter','Apple','Facebook','Amazon','Test']
# for i in range(100000):
#     res = test[i%6]
#     f.write(res+'\n')
# f.close()
# test = ['Google','Twitter','Apple','Facebook','Amazon','Test']
# ans = {}
# f = open("qaq.txt", "r")
# lines = f.readlines(100000)
# i = 0
# for line in lines:
#     word = line.split()
#     print(word)
#     if 'Google' in word:
#         i = i+ 1
# print(i)
import socket

TCP_IP = '127.0.0.1'
TCP_PORT = 5005
BUFFER_SIZE = 20  # Normally 1024, but we want fast response

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

conn, addr = s.accept()
print('Connection address:', addr)
while 1:
    data = conn.recv(BUFFER_SIZE)
    if not data:
        break
    print("received data:", data)
    conn.send(data)  # echo
conn.close()