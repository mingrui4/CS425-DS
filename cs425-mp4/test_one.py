# from queue import Queue
# # from test_three import var
#
# q = Queue()
# p = Queue()
# for i in range(5):
#     q.put(i)
# q.queue.clear()
# print("___________")
# while not q.empty():
#     print(q.get())
# print("___________")
# while not p.empty():
#     print(p.get())


import random
seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%^*()_+=-"
test = ['Google','Twitter','Apple','Facebook','Amazon','Test']
f = open("qaq.txt", "w")
# for i in range(20000):
#
#     start = test[i % 6]
#     res = start
#     salt = ""
#     for k in range(10):
#         sa = []
#
#         for j in range(10):
#             sa.append(random.choice(seed))
#         salt = salt + ''.join(sa) + " "
#
#     res = res + " " + salt
#     f.write(res + '\n')
for i in range(20000):
    ans = ""
    for j in range(10):
        res = random.choice(test)
        ans = ans + " " + res
    print(ans)
    f.write(ans + '\n')
f.close()