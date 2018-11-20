# res=['1 is shut down', 'Thisisthelogofmachine.\n', '3 is shut down', '4 is shut down', '5 is shut down', '6 is shut down', '7 is shut down', '8 is shut down', '9 is shut down', '10 is shut down']
# length = len(res)
# for i in range(length):
#     if 'is shut down' in res[i]:
#         res[i] = ''
# res = ''.join(res)
# print(res)
# res = res.strip().split('\n')
# print(res)
# print(len(res))

import matplotlib.pyplot as plt
x=['224', '3710', '49795', '178365', '1037946', '3948968']
y=[0.1842, 0.204, 0.63, 2.44, 8.664, 34.746]
plt.plot(x,y,marker='*', ms=10)
plt.show()