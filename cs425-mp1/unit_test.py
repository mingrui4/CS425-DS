import unittest
from client import createThread

class MyTest(unittest.TestCase):
    #tesing query partterns that are frequent
    def test_frequent(self):
        frequent = '2018-09-15'
        res = createThread('machine.log', frequent)
        length = len(res)
        for i in range(length):
            if 'is shut down' in res[i]:
                res[i] = ''
        res = ''.join(res)
        res = res.strip().split('\n')
        self.assertEqual(len(res), 90)
        self.assertTrue(res[0].startswith(frequent))

    # tesing query partterns that are infrequent
    def test_infrequent(self):
        infrequent = 'xxxx-xx-xx'
        res = createThread('machine.log', infrequent)
        length = len(res)
        for i in range(length):
            if 'is shut down' in res[i]:
                res[i] = ''
        res = ''.join(res)
        res = res.strip().split('\n')
        self.assertEqual(len(res), 10)
        self.assertTrue(res[0].startswith(infrequent))

    # tesing query partterns that are rare
    def test_rare(self):
        rare ='Thisisthelogofmachine'
        res = createThread('machine.log', rare)
        length = len(res)
        for i in range(length):
            if 'is shut down' in res[i]:
                res[i] = ''
        res = ''.join(res)
        res = res.strip().split('\n')
        self.assertEqual(len(res), 1)
        self.assertTrue(res[0].startswith(rare))

if __name__ == '__main__':
    unittest.main()