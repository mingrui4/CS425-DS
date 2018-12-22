import collections
import os

class Slave():
    def __init__(self):
        self.lookup = collections.defaultdict(list) #key:fname, value:[ver,...]

    def get_versions(self, fname):
        return self.lookup[fname]

    def update_version(self, fname, version):
        self.lookup[fname].append(version)

    def get_lookup(self):
        return self.lookup

    def delete_file(self,fname):
        del self.lookup[fname]
        os.system("rm sdfs/"+fname + "--*")

    def ls_file(self):
        return self.lookup


