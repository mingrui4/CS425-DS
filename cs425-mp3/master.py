import time
import random
import collections

class Master():
    def __init__(self):
        self.lookup = collections.defaultdict(list)#key:fname, value:[[replicas],[ver,...],[time,...]]
        self.members = []

    def update_memeber(self, members):
        self.members = members

    def set_replicas(self,fname):
        if not self.lookup[fname]:
            self.lookup[fname].append([])
        while len(self.lookup[fname][0]) < 4:
            id = random.randint(0, len(self.members)-1)
            ip = self.members[id][0]
            if ip not in self.lookup[fname][0] and ip != self.members[0][0]:
                self.lookup[fname][0].append(ip)

    def put_response(self,fname):
        t = time.time()
        if not self.lookup[fname]:
            self.set_replicas(fname)
            self.lookup[fname].append([1])
            self.lookup[fname].append([t])
        else:
            ver = self.lookup[fname][1][-1]
            self.lookup[fname][1].append(ver+1)
            self.lookup[fname][2].append(t)
        return self.lookup[fname]

    def update_lookup(self, members):
        new_lookup = collections.defaultdict(list)
        member_list = [x[0] for x in members]
        for fname, val in self.lookup.items():
            alive = []
            if val:
                for replica in val[0]:
                    if replica in member_list:
                        alive.append(replica)
                if len(alive) < 4:
                    self.lookup[fname][0] = list(alive)
                    self.set_replicas(fname)
                    new_replicas = list(set(self.lookup[fname][0]).difference(set(alive)))
                    new_lookup[fname] = [alive[0],self.lookup[fname][1],new_replicas]#use alive[0] for re-relicated
        return new_lookup

    def get_version(self,fname):
        return self.lookup[fname][1] if not self.lookup[fname] else -1

    def put_lookup(self, fname, value):
        self.lookup[fname] = value

    def get_lookup(self, fname):
        return self.lookup[fname]

    def delete_file(self, fname):
        if not self.lookup[fname]:
            return []
        temp = self.lookup[fname][0]
        del self.lookup[fname]
        return temp














