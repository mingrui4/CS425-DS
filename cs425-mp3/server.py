from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import subprocess
import base64

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create server
tcpserver = SimpleXMLRPCServer(('0.0.0.0', 8080), requestHandler=RequestHandler, logRequests=False, allow_none=True)
tcpserver.register_introspection_functions()

class Server():
    def __init__(self, master, node, log):
        self.master = master
        self.node = node
        self.log = log

    # Query distributed log files
    def query(self, file, parttern):
        cmd = 'cat ' + file + ' | grep ' + parttern
        res = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, encoding='utf-8')
        # using base64 for trasferring data in xmlRPC
        result = base64.b64encode(res.stdout.encode('utf-8'))
        return result

    # act as the master
    # return 4 replicas and the final version about the request file to requester
    def file_info(self, fname):
        result = {}
        result_save = self.master.put_response(fname)
        result['replicas'] = result_save[0]
        result['version'] = result_save[1][-1]
        return result

    def get_info(self,fname):
        result = {}
        result_save = self.master.get_lookup(fname)
        if not result_save:
            return []
        result['replicas'] = result_save[0]
        result['version'] = result_save[1][-1]
        return result

    # return the replicas info about the deleted file
    def delete_sdfsfile_info(self, fname):
        return self.master.delete_file(fname)

    #act as the slave
    def re_replicate(self, replica_ip, localname, sdfsname, ver):
        self.node.put_replica(replica_ip, localname, sdfsname, ver)
        return True

    def get_file(self, fname, ver):
        return self.node.get_data(fname, ver)

    def delete_node_file(self, fname):
        self.node.delete_node_file(fname)
        return True

    def reelect_master(self, new_master):
        return self.node.reelect_master(new_master)

    def update_version(self, fname, ver):
        self.node.update_version(fname, ver)
        return True

    def get_versions_file(self, fname, num, ver):
        return self.node.get_versions_data(fname, num, ver)

    def vote(self, voter):
        return self.node.get_vote(voter)
