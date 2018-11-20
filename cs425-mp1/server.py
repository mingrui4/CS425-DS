from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import subprocess
import base64

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create server
server = SimpleXMLRPCServer(('localhost', 8080), requestHandler=RequestHandler)
server.register_introspection_functions()

#Query distributed log files
def query(file, parttern):
    cmd = 'cat ' + file + ' | grep ' + parttern
    res = subprocess.run(cmd, stdout = subprocess.PIPE, shell = True, encoding = 'utf-8')
    #using base64 for trasferring data in xmlRPC
    result = base64.b64encode(res.stdout.encode('utf-8'))
    return result
server.register_function(query,'query')

# Run the server's main loop
server.serve_forever()