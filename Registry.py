import random
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2
import grpc
import sys
random.seed(0)
print (random.random())

REGISTRY_HOST = (sys.argv[1]).split(':')[0]
REGISTRY_PORT = (sys.argv[1]).split(':')[1]
MAX_CHORD_SIZE = sys.argv[2]

def register(ipaddr, port):
    print(f'register {ipaddr} <> {port}')

def deregister(id):
    print(f'deregister {id}')

def populate_finger_table(id):
    print(f'populate finger table {id}')

def get_chord_info():
    print(f'get chord info')

class Handler(pb2_grpc.Service):
    def __init__(self, *args, **kwargs):
        pass  
    
    def RegistryRegister(self, request, context):
        ipaddr = request.ipaddr
        port = request.port
        success = True
        id = 1 #random ? 
        m = 1 #MAX_CHORD_SIZE ? 
        if True: #put a condition after you make the dictonary 
            success = True
        reply = {"success": success, "id": id, "m": m}
        return pb2.RegisterMessageResponse(**reply)

    def RegistryDeregister(self, request, context):
        id = request.id
        success = True
        message = ""
        if True: #some condition for sucess
            success=True
        reply = {"success": success, "message":message}
        return pb2.DeregisterMessageResponse(**reply)


if __name__ == "__main__":
    print("something")
