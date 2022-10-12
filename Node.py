from ast import Global
from glob import glob
import random
import sys
import grpc
import zlib
import chord_pb2 as pb2
import chord_pb2_grpc as pb2_grpc
from concurrent import futures

REGISTRY_HOST = (sys.argv[1]).split(':')[0]
REGISTRY_PORT = (sys.argv[1]).split(':')[1]
NODE_HOST = (sys.argv[2]).split(':')[0]
NODE_PORT = (sys.argv[2]).split(':')[1]
M = 0
ID = 0
FINGER_TABLE = {}
DATA = {}

def get_finger_table():
    finger_table = [(k, v) for k, v in FINGER_TABLE.items()]
    return finger_table

def save(key, text):
    print(f'save {key} <> {text}')

def remove(key):
    print(f'remove {key}')

def find(key):
    print(f'find {key}')

class Handler(pb2_grpc.NodeServiceServicer):
    def __init__(self, *args, **kwargs):
        pass  
    
    def NodeSave(self, request, context):
        global DATA
        key = request.key
        text = request.text
        
        hash_value = zlib.adler32(key.encode())
        target_id = hash_value % 2**M
        
        success = False
        message = ""  
        if target_id == ID:
            if DATA.get(key) == text:
                success = False
                message = f'{key} is already exist in node {ID}'
            else:
                success = True
                message = f'{key} is saved in node {ID}'
                DATA[key] = text
        else:
            #lookup
            test = 1
        reply = {"success": success, "id": target_id, "message": message}
        return pb2.SaveMessageResponse(**reply)

    def NodeRemove(self, request, context):
        global DATA
        key = request.key
        hash_value = zlib.adler32(key.encode())
        target_id = hash_value % 2**M
        success = True
        id = 1
        message = ""
        if target_id == ID:
            if DATA.get(key) != None:
                success = True
                del DATA[key]
                message = f'{key} is removed from node {ID}'
            else:
                success = False
                message = f'{key} does not exist in node {ID}'
        else:
            #lookup
            test = 1
        reply = {"success": success, "id": id, "message":message}
        return pb2.DeregisterMessageResponse(**reply)

    def NodeFind(self, request, context):
        global DATA
        key = request.key 
        hash_value = zlib.adler32(key.encode())
        target_id = hash_value % 2**M
        success = True
        id = 1
        message = ""
        ipaddr = ""
        port = ""
        if target_id == ID:
            if DATA.get(key) != None:
                success = True
                del DATA[key]
                message = f'{key} is removed from node {ID}'
            else:
                success = False
                message = f'{key} does not exist in node {ID}'
        else:
            #lookup
            test = 1
        reply = {"success": success, "id": id, "message":message, "ipaddr": ipaddr, "port": port}
        return pb2.FindMessageResponse(**reply)

    def NodeGetFingerTable(self, request, context):
        reply = {"finger_table": get_finger_table()}
        return pb2.GetFingerTableMessageResponse(**reply)

def serve():
    global M, ID
    #connecting to registry to register ourselves 
    channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
    stub = pb2_grpc.RegistryServiceStub(channel)
    #registering
    message = pb2.RegisterMessage(ipaddr=NODE_HOST, port=NODE_PORT)
    response = stub.RegistryRegister(message)
    my_success = response.success
    if not my_success:
        print(f'Registring Unsuccessful')
    ID = response.id
    M = response.m
    message = pb2.GetChordInfoMessage()
    response = stub.RegistryGetChordInfo(message)
    nodes = []
    for node in response:
        nodes.append(node.chord_id)
    nodes = nodes.sort()
    successor = -1
    predecessor = -1
    for i in range(1,len(nodes)-1):
        if nodes[i-1] == ID:
            successor = nodes[i]
        if nodes[i+1] == ID:
            predecessor = nodes[i]
    if successor == -1:
        successor=nodes[0]
    if predecessor == -1:
        predecessor=nodes[len(nodes)-1]
    print(f'assigned node_id={ID}, successor_id={successor}, predecessor_id={predecessor}')   
    
    #starting a server to receive queries 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_NodeServiceServicer_to_server(Handler(), server)
    server.add_insecure_port(f'{NODE_HOST}:{NODE_PORT}')
    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        #Deregistering
        #notify successor
        #transfer data to successor
        #notify predecessor about new successor
        message = pb2.DeregisterMessage(id=ID)
        response = stub.RegistryDeregister(message)
        if response.success:
            print(f'Deregistered Successfully')
        else:
            print(f'Deregistring Unsuccessful: {response.message}')
        print("Terminating\n")
        sys.exit(0)

if __name__ == "__main__":
    random.seed(0)
    print (random.random())
    serve()