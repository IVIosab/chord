from ast import Global
from glob import glob
import grp
import random
import sys
from traceback import print_tb
import grpc
import zlib
import chord_pb2 as pb2
import chord_pb2_grpc as pb2_grpc
from concurrent import futures

REGISTRY_HOST = (sys.argv[1]).split(':')[0]
REGISTRY_PORT = (sys.argv[1]).split(':')[1]
NODE_HOST = (sys.argv[2]).split(':')[0]
NODE_PORT = (sys.argv[2]).split(':')[1]
M = -1
ID = -1
PRED = -1
SUCC = -1
FINGER_TABLE = []
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

def get_chord_map():
    key_to_node = {}
    pointer_to_ft_item = 0
    for i in range(2**M):
        cur_key = i+ID%(2**M)
        key_to_node[cur_key] = pointer_to_ft_item
        if cur_key==FINGER_TABLE[pointer_to_ft_item][0]:
            pointer_to_ft_item+=1
        if pointer_to_ft_item > len(FINGER_TABLE):
            break
    return key_to_node
        
def get_target(key):
    hash_value = zlib.adler32(key.encode())
    target_id = hash_value % 2**M
    return target_id
        

class Handler(pb2_grpc.ServiceServicer):
    def __init__(self, *args, **kwargs):
        pass  
    
    def NodeSave(self, request, context):
        global DATA
        #input
        key = request.key
        text = request.text
        #connection
        channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
        stub = pb2_grpc.ServiceServicer(channel)
        #precalcs
        target_id = get_target(key)
        chord_map = get_chord_map()
        
        #output init
        success = False
        message = ""  
        id = -1

        #algo
        if chord_map[target_id] == my_id :
            if DATA.get(key) == text:
                success = False
                message = f'{key} is already exist in node {ID}'
            else:
                success = True
                message = f'{key} is saved in node {ID}'
                DATA[key] = text
        elif chord_map[target_id] == my_succ:
            succ_node = ()
            for node in FINGER_TABLE:
                if node[0] == ID:
                    succ_node = node
            channel = grpc.insecure_channel(f'{succ_node[1]}')
            stub = pb2_grpc.ServiceServicer(channel)
            response = stub.NodeSave(key, text)
            if response.success:
                success = True
                id = response.id
                message = response.message
            else:
                success = False
                id = -1
                message = response.message
        else:
            next_node = FINGER_TABLE[len(FINGER_TABLE)-1]
            for i in range(len(FINGER_TABLE)):
                cur = FINGER_TABLE[i]
                if cur[0] == target_id:
                    next_node = FINGER_TABLE[i-1]
            channel = grpc.insecure_channel(f'{next_node[1]}')
            stub = pb2_grpc.ServiceServicer(channel)
            response = stub.NodeSave(key, text)
            if response.success:
                success = True
                id = response.id
                message = response.message
            else:
                success = False
                id = -1
                message = response.message
        reply = {"success": success, "id": id, "message": message}
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

    def Identify(self, request, context):
        reply = {"service": "Node"}
        return pb2.IdentifyMessageResponse(**reply)

    
    def GetInfo(self,request,context):
        reply = {"nodes": get_finger_table()}
        return pb2.GetInfoMessageResponse(**reply)


def populate_ft(stub):
    global FINGER_TABLE,SUCC, PRED
    message = pb2.PopulateFingerTableRegistryMessage(id=ID)
    response = stub.RegistryPopulateFingerTable(message)
    temp_ft = []
    for item in response.nodes:
        chord_id = item.chord_id
        chord_ip_address = item.chord_ip_address 
        temp_ft.append((chord_id, chord_ip_address))        
    FINGER_TABLE = temp_ft
    PRED = response.pred
    SUCC = FINGER_TABLE[0][0]
    
def register(stub):
    global ID, PRED, SUCC, M
    message = pb2.RegisterMessage(ipaddr=NODE_HOST, port=NODE_PORT)
    response = stub.RegistryRegister(message)
    ID = response.id
    PRED = ID
    SUCC = ID
    M = response.m
    my_success = response.success
    if not my_success:
        print(f'Registring Unsuccessful')

def serve():
    global M, ID, PRED, SUCC, FINGER_TABLE
    #connecting to registry to register ourselves 
    channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
    stub = pb2_grpc.ServiceStub(channel)
    
    #registering
    register(stub)    
    #populate finger table
    populate_ft(stub)
    
    print(f'assigned node_id={ID}, successor_id={SUCC}, predecessor_id={PRED}')   

    #starting a server to receive queries 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ServiceServicer_to_server(Handler(), server)
    server.add_insecure_port(f'{NODE_HOST}:{NODE_PORT}')
    server.start()
    while True:
        try:
            while True:
                try: 
                    x=server.wait_for_termination(1)
                    if x:
                        populate_ft(stub)
                        print(f'assigned node_id={ID}, successor_id={SUCC}, predecessor_id={PRED}')
                except grpc.RpcError:
                    print("Registry Terminated")
                    sys.exit(0)
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
    serve()