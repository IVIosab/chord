import random
import sys
import os
import grpc
import zlib
import chord_pb2 as pb2
import chord_pb2_grpc as pb2_grpc
from concurrent import futures

DEBUG = 0 #put 1 to see info about node every second

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

#registry connection
def registry_connection():
    channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
    stub = pb2_grpc.ServiceStub(channel)
    return stub

#node connection
def node_connection(node):
    channel = grpc.insecure_channel(f'{node}')
    stub = pb2_grpc.ServiceStub(channel)
    return stub

#delegating save 
def delegate_save(key, text, stub):
    message = pb2.SaveMessage(key=key, text=text)
    response = stub.NodeSave(message)
    
    if response.success:
        return True, response.id, response.message
    else:
        return False, -1, response.message

#delegating remove
def delegate_remove(key, stub):
    message = pb2.RemoveMessage(key=key)
    response = stub.NodeRemove(message)
    
    if response.success:
        return True, response.id, response.message
    else:
        return False, -1, response.message

#delegating find
def delegate_find(key, stub):
    message = pb2.FindMessage(key=key)
    response = stub.NodeFind(message)
    
    if response.success:
        return True, response.id, response.message, response.ipaddr, response.port
    else:
        return False, -1, response.message, "", ""

#Naive implementaion for checking if a target id is between two other id's X and Y in a circular structure
def between_X_and_Y(target_id, X, Y):
    for i in range (2**M):
        if ((X+i)%(2**M)) == Y+1:
            return False
        if ((X+i)%(2**M)) == target_id:
            return True

#Responsible for removing data that should be assigned to node with id=id and returns that data
def remove_data(id):
    global DATA
    data = []
    for item in list(DATA.items()):
        k = item[0]
        v = item[1]
        if not between_X_and_Y(k, id+1, ID):
            data.append({'first': k, 'second':v})
            del(DATA[k])
    return data

#Responsible for adding some data to our global dict
def add_data(keys, values):
    global DATA
    for i in range(len(keys)):
        DATA[keys[i]] = values[i]
    return True

#Get the furthest node in my Finger_table that doesn't overstep the target
def get_next(target_id):
    next_node = FINGER_TABLE[len(FINGER_TABLE)-1]
    for i in range(len(FINGER_TABLE)-1):
        cur = FINGER_TABLE[i]
        next = FINGER_TABLE[i+1]
        if between_X_and_Y(target_id, cur[0], next[0]):
            next_node = cur
    return next_node

#Returns finger_table im specific representation for grpc
def get_finger_table():
    finger_table = []
    for i in range(len(FINGER_TABLE)):
        finger_table.append({"first": FINGER_TABLE[i][0], "second": FINGER_TABLE[i][1]})
    return finger_table

#calculates the hash value of a key and returns it module 2^M which will be the target id
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
        stub = registry_connection()
        
        #precalcs
        target_id = get_target(key)
        
        #output init
        success = False
        message = ""  
        id = -1
        
        #algo
        if between_X_and_Y(target_id, PRED, ID) or PRED == ID:#between me and predecssor
            if DATA.get(target_id):
                message = f'{key} is already exist in node {ID}'
            else:
                success = True
                id = ID
                message = f'{key} is saved in node {ID}'
                DATA[target_id] = text
        elif between_X_and_Y(target_id, ID, SUCC):#between me and successor
            succ_node = FINGER_TABLE[0]
            stub = node_connection(succ_node[1])
            success, id, message = delegate_save(key, text, stub)
        else:
            next_node = get_next(target_id)
            stub = node_connection(next_node[1])    
            success, id, message = delegate_save(key, text, stub)

        reply = {"success": success, "id": id, "message": message}
        print(reply)
        return pb2.SaveMessageResponse(**reply)

    def NodeRemove(self, request, context):
        global DATA
        #input
        key = request.key
        
        #connection
        stub = registry_connection()
        
        #precalcs
        target_id = get_target(key)
        
        #output init
        success = False
        message = ""  
        id = -1
        
        #algo
        if between_X_and_Y(target_id, PRED, ID) or PRED == ID:
            if DATA.get(target_id):
                success = True
                id = ID
                message = f'{key} is removed from node {ID}'
                del(DATA[target_id])
            else:
                message = f'{key} does not exist in node {ID}'
        elif between_X_and_Y(target_id, ID, SUCC):
            succ_node = FINGER_TABLE[0]
            stub = node_connection(succ_node[1])
            success, id, message = delegate_remove(key, stub)
        else:
            next_node = get_next(target_id)
            stub = node_connection(next_node[1])
            success, id, message = delegate_remove(key, stub)

        reply = {"success": success, "id": id, "message": message}
        print(reply)
        return pb2.RemoveMessageResponse(**reply)

    def NodeFind(self, request, context):
        #input
        key = request.key
        
        #connection
        stub = registry_connection()
        
        #precalcs
        target_id = get_target(key)
        
        #output init
        success = False
        message = ""  
        id = -1
        ipaddr = ""
        port = ""
        
        #algo
        if between_X_and_Y(target_id, PRED, ID) or PRED == ID: 
            if DATA.get(target_id):
                success = True
                id = ID
                message = f'{key} is saved in node {ID}'
                ipaddr = NODE_HOST
                port = NODE_PORT
            else:
                message = f'{key} does not exist in node {ID}'
        elif between_X_and_Y(target_id, ID, SUCC): 
            succ_node = FINGER_TABLE[0]
            stub = node_connection(succ_node[1])
            success, id, message, ipaddr, port = delegate_find(key, stub)
        else:
            next_node = get_next(target_id)
            stub = node_connection(next_node[1])
            success, id, message, ipaddr, port = delegate_find(key, stub)

        reply = {"success": success, "id": id, "message": message, "ipaddr": ipaddr, "port": port}
        print(reply)
        return pb2.FindMessageResponse(**reply)

    def NodeGetFingerTable(self, request, context):
        reply = {"nodes": get_finger_table()}
        return pb2.GetFingerTableMessageResponse(**reply)

    def Identify(self, request, context):
        reply = {"service": "Node"}
        return pb2.IdentifyMessageResponse(**reply)

    def GetDataFromSuccessor(self, request, context):
        id = request.id
        reply = {"nodes": remove_data(id)}
        return pb2.GetDataFromSuccessorMessageResponse(**reply)
        
    def GiveDataToSuccessor(self, request, context):
        keys = request.keys
        values = request.values
        reply = {"success": add_data(keys, values)}
        return pb2.GiveDataToSuccessorMessageResponse(**reply)
        
#populates the finger table by calling registry's populate finger table
#updates global predecessor, successor, and finger table
def populate_ft(stub):
    global FINGER_TABLE,SUCC, PRED
    message = pb2.PopulateFingerTableRegistryMessage(id=ID)
    response = stub.RegistryPopulateFingerTable(message)
    temp_ft = []
    for item in response.nodes:
        first = item.first
        second = item.second 
        temp_ft.append((first, second))        
    FINGER_TABLE = temp_ft
    PRED = response.pred
    SUCC = FINGER_TABLE[0][0]

#registering the node into the chord, updates global ID and M
def register(stub):
    global ID, PRED, SUCC, M
    message = pb2.RegisterMessage(ipaddr=NODE_HOST, port=NODE_PORT)
    response = stub.RegistryRegister(message)
    ID = response.id
    M = response.m
    success = response.success
    
    return success

def serve():
    global DATA, M, ID, PRED, SUCC, FINGER_TABLE
    #connecting to registry to register ourselves 
    channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
    stub = pb2_grpc.ServiceStub(channel)
    
    #registering
    success = register(stub)  
    if not success:
        print("Unexpected Error")
        return  
    #populate finger table
    populate_ft(stub)
    
    print(f'assigned node_id={ID}, successor_id={SUCC}, predecessor_id={PRED}')   
    
    if ID != SUCC: #basically checking if i'm not the only node in the chord
        channel = grpc.insecure_channel(f'{FINGER_TABLE[0][1]}') #connect to successor
        stub = pb2_grpc.ServiceStub(channel)
        message = pb2.GetDataFromSuccessorMessage(id=ID) 
        response = stub.GetDataFromSuccessor(message)#get the data that should be assignd to me
        keys = []
        values = []
        for item in response.nodes:
            keys.append(item.first)
            values.append(item.second)
        add_data(keys=keys,values=values)
    
    #starting a server to receive queries 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ServiceServicer_to_server(Handler(), server)
    server.add_insecure_port(f'{NODE_HOST}:{NODE_PORT}')
    server.start()

    channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}') #connecting to the registry since we will need to populate finger table every second
    stub = pb2_grpc.ServiceStub(channel)
    while True:
        try: 
            x=server.wait_for_termination(1) #parameter 1 indicates that we have a timeout of 1, which will make x = true if 1 second passes with no requests
            if x: #if 1 second pass then populate the finger table again
                populate_ft(stub)
                if DEBUG == 1:
                    print(f'assigned node_id={ID}, successor_id={SUCC}, predecessor_id={PRED}')
                    print(f'FINGER_TABLE: {FINGER_TABLE}')
                    print(f'DATA: {DATA}')
        except grpc.RpcError: 
            print("Unexpected Error: Registry Terminated")
            os._exit(1)
        except KeyboardInterrupt: #terminating the node
            #deregistering from the registry
            message = pb2.DeregisterMessage(id=ID)
            response = stub.RegistryDeregister(message)
            #transferring my data to the successor
            if ID != SUCC:
                keys = []
                values = []
                for item in list(DATA.items()):
                    k = item[0]
                    v = item[1]
                    keys.append(k)
                    values.append(v)
                    del(DATA[k])
                channel = grpc.insecure_channel(f'{FINGER_TABLE[0][1]}') #connecting with successor
                stub = pb2_grpc.ServiceStub(channel)
                message = pb2.GiveDataToSuccessorMessage(keys=keys, values=values)
                response = stub.GiveDataToSuccessor(message) #giving our data to our successor to keep track of
            print("Terminating\n")
            os._exit(1)

if __name__ == "__main__":
    random.seed(0) #setting random seed to 0 to follow the lab instructions
    serve() 