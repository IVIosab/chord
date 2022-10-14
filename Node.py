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
M = -1
ID = -1
PRED = -1
SUCC = -1
FINGER_TABLE = []
DATA = {}

def between_pred_and_me(target_id):
    if PRED == ID:
        return True
    for i in range (2**M):
        if ((PRED+i)%(2**M)) == ID+1:
            return False
        if ((PRED+i)%(2**M)) == target_id:
            return True

def between_me_and_succ(target_id):
    for i in range (2**M):
        if ((ID+i)%(2**M)) == SUCC+1:
            return False
        if ((ID+i)%(2**M)) == target_id:
            return True

def between_X_and_Y(target_id, X, Y):
    for i in range (2**M):
        if ((X+i)%(2**M)) == Y+1:
            return False
        if ((X+i)%(2**M)) == target_id:
            return True

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

def add_data(nodes):
    global DATA
    for item in nodes:
        k = item.first
        v = item.second
        DATA[k] = v
    return True

def get_next(target_id):
    next_node = FINGER_TABLE[len(FINGER_TABLE)-1]
    for i in range(len(FINGER_TABLE)-1):
        cur = FINGER_TABLE[i]
        next = FINGER_TABLE[i+1]
        if between_X_and_Y(target_id, cur[0], next[0]):
            next_node = cur
    return next_node

def get_finger_table():
    finger_table = []
    for i in range(len(FINGER_TABLE)):
        finger_table.append({"first": FINGER_TABLE[i][0], "second": FINGER_TABLE[i][1]})
    return finger_table

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
        stub = pb2_grpc.ServiceStub(channel)
        
        #precalcs
        target_id = get_target(key)
        
        #output init
        success = False
        message = ""  
        id = -1
        
        #algo
        if between_pred_and_me(target_id) : #Works
            if DATA.get(target_id):
                success = False
                message = f'{key} is already exist in node {ID}'
            else:
                success = True
                id = ID
                message = f'{key} is saved in node {ID}'
                DATA[target_id] = text
        elif between_me_and_succ(target_id): #Works
            succ_node = FINGER_TABLE[0]
            
            channel = grpc.insecure_channel(f'{succ_node[1]}')
            stub = pb2_grpc.ServiceStub(channel)
            
            message = pb2.SaveMessage(key=key, text=text)
            response = stub.NodeSave(message)
            
            if response.success:
                success = True
                id = response.id
                message = response.message
            else:
                success = False
                message = response.message
        else:
            next_node = get_next(target_id)
            
            channel = grpc.insecure_channel(f'{next_node[1]}')
            stub = pb2_grpc.ServiceStub(channel)
            
            message = pb2.SaveMessage(key=key, text=text)
            response = stub.NodeSave(message)
            
            if response.success:
                success = True
                id = response.id
                message = response.message
            else:
                success = False
                message = response.message
        reply = {"success": success, "id": id, "message": message}
        print(reply)
        return pb2.SaveMessageResponse(**reply)

    def NodeRemove(self, request, context):
        global DATA
        #input
        key = request.key
        
        #connection
        channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
        stub = pb2_grpc.ServiceStub(channel)
        
        #precalcs
        target_id = get_target(key)
        
        #output init
        success = False
        message = ""  
        id = -1
        
        #algo
        if between_pred_and_me(target_id) :
            if DATA.get(target_id):
                success = True
                id = ID
                message = f'{key} is deleted from node {ID}'
                del(DATA[target_id])
            else:
                success = False
                message = f'{key} does not exist in node {ID}'
        elif between_me_and_succ(target_id):
            succ_node = FINGER_TABLE[0]
            
            channel = grpc.insecure_channel(f'{succ_node[1]}')
            stub = pb2_grpc.ServiceStub(channel)
            
            message = pb2.RemoveMessage(key=key)
            response = stub.NodeRemove(message)
            
            if response.success:
                success = True
                id = response.id
                message = response.message
            else:
                success = False
                id = -1
                message = response.message
        else:
            next_node = get_next(target_id)
            
            channel = grpc.insecure_channel(f'{next_node[1]}')
            stub = pb2_grpc.ServiceStub(channel)
            
            message = pb2.RemoveMessage(key=key)
            response = stub.NodeRemove(message)
            
            if response.success:
                success = True
                id = response.id
                message = response.message
            else:
                success = False
                id = -1
                message = response.message
        reply = {"success": success, "id": id, "message": message}
        print(reply)
        return pb2.RemoveMessageResponse(**reply)

    def NodeFind(self, request, context):
        #input
        key = request.key
        
        #connection
        channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
        stub = pb2_grpc.ServiceStub(channel)
        
        #precalcs
        target_id = get_target(key)
        
        #output init
        success = False
        message = ""  
        id = -1
        ipaddr = ""
        port = ""
        
        #algo
        if between_pred_and_me(target_id) : #Works
            if DATA.get(target_id):
                success = True
                id = ID
                message = f'{key} is saved in node {ID}'
                ipaddr = NODE_HOST
                port = NODE_PORT
            else:
                success = False
                message = f'{key} does not exist in node {ID}'
        elif between_me_and_succ(target_id): #Works
            succ_node = FINGER_TABLE[0]
            
            channel = grpc.insecure_channel(f'{succ_node[1]}')
            stub = pb2_grpc.ServiceStub(channel)
            
            message = pb2.FindMessage(key=key)
            response = stub.NodeFind(message)
            
            if response.success:
                success = True
                id = response.id
                message = response.message
                ipaddr = response.ipaddr
                port = response.port
            else:
                success = False
                id = -1
                message = response.message
                ipaddr = ""
                port = ""
        else:
            next_node = get_next(target_id)
            
            channel = grpc.insecure_channel(f'{next_node[1]}')
            stub = pb2_grpc.ServiceStub(channel)
            
            message = pb2.FindMessage(key=key)
            response = stub.NodeFind(message)
            
            if response.success:
                success = True
                id = response.id
                message = response.message
                ipaddr = response.ipaddr
                port = response.port
            else:
                success = False
                id = -1
                message = response.message
                ipaddr = ""
                port = ""
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
        nodes = request.nodes
        reply = {"success": add_data(nodes)}
        return pb2.GetDataFromSuccessorMessageResponse(**reply)
        


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

    if ID != SUCC:
        channel = grpc.insecure_channel(f'{FINGER_TABLE[0][1]}')
        stub = pb2_grpc.ServiceStub(channel)
        message = pb2.GetDataFromSuccessorMessage(id=ID)
        response = stub.GetDataFromSuccessor(message)
        add_data(response.nodes)
    
    print(f'assigned node_id={ID}, successor_id={SUCC}, predecessor_id={PRED}')   

    #starting a server to receive queries 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ServiceServicer_to_server(Handler(), server)
    server.add_insecure_port(f'{NODE_HOST}:{NODE_PORT}')
    server.start()
    channel = grpc.insecure_channel(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
    stub = pb2_grpc.ServiceStub(channel)
    while True:
        try: 
            x=server.wait_for_termination(1)
            if x:
                populate_ft(stub)
                print(f'assigned node_id={ID}, successor_id={SUCC}, predecessor_id={PRED}')
                print(f'FINGER_TABLE: {FINGER_TABLE}')
                print(f'DATA: {DATA}')
        except grpc.RpcError:
            print("Registry Terminated")
            sys.exit(0)
        except KeyboardInterrupt:
            #transfer data to successor TODO
            message = pb2.DeregisterMessage(id=ID)
            response = stub.RegistryDeregister(message)
            

            print("Terminating\n")
            sys.exit(0)

if __name__ == "__main__":
    random.seed(0)
    serve()