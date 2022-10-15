import random
import chord_pb2_grpc
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2
import grpc
import os
import sys
from concurrent import futures


REGISTRY_HOST = (sys.argv[1]).split(':')[0]
REGISTRY_PORT = (sys.argv[1]).split(':')[1]
MAX_CHORD_SIZE = int(sys.argv[2])

NODES_MAX_NUM = 2 ** MAX_CHORD_SIZE - 1
NODES_COUNT = 0
CHORD = {}


def  get_successor(node_id):
    for i in range(node_id, NODES_MAX_NUM+1):
        if i in CHORD:
            return i
    for i in range(0, node_id):
        if i in CHORD:
            return i
    return -1


def get_predecessor(node_id):
    for i in range(node_id-1, 0, -1):
        if i in CHORD:
            return i
    for i in range(NODES_MAX_NUM, node_id-1, -1):
        if i in CHORD:
            return i
    return -1


class Handler(pb2_grpc.ServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def RegistryRegister(self, request, context):
        global CHORD, NODES_COUNT
        
        #Input
        ipaddr = request.ipaddr
        port = request.port

        #check if chord is full
        if NODES_COUNT == NODES_MAX_NUM:
            return pb2.RegisterMessageResponse(**{"success": False, "id": -1, "m":MAX_CHORD_SIZE})

        generated_id = random.randint(0, NODES_MAX_NUM)
        while CHORD.get(generated_id): #check if the id already exists
            generated_id = random.randint(0, NODES_MAX_NUM)
        CHORD[generated_id] = f'{ipaddr}:{port}'
        NODES_COUNT += 1
        return pb2.RegisterMessageResponse(**{"success": True, "id": generated_id, "m": MAX_CHORD_SIZE})

    def RegistryDeregister(self, request, context):
        global CHORD, NODES_COUNT
        node_id = request.id
        sucess = False
        message = "Id does not exist"
        if CHORD.get(node_id):
            del CHORD[node_id]
            NODES_COUNT -= 1
            sucess = True
            message = "Node deregistered"
        return pb2.DeregisterMessageResponse(**{"success": sucess, "message": message})

    def RegistryGetChordInfo(self, request, context):
        chord_info = []
        for k, v in CHORD.items():
            chord_info.append({"first": k, "second": v})
        return pb2.GetChordInfoMessageResponse(**{"nodes": chord_info})

    def RegistryPopulateFingerTable(self, request, context):
        node_id = request.id
        FT = []
        for i in range(1, MAX_CHORD_SIZE+1):
            tmp = get_successor((node_id + 2 ** (i - 1))% (2 ** MAX_CHORD_SIZE)) 
            if tmp != -1 and not {"first":tmp, "second": CHORD.get(tmp)} in FT:
                FT.append({"first": tmp, "second": CHORD.get(tmp)})
        predecessor = get_predecessor(node_id)
        return pb2.PopulateFingerTableRegistryMessageResponse(**{"pred": predecessor, "nodes": FT})

    def Identify(self, request, context):
        return pb2.IdentifyMessageResponse(**{"service": "Registry"})


if __name__ == "__main__":
    random.seed(0)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chord_pb2_grpc.add_ServiceServicer_to_server(Handler(), server)
    server.add_insecure_port(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Registry shutting down\n")
        os._exit(1)
