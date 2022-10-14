import random
import chord_pb2_grpc
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2
import grpc
import sys
from concurrent import futures


REGISTRY_HOST = (sys.argv[1]).split(':')[0]
REGISTRY_PORT = (sys.argv[1]).split(':')[1]
MAX_CHORD_SIZE = int(sys.argv[2])

NODES_MAX_NUM = 2 ** MAX_CHORD_SIZE - 1
curr_nodes_size = 0
chord = {}




def  get_successor(node_id):
    for i in range(node_id, NODES_MAX_NUM+1):
        if not chord.get(i) is None:
            return i
    for i in range(0, node_id):
        if not chord.get(i) is None:
            return i
    return -1


def get_predecessor(node_id):
    for i in range(node_id-1, 0, -1):
        if not chord.get(i) is None:
            return i
    for i in range(NODES_MAX_NUM, node_id-1, -1):
        if not chord.get(i) is None:
            return i
    return -1


class Handler(pb2_grpc.ServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def RegistryRegister(self, request, context):
        global chord, curr_nodes_size
        
        ipaddr = request.ipaddr
        port = request.port

        if curr_nodes_size == NODES_MAX_NUM:
            return -1, "The chord ring is full already"

        generated_id = random.randint(0, NODES_MAX_NUM)
        while chord.get(generated_id):
            generated_id = random.randint(0, NODES_MAX_NUM)
        chord[generated_id] = f'{ipaddr}:{port}'
        curr_nodes_size += 1
        
        m = MAX_CHORD_SIZE

        if generated_id != -1:
            reply = {"success": True, "id": generated_id, "m": m}
        else:
            reply = {"success": False, "id": generated_id, "m": m}

        return pb2.RegisterMessageResponse(**reply)

    def RegistryDeregister(self, request, context):
        global chord, curr_nodes_size
        node_id = request.id
        sucess = False
        message = "Id does not exist"
        if chord.get(node_id):
            del chord[node_id]
            curr_nodes_size -= 1
            sucess = True
            message = "Node deregistered"
        reply = {"success": sucess, "message": message}
        return pb2.DeregisterMessageResponse(**reply)

    def RegistryGetChordInfo(self, request, context):
        chord_info = []
        for k, v in chord.items():
            chord_info.append({"chord_id": k, "chord_ip_address": v})
        print(chord_info)
        reply = {"nodes": chord_info}
        return pb2.GetChordInfoMessageResponse(**reply)

    def RegistryPopulateFingerTable(self, request, context):
        print(chord)
        node_id = request.id
        FT = []
        for i in range(1, MAX_CHORD_SIZE+1):
            tmp = get_successor((node_id + 2 ** (i - 1))% (2 ** MAX_CHORD_SIZE)) 
            if tmp != -1 and not {"chord_id":tmp, "chord_ip_address": chord.get(tmp)} in FT:
                FT.append({"chord_id": tmp, "chord_ip_address": chord.get(tmp)})
        predecessor = get_predecessor(node_id)
        reply = {"pred": predecessor, "nodes": FT}
        return pb2.PopulateFingerTableRegistryMessageResponse(**reply)

    def Identify(self, request, context):
        reply = {"service": "Registry"}
        return pb2.IdentifyMessageResponse(**reply)


if __name__ == "__main__":
    random.seed(0)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chord_pb2_grpc.add_ServiceServicer_to_server(Handler(), server)
    server.add_insecure_port(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Registry shutting down")
