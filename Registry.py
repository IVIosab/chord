import random

import chord_pb2_grpc
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2
import grpc
import sys
from concurrent import futures

random.seed(0)
print(random.random())

REGISTRY_HOST = (sys.argv[1]).split(':')[0]
REGISTRY_PORT = (sys.argv[1]).split(':')[1]
MAX_CHORD_SIZE = int(sys.argv[2])

NODES_MAX_NUM = 2 ** MAX_CHORD_SIZE - 1
curr_nodes_size = 0
chord = {}


def register(ipaddr, port):
    global chord, curr_nodes_size
    if curr_nodes_size == NODES_MAX_NUM:
        return -1, "The chord ring is full already"

    generated_id = random.randrange(0, NODES_MAX_NUM)
    while chord[generated_id]:
        generated_id = random.randrange(0, NODES_MAX_NUM)
    chord[generated_id] = f'{ipaddr}:{port}'
    chord = sorted(chord)
    curr_nodes_size += 1
    return generated_id, MAX_CHORD_SIZE


def deregister(node_id):
    global chord, curr_nodes_size
    id_from_chord = chord[node_id]
    if id_from_chord:
        del chord[id_from_chord]
        curr_nodes_size -= 1
        chord = sorted(chord)
        return True, "Node deregistered"
    return False, "Id does not exist"


def populate_finger_table(node_id):
    FT = []
    for i in range(1, MAX_CHORD_SIZE):
        tmp = get_successor(node_id + 2 ** (i - 1)) % (2 ** MAX_CHORD_SIZE)
        if tmp != -1 and tmp != node_id:
            FT.append((tmp, chord[tmp]))
    predecessor = get_predecessor(node_id)
    return predecessor, FT


def get_chord_info():
    chord_info = [(k, v) for k, v in chord.items()]
    return chord_info


def get_successor(node_id):
    for i in range(node_id, NODES_MAX_NUM):
        if not chord.get(i) is None:
            return i
    for i in range(0, node_id):
        if not chord.get(i) is None:
            return i
    return -1


def get_predecessor(node_id):
    for i in range(node_id, 0):
        if not chord.get(i) is None:
            return i
    for i in range(NODES_MAX_NUM, node_id):
        if not chord.get(i) is None:
            return i
    return -1


class Handler(pb2_grpc.RegistryServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def RegistryRegister(self, request, context):
        ipaddr = request.ipaddr
        port = request.port

        generated_id, m = register(ipaddr, port)

        if generated_id != -1:
            reply = {"success": True, "id": generated_id, "m": m}
        else:
            reply = {"success": False, "id": generated_id, "m": m}

        return pb2.RegisterMessageResponse(**reply)

    def RegistryDeregister(self, request, context):
        state, message = deregister(request.id)
        reply = {"success": state, "message": message}
        return pb2.DeregisterMessageResponse(**reply)

    def RegistryGetChordInfo(self, request, context):
        reply = dict(get_chord_info())
        return pb2.GetChordInfoMessageResponse(**reply)

    def RegistryGetFingerTable(self, request, context):
        reply = dict(populate_finger_table(request.id))
        return pb2.GetFingerTableFromRegistryMessageResponse(**reply)


if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chord_pb2_grpc.add_GreeterServicer_to_server(Handler(), server)
    server.add_insecure_port(f'{REGISTRY_HOST}:{REGISTRY_PORT}')
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Registry shutting down")
