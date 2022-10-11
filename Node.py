import random
import chord_pb2 as pb2
import chord_pb2_grpc as pb2_grpc
import sys
random.seed(0)
print (random.random())

REGISTRY_HOST = (sys.argv[1]).split(':')[0]
REGISTRY_PORT = (sys.argv[1]).split(':')[1]
NODE_HOST = (sys.argv[2]).split(':')[0]
NODE_PORT = (sys.argv[2]).split(':')[1]


class Handler(pb2_grpc.Service):
    def __init__(self, *args, **kwargs):
        pass  
    
    def NodeSave(self, request, context):
        key = request.ipaddr
        text = request.port
        success = True
        id = 1  
        message = ""  
        if True: #some condition for success 
            success = True
        reply = {"success": success, "id": id, "message": message}
        return pb2.SaveMessageResponse(**reply)

    def NodeRemove(self, request, context):
        key = request.key
        success = True
        message = ""
        if True: #some condition for sucsess
            success=True
        reply = {"success": success, "message":message}
        return pb2.DeregisterMessageResponse(**reply)


def get_finger_table():
    print("get finger table")

def save(key, text):
    print(f'save {key} <> {text}')

def remove(key):
    print(f'remove {key}')

def find(key):
    print(f'find {key}')

def quit():
    print('quit')

if __name__ == "__main__":

    print("something")
