from email import message
from glob import glob
import grpc
import chord_pb2 as pb2
import chord_pb2_grpc as pb2_grpc
import sys


channel = -1
stub = -1
service = "Unknown"
if __name__ == '__main__':
    while True:
        try:
            inp = input("> ")
            if len(inp) <= 1:
                continue
            type = inp.split()[0]
            rest = inp.split()[1:]
            query = ' '.join(inp.split()[1:])
            if type == "connect":
                print(rest[0])
                channel = grpc.insecure_channel(rest[0])
                stub = pb2_grpc.RegistryServiceStub(channel)
                message = pb2.IdentifyMessage()
                try:
                    service = stub.Identify(message).service
                except:
                    service = "Node"
                print(service)
                if service == "Node":
                    stub = pb2_grpc.NodeServiceStub
            elif type == "get_info":
                message = pb2.GetInfoMessage()
                info = stub.GetInfo(message).nodes
                print(info)
            elif type == "save":
                print('save')
            elif type == "remove":
                print('remove')
            elif type == "find":
                print('find')
            elif type == 'quit':
                print("Shutting Down\n")
                sys.exit(0)
            else:
                print(f'"{type}" is not a valid method.\n')        
        except KeyboardInterrupt:
            print("Terminating\n")
            sys.exit(0)
            

