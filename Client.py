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
                stub = pb2_grpc.ServiceStub(channel)
                message = pb2.IdentifyMessage()
                service = stub.Identify(message).service
            elif type == "get_info":
                if service == "Node":
                    message = pb2.GetFingerTableMessage()
                    print(message)
                    response = stub.NodeGetFingerTable(message)
                    print(response)
                else:
                    message = pb2.GetChordInfoMessage()
                    print(message)
                    response = stub.RegistryGetChordInfo(message)
                    print(response) 
            elif type == "save":
                message = pb2.SaveMessage(key=str(rest[0]), text=str(rest[1]))
                print(message)
                response = stub.NodeSave(message)
                print(response)
            elif type == "remove":
                message = pb2.RemoveMessage(key=str(rest[0]))
                print(message)
                response = stub.NodeRemove(message)
                print(response)
            elif type == "find":
                message = pb2.FindMessage(key=str(rest[0]))
                print(message)
                response = stub.NodeFind(message)
                print(response)
            elif type == 'quit':
                print("Shutting Down\n")
                sys.exit(0)
            else:
                print(f'"{type}" is not a valid method.\n')        
        except KeyboardInterrupt:
            print("Terminating\n")
            sys.exit(0)
            

