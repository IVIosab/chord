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
            input_buffer = input("> ")
            if len(input_buffer) <= 1: #User entered an empty line
                continue

            command = input_buffer.split()[0] #command type
            command_args = input_buffer.split()[1:]
            query = ' '.join(input_buffer.split()[1:])
            if command == "connect":
                addr = command_args[0]
                channel = grpc.insecure_channel(addr)
                stub = pb2_grpc.ServiceStub(channel)
                message = pb2.IdentifyMessage()
                service = stub.Identify(message).service #identify if we're connected to a Node or Registry
                print(f'Connected to {service}')
            elif command == "get_info":
                if service == "Node":
                    message = pb2.GetFingerTableMessage()
                    response = stub.NodeGetFingerTable(message)
                    for item in response.nodes:
                        print(f'{item.first}:\t{item.second}')
                else:
                    message = pb2.GetChordInfoMessage()
                    response = stub.RegistryGetChordInfo(message)
                    for item in response.nodes:
                        print(f'{item.first}:\t{item.second}')
            elif command == "save":
                key = str(command_args[0])
                text = str(' '.join(command_args[1:]))
                message = pb2.SaveMessage(key=key, text=text)
                response = stub.NodeSave(message)
                print(f'{response.success}, {response.message}')
            elif command == "remove":
                key = str(command_args[0])
                message = pb2.RemoveMessage(key=key)
                response = stub.NodeRemove(message)
                print(f'{response.success}, {response.message}')
            elif command == "find":
                key = str(command_args[0])
                message = pb2.FindMessage(key=key)
                response = stub.NodeFind(message)
                print(f'{response.success}, {response.message}')
            elif command == 'quit':
                print("Shutting Down\n")
                sys.exit(0)
            else:
                print(f'"{command}" is not a valid method.\n')        
        except KeyboardInterrupt:
            print("Terminating\n")
            sys.exit(0)
            

