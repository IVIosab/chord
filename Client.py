import grpc
import chord_pb2 as pb2
import chord_pb2_grpc as pb2_grpc
import sys
import os

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
            command_args = input_buffer.split()[1:] #command arguments 

            if command == "connect": #connecting to a service
                addr = command_args[0]
                try:
                    channel = grpc.insecure_channel(addr)
                    stub = pb2_grpc.ServiceStub(channel)
                    message = pb2.IdentifyMessage()
                    #what happens if we try to connect to a non-open ip TODO
                    service = stub.Identify(message).service #identify if we're connected to a Node or Registry
                    print(f'Connected to {service}\n')
                except grpc.RpcError:
                    print(f'Connection failed\n')
            elif command == 'quit': #quitting client
                print("Shutting Down\n")
                os._exit(1)
            elif service != "Unknown":
                if command == "get_info": #get_chord_info || get_finger_table
                    if service == "Node": #get_finger table
                        message = pb2.GetFingerTableMessage()
                        response = stub.NodeGetFingerTable(message)
                        for item in response.nodes:
                            print(f'{item.first}:\t{item.second}')
                    else: #get_chord_info
                        message = pb2.GetChordInfoMessage()
                        response = stub.RegistryGetChordInfo(message)
                        for item in response.nodes:
                            print(f'{item.first}:\t{item.second}')
                    print()
                elif service == "Node": 
                    if command == "save": 
                        key = str(command_args[0])
                        key = key.lstrip('"').rstrip('"') #Remove the quotes 
                        text = str(' '.join(command_args[1:]))
                        message = pb2.SaveMessage(key=key, text=text)
                        response = stub.NodeSave(message)
                        print(f'{response.success}, {response.message}\n')
                    elif command == "remove":
                        key = str(command_args[0])
                        message = pb2.RemoveMessage(key=key)
                        response = stub.NodeRemove(message)
                        print(f'{response.success}, {response.message}\n')
                    elif command == "find":
                        key = str(command_args[0])
                        message = pb2.FindMessage(key=key)
                        response = stub.NodeFind(message)
                        print(f'{response.success}, {response.message}\n')
                    else:        
                        print(f'You can not run "{command}" on a Node.\n')     
                else:
                    print(f'You can not run "{command}" on a Registry.\n')     
            else:
                print(f'You have to connect to a service first.\n')
        except KeyboardInterrupt:
            print("Terminating\n")
            os._exit(1)
            

