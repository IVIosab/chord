import grpc
import chord_pb2 as pb2
import chord_pb2_grpc as pb2_grpc
import sys

SERVER_HOST = (sys.argv[1]).split(':')[0]
SERVER_PORT = (sys.argv[1]).split(':')[1]

if __name__ == '__main__':
    channel = grpc.insecure_channel(f'{SERVER_HOST}:{SERVER_PORT}')
    stub = pb2_grpc.ServiceStub(channel)
    while True:
        try:
            inp = input("> ")
            if len(inp) <= 1:
                continue
            type = inp.split()[0]
            query = ' '.join(inp.split()[1:])
            if type == "connect":
                print('connect')    
            elif type == "get_info":
                print('get_info')
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
            

