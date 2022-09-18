import hashlib
from msilib.schema import Class
from socket import *
import threading
from random import *
import traceback

N = 3

serverName = '127.0.0.1'
lock = threading.Lock()

class Client:
    def __init__(self,a):
        self.a = a
        self.clientSocketTCPInitialTransfer = socket(AF_INET, SOCK_STREAM)
        self.clientSocketUDPRequest = socket(AF_INET, SOCK_DGRAM)
        self.clientSocketTCPRequest = socket(AF_INET, SOCK_STREAM)
        self.clientSocketUDPRequest.settimeout(2)
        self.fileSize = 0
        self.chunkRange = []
        self.dict = {}
        self.fileArray = []

initialTransferDone = 0

def initialTransfer(client:Client):
    print("Initial Transfer Started")
    while True:
        try:
            client.clientSocketTCPInitialTransfer.connect((serverName, randint(12000, 12000+N)))
            break
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            m = template.format(type(ex).__name__, ex.args)
            print(m)
            continue
    print("Connected")
    client.clientSocketTCPInitialTransfer.send(str(client.a).encode())
    message = client.clientSocketTCPInitialTransfer.recv(1024).decode()
    chunkRange = message.split(" + ")
    client.chunkRange = chunkRange
    print(chunkRange)
    client.fileSize = int(chunkRange[2])

    modifiedMessage = bytearray()

    for i in range(int(chunkRange[0]), int(chunkRange[1])):
        data = client.clientSocketTCPInitialTransfer.recv(1024)
        modifiedMessage += data
        client.dict[i] = data

    for i in range(client.fileSize):
        client.fileArray.append(i)
        shuffle(client.fileArray)

    print(hashlib.md5(modifiedMessage).hexdigest())
    client.clientSocketTCPInitialTransfer.close()
    with lock:
        global initialTransferDone
        initialTransferDone += 1


completed = 0

def handleTCP(client:Client):
    global N
    while(True):
        try:
            # print(clientList.index(client), client.clientSocketTCPRequest.getsockname())
            client.clientSocketTCPRequest.listen(2*N)
            client.clientSocketTCPRequest.settimeout(2)
            connectionSocket, addr = client.clientSocketTCPRequest.accept()
            print("Connected")
            message = connectionSocket.recv(1024).decode()
            if message.decode() == "SEND":
                i = connectionSocket.recv(1024).decode()
                connectionSocket.send(client.dict[int(i)])
                connectionSocket.close()
            elif message.decode() == "GET":
                i = connectionSocket.recv(1024).decode()
                client.dict[int(i)] = connectionSocket.recv(1024)
                connectionSocket.close()
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            m = template.format(type(ex).__name__, ex.args)
            print(m,client.clientSocketTCPRequest.getsockname())
            # print(traceback.format_exc())
            continue

def requestChunk(client:Client):
    global completed
    while (len(client.dict) < client.fileSize):
        for i in client.fileArray:
            if i not in client.dict:
                message = str(i)
                # print(clientList.index(client), "Requesting Chunk", i)              
                while True:
                    try:
                        serverPortUDPRequest = randint(13000, 13000+N)
                        serverPortTCPRequest = serverPortUDPRequest - 1000
                        client.clientSocketUDPRequest.sendto(message.encode(), (serverName, serverPortUDPRequest))
                        # print(clientList.index(client))
                        checkMessage, serverAddress = client.clientSocketUDPRequest.recvfrom(1024)
                        if(checkMessage.decode() == "Received"):
                            break
                    except Exception as ex:
                        # print(traceback.format_exc())
                        requestChunk(client)
                        if(type(ex).__name__ == "timeout"):
                            requestChunk(client)
                            return
                        continue
    finalFile = bytearray()
    for i in range(client.fileSize):
        finalFile += client.dict[i]
    print(hashlib.md5(finalFile).hexdigest(), " == ", client.chunkRange[3]," = ", hashlib.md5(finalFile).hexdigest()==client.chunkRange[3])
    filename = "client" + str(clientList.index(client)) + ".txt"
    with open(filename, "wb") as f:
        f.write(finalFile)
    message = "Done"
    while True:
        try:
            client.clientSocketUDPRequest.sendto(message.encode(), (serverName, randint(13000, 13000+N)))
            checkMessage, serverAddress = client.clientSocketUDPRequest.recvfrom(1024)
            if(checkMessage.decode() == "Received"):
                break
        except Exception as ex:
            print(traceback.format_exc())
            continue
    completed+=1



def sendChunk(client:Client):
    while completed < N:
        while True:
            try:
                message, serverAddress = client.clientSocketUDPRequest.recvfrom(1024)
                if(message.decode() != "Received" and message.decode() != "True"):
                    break
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                m = template.format(type(ex).__name__, ex.args)
                # print(m)
                # print(traceback.format_exc())
                continue
        # print(message.decode(), client.chunkRange[0], client.chunkRange[1])
        serverPortTCPRequest = serverAddress[1] - 1000
        if int(message.decode()) in client.dict:
            # print("Sending Chunk")
            while True:
                try:
                    client.clientSocketUDPRequest.sendto("True".encode(), serverAddress)
                    break
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    m = template.format(type(ex).__name__, ex.args)
                    print(m)
                    print(traceback.format_exc())
                    continue
        else:
            while True:
                try:
                    client.clientSocketUDPRequest.sendto("False".encode(), serverAddress)
                    break
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    m = template.format(type(ex).__name__, ex.args)
                    # print(m)
                    # print(traceback.format_exc())
                    continue
    print("Done")

def starting(client):
        initialTransfer(client,)
        print("Initial Transfer Done")
        threadRequest = threading.Thread(target=requestChunk, args=(client,))
        threadRequest.start()
        threadSend = threading.Thread(target=sendChunk, args=(client,))
        threadSend.start()
        threadTCP = threading.Thread(target=handleTCP, args=(client,))
        threadTCP.start()

clientList = []

for i in range(N):
    client = Client(i)
    client.clientSocketTCPRequest.bind(('127.0.0.1', i + 14000))
    client.clientSocketUDPRequest.bind(('127.0.0.1', i + 15000))
    clientList.append(client) 

for i in range(N):
    thread = threading.Thread(target=starting, args=(clientList[i],))
    thread.start()