import hashlib
from multiprocessing.connection import wait
# from msilib.schema import Class
from socket import *
import threading
from random import *
import traceback
import time

N = 5

serverName = '127.0.0.1'
lock = threading.Lock()

class Client:
    def __init__(self,a):
        self.a = a
        self.clientSocketTCP = socket(AF_INET, SOCK_STREAM)
        self.clientSocketTCP.bind(('', 14000+a))
        self.UDPRequest = socket(AF_INET, SOCK_DGRAM)
        self.UDPRequest.bind(('', 15000+a))
        self.UPDSend = socket(AF_INET, SOCK_DGRAM)
        self.UPDSend.bind(('', 16000+a))
        # self.UDPRequest.settimeout(100)
        # self.clientSocketUDPRequest.settimeout(2)
        self.fileSize = 0
        self.chunkRange = []
        self.dict = {}
        self.remaining = []
        self.hash = ""

def initialTransfer(i:int,client:Client):
    print("Initial Transfer Started")
    client.clientSocketTCP.connect((serverName, 12000+i))    
    print("Connected")
    message = client.clientSocketTCP.recv(1024).decode()
    chunkRange = message.split(" + ")
    client.chunkRange = chunkRange
    print('client',' ',i,' ',chunkRange)
    client.fileSize = int(chunkRange[2])
    for j in range(client.fileSize):
        client.remaining.append(j)

    modifiedMessage = bytearray()

    for j in range(int(chunkRange[0]), int(chunkRange[1])):
        data = client.clientSocketTCP.recv(1024)
        print("Client "+str(i)+" Chunk: "+str(j)+"Range: "+str(chunkRange[0])+" "+str(chunkRange[1]))
        modifiedMessage += data
        client.dict[j] = data
        client.remaining.remove(j)

    print("Client: ",i," ", hashlib.md5(modifiedMessage).hexdigest())
    client.hash = hashlib.md5(modifiedMessage).hexdigest()
    

def UDPRequest(client:Client, i:int):
    print("UDP Request Started")
    shuffle(client.remaining)
    while len(client.remaining) > 0:
        try:
            client.UDPRequest.sendto(str(client.remaining[0]).encode(), (serverName, 13000+client.a))
            data = client.clientSocketTCP.recv(1024)
            client.dict[client.remaining[0]] = data
            client.remaining.remove(client.remaining[0])
            print("Client "+str(client.a)+" Remaining: "+str(len(client.remaining)))
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            m = template.format(type(ex).__name__, ex.args)
            print(m)
            continue

    bytearrayMessage = bytearray()
    for j in range(client.fileSize):
        bytearrayMessage += client.dict[j]
    print("Client: ",i," ", hashlib.md5(bytearrayMessage).hexdigest())
    filename = "client" + str(i) + ".txt"
    with open(filename, "wb") as f:
        f.write(bytearrayMessage)
    

def UPDSend(client:Client, i:int):
    print("UDP Send Started")
    while(True):
        try:
            message, serverAddress = client.UDPRequest.recvfrom(1024)
            # print("Client "+str(i)+" ", message.decode(),"Received")
            index= int(message.decode())
            if(index in client.dict):
                client.UDPRequest.sendto("HAVE".encode(), serverAddress)
                # print("Client "+str(i)+" ", message.decode(),"Received","Have Sent",len(client.dict))
                client.clientSocketTCP.send(client.dict[index])
                # print("Client "+str(i)+" ", message.decode(),"Received","Have Sent","Data Sent")                
            else:
                client.UDPRequest.sendto("DONT".encode(), serverAddress)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            m = template.format(type(ex).__name__, ex.args)
            print(m)
            continue

Clients = []

def startClient(i:int):
    client = Client(i)
    Clients.append(client)
    initialTransfer(i,client)

threads = []


for i in range(N):
    thread = threading.Thread(target=startClient, args=(i,))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

print("Initial Transfer Completed")

time.sleep(1)

for i in range(N):
    threading.Thread(target=UDPRequest, args=(Clients[i],i,)).start()
    threading.Thread(target=UPDSend, args=(Clients[i],i,)).start()

