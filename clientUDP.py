import hashlib
from socket import *
import threading
from random import *
import traceback
import time

N = 3

serverName = '127.0.0.1'
lock = threading.Lock()

class Client:
    def __init__(self,a):
        self.a = a
        self.TCPSend = socket(AF_INET, SOCK_STREAM)
        self.TCPRequest = socket(AF_INET, SOCK_STREAM)
        self.UDPRequest = socket(AF_INET, SOCK_DGRAM)
        self.UPDSend = socket(AF_INET, SOCK_DGRAM)
        # while True:
        #     try:
        #         self.TCPSend.bind(('', 14000+a))
        #         break
        #     except:
        #         pass
        # while True:
        #     try:
        #         self.TCPRequest.bind(('', 15000+a))
        #         break
        #     except:
        #         pass
        # while True:
        #     try:
        #         self.UDPRequest.bind(('', 16000+a))
        #         break
        #     except:
        #         pass
        # while True:
        #     try:
        #         self.UPDSend.bind(('', 17000+a))
        #         break
        #     except:
        #         pass
        
        self.TCPRequest.bind(('', 15000+a))
        self.UDPRequest.bind(('', 16000+a))
        self.UPDSend.bind(('', 17000+a))
        self.TCPSend.bind(('', 14000+a))
        
        # self.UDPRequest.settimeout(100)
        # self.clientSocketUDPRequest.settimeout(2)
        self.fileSize = 0
        self.chunkRange = []
        self.dict = {}
        self.remaining = []
        self.hash = ""

def initialTransfer(i:int,client:Client):
    print("Initial Transfer Started")
    client.TCPSend.connect((serverName, 12000+i))   
    client.TCPRequest.connect((serverName, 11000+i))
    print("Connected")
    message = client.TCPRequest.recv(1024).decode()
    chunkRange = message.split(" + ")
    client.chunkRange = chunkRange
    print('client',' ',i,' ',chunkRange)
    client.fileSize = int(chunkRange[2])
    for j in range(client.fileSize):
        client.remaining.append(j)

    modifiedMessage = bytearray()

    for j in range(int(chunkRange[0]), int(chunkRange[1])):
        while True:
            try:
                client.TCPSend.send(str(j).encode())
                data, serverAddress = client.UDPRequest.recvfrom(1024)
                print("Client "+str(i)+" Chunk: "+str(j)+"Range: "+str(chunkRange[0])+" "+str(chunkRange[1]))
                modifiedMessage += data
                client.dict[j] = data
                client.remaining.remove(j)
                break
            except:
                print("Client "+str(i)+" Chunk: "+str(j)+"Range: "+str(chunkRange[0])+" "+str(chunkRange[1]))
                print("Retransmitting")
                continue     
        
    client.TCPSend.send("Done".encode())
    print("Client: ",i," ", hashlib.md5(modifiedMessage).hexdigest())
    client.hash = hashlib.md5(modifiedMessage).hexdigest()

def UDPRequest(client:Client, i:int):
    print("UDP Request Started")
    shuffle(client.remaining)
    while len(client.remaining) > 0:
        chunk = client.remaining[0]
        try:
            client.TCPSend.send(str(chunk).encode())
            data, serverAddress = client.UDPRequest.recvfrom(1024)
            client.TCPSend.send("GOT".encode())
            print("Client "+str(i)+" Chunk: "+str(chunk)+"Range: "+str(client.chunkRange[0])+" "+str(client.chunkRange[1]))
        except:
            continue
        client.dict[chunk] = data
        client.remaining.remove(chunk)
    
    bytearrayMessage = bytearray()
    for j in range(client.fileSize):
        bytearrayMessage += client.dict[j]
    print("Client: ",i," ", hashlib.md5(bytearrayMessage).hexdigest())
    filename = "clientUDP" + str(i) + ".txt"
    with open(filename, "wb") as f:
        f.write(bytearrayMessage)

def UDPSend(client:Client, i:int):
    print("UDP Send Started")
    while True:
        message = client.TCPRequest.recv(1024).decode()
        index = int(message)
        try:
            if(index in client.dict):
                client.TCPSend.send("HAVE".encode())
                checkMessage = client.TCPRequest.recv(1024).decode()
                while(checkMessage != "GOT"):
                    client.UPDSend.sendto(client.dict[index], (serverName, 13000+i))
                    checkMessage = client.TCPRequest.recv(1024).decode()
            else:
                client.TCPSend.send("DONT".encode())
        except Exception as e:
            traceback.print_exc()
            continue

    

threads = []
Clients = []
for i in range(N):
    client = Client(i)
    Clients.append(client)
    thread = threading.Thread(target=initialTransfer, args=(i,client))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

print("Initial Transfer Done")

time.sleep(1)

for i in range(N):
    threading.Thread(target=UDPRequest, args=(Clients[i],i)).start()
    threading.Thread(target=UDPSend, args=(Clients[i],i)).start()



