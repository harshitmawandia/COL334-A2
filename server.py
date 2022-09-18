from email import message
import hashlib
from socket import *
import sys
import threading
import math
import random
import os
import traceback
from urllib import request

lock = threading.Lock()

data = open("A2_small_file.txt", "rb")
file = data.read()
data.close()
# print(len(file))

hash = hashlib.md5(file)

N = 3

serverSocketTCPList = []
serverSocketUDPList = []

for i in range(N):
    serverPortInitialTransfer = 12000 + i
    serverSocketInitialTransfer = socket(AF_INET, SOCK_STREAM)
    serverSocketInitialTransfer.bind(('127.0.0.1', serverPortInitialTransfer))
    serverSocketInitialTransfer.listen(N)
    serverSocketTCPList.append(serverSocketInitialTransfer)

# serverPortTCPRequest = 12002
# serverSocketTCPRequest = socket(AF_INET, SOCK_STREAM)
# serverSocketTCPRequest.bind(('127.0.0.1', serverPortTCPRequest))

setSent = set()
completedSet = set()
fileSize = len(file)
i = 0
j = 0
def handle_client(serverSocketInitialTransfer):
    global j 
    global setSent, completedSet

    serverSocketInitialTransfer.settimeout(5)
    try:
        conn, addr = serverSocketInitialTransfer.accept()
        a = int(conn.recv(1024).decode())
        x = math.ceil(len(file)/1024)
        # print(x)
        if(a < N-1):
            message = "{z} + {y} + {w} + {u}".format(z=a*int(x/N), y=(a+1)*int(x/N), w = x, u = hash.hexdigest())
            print(message)
            conn.send(message.encode())
            for j in range(a*int(x/N), (a+1)*int(x/N)):
                conn.send(file[j*1024:(j+1)*1024])
        else:
            message = "{z} + {y} + {w} + {u}".format(z=a*int(x/N), y=x, w = x, u = hash.hexdigest())
            print(message)
            conn.send(message.encode())
            for j in range(a*int(x/N), x):
                conn.send(file[j*1024:(j+1)*1024])
        conn.close()
        completedSet.add(a)
    except:
        pass
    for i in range(N):
        if(i not in completedSet):
            handle_client(serverSocketInitialTransfer,)
            break


print("The server is ready to send")

threads = []

for j in range(N):
    thread = threading.Thread(target=handle_client, args=(serverSocketTCPList[j],))
    i+=1
    threads.append(thread)
    thread.start()

x = True
while x:
    y = True
    for i in range(N):
        if (i not in completedSet):
            y = False
    if y:
        x = False
    

totalSize = math.ceil(len(file)/1024)

del file

cahceDict = {}
stack = []

chunkDict = {}
for i in range(fileSize):
    chunkDict[i] = []
# requestDict = {}
completedList = []

clientAddresses = []

for i in range(N):
    clientAddresses.append(('127.0.0.1', 15000 + i))


for i in range(N):
    serverPortUDPRequest = 13000 + i
    serverSocketUDPRequest = socket(AF_INET, SOCK_DGRAM)
    serverSocketUDPRequest.bind(('127.0.0.1', serverPortUDPRequest))
    serverSocketUDPList.append(serverSocketUDPRequest)
    serverSocketUDPRequest.settimeout(2)

completed = 0

def getChunks(chunkID,serverSocketTCPRequest):
    global chunkDict
    # global requestDict
    l = chunkDict[chunkID]
    random.shuffle(l)
    b = True;
    while b:
        try:
            for i in l:
                print(i)
                serverSocketTCPRequest.connect((i[0],i[1]))
                serverSocketTCPRequest.send("SEND".encode())
                serverSocketTCPRequest.send(str(chunkID).encode())
                data = serverSocketTCPRequest.recv(1024)
                serverSocketTCPRequest.close()
                b = False
                return data
        except Exception as ex:
            # print(traceback.format_exc())
            # print(l)
            pass
        
    

def sendChunks(chunkID,serverSocketTCPRequest, clientAddress):
    global cahceDict
    # global requestDict
    b = True;
    while b:
        try:
            if(chunkID not in cahceDict):
                getChunks(chunkID,serverSocketTCPRequest)
            else:
                data = cahceDict[chunkID]
                serverSocketTCPRequest.connect(clientAddress[0],clientAddress[1]-1000)
                serverSocketTCPRequest.send("GET".encode())
                serverSocketTCPRequest.send(str(chunkID).encode())
                serverSocketTCPRequest.send(data)
                serverSocketTCPRequest.close()
                b = False
        except Exception as ex:
            print(traceback.format_exc())



def sendChunkRequest(i, message, clientAddress):
    random.shuffle(clientAddresses)
    serverSocketTCPRequest = serverSocketTCPList[i]
    b = True
    while(b):
        print("Length=",len(clientAddresses))
        for x in clientAddresses:
            if x != clientAddress:
                while True:
                    try:
                        serverSocketUDPRequest.sendto(message, x)
                        checkMessage, serverAddress = serverSocketUDPRequest.recvfrom(1024)
                        print(type(serverAddress))
                        break
                    except Exception as ex:
                        print(traceback.format_exc())
                        if(type(ex).__name__ == "timeout"):
                            sendChunkRequest(i, message, clientAddress)
                            return
                        continue
                print(checkMessage.decode())
                if checkMessage.decode() == "True":
                    chunkDict[int(message.decode())].append((serverAddress[0],serverAddress[1]-1000))
                    while True:
                        try:
                            data = getChunks(int(message.decode()),serverSocketTCPRequest)
                            break
                        except:
                            continue
                    if(cahceDict.__len__() < N):
                        cahceDict[int(message.decode())] = data
                        stack.append(int(message.decode()))
                    else:
                        cahceDict.pop(stack[0])
                        stack.pop(0)
                        cahceDict[int(message.decode())] = data
                        stack.append(int(message.decode()))
                    return


def handleChunkRequest(serverSocketUDPRequest):
    global completed
    while True:
        while True:
            try:
                message, clientAddress = serverSocketUDPRequest.recvfrom(1024)
                serverSocketUDPRequest.sendto("Received".encode(), clientAddress)
                # if(clientAddress not in clientAddresses):
                #     with lock:
                #         clientAddresses.append(clientAddress)
                break
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                m = template.format(type(ex).__name__, ex.args)
                print(m)
                print(traceback.format_exc())
                continue
        print(message.decode())
        if message.decode() == "Done":
            with lock:
                completed += 1
                completedList.append(clientAddress)
                handleChunkRequest(serverSocketUDPRequest)
                return
        i = serverSocketUDPList.index(serverSocketUDPRequest)
        serverSocketTCPRequest = serverSocketTCPList[i]
        if int(message.decode()) in cahceDict:
            while True:
                try:
                    sendChunks(int(message.decode()),serverSocketTCPRequest, clientAddress)
                    break
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    m = template.format(type(ex).__name__, ex.args)
                    print(m)
                    print(traceback.format_exc())
                    continue
            stack.remove(int(message.decode()))
            stack.append(int(message.decode()))
        else:
            sendChunkRequest(i, message, clientAddress)
            while True:
                try:
                    sendChunks(int(message.decode()),serverSocketTCPRequest, clientAddress)
                    # requestDict[int(message.decode())].remove(clientAddress[1]-1000)
                    break
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    m = template.format(type(ex).__name__, ex.args)
                    print(m)  
                    print(traceback.format_exc())  
                    continue
            if(int(message.decode()) in stack):
                stack.remove(int(message.decode()))
            stack.append(int(message.decode()))
            break
            

UDPthreads = []
        
for i in range(N):
    thread = threading.Thread(target=handleChunkRequest, args=(serverSocketUDPList[i],))
    UDPthreads.append(thread)
    thread.start()

def checkCompletion():
    global completed
    while True:
        if completed == N:
            sys.exit()

thread = threading.Thread(target=checkCompletion)
thread.start()
