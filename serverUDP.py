from email import message
import hashlib
from socket import *
import sys
from tabnanny import check
import threading
import math
import random
import os
import traceback
from urllib import request
import time

lock = threading.Lock()

data = open("A2_small_file.txt", "rb")
file = data.read()
data.close()
# print(len(file))

hash = hashlib.md5(file).hexdigest()

serverName = '127.0.0.1'

N = 3

connSendDict = {}
connRequestDict = {}
UDPSendDict = {}
UDPRequestDict = {}

def initialTransfer(i:int):
    global file, N
    print("Initial Transfer Started")
    TCPSend = socket(AF_INET, SOCK_STREAM)
    TCPRequest = socket(AF_INET, SOCK_STREAM)
    UDPRequest = socket(AF_INET, SOCK_DGRAM)
    UDPSend = socket(AF_INET, SOCK_DGRAM)
    while True:
        try:
            TCPSend.bind(('', 11000+i))
            TCPRequest.bind(('', 12000+i))
            UDPRequest.bind(('', 13000+i))
            UDPSend.bind(('', 10000+i))
            break
        except:
            continue
    TCPSend.listen(1)
    TCPRequest.listen(1)
    UDPRequestDict[i] = UDPRequest
    UDPSendDict[i] = UDPSend
    try:
        connSend, addrSend = TCPSend.accept()
        connRequest, addrRequest = TCPRequest.accept()
        connSendDict[i] = connSend
        connRequestDict[i] = connRequest
        a = i
        x = math.ceil(len(file)/1024)
        # print(x)
        # if(a < N-1):
        message = "{z} + {y} + {w} + {u}".format(z=a*int(x/N), y=(a+1)*int(x/N), w = x, u = hash)
        print(message)
        connSend.send(message.encode())
        message = connRequest.recv(1024).decode()
        while message != "Done":
            j = int(message)
            print("Client "+str(i)+" Chunk: "+str(j))
            if(j!=x-1):
                UDPSend.sendto(file[j*1024:(j+1)*1024], (serverName, 16000+i))
            else:
                UDPSend.sendto(file[j*1024:], (serverName, 16000+i))
            message = connRequest.recv(1024).decode()                 
        # else:
        #     message = "{z} + {y} + {w} + {u}".format(z=a*int(x/N), y=x, w = x, u = hash)
        #     print(message)
        #     connSend.send(message.encode())
        #     time.sleep(0.2)
        #     for j in range(a*int(x/N), x):
        #         print(a,' ',j)
        #         if(j == x-1):
        #             conn.send(file[j*1024:])
        #         else:
        #             conn.send(file[j*1024:(j+1)*1024])
            # conn.send(file[x-1*1024:])
    except:
        traceback.print_exc()
        print("Error in initial transfer")

cacheDict = {}
stack = []

def handleRequest(i:int):
    global N, connRequestDict, connSendDict
    print("Handle Request Started")
    TCPRequest = connRequestDict[i]
    TCPSend = connSendDict[i]
    UDPRequest = UDPRequestDict[i]
    UDPSend = UDPSendDict[i]
    while True:
        message = TCPRequest.recv(1024).decode()
        data = ""
        while(message!="GOT"):
            index = int(message)
            if(index in cacheDict):
                UDPSend.send(cacheDict[index])
                message = TCPRequest.recv(1024).decode()
                stack.remove(index)
                stack.append(index)
            else:
                for j in range(N):
                    if(j!=i):
                        connSendDict[j].send(str(index).encode())
                        checkMessage = connRequestDict[j].recv(1024).decode()
                        
                        if(checkMessage == "HAVE"):
                            while True:
                                try:
                                    data, addr = UDPRequest.recvfrom(1024)
                                    connSendDict[j].send("GOT".encode())
                                    break
                                except:
                                    continue
                            UDPSend.sendto(data, (serverName, 16000+i))
                            message = TCPRequest.recv(1024).decode()
                            break
        if(index in stack):
            stack.remove(index)
            stack.append(index)
        else:
            if(cacheDict.__len__() < N):
                cacheDict[index] = data
                stack.append(index)
            else:
                cacheDict.pop(stack[0])
                stack.pop(0)
                cacheDict[index] = data
                stack.append(index)


threads = []
for i in range(N):
    thread = threading.Thread(target=initialTransfer, args=(i,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("Initial Transfer Completed")

del file

for i in range(N):
    threading.Thread(target=handleRequest, args=(i,)).start()