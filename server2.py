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
import time

N = 5

serverName = '127.0.0.1'
lock = threading.Lock()

serverTCPList = []
connectionSocketList = []

data = open("A2_small_file.txt", "rb")
file = data.read()
data.close()

hash = hashlib.md5(file)

def initialTransfer(i):
    global file, serverTCPList
    print("Initial Transfer Started")
    serverSocketTCP = socket(AF_INET, SOCK_STREAM)
    while True:
        try:
            serverSocketTCP.bind(('', 12000+i))
            break
        except:
            continue
    serverSocketTCP.listen(1)
    with lock:
        serverTCPList.append(serverSocketTCP)
    try:
        conn, addr = serverSocketTCP.accept()
        connectionSocketList.append(conn)
        a = i
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
                print(j)
                conn.send(file[j*1024:(j+1)*1024])
    except:
        traceback.print_exc()
        print("Error in initial transfer")

cacheDict = {}
stack = []

def handleRequest(i):
    global connectionSocketList
    serverUDP = socket(AF_INET, SOCK_DGRAM)
    while True:
        try:
            serverUDP.bind(('', 13000+i))
            break
        except:
            continue
    while True:
        print("Waiting for request")
        try:
            message, clientAddress = serverUDP.recvfrom(1024)
        except Exception as ex:
            print(ex)
            traceback.print_exc()
            print("Error in handle request")
        print("Request received")
        index = int(message.decode())
        if(index in cacheDict):
            connectionSocketList[i].send(cacheDict[index])
            stack.remove(index)
            stack.append(index)
        else:
            for j in range(N):
                if(j != i):
                    serverUDP.sendto(message, (serverName, 15000+j))
                    checkMessage, clientAddress = serverUDP.recvfrom(1024)
                    if(checkMessage.decode() == "HAVE"):
                        print("Have")
                        data = connectionSocketList[j].recv(1024)
                        print("Data received")
                        connectionSocketList[i].send(data)
                        if(index in stack):
                            stack.remove(index)
                            stack.append(index)
                        else:
                            # if(cacheDict.__len__() < N):
                                cacheDict[index] = data
                                stack.append(index)
                            # else:
                            #     cacheDict.pop(stack[0])
                            #     stack.pop(0)
                            #     cacheDict[index] = data
                            #     stack.append(index)
                        break
                    else:
                        continue
threads = []
for i in range(N):
    thread = threading.Thread(target=initialTransfer, args=(i,))
    thread.start()
    threads.append(thread)

for i in range(N):
    threads[i].join()

print("Initial Transfer Completed")
del(file)



for i in range(N):
    threading.Thread(target=handleRequest, args=(i,)).start()            