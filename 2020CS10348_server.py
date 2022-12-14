
import hashlib
from socket import *
import sys
import threading
import math
import random
import os
import traceback
import time

N = 5

serverName = '127.0.0.1'
lock = threading.Lock()

serverTCPList = []
connectionSocketList = {}

data = open("A2_small_file.txt", "rb")
file = data.read()
data.close()

hash = hashlib.md5(file).hexdigest()

begin = time.time()

def initialTransfer(i):
    global file, serverTCPList
    # print("Initial Transfer Started")
    serverSocketTCP = socket(AF_INET, SOCK_STREAM)
    serverSocketTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
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
        connectionSocketList[i] = conn
        a = i
        # print(len(file))
        x = math.ceil(len(file)/1024)
        # print(x)
        if(a < N-1):
            message = "{z} + {y} + {w} + {u}".format(z=a*int(x/N), y=(a+1)*int(x/N), w = x, u = hash)
            # print(message)
            conn.send(message.encode())
            time.sleep(0.5)
            for j in range(a*int(x/N), (a+1)*int(x/N)):
                conn.send(file[j*1024:(j+1)*1024])
        else:
            message = "{z} + {y} + {w} + {u}".format(z=a*int(x/N), y=x, w = x, u = hash)
            # print(message)
            conn.send(message.encode())
            time.sleep(0.5)
            for j in range(a*int(x/N), x):
                # print(a,' ',j)
                if(j == x-1):
                    conn.send(file[j*1024:])
                else:
                    conn.send(file[j*1024:(j+1)*1024])
            # conn.send(file[x-1*1024:])
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
        # print("Waiting for request")
        try:
            message, clientAddress = serverUDP.recvfrom(1024)
        except Exception as ex:
            print(ex)
            traceback.print_exc()
            print("Error in handle request")
        # print("Request received")
        if(message.decode() == "DONE"):
            break
        index = int(message.decode())
        if(index in cacheDict):
            connectionSocketList[i].send(cacheDict[index])
            with lock:
                if(index in stack):
                    stack.remove(index)
                    stack.append(index)
        else:
            for j in range(N):
                if(j != i):
                    serverUDP.sendto(message, (serverName, 15000+j))
                    checkMessage, clientAddress = serverUDP.recvfrom(1024)
                    if(checkMessage.decode() == "HAVE"):
                        # print("Have")
                        data = connectionSocketList[j].recv(1024)
                        # print("Data received")
                        connectionSocketList[i].send(data)
                        # print("Data sent")
                        if(index in stack):
                            with lock:
                                stack.remove(index)
                                stack.append(index)
                        else:
                            if(cacheDict.__len__() < N):
                                cacheDict[index] = data
                                stack.append(index)
                            else:
                                with lock:
                                    cacheDict.pop(stack[0])
                                    stack.pop(0)
                                    cacheDict[index] = data
                                    stack.append(index)
                        break
threads = []
for i in range(N):
    thread = threading.Thread(target=initialTransfer, args=(i,))
    thread.start()
    threads.append(thread)

for i in range(N):
    threads[i].join()

# print("Initial Transfer Completed")
del(file)

threads = []

for i in range(N):
    thread = threading.Thread(target=handleRequest, args=(i,))
    thread.start()
    threads.append(thread) 

for i in range(N):
    threads[i].join()

for i in range(N):
    connectionSocketList[i].close()
    serverTCPList[i].close()

for i in range(N):
    serverUDP = socket(AF_INET, SOCK_DGRAM)
    serverUDP.sendto("DONE".encode(), (serverName, 15000+i))

end = time.time()
print(end - begin)

# print("All Done")