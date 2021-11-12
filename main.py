from concurrent import futures
import logging
import grpc
import bankworld_pb2
import bankworld_pb2_grpc
import json
from Customer import Customer
from multiprocessing import Process
import time
import threading

finalmsg = ""
    
# instantiate Customer object, create stub, and execute Events
def Cust(custid, custevents):
    global finalmsg
    
    cust = Customer(custid, custevents)
    out = cust.createStub()

    finalmsg = cust.executeEvents()
    # print to string
    print ("PRE-FINAL MESSAGE is " + finalmsg)
    with open("output.json", "a") as thefile:
        thefile.write("\n" + finalmsg)
 
# Opening JSON file
f = open('input.json',)
data = json.load(f)

p = list()

# main function
if __name__ == '__main__':
    logging.basicConfig()

    # print starting messages to output.json
    for z in data:
        if z['type'] == 'client':
            with open("output.json", "a") as myfile1:
                myfile1.write("Starting server. Listening on port " + str(50050+z['id'])+"\n")
    with open("output.json", "a") as myfile1:
        myfile1.write("[")            
    
    # send appropriate events to all customers
    for i in data:
        if i['type'] == 'client':
            proc = Process(target=Cust, args=(i['id'], str(i['events']),))
            proc.start()
            p.append(proc)
    for proc in p:
        proc.join()    
    with open("output.json", "a") as thefile:
        # add closing bracket
        thefile.write("]")

    # convert output to valid JSON format    
    reading_file = open("output.json", "r")
    new_file_content = ""
    for line in reading_file:
        new_file_content += line.replace(",]", "\n]")
    reading_file.close()

    writing_file = open("output.json", "w")
    writing_file.write(new_file_content)
    writing_file.close()

f.close()
