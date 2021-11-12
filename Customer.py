import grpc
import bankworld_pb2
import bankworld_pb2_grpc
import time

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None

    # TODO: students are expected to create the Customer stub
    def createStub(self):
        channelnumber = 50050+self.id
        channel = grpc.insecure_channel('localhost:'+str(channelnumber))
        self.stub = bankworld_pb2_grpc.BranchStub(channel)
            
        return ("Done creating stub " + str(channelnumber))

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        print ("Executing events.." + self.events)
        response = self.stub.MsgDelivery(bankworld_pb2.BranchRequest(msg=self.events))

        return (response.branch_msg)
