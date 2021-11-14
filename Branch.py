from concurrent import futures
import logging
import time
import grpc
import bankworld_pb2
import bankworld_pb2_grpc
import json
from multiprocessing import Process

class Branch(bankworld_pb2_grpc.BranchServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # the Branch's clock
        self.clock = 0
        # iterate the processID of the branches

    # TODO: students are expected to process requests from both Client and Branch

    # create the stubs to all other branches
    def createStubsss(self, branches):
        for i in range(branches):
            if (i+1) != self.id :
                channelnumber = 50050+i+1
                channel = grpc.insecure_channel('localhost:'+str(channelnumber))
                self.stubList.append(bankworld_pb2_grpc.BranchStub(channel))
            else :
                self.stubList.append(None)
        return ("Done creating BRANCH stubsss!!")

    # add the deposit amount to this branch's balance
    def Propagate_Deposit(self, amount, context):
        new_bal = self.balance + int(amount.msg)

        if new_bal >= 0:
            self.balance = new_bal
            depositmsg = "success"
        else :
            depositmsg = "fail"
        return bankworld_pb2.DepositReply(deposit_msg=depositmsg)

    # subtract the withdrawal amount from this branch's balance
    def Propagate_Withdraw(self, prop_msg, context):
#        print ("DEBUGGGGGGGGGG = " + prop_msg.msg)
        msgs = prop_msg.msg.split(',')
        amount_str = msgs[0]
        remote_clk_str = msgs[1]
        event_id = msgs[2]
        amount = int(amount_str)
        remote_clk = int(remote_clk_str)
        Branch.Propagate_Request(self, remote_clk)
        response_to_branch = "\n  {\"id\": " + event_id + ", \"name\": \"withdraw_propagate_request\", \"clock\": " + str(self.clock) + " },"
        Branch.Propagate_Execute(self, -amount)
        response_to_branch += "\n  {\"id\": " + event_id + ", \"name\": \"withdraw_propagate_execute\", \"clock\": " + str(self.clock) + " },"
#        Branch.Propagate_Response(self)
#        response_to_branch += "\n  {\"id\": " + event_id  + ", \"name\": \"withdraw_response\", \"clock\": " + str(self.clock) + " },"
        response_to_branch += ";" + str(self.clock)
        return bankworld_pb2.WithdrawReply(withdraw_msg=response_to_branch)

    def Propagate_Request(self, remote_clk):
        self.clock = max(remote_clk, self.clock) + 1
        
    def Propagate_Execute(self, amount):
        self.balance = self.balance + amount
        self.clock += 1
 

    # return balance
    def Query(self):
        time.sleep(3)
        return self.balance

    # add deposit amount to this branch balance and then use branch stubs to send the transaction to all other branches
    def Deposit(self, amount, remote_clk, event_id):
        Branch.Event_Request(self, remote_clk)
        response_to_client = "\n  {\"id\": " + event_id  + ", \"name\": \"deposit_request\", \"clock\": " + str(self.clock) + " },"
        Branch.Event_Execute(self, amount)
        response_to_client += "\n  {\"id\": " + event_id  + ", \"name\": \"deposit_execute\", \"clock\": " + str(self.clock) + " },"
        for i in range(len(self.stubList)) :
            if (i+1) != self.id :
                response = self.stubList[i].Propagate_Deposit(bankworld_pb2.DepositRequest(msg=str(amount)))

        Branch.Event_Response(self)
        response_to_client += "\n  {\"id\": " + event_id  + ", \"name\": \"deposit_response\", \"clock\": " + str(self.clock) + " },"
        return (response_to_client)

    # subtract withdrawal amount from this branch balance and then use branch stubs to send the transaction to all other branches
    def Withdraw(self, amount, remote_clk, event_id):
        Branch.Event_Request(self, remote_clk)
        response_to_client = "\n  {\"id\": " + event_id  + ", \"name\": \"withdraw_request\", \"clock\": " + str(self.clock) + " },"
        Branch.Event_Execute(self, -amount)
        response_to_client += "\n  {\"id\": " + event_id  + ", \"name\": \"withdraw_execute\", \"clock\": " + str(self.clock) + " },"

        prop_msg = str(amount) + "," + str(self.clock) + "," + event_id
        for i in range(len(self.stubList)) :
            if (i+1) != self.id :
                response = self.stubList[i].Propagate_Withdraw(bankworld_pb2.WithdrawRequest(msg=prop_msg))
                [msg, remote_branch_clk] = response.withdraw_msg.split(';')
                Branch.Propagate_Response(self, int(remote_branch_clk))
                response_to_client += msg + "\n  {\"id\": " + event_id + ", \"name\": \"withdraw_propagate_response\", \"clock\": " + str(self.clock) + " },"
        
        Branch.Event_Response(self)
        response_to_client += "\n  {\"id\": " + event_id + ", \"name\": \"withdraw_response\", \"clock\": " + str(self.clock) + " },"
        return (response_to_client)

    def Event_Request(self, remote_clk):
        self.clock = max(remote_clk, self.clock) + 1
        
    def Event_Execute(self, amount):
        self.balance = self.balance + amount
        self.clock += 1

    def Propagate_Response(self, remote_branch_clk):
        self.clock = max(self.clock, remote_branch_clk) + 1
        
    def Event_Response(self):
        self.clock += 1
        
    # parse the message received from customer and call appropriate branch routines
    def MsgDelivery(self, request, context):
        [request.msg, remote_clk] = request.msg.split(']')
        request.msg += "]"
        branchmsg = " {\n  \"pid\": " + str(self.id) + ",\n \"data\":  ["
        self.recvMsg.append(request.msg)
        request.msg = request.msg.replace("\'", "\"")

        reqmsg = json.loads(request.msg)
        for i in reqmsg:
            if i['interface'] == 'deposit':
                result = Branch.Deposit(self,i['money'],int(remote_clk),str(i['id']))
                branchmsg += result
            elif i['interface'] == 'withdraw':
                result = Branch.Withdraw(self,i['money'],int(remote_clk),str(i['id']))
                branchmsg += result
            elif i['interface'] == 'query':
                bal = Branch.Query(self)
        branchmsg = branchmsg[:-1] + "\n  ]\n },"
        return bankworld_pb2.BranchReply(branch_msg=branchmsg)


p = list()
count = 0

# instantiate all Branch objects, create branch stubs over ports 50051 to 50050+id
def Serve(id, balance, branches):    
    channelnumber = 50050+id
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10,))
    bankbranch = Branch(id, balance, branches)
    print ("IIIIIIIIIDDDDDDDDDDDDDDDDDD = " + str(id))
    print ("SERVERLIST = " + str(server))
    print ("BANKBRANCH = " + str(bankbranch))
    bankworld_pb2_grpc.add_BranchServicer_to_server(bankbranch, server)

    server.add_insecure_port('[::]:'+str(channelnumber))
    server.start()

    out = bankbranch.createStubsss(branches)
    server.wait_for_termination()
    
# Call Serve() for each branch
def run():
    for y in data:
        if y['type'] == 'branch':
            proc = Process(target=Serve, args=(y['id'], y['balance'], count,))
            proc.start()
            p.append(proc)
    for proc in p:
        proc.join()

            
# main function
if __name__ == '__main__':
    logging.basicConfig()
    
    # Opening JSON file
    f = open('input.json',)
    data = json.load(f)

# get number of banks N from input.json    
    for x in data:
        if x['type'] == 'branch':
            count += 1

# call run() to create branches
    run()


