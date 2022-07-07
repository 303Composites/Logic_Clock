import logging
import multiprocessing
import argparse
import datetime
import json
import time
from concurrent import futures

import grpc

import example_pb2

import example_pb2_grpc
import logger
from logger import get_interface_name, get_result_name, listener_configurer

# Logger for branches
Logger = 'branch'
# Limit of users (10 is the test requirement)
MAX_USERS = 11
# Day in seconds
ONE_DAY = datetime.timedelta(1)
# cores allowed
THREAD_CONCURRENCY = multiprocessing.cpu_count()
# sleep timer
SLEEP_TIME_IN_SECONDS = 3


# branch interface from .proto
class Branch(example_pb2_grpc.RPCServicer):

    def __init__(self, id_, balance, branches):
        # unique ID of the Branch
        self.id_ = id_
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # clock tick
        self.tick = 0
        # a list of the events and its clock tick
        self.events = list()
        # TODO: students are expected to store the processID of the branches

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        # Gets interface/information from logger.py
        log = logging.getLogger(Logger)
        self.recvMsg.append(request)
        if request.dest != MAX_USERS:
            # send the request tick
            self.event_request(request.tick) 
            if request.interface == example_pb2.Deposit:
                self.register_event(request.id_, 'Deposit_Request', self.tick)
            if request.interface == example_pb2.Withdraw:
                self.register_event(request.id_, 'Withdraw_Request', self.tick)
        else:
            # sending the received tick
            self.propagate_request(request.tick)
            if request.interface == example_pb2.Deposit:
                self.register_event(request.id_, 'Deposit_Broadcast_Request', self.tick)
            if request.interface == example_pb2.Withdraw:
                self.register_event(request.id_, 'Withdraw_Broadcast_Request', self.tick)

        balance_result = None
        # wait for response
        if request.interface == example_pb2.Query:
            time.sleep(SLEEP_TIME_IN_SECONDS)
            balance_result = self.query()
        # request deposit
        if request.interface == example_pb2.Deposit:
            balance_result = self.deposit(request.money)
        # request withdraw
        if request.interface == example_pb2.Withdraw:
            balance_result = self.withdraw(request.money)

        if request.dest != MAX_USERS:
            # configure interface for id's
            self.event_execute()
            if request.interface == example_pb2.Deposit:
                self.register_event(request.id_, 'Deposit_Execute', self.tick)
            if request.interface == example_pb2.Withdraw:
                self.register_event(request.id_, 'Withdraw_Execute', self.tick)
        else:
            self.propagate_execute()  # Propagate Execute
            if request.interface == example_pb2.Deposit:
                self.register_event(request.id_, 'deposit_broadcast_execute', self.tick)
            if request.interface == example_pb2.Withdraw:
                self.register_event(request.id_, 'withdraw_broadcast_execute', self.tick)

        response_result = example_pb2.Transaction_Successful \
            if balance_result \
            else example_pb2.Transaction_Failed
        log.info(
            # prints id, interface, result, and balance
            f'Branch {self.id_} response to Customer request {request.id_} '
            f'interface {get_interface_name(request.interface)} result {get_result_name(response_result)} '
            f'balance {balance_result}'
        )
        # from example.proto file interfaces
        response = example_pb2.MsgDeliveryResponse(
            id_=request.id_,
            result=response_result,
            money=balance_result,
            tick=self.tick,
        )
        if request.dest != MAX_USERS and request.interface == example_pb2.Deposit:
            self.propagate_deposit(request.id_, request.money)
        if request.dest != MAX_USERS and request.interface == example_pb2.Withdraw:
            self.propagate_withdraw(request.id_, request.money)
        # send response tick for execute
        if request.dest != MAX_USERS:
            self.event_execute()
            if request.interface == example_pb2.Deposit:
                self.register_event(request.id_, 'deposit_response', self.tick)
            if request.interface == example_pb2.Withdraw:
                self.register_event(request.id_, 'withdraw_response', self.tick)
        return response

        """
        balance_result = None
        if request.interface == example_pb2.Query:
            time.sleep(SLEEP_TIME_IN_SECONDS)
            balance_result = self.query()
        if request.interface == example_pb2.Deposit:
            balance_result = self.deposit(request.money)
        if request.interface == example_pb2.Withdraw:
            balance_result = self.withdraw(request.money)
        response_result = example_pb2.Transaction_Successful \
            if balance_result \
            else example_pb2.Transaction_Failed
        logger.info(
            f'Branch {self.id_} response to Customer request {request.id_} '
            f'interface {get_interface_name(request.interface)} '
            f'result {get_result_name(response_result)} '
            f'balance {balance_result}'
        )
        response = example_pb2.MsgDeliveryResponse(
            id_=request.id_,
            result=response_result,
            money=balance_result,
        )
        if request.dest != MAX_USERS and request.interface == example_pb2.Deposit:
            self.propagate_deposit(request.id_, request.money)
        if request.dest != MAX_USERS and request.interface == example_pb2.Withdraw:
            self.propagate_withdraw(request.id_, request.money)
        return response
    """

    def populate_stub_list(self):

        if len(self.stubList) == len(self.branches):
            return
        log = logging.getLogger(Logger)

        # Picking an arbitrary port for the localhost if this was a real service more thought should be given
        for z in self.branches:
            if z != self.id_:
                branch_address = f'localhost:{3000 + z}'
                log.info(f'Initializing branch to branch stub at {branch_address}')
                self.stubList.append(example_pb2_grpc.RPCStub(grpc.insecure_channel(branch_address)))

    # withdrawls from account must be less that the total account
    def withdraw(self, amount):
        if self.balance < 0:
            print('add more funds')
            return None
        if amount <= 0:
            return None
        new_balance = self.balance - amount
        if new_balance < 0:
            return None
        self.balance = new_balance
        return self.balance

    # deposits for accounts need to be more than 0
    def deposit(self, amount):
        if amount < 0:
            return None
        new_balance = self.balance + amount
        self.balance = new_balance
        return self.balance

    # sends deposit info to logger
    def propagate_deposit(self, request_id, amount):
        # logger = logging.getLogger(Logger)
        logger.info(f'Propagate {get_interface_name(example_pb2.Deposit)} id {request_id} amount {amount}')
        if not self.stubList:
            self.populate_stub_list()
        for stub in self.stubList:
            response = stub.MsgDelivery(
                example_pb2.MsgDeliveryRequest(
                    id_=request_id,
                    interface=example_pb2.Deposit,
                    money=amount,
                    dest=MAX_USERS,
                    tick=self.tick
                )
            )
            logger.info(
                f'Branch {self.id_} sent request {request_id} to Branch '
                f'interface {get_interface_name(example_pb2.Deposit)} result {get_result_name(response.result)} '
                f'money {response.money}')

    # sends withdrawl info to logger
    def propagate_withdraw(self, request_id, amount):
        # logger = logging.getLogger(Logger)
        logger.info(f'Propagate {get_interface_name(example_pb2.Withdraw)} id {request_id} amount {amount}')
        if not self.stubList:
            self.populate_stub_list()
        for stub in self.stubList:
            response = stub.MsgDelivery(
                example_pb2.MsgDeliveryRequest(
                    id_=request_id,
                    interface=example_pb2.Withdraw,
                    money=amount,
                    dest=MAX_USERS,
                    tick=self.tick
                )
            )
            logger.info(
                f'Branch {self.id_} sent request {request_id} to Branch '
                f'interface {get_interface_name(example_pb2.Withdraw)} result {get_result_name(response.result)} '
                f'money {response.money}')

    # returns the account balance to user
    def query(self):
        return self.balance

    # TODO
    # all requests advance a tick
    def event_request(self, tick, name=None):
        self.tick = max(self.tick, tick) + 1

    def event_execute(self, name=None):
        self.tick = self.tick + 1

    def propagate_execute(self, name=None):
        self.tick = self.tick + 1

    def propagate_request(self, tick, name=None):
        self.tick = max(self.tick, tick) + 1

    def propagate_response(self, tick):
        self.tick = max(self.tick, tick) + 1

    def register_event(self, id_, name, clock):
        self.events.append({'id': id_, 'name': name, 'clock': clock})


# same as Customer.py
def _parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--Input',
                        help="shows input")
    args = parser.parse_args()
    return args.Input


# takes provided input file and parses it
def _parse_input(filename):
    log = logging.getLogger(Logger)
    file = open(f'input/{filename}')
    ops = json.load(file)
    branches = list()
    for op in ops:
        if op['type'] == 'branch':
            branch = Branch(op['id'], op['balance'], list())
            branches.append(branch)
    for z in branches:
        for z1 in branches:
            z.branches.append(z1.id_)
        log.info(f'Branch {z.id_}, {z.balance}, {z.branches}')
    file.close()
    return branches


def GetEvents(self):
    grpc_events = [
        example_pb2.Event(
            id_=e['id'],
            name=e['name'],
            clock=e['clock'],
        ) for e in self.events
    ]
    response = example_pb2.EventsResponse(
        events=grpc_events
    )
    return response

# runs on port 3000 + id for each branch
def _run_branch(branch):
    log = logging.getLogger(Logger)
    address = f'[::]:{3000 + branch.id_}'
    # options = (('grpc.so_reuseport', 1),)
    log.info(f'Branch {branch.id_} is located at {address}.')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=THREAD_CONCURRENCY))
    # , options=options)
    example_pb2_grpc.add_RPCServicer_to_server(branch, server)
    server.add_insecure_port(address)
    server.start()
    _wait_forever(server)


# multiprocessing the input file
def _main():
    in_filename = _parse_arguments()
    if not in_filename:
        raise Exception(f'Wrong Input name')
    branches = _parse_input(in_filename)
    workers = list()
    # Starting service on the branch side
    for branch in branches:
        worker = multiprocessing.Process(target=_run_branch, args=(branch,))
        worker.start()
        workers.append(worker)

        # joining services
    for worker in workers:
        worker.join()


# close the server with a keyboard interrupt
def _wait_forever(server):
    try:
        while True:
            time.sleep(ONE_DAY.total_seconds())
    except KeyboardInterrupt:
        server.stop(None)


# triggers the logger.py
listener_configurer(Logger)
if __name__ == '__main__':
    _main()
