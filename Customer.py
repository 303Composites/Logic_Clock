import logging
import multiprocessing
import argparse
import json
import time

import grpc

import example_pb2
import example_pb2_grpc
from logger import listener_configurer, get_interface_name, get_result_name, get_interface

Logger = 'customer'


class Customer:

    def __init__(self, id_, events):
        # unique ID of the Customer
        self.id_ = id_
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the channel
        self.stub = None
        # clock tick
        self.tick = 0

    # TODO: students are expected to create the Customer stub
    def createStub(self):
        # Gets interface/information from logger.py
        logger = logging.getLogger(Logger)
        # Picking an arbitrary port for the localhost if this was a real service more thought should be given
        branch_port = f'localhost:{3000 + self.id_}'
        logger.info(f'Initializing customer stub {self.id_} to branch stub at {branch_port}')
        # pulling in information from example.proto
        self.stub = example_pb2_grpc.RPCStub(grpc.insecure_channel(branch_port))

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self, output_file):
        # Gets interface/information from logger.py
        logger = logging.getLogger(Logger)
        # record = {'id': self.id_, 'recv': []}
        # pulling information from example.proto
        for event in self.events:
            request_id = event['id']
            request_interface = get_interface(event['interface'])
            request_money = event['money']
            response = self.stub.MsgDelivery(
                example_pb2.MsgDeliveryRequest(
                    id_=request_id,
                    interface=request_interface,
                    money=request_money,
                    dest=self.id_,
                    tick=self.tick
                )
            )
            logger.info(
                f'Customer ID {self.id_} requesting {request_id} to Branch {self.id_} '
                f'interface {get_interface_name(request_interface)}'
                f' result {get_result_name(response.result)} '
                f'money {response.money}'
            )
            # advancing the clock
            self.increment_tick()

    # write to output file the results
    def generate_output(self, out_file):
        logger = logging.getLogger(Logger)
        logger.info('Output/Results')
        response = self.stub.GetEvents(example_pb2.EventsRequest())
        logger.info(f'Event PID:{self.id_}')
        write = {'PID': self.id_, 'data': []}
        for event in response.events:
            logger.info(f'id: {event.id_} name: {event.name} clock: {event.clock}')
            write['data'].append(
                {
                    'id': event.id_,
                    'name': event.name,
                    'clock': event.clock,
                }
            )
        with open(f'output/{out_file}', 'a') as out:
            json.dump(write, out)
            out.write('\n write complete')

    # advance clock
    def increment_tick(self):
        self.tick = self.tick + 1


# advance clock
def increment_tick(self):
    self.tick = self.tick + 1


def _parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--Input',
                        help="shows input")

    parser.add_argument('-o', '--Output',
                        help="shows output")

    args = parser.parse_args()
    return args.Input, args.Output


# takes information from the provided input file
def _parse_input(filename):
    logger = logging.getLogger(Logger)
    file = open(f'input/{filename}')
    ops = json.load(file)
    customers = list()

    for op in ops:
        if op['type'] == 'customer':
            events = list()
            for event in op['events']:
                events.append(event)
            customer = Customer(op['id'], events)
            customers.append(customer)
    for x in customers:
        logger.info(f'Customer {x.id_}')
        for y in x.events:
            logger.info(f'Event {y["id"]}, {y["interface"]}, {y["money"]}')
    file.close()
    return customers


def _run_customer(customer, out_file):
    customer.createStub()
    customer.executeEvents(out_file)


def join_results(output_file):
    records = []
    with open(f'output/{output_file}', 'r') as fp:
        lines = fp.readlines()
        for line in lines:
            record = json.loads(line)
            records.append(record)
    events = {}
    for record in records:
        for event in record['data']:
            events[event['id']] = events.get(event['id'], list())
            events[event['id']].append(
                {'clock': event['clock'], 'name': event['name']}
            )
    events = sorted(events.items())
    for event in events:
        event[1].sort(key=lambda e: e['clock'])
    for event in events:
        record = {'eventid': event[0], 'data': event[1]}
        records.append(record)
    with open(f'output/{output_file}', 'w+') as outfile:
        json.dump(records, outfile)
        outfile.write('\n Write Complete')


def _main():
    input_file, output_file = _parse_arguments()
    if not output_file:
        Exception(f'No output file found')

    if not input_file:
        Exception(f'No input file found')

    customers = _parse_input(input_file)
    workers = list()
    # Starting the multiprocess customer side
    for customer in customers:
        worker = multiprocessing.Process(target=_run_customer, args=(customer, output_file))

        worker.start()
        workers.append(worker)
        # joining services
    for worker in workers:
        worker.join()
    join_results(output_file)


# triggers log from logger.py
listener_configurer(Logger)
if __name__ == '__main__':
    _main()
