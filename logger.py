import logging
import sys

import logging.handlers

import example_pb2


def listener_configurer(name):
    root = logging.getLogger(name)
    # file_handler = logging.handlers.RotatingFileHandler('outfile.json', 'a')
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[PID %(process)d %(asctime)s] %(message)s')
    console_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)
    root.addHandler(console_handler)
    root.setLevel(logging.INFO)


def get_interface(interface):
    if interface == 'query':
        return example_pb2.Query
    if interface == 'deposit':
        return example_pb2.Deposit
    if interface == 'withdraw':
        return example_pb2.Withdraw


def get_interface_name(interface):
    if interface == example_pb2.Query:
        return 'Query'
    if interface == example_pb2.Deposit:
        return 'Deposit'
    if interface == example_pb2.Withdraw:
        return 'Withdraw'


def get_result_name(name):
    if name == example_pb2.Transaction_Successful:
        return 'Transaction_Successful'
    if name == example_pb2.Transaction_Failed:
        return 'Transaction_Failed'
