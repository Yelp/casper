"""
Utilities for parsing zipkin log lines, which are base64 and thrift encoded.
"""
import base64
import os

import thriftpy
from thriftpy.utils import deserialize

filepath = os.path.join(os.path.dirname(__file__), 'zipkinCore.thrift')
zipkin_core = thriftpy.load(filepath, module_name='zipkinCore_thrift')


def load_zipkin_spans(log_path):
    """Deserialize the zipkin log lines from log_path"""
    with open(log_path) as fd:
        lines = fd.readlines()
        return [decode_zipkin_log_line(line) for line in lines]


def decode_zipkin_log_line(line):
    return deserialize(zipkin_core.Span(), base64.b64decode(line))


def int2hex(num):
    return "{:016x}".format((num + (1 << 64)) % (1 << 64))
