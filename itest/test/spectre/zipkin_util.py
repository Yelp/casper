import os
import re
import time
from collections import namedtuple

Span = namedtuple('Span', ['trace_id', 'id', 'parent_id'])

regex = re.compile(
    'spectre/zipkin ([a-fA-F0-9]*) ([a-fA-F0-9]*) ([a-fA-F0-9]*) 0 ([01]) ([0-9]*) ([0-9]*)'
    '[^,]*, client: ([0-9\.]*), server: [^,]*, '
    'request: "([A-Z]*) ([^ "]*) ([^ "]*)"'
)


def load_zipkin_spans(log_path):
    """Deserialize the zipkin log lines from log_path"""
    # We generate the zipkin span after returning the response, so we need to sleep for
    # a bit here to make sure spectre had time to do it
    time.sleep(0.2)
    with open(log_path) as fd:
        lines = fd.readlines()
        return [get_zipkin_span_from_line(line) for line in lines]


def get_zipkin_span_from_line(line):
    match = re.search(regex, line)
    return Span(
        trace_id=match.group(1),
        id=match.group(2),
        parent_id=match.group(3),
    )
