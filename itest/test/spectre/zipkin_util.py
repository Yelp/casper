import os
import re
import time
from collections import namedtuple

Span = namedtuple('Span', ['trace_id', 'id', 'parent_id'])

regex = re.compile(
    'spectre/zipkin (?P<trace_id>[a-fA-F0-9]*) (?P<id>[a-fA-F0-9]*) (?P<parent_id>[a-fA-F0-9]*) '
    '0 (?P<sampled>[01]) (?P<start_time_us>[0-9]*) (?P<end_time_us>[0-9]*)'
    '[^,]*, client: (?P<client>[0-9\.]*), server: [^,]*, '
    'request: "[A-Z]* [^ "]* (?P<method>[^ "]*)"'
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
        trace_id=match.group('trace_id'),
        id=match.group('id'),
        parent_id=match.group('parent_id'),
    )
