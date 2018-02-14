#!/usr/bin/env python3.6

import re


def verify_escaping(regex):
    regex_without_backslash = regex.replace('\\\\', '')
    if '\\' in regex_without_backslash:
        print("Invalid regex! All backslashes must be escaped")
        return False
    return True


def verify_capture_groups(regex, path):
    matcher = re.match(regex, path)
    if not matcher:
        return("Your regex doesn't match the provided path!")
    if matcher.lastindex != 3:
        return("You should have 3 capture groups in the regex")
    constructed_str = matcher.group(1) + matcher.group(2) + matcher.group(3)
    if constructed_str != path:
        return("Your reconstructed string ({}) doesn't form the original".format(constructed_str))
    return "Your regex works. The ids in captured are {}".format(matcher.group(2))

def check_bulk_endpoint_regex():
    regex = input('Please enter an a regex pattern without quotes: \n')
    print("Your regex is {}".format(regex))

    if not verify_escaping(regex):
        exit(0)

    regex = regex.replace('\\\\','\\')

    while True:
        print()
        path = input('Please enter a path (e.g. "/foo/bar") to match on, without quotes: \n')
        print(verify_capture_groups(regex, path))


if __name__ == "__main__":
    check_bulk_endpoint_regex()
