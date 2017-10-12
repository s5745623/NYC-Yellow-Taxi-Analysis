#!/usr/bin/python34

#
# q2_mapper.py:
# Reads whole lines stdin; writes key/value pairs to stdout
# http://docs.aws.amazon.com/emr/latest/ReleaseGuide/UseCase_Streaming.html

import sys
import re


if __name__ == "__main__":
    for line in sys.stdin:
        if re.findall("nard:-1 fnok:-1 cark:-1 gnuck:-1", line):
            print("{}\t1".format("error"))

