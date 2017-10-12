#!/usr/bin/python34
#
# q2_reducer.py:
# Reads key/value pairs from stdin, writes key/value pairs to stdout

import sys

if __name__ == "__main__":
    curkey = None
    total = 0
    for line in sys.stdin:
        key, val = line.split("\t")
        val = int(val)

        if key == curkey:
            total += val
        else:
            if curkey is not None:
                print("{}\t{}".format(curkey,total))

       
            curkey = key
            total = val
            
    if total!=0:
        print("{}\t{}".format(curkey,total))


