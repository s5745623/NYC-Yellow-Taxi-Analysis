#!/usr/bin/env python35
#
# Parse an MTR file in text mode and turn it into a tinydata file.
#
import sys
if sys.version < "3.4":
    raise RuntimeError("requires Pyton 3.4 or above")

import os,re
import csv
from datetime import datetime

mtr_line_exp = re.compile("([0-9]{1,2}).\|--\s+(.+?)\s+([^\s\%]+)[\%]*\s+\d+\s+([\d.]+)\s")   # put something here


def parse_timestamp(line):
    assert line.startswith("Start:")
    # ignore label of weekday
    val = line.lstrip('Start:').strip()#delete Start: and the space on the left then strip the right string
    date_time = datetime.strptime(val, "%a %b %d %H:%M:%S %Y")

    return date_time.isoformat()


class MtrLine:
    def __init__(self,ts,line):
        assert re.match(mtr_line_exp,line)

        # Parse line and fill these in.
        # ts is the timestamp that we previously found
        line = line.strip()
        self.timestamp = ts
        self.hop_number = ''
        self.ipaddr = ''
        self.hostname = ''
        self.pct_loss = ''
        self.time = ''

        pat_ip = re.compile('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
        pat_hostname = re.compile('[a-zA-Z]{2}-.+')

        results = mtr_line_exp.findall(line)[0]

        self.hop_number = int(results[0])

        # parse ip or hostname
        if pat_ip.match(results[1]):
            self.ipaddr = results[1]
            for key in ip_to_host:
                if key == self.ipaddr:
                    self.hostname = ip_to_host[key]

        elif pat_hostname.match(results[1]):
            self.hostname = results[1]#if the hostname cannot be found, return the original hostname
            # find complete hostname in dictionary
            for key in host_to_ip:
                if key.startswith(results[1]):
                    self.hostname = key
                    self.ipaddr = host_to_ip[key]

        self.pct_loss = int(float(results[2]))
        self.time = round(float(results[3]),2)



def host_ip_dic():
    # input mtr.record.2016.txt
    # create dictionary for all the <hostname, ip> in the txt
    host_to_ip = {}
    ip_to_host = {}

    pat = re.compile('[0-9]+,([^,]+?),([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+),[0-9]+,')

    with open('mtr.records.2016.txt','r') as f:
        reader = f.readlines()

        for row in reader:
            lists = pat.findall(row.strip())
            for list in lists:
                host_to_ip[list[0]] = list[1]
                ip_to_host[list[1]] = list[0]

    return (host_to_ip,ip_to_host)

def fix_mtr(infile,outfile):
    count = 0
    current_timestamp = None
    for line in infile:
        line = line.strip()     # remove leading and trailing white space
        line = line.replace('\x00', '')#replace the extra words
        if line.startswith('Start:'):
            # Beginning of a new record...
            # print("Replace this print statement with new code. Probably need to set a variable with the time...")
            ts = parse_timestamp(line)
            continue

        if line.startswith('HOST:'):
            # This can be ignored, since we always start at the same location
            continue

        # print(line)
        m= MtrLine(ts,line)
        if m.timestamp:
            # print("Regular expression matched. Replace this with code...")
            outfile.write("%s,%d,%s,%s,%d,%s\n"%(m.timestamp,m.hop_number,m.ipaddr,m.hostname,m.pct_loss,m.time))
            count += 1
    return count

        

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--infile",help="input file",default="mtr.www.cnn.com.txt")
    parser.add_argument("--outfile",help="output file",default="tidy_dataset.txt")
    args = parser.parse_args()

    if os.path.exists(args.outfile):
        raise RuntimeError(args.outfile + " already exists. Please delete it.")

    # input dictionary as (hostname, ip address)
    host_to_ip,ip_to_host = host_ip_dic()

    print("{} -> {}".format(args.infile,args.outfile))

    count = fix_mtr(open(args.infile,"rU"), open(args.outfile,"w"))
    print("{} records converted".format(count))
