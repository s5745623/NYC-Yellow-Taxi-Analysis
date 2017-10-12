#!/usr/bin/python34
#
# Template to compute the number of links
#

from mrjob.job import MRJob
import re,mrjob
import statistics

#line = '2016-07-01T12:01:01,1,,192.168.10.1,1798,2,,96.120.104.177,9739,3,,68.87.130.233,11766,4,ae-53-0-ar01.capitolhghts.md.bad.comcast.net,68.86.204.217,11203,5,be-33657-cr02.ashburn.va.ibone.comcast.net,68.86.90.57,14575,6,he-0-2-0-0-ar01-d.westchester.pa.bo.comcast.net,68.86.94.226,17923,7,bu-101-ur21-d.westchester.pa.bo.comcast.net,68.85.137.213,16070,8,,68.87.29.59,16761'

class DistinctHop23(MRJob):

    OUTPUT_PROTOCOL =  mrjob.protocol.TextProtocol
    def mapper(self, _, line):
        # Your code goes here
        pattern_time = re.compile('([0-9]{2}):[0-9]{2}:[0-9]{2},')
        pattern_ip_sec = re.compile(',([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+),([0-9]+),')
        hour = pattern_time.findall(line)
        responsec = pattern_ip_sec.findall(line)

        if len(responsec) > 2:
            yield hour[0], (int(responsec[2][1])- int(responsec[1][1]))

    def reducer(self, time, val):
        # Your code goes here

        list_time = []
        
        for v in val:
            list_time.append(int(v))

        yield time, str(statistics.pstdev(list_time))


if __name__ == '__main__':
    DistinctHop23.run()