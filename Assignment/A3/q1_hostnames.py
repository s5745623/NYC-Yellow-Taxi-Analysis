#!/usr/bin/python34
#
# Template to compute the number of distinct hostname
#

from mrjob.job import MRJob
import re,mrjob

#ex. 2016-07-01T00:01:01,1,,192.168.10.1,1798,2,,96.120.104.177,9739,3,,68.87.130.233,11766,4,ae-53-0-ar01.capitolhghts.md.bad.comcast.net,68.86.204.217,11203,5,be-33657-cr02.ashburn.va.ibone.comcast.net,68.86.90.57,14575,6,he-0-2-0-0-ar01-d.westchester.pa.bo.comcast.net,68.86.94.226,17923,7,bu-101-ur21-d.westchester.pa.bo.comcast.net,68.85.137.213,16070,8,,68.87.29.59,16761

class DistinctHostname(MRJob):

    OUTPUT_PROTOCOL =  mrjob.protocol.TextProtocol
    def mapper(self, _, line):
        # Your code goes here
        
        pattern = re.compile(',([a-zA-Z]+.+?),')
        # hostnames comes after previous ip then time count and then num of hops, and hostnames start with two lowercase words
        for hostnames in pattern.findall(line):
            yield hostnames,1

    def reducer(self, word, counts):
        # Your code goes here
        
        yield word, str(sum(counts))
        


if __name__ == '__main__':
    DistinctHostname.run()