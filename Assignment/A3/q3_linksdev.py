#!/usr/bin/python34
#
# Template to compute the number of links
#

from mrjob.job import MRJob
import re,mrjob
import statistics

#ex. 2016-07-01T00:01:01,1,,192.168.10.1,1798,2,,96.120.104.177,9739,3,,68.87.130.233,11766,4,ae-53-0-ar01.capitolhghts.md.bad.comcast.net,68.86.204.217,11203,5,be-33657-cr02.ashburn.va.ibone.comcast.net,68.86.90.57,14575,6,he-0-2-0-0-ar01-d.westchester.pa.bo.comcast.net,68.86.94.226,17923,7,bu-101-ur21-d.westchester.pa.bo.comcast.net,68.85.137.213,16070,8,,68.87.29.59,16761

class DistinctLinks(MRJob):

    OUTPUT_PROTOCOL =  mrjob.protocol.TextProtocol
    def mapper(self, _, line):
        # Your code goes here
        pattern = re.compile(',([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+),([0-9]+)')
        links = pattern.findall(line)
        for i in range(len(links)-1):
            yield "%s->%s"%(links[i][0],links[i+1][0]),(int(links[i+1][1])-int(links[i][1]))

    def reducer(self, link, times_val):
        # Your code goes here
        # yield word, sum(counts)
        cnt = 0
        time = []

        for times in times_val:
            cnt += 1
            time.append(int(times))
        
        yield link, "{}\t{}".format(cnt,statistics.pstdev(time))
        


if __name__ == '__main__':
    DistinctLinks.run()